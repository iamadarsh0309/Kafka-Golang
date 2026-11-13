// consumer — reads a topic as part of a consumer group.
//
//	go run ./cmd/consumer -group billing
//	# then in ANOTHER terminal (watch the rebalance):
//	go run ./cmd/consumer -group billing
//	# an independent reader of the same data:
//	go run ./cmd/consumer -group analytics
//
// Watch the group, its members and lag at
// http://localhost:8080 → Consumers → <group>.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iamadarsh0309/Kafka-Golang/internal/admin"
	"github.com/iamadarsh0309/Kafka-Golang/internal/common"
	"github.com/iamadarsh0309/Kafka-Golang/internal/config"
)

func main() {
	var (
		group         = flag.String("group", config.GroupBilling, "consumer group id (same id = share the work)")
		topic         = flag.String("topic", config.TopicOrders, "topic to consume")
		fromBeginning = flag.Bool("from-beginning", false, "for a NEW group: start at the oldest retained message instead of the newest")
		replay        = flag.Bool("replay", false, "join as a fresh reader from the earliest offset, after resetting this group's committed offsets")
	)
	flag.Parse()

	if *replay {
		// Reset this group's cursor to the log start, then re-consume
		// history. Run with no other members of the group alive (ctrl-c the
		// others first): Kafka rejects offset commits from outside an
		// active group.
		replayOffsets(*group, *topic)
	}

	// ---------------------------------------------------------------------------
	// The consumer. Key options — docs/04-consumers.md walks through them all:
	//
	//   kgo.ConsumerGroup(group): join (or create) this group. Members of the
	//     same group SPLIT the topic's partitions; different groups each get
	//     their own copy of the stream.
	//
	//   kgo.ConsumeTopics(topic): which topics we want; the group coordinator
	//     assigns us a subset of the partitions (docs/01-fundamentals.md §6).
	//
	//   kgo.ConsumeResetOffset: where a group with NO committed offsets
	//     starts. Default = newest; AtStart = oldest (-from-beginning).
	//
	//   kgo.DisableAutoCommit + explicit CommitRecords in the loop: we commit
	//     AFTER processing each batch => at-least-once semantics (a crash may
	//     re-deliver a batch, but never skips one).
	//
	//   Rebalance callbacks: they make the invisible group protocol visible.
	//     Run two consumers in the same group, then ctrl-c one, and watch.
	// ---------------------------------------------------------------------------
	opts := []kgo.Opt{
		kgo.SeedBrokers(config.Brokers()...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
		// RangeBalancer = the classic "eager" strategy: on every rebalance
		// the group revokes ALL partitions and reassigns from scratch —
		// noisy, but makes the protocol visible in these logs.
		// (franz-go's default is cooperative-sticky, the modern low-churn
		// strategy — see docs/04-consumers.md §rebalancing.)
		kgo.Balancers(kgo.RangeBalancer()),
		// Faster membership timeouts than the (default 45s / 3s) pair:
		// when you ctrl-c a consumer, the group notices within seconds,
		// which makes the rebalance lab much more responsive.
		kgo.SessionTimeout(10 * time.Second),
		kgo.HeartbeatInterval(3 * time.Second),
		kgo.DisableAutoCommit(),
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, assigned map[string][]int32) {
			fmt.Printf("⚖️  ASSIGNED: %v   (I now own these partitions)\n", assigned)
		}),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoked map[string][]int32) {
			// Our loop commits after every batch, so our commits are always
			// current when a revoke lands. If you commit less frequently,
			// commit here too — before the new owner takes over.
			fmt.Printf("↪️  REVOKED:  %v   (giving these up — group is rebalancing)\n", revoked)
		}),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, lost map[string][]int32) {
			fmt.Printf("💥 LOST:     %v   (we were too slow — session/heartbeat timeout)\n", lost)
		}),
	}
	if *fromBeginning || *replay {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	admin.EnsureTopic(cl, *topic, 3)
	defer cl.Close()

	fmt.Printf("consumer group=%q topic=%q  (ctrl-c to stop)\n", *group, *topic)

	// Graceful shutdown on ctrl-c: ctx cancels the poll, loop exits cleanly.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ---------------------------------------------------------------------------
	// The poll loop — the entire consumer API in ~20 lines.
	// PollRecords blocks until records OR fetch errors arrive, then returns up
	// to maxRecords. franz-go manages fetching (batched requests to partition
	// leaders), heartbeats (group membership), and rebalancing for us.
	// ---------------------------------------------------------------------------
	for {
		fetches := cl.PollRecords(ctx, 100)
		// Client closed (ctrl-c / shutdown) or ctx canceled: leave the loop.
		// (Without the ctx check, a canceled poll spams context-canceled
		// fetch errors forever.)
		if fetches.IsClientClosed() || ctx.Err() != nil {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			log.Printf("fetch error topic=%s partition=%d: %v", t, p, err)
		})

		if n := fetches.NumRecords(); n > 0 {
			fetches.EachRecord(func(r *kgo.Record) {
				// r.Topic/r.Partition/r.Offset identify the message exactly;
				// r.Timestamp is when the producer wrote it (event time).
				ev, err := common.UnmarshalOrder(r.Value)
				fmt.Printf("📩 p%d o%-4d key=%-8s %s\n",
					r.Partition, r.Offset, string(r.Key), summary(ev, err))
			})
			// Commit AFTER processing: a crash between process & commit
			// re-delivers (at-least-once). CommitRecords is Kafka's
			// transaction: "my group has consumed through these offsets" —
			// it lands in the compacted __consumer_offsets topic.
			if err := cl.CommitRecords(context.Background(), fetches.Records()...); err != nil {
				log.Printf("commit: %v", err)
			}
			fmt.Printf("   ✔ processed %d records, committed. (watch lag hit 0 in the UI)\n", fetches.NumRecords())
		}
	}
}

// replayOffsets runs the admin-side reset before this process joins the group.
func replayOffsets(group, topic string) {
	// A throwaway client just for admin work (no group membership).
	cl, err := kgo.NewClient(kgo.SeedBrokers(config.Brokers()...))
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer cl.Close()
	if err := admin.ResetGroupToStart(cl, group, topic); err != nil {
		log.Fatalf("replay reset failed: %v", err)
	}
	fmt.Printf("⏪ group %q offsets reset to earliest — replaying history of %s\n", group, topic)
}

func summary(ev common.OrderEvent, err error) string {
	if err != nil {
		return fmt.Sprintf("(unmarshal failed: %v)", err)
	}
	return fmt.Sprintf("order %s: %dx %s @ $%.2f", ev.ID, ev.Quantity, ev.Product, ev.Price)
}
