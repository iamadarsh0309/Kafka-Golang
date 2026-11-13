// producer — writes OrderEvents to a Kafka topic.
//
// Modes:
//
//	Interactive: go run ./cmd/producer                 (type: key product qty price)
//	One-shot:    go run ./cmd/producer -key o1 -msg "laptop 1 999.9"
//	Burst/load:  go run ./cmd/producer -n 100 -rate 20 -topic orders
//
// Watch the results live at http://localhost:8080 (topic → Messages → live).
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iamadarsh0309/Kafka-Golang/internal/common"
	"github.com/iamadarsh0309/Kafka-Golang/internal/config"
)

func main() {
	var (
		topic    = flag.String("topic", config.TopicOrders, "topic to produce to")
		key      = flag.String("key", "", "message key (determines the partition!)")
		msg      = flag.String("msg", "", "one-shot: product quantity price")
		n        = flag.Int("n", 0, "burst mode: send N random messages")
		rate     = flag.Int("rate", 10, "burst mode: messages per second")
		interact = flag.Bool("i", false, "interactive stdin mode: key product qty price")
	)
	flag.Parse()

	// ---------------------------------------------------------------------------
	// The client. Options chosen deliberately — docs/03-producer.md explains each:
	//
	//   kgo.RequiredAcks(kgo.AllISRAcks()) (acks=all): a write is only acknowledged after the
	//     leader AND every in-sync replica have it. Slower, but crash-proof.
	//     Alternatives: kgo.Acks()=acks:0 (fire&forget), kgo.RequireOne (acks=1).
	//
	//   Idempotent producer (ON by default in franz-go ≥1.15): the broker rejects
	//     duplicates if a retried request re-lands after a timeout. We keep it on
	//     (kgo.ProduceRetries is also on by default).
	//
	//   Batching: franz-go batches messages into one network request when
	//     possible. Tuned by kgo.ProducerLinger and kgo.ProducerBatchMaxSize.
	//     Default linger 0 = send ASAP (low latency); linger>0 waits to build
	//     bigger batches (higher throughput).
	// ---------------------------------------------------------------------------
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // acks=all
		kgo.ProducerLinger(0),              // send immediately (low latency)
		kgo.ClientID("producer-demo"),
	)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer cl.Close()

	// Make sure the topic exists (auto-create is not enabled in compose).
	ensureTopic(cl, *topic)

	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout()*2)
	defer cancel()

	switch {
	case *n > 0:
		burst(ctx, cl, *topic, *n, *rate)
	case *interact:
		interactive(cl, *topic)
	case *msg != "":
		oneShot(ctx, cl, *topic, *key, *msg)
	default:
		fmt.Println("nothing to do: use -i, or -msg, or -n (see -h)")
		os.Exit(2)
	}

	// Flush = block until every buffered/queued record is acknowledged.
	// Without this, deferring cl.Close() may drop unsent records on exit.
	if err := cl.Flush(context.Background()); err != nil {
		log.Fatalf("flush: %v", err)
	}
}

// oneShot sends a single, explicit message and prints WHERE it went.
// The partition/offset printed here is exactly what you see in Kafka UI.
func oneShot(ctx context.Context, cl *kgo.Client, topic, key, msg string) {
	ev := parseOrDie(key, msg)

	// Produce is async: it appends to an internal queue and returns immediately.
	// The callback fires when the broker acks (or an error gives up).
	rec := kgo.KeyStringRecord(ev.ID, string(mustJSON(ev)))
	rec.Topic = topic

	cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
		if err != nil {
			log.Printf("❌ produce failed (key=%s): %v", key, err)
			return
		}
		fmt.Printf("✅ topic=%s partition=%d offset=%d key=%s value=%s\n",
			r.Topic, r.Partition, r.Offset, key, r.Value)
		// Which partition and why? partition = hash(key) % numPartitions
		// (docs/01-fundamentals.md §4). Produce with the same key again and
		// confirm you get the same partition number.
	})
}

// burst is a mini load test: N messages/second with varied keys so you can
// watch partition distribution in the UI (Topics → orders → Partitions).
func burst(ctx context.Context, cl *kgo.Client, topic string, n, rate int) {
	interval := time.Second / time.Duration(max(rate, 1))
	products := []string{"laptop", "phone", "desk", "monitor", "chair"}

	t0 := time.Now()
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("o-%d", i)
		ev := common.MustOrder(
			id,
			products[i%len(products)],
			1+i%3,
			float64(100+i%9)*10,
		)
		rec := kgo.KeyStringRecord(ev.ID, string(mustJSON(ev)))
		rec.Topic = topic
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				log.Printf("❌ %s: %v", id, err)
				return
			}
			fmt.Printf("✅ %s → partition=%d offset=%d\n", id, r.Partition, r.Offset)
		})
		time.Sleep(interval)
	}
	fmt.Printf("sent %d messages in %s\n", n, time.Since(t0).Round(time.Millisecond))
}

// interactive reads lines "key product qty price" and produces each one.
func interactive(cl *kgo.Client, topic string) {
	fmt.Println("interactive producer — enter: key product quantity price   (ctrl-c to quit)")
	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print("> ")
		if !sc.Scan() {
			return
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		oneShot(context.Background(), cl, topic, "", line)
	}
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

func parseOrDie(key, msg string) common.OrderEvent {
	f := strings.Fields(msg)
	if len(f) != 3 {
		log.Fatalf("msg must be: product quantity price (got %q)", msg)
	}
	qty, err := strconv.Atoi(f[1])
	if err != nil {
		log.Fatalf("quantity not a number: %v", err)
	}
	price, err := strconv.ParseFloat(f[2], 64)
	if err != nil {
		log.Fatalf("price not a number: %v", err)
	}
	if key == "" {
		key = f[0] // fall back to product name as key
	}
	return common.MustOrder(key, f[0], qty, price)
}

func mustJSON(ev common.OrderEvent) []byte {
	b, err := ev.Marshal()
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return b
}

// ensureTopic creates the topic with 3 partitions if missing.
// (In production you'd manage topics with IaC / the operator, not the app.)
func ensureTopic(cl *kgo.Client, topic string) {
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout())
	defer cancel()

	// kadm is franz-go's admin API (list/create topics, inspect groups,
	// alter configs — everything the kafka-topics.sh CLI can do).
	// NOTE: we deliberately do NOT call admin.Close() — it would close the
	// same underlying *kgo.Client the producer uses.
	admin := kadm.NewClient(cl)

	topics, err := admin.ListTopics(ctx, topic)
	if err != nil {
		log.Fatalf("list topics: %v", err)
	}
	// NOTE: ListTopics returns an entry even for a *missing* topic — with
	// UNKNOWN_TOPIC_OR_PARTITION inside it. So check both presence AND Err.
	if t, found := topics[topic]; found && t.Err == nil {
		return // exists
	}
	res, err := admin.CreateTopics(ctx, 3, 1, nil, topic)
	if err != nil {
		log.Fatalf("create topic %s: %v", topic, err)
	}
	for _, crt := range res {
		if crt.Err != nil && !errors.Is(crt.Err, kerr.TopicAlreadyExists) {
			log.Fatalf("create topic %s: %v", topic, crt.Err)
		}
	}
	log.Printf("topic %s created (3 partitions, rf=1)", topic)
}
