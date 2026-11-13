// ordering demo — shows WHERE Kafka's ordering guarantee lives:
// within a partition, never across partitions.
//
//	go run ./cmd/demos/ordering
//	go run ./cmd/consumer -group ordering-demo -from-beginning
//
// The consumer output shows:
//   - keyed events (order-42 steps 1..5): ONE partition, offsets 0,1,2,3,4 → in order
//   - unkeyed events (steps 1..5):        SCATTERED partitions → shuffled order
package main

import (
	"context"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iamadarsh0309/Kafka-Golang/internal/admin"
	"github.com/iamadarsh0309/Kafka-Golang/internal/common"
	"github.com/iamadarsh0309/Kafka-Golang/internal/config"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ClientID("ordering-demo"),
		// Default partitioner: keyed records → murmur2(key) % partitions
		// (consistent per key); unkeyed records → sticky partitioning.
	)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer cl.Close()

	// Second client with a MANUAL partitioner, used for the unkeyed section:
	// it honors the rec.Partition we set, so we can force visible
	// round-robin instead of the default sticky behavior.
	mcl, err := kgo.NewClient(
		kgo.SeedBrokers(config.Brokers()...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ClientID("ordering-demo-manual"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		log.Fatalf("create client: %v", err)
	}
	defer mcl.Close()
	admin.EnsureTopic(cl, config.TopicOrders, 3)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	produce := func(cl *kgo.Client, rec *kgo.Record, what string) {
		rec.Topic = config.TopicOrders
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				log.Fatalf("%s: %v", what, err)
			}
			log.Printf("sent %-28s → partition=%d offset=%d", what, r.Partition, r.Offset)
		})
	}

	// ── A) SAME key, 5 lifecycle steps ─────────────────────────────────────
	// Key = order id: all steps of order-42 hash to the SAME partition, so
	// offsets are sequential and any consumer sees them in order.
	log.Println("--- keyed events: order-42 created → paid → shipped ---")
	for i, step := range []string{"created", "paid", "picked", "shipped", "delivered"} {
		ev := common.MustOrder("order-42", step, i+1, float64(100+i))
		rec := kgo.KeyStringRecord(ev.ID, string(mustJSON(ev)))
		produce(cl, rec, fmtStep("keyed", i+1, step))
	}

	// ── B) NO key, same 5 steps ────────────────────────────────────────────
	// Without a key there is NO ordering guarantee: different steps can land
	// on different partitions and a consumer may see "paid" before
	// "created". (franz-go normally sticky-partitions unkeyed records for
	// batching throughput; here we set the partition explicitly so the
	// shuffling is deterministic and visible.)
	log.Println("--- unkeyed events: same steps, no ordering guarantee ---")
	for i, step := range []string{"created", "paid", "picked", "shipped", "delivered"} {
		ev := common.MustOrder("", step, i+1, float64(100+i))
		rec := kgo.StringRecord(string(mustJSON(ev)))
		rec.Partition = int32(i % 3) // explicit round-robin for demo clarity
		produce(mcl, rec, fmtStep("UNKEYED", i+1, step))
	}

	if err := cl.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	if err := mcl.Flush(ctx); err != nil {
		log.Fatalf("flush: %v", err)
	}
	log.Println("now run:  go run ./cmd/consumer -group ordering-demo -from-beginning")
}

func fmtStep(kind string, i int, step string) string {
	return kind + " step-" + string(rune('0'+i)) + " " + step
}

func mustJSON(ev common.OrderEvent) []byte {
	b, err := ev.Marshal()
	if err != nil {
		log.Fatalf("marshal: %v", err)
	}
	return b
}
