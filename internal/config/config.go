// Package config centralizes the knobs every CLI in this repo uses.
// Everything can be overridden with environment variables so you can point
// the tools at any cluster (e.g. a remote one) without code changes.
package config

import (
	"os"
	"strings"
	"time"
)

// Brokers returns the bootstrap address list (comma separated).
//
// Internals reminder (docs/01-fundamentals.md §2): the bootstrap address is
// only used to fetch cluster metadata. After that, clients talk directly to
// the leader broker of each partition, using the addresses the broker
// *advertises* (that's why KAFKA_ADVERTISED_LISTENERS in docker-compose.yml
// must be reachable from this program — localhost:9092 for us).
func Brokers() []string {
	v := os.Getenv("KAFKA_BROKERS")
	if v == "" {
		v = "localhost:9092"
	}
	return strings.Split(v, ",")
}

// Default topic names used across the demos.
const (
	TopicOrders = "orders"            // 3 partitions — partitioning demos
	TopicEvents = "events"            // 1 partition  — simplest possible topic
	TopicCompacted = "orders-compacted" // cleanup.policy=compact — compaction demo
)

// GroupIDs for the consumer demos. Different groups = independent readers.
const (
	GroupBilling   = "billing"
	GroupAnalytics = "analytics"
)

// Timeouts shared by clients so behavior is consistent across demos.
func DialTimeout() time.Duration { return 10 * time.Second }
