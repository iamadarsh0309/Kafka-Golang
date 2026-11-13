// Package admin wraps franz-go's admin API (kadm) with the two operations
// every demo needs: topic creation and group offset reset.
package admin

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/iamadarsh0309/Kafka-Golang/internal/config"
)

// EnsureTopic creates the topic (partitions partitions, replication factor 1)
// if it does not exist.
//
// Gotcha this repo documents: kadm.ListTopics returns an entry even for a
// *missing* topic, with UNKNOWN_TOPIC_OR_PARTITION inside the entry — so the
// existence check must look at both presence and Err.
//
// NOTE: kadm.Client wraps the SAME *kgo.Client you pass in — never call
// admin.Close(), it would close your producer/consumer underneath.
func EnsureTopic(cl *kgo.Client, topic string, partitions int32) {
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout())
	defer cancel()

	a := kadm.NewClient(cl)
	topics, err := a.ListTopics(ctx, topic)
	if err != nil {
		log.Fatalf("list topics: %v", err)
	}
	if t, found := topics[topic]; found && t.Err == nil {
		return // exists
	}
	res, err := a.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		log.Fatalf("create topic %s: %v", topic, err)
	}
	for _, crt := range res {
		if crt.Err != nil && !errors.Is(crt.Err, kerr.TopicAlreadyExists) {
			log.Fatalf("create topic %s: %v", topic, crt.Err)
		}
	}
	log.Printf("topic %s created (%d partitions, rf=1)", topic, partitions)
}

// ResetGroupToStart rewinds a consumer group's committed offsets for topic
// back to each partition's log start offset — i.e. "replay".
//
// This must run while the group has NO active members (stop your consumers
// first), exactly like kafka-consumer-groups.sh --reset-offsets: Kafka
// rejects offset commits from outside an active group's membership.
//
// Internals (docs/01-fundamentals.md §5): replay does not touch the data.
// Old messages are still on disk (retention) — we only move the group's
// cursor in the compacted __consumer_offsets topic.
func ResetGroupToStart(cl *kgo.Client, group, topic string) error {
	ctx, cancel := context.WithTimeout(context.Background(), config.DialTimeout())
	defer cancel()

	a := kadm.NewClient(cl)

	start, err := a.ListStartOffsets(ctx, topic)
	if err != nil {
		return fmt.Errorf("list start offsets: %w", err)
	}

	offs := kadm.Offsets{}
	for topicName, parts := range start {
		m := map[int32]kadm.Offset{}
		for p, lo := range parts {
			if lo.Err != nil {
				return fmt.Errorf("start offset p%d: %v", p, lo.Err)
			}
			m[p] = kadm.Offset{Topic: topicName, Partition: p, At: lo.Offset, LeaderEpoch: lo.LeaderEpoch}
		}
		offs[topicName] = m
	}

	// The coordinator keeps killed members in the group until their session
	// timeout expires, and rejects commits in the meantime
	// (UNKNOWN_MEMBER_ID). Retry for a bit so "ctrl-c, then run" just works
	// — this is also exactly why the kafka CLI refuses to reset offsets of
	// an active group.
	var last error
	for attempt := 0; attempt < 12; attempt++ {
		res, err := a.CommitOffsets(ctx, group, offs)
		if err != nil {
			return fmt.Errorf("commit offsets: %w", err)
		}
		last = nil
		for _, parts := range res {
			for _, r := range parts {
				if r.Err != nil {
					last = fmt.Errorf("commit offset p%d: %v", r.Partition, r.Err)
				}
			}
		}
		if last == nil {
			return nil
		}
		time.Sleep(2 * time.Second) // wait for dead members to be evicted
	}
	return fmt.Errorf("%w (are all consumers of group %q stopped? the coordinator evicts dead members only after the session timeout)", last, group)
}
