# 06 — Where to Go Next

You now have working producer/consumer CLIs, a live UI, and the core mental
model: partitions, offsets, groups, commits, rebalances, replay. Here's the
roadmap from "learning starter" to "production Kafka".

## 1. Exactly-once with transactions

franz-go: `kgo.TransactionalGroupSession` — consume → process → produce →
commit inside one broker-managed transaction. Combine with the idempotent
producer (already on) for true exactly-once *into another Kafka topic*
(never to external systems — that needs idempotent writes on the far side).

Demo idea: extend `cmd/consumer` with a `-txn` flag that re-produces each
order into `orders-processed` transactionally; kill -9 mid-batch and show
the partially-written batch is rolled back.

## 2. Schema Registry + Avro/Protobuf

JSON is flexible but has no contract. [Confluent Schema Registry](https://docs.confluent.io/platform/current/schema-registry/index.html)
stores schemas; producers/consumers negotiate versions. franz-go has
`pkg/sr` + `pkg/kgo` serializers. Add `confluentinc/cp-schema-registry` to
compose and evolve `OrderEvent` (add a field, keep old consumers alive).

## 3. Dead-letter queues (DLQ)

Real consumers poison themselves on bad messages. Pattern: on processing
error, re-produce to `<topic>-dlq` with headers (original topic/partition/
offset/error), commit, move on. Our `common.UnmarshalOrder` error path is
the natural place to demo it.

## 4. Retention & compaction — hands on

```bash
docker exec kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=60000
```
Set 1-minute retention, watch segments (and old offsets!) disappear in the
UI, then try to replay. Then create a `cleanup.policy=compact` topic and
watch old values for a key get compacted away (Kafka only compacts the
"active" segment under specific conditions — use `kafka-log-dirs.sh` /
`kafka-dump-log.sh` to see segment files).

## 5. A real multi-broker cluster

Scale the compose file to 3 brokers (node ids 1-3, one controller, or all
KRaft-combined), replication-factor 3, min.insync.replicas=2. Then:
kill a broker and watch leader elections + ISR shrink in the UI. This is
the closest local approximation of production failure modes.

## 6. Production hygiene checklist

- `max.poll.interval.ms` awareness: processing must finish within it or
  the member is evicted (the `💥 LOST` callback you've seen).
- Commit granularity: batch-size commits are a throughput/latency lever.
- Graceful shutdown: stop polling → commit → close (this repo does it).
- Metrics: franz-go exposes client metrics via `pkg/kadm`-free hooks and
  Prometheus (`github.com/twmb/franz-go/pkg/kadm`... see `kgo.Client.Metrics`
  hooks / kgo's `WithHooks`); dashboards usually track consume-lag,
  produce-error-rate, rebalance frequency.
- Idempotent processing: at-least-once means duplicates WILL happen;
  upserts keyed by business id are the standard defense.

## Reference material

- [Kafka: The Definitive Guide, 2nd ed.](https://www.confluent.io/resources/kafka-the-definitive-guide-2nd-edition/) (free PDF)
- [franz-go docs & examples](https://github.com/twmb/franz-go/tree/master/pkg/kgo)
- [Kafka protocol & KIPs](https://cwiki.apache.org/confluence/display/KAFKA/KIPs) — KIP-429 (transactions), KIP-848 (next-gen consumer groups) explain the "why" behind behaviors you've seen here.
