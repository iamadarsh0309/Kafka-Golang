# Kafka-Golang — Learn Kafka by Building It With Go

A hands-on starter that teaches **Apache Kafka internals** by building each
concept in Go and watching it happen live in a Kafka UI.

**Stack:** Go 1.25 · [franz-go](https://github.com/twmb/franz-go) · Kafka 3.9 (KRaft, single node, Docker) · [Kafka UI](https://github.com/provectus/kafka-ui)

---

## The 5-minute tour

```bash
make up                                   # Kafka on :9092 + UI on :8080
open http://localhost:8080                # keep this open — it's the observability

make send ARGS="o-1 \"laptop 1 999.9\""   # produce one keyed message
make load N=200 RATE=50                   # produce a burst, watch partitions grow

make consume                              # consume in group "billing"
# ...then run it again in a 2nd terminal and watch the REBALANCE
# ...ctrl-c the 1st and watch the survivor take everything back

make replay                               # reset offsets, re-read history
make ordering                             # keyed (ordered) vs unkeyed (shuffled)
```

---

## The lesson path (read in order)

| # | Doc | Concept | Try with |
|---|-----|---------|----------|
| 1 | [docs/01-fundamentals.md](docs/01-fundamentals.md) | **The internals mental model**: brokers, topics, **partitions**, **offsets**, consumer groups, replication/ISR, retention & compaction | — |
| 2 | [docs/02-infrastructure.md](docs/02-infrastructure.md) | KRaft vs ZooKeeper, **listeners & advertised listeners** (the classic Docker trap), healthchecks | `make up` |
| 3 | [docs/03-producer.md](docs/03-producer.md) | **acks** (0/1/all), batching & linger, the **idempotent producer**, key→partition hashing, async produce + callbacks | `make send`, `make load` |
| 4 | [docs/04-consumers.md](docs/04-consumers.md) | the poll loop, **offset commits** & delivery semantics, **consumer groups**, rebalancing, lag, **replay** | `make consume`, `make replay` |
| 5 | [docs/05-demos.md](docs/05-demos.md) | guided labs: partition distribution, rebalancing, replay, **ordering guarantees**, independent groups | `make load/replay/ordering` |
| 6 | [docs/06-next-steps.md](docs/06-next-steps.md) | where to go from here: transactions/exactly-once, schema registry, DLQs, KRaft tuning | — |

## What each commit teaches

```
docs:        Kafka fundamentals - brokers, topics, partitions, offsets,
             consumer groups, replication, retention
feat: infra  docker compose (Kafka KRaft + Kafka UI) + internals doc
feat:        producer CLI (acks, batching, key-based partitioning)
feat:        consumer CLI (groups, rebalancing, commits, replay)
feat:        Makefile labs (load, replay, ordering)
docs:        master guide + next steps          ← you are here
```

## Layout

```
docker-compose.yml     Kafka (KRaft, single node) + Kafka UI
Makefile               one command per lab (make help)
docs/                  the lessons — the real meat, read in order
internal/config/       brokers/topics/groups (env-overridable)
internal/common/       shared message type (OrderEvent, JSON)
internal/admin/        topic creation + group offset reset (kadm wrapper)
cmd/producer/          produce: one-shot / burst / interactive
cmd/consumer/          consume: groups, manual commits, replay
cmd/demos/ordering/    keyed-vs-unkeyed ordering demo
```

## Cheat sheet — the concepts you can now demo

| Concept | Where to see it |
|---|---|
| Partition = ordered append-only log | UI topic → partition offsets 0,1,2,... |
| key → partition (murmur2 hash) | `make send` twice with same key → same partition |
| Order is per-partition only | `make ordering` |
| Offset = consumer cursor, commit = checkpoint | consumer logs + UI group offsets |
| Lag = latest − committed | UI Consumers → group |
| Group = partition split; fan-out = more groups | two `make consume` / `make consume-analytics` |
| Rebalance on membership change | ctrl-c a consumer |
| Replay = move the cursor back | `make replay` |
| acks=all durability vs fire-and-forget | `kgo.RequiredAcks` in `cmd/producer` |
| Retention: data outlives consumption | messages stay after consumers stop |

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| Client hangs after metadata fetch | Advertised listener unreachable — see docs/02 §2. `docker compose down -v && make up` |
| `UNKNOWN_TOPIC_OR_PARTITION` right after topic create | Metadata propagation; franz-go retries automatically |
| Replay reset fails with `UNKNOWN_MEMBER_ID` | Dead members linger until the session timeout (10s here) — wait a few seconds |
| Consumer logs `context canceled` spam on ctrl-c | Shutdown race; handled in `cmd/consumer` (`ctx.Err()` check) |
| UI blank / no cluster | Wait for `kafka` healthy (`docker compose ps`), refresh UI |
