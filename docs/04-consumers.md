# 04 — Consuming: Consumer Groups, Offsets & Rebalancing

The consumer is `cmd/consumer`. Run it and watch
http://localhost:8080 → **Consumers** → your group (members, assigned
partitions, and lag updating live).

```bash
go run ./cmd/consumer -group billing                 # terminal 1
go run ./cmd/consumer -group billing                 # terminal 2 → rebalance!
go run ./cmd/consumer -group analytics               # independent reader
go run ./cmd/consumer -group billing -from-beginning # new group: read history
go run ./cmd/consumer -group billing -replay         # reset offsets, re-read
```

---

## 1. The poll loop — what the "API" really is

```go
for {
    fetches := cl.PollRecords(ctx, 100)   // block until records arrive
    fetches.EachRecord(func(r *kgo.Record) { process(r) })
    cl.CommitRecords(ctx, fetches.Records()...)  // checkpoint progress
}
```

Under the hood, franz-go continuously:

1. Sends **Fetch requests** to the leader broker of each partition it owns
   ("give me everything from offset X"), asking for up to a few MB at a
   time — consumers PULL; Kafka never pushes.
2. Buffers fetched records in memory until your poll loop takes them.
3. Sends **heartbeats** to the group coordinator every `HeartbeatInterval`
   — proof you're alive. Stop heartbeating past `SessionTimeout` (ours:
   10s, production default 45s) and the coordinator declares you dead and
   triggers a **rebalance**. That's why the `💥 LOST` callback exists.

The `100` in `PollRecords(ctx, 100)` is Kafka's `max.poll.records`: a
safety cap so one poll can't explode your memory.

> Full loop heartbeat: pull → process → **commit** → repeat. That's the
> whole consumer API. Everything else (groups, lag, replay) hangs off the
> commit step.

---

## 2. Committing offsets — the semantics dial

A commit writes "my group reached offset N on partition P" into
`__consumer_offsets` (an internal, *compacted* Kafka topic — see
docs/01 §8). When the group restarts, the coordinator reads that topic to
know where everyone left off.

| When you commit | Crash outcome | Semantics |
|---|---|---|
| Before processing | batch never processed | at-most-once |
| **After processing (ours)** | batch may re-run | **at-least-once** |
| Transactional write-through | neither | exactly-once |

We chose **commit-after-process** (the production default): losing nothing
beats occasionally doing work twice — and "twice" is handled by making your
processing idempotent (e.g. upsert keyed by order id).

`DisableAutoCommit()` + `CommitRecords` makes the mechanism explicit; in
production code, franz-go's `CommitMarkedOffsets` after acked processing is
a common pattern. What matters for the mental model is *when* the commit
lands relative to processing.

---

## 3. Consumer groups — split work, multiply readers

```bash
go run ./cmd/consumer -group billing      # member 1
go run ./cmd/consumer -group billing      # member 2  → splits partitions
go run ./cmd/consumer -group billing      # member 3  → 1 partition each
go run ./cmd/consumer -group billing      # member 4  → sits IDLE (3 partitions max!)
```

Rules you can verify live in the UI (Consumers → billing → Topic
Partitions):

1. **Partition ↔ member is exclusive** within a group — no duplicate work,
   no competing like a message queue.
2. **More members than partitions = idle members.** The partition count is
   your max parallelism for a topic.
3. **Different group = different cursor.** `analytics` starts at its own
   offset and consumes at its own speed. Fan-out = more groups.

### Rebalancing — the protocol you just watched

Run two members, then ctrl-c one. The logs tell the story:

```
member1: ↪️  REVOKED:  map[orders:[0 1 2]]     ← group freezes, everyone gives up...
member1: ⚖️  ASSIGNED: map[orders:[2]]         ← ...and is re-dealt (member1 keeps p2)
member2: ⚖️  ASSIGNED: map[orders:[0 1]]       ← member2 gets p0+p1
```

A **rebalance** happens whenever membership changes: member joins, member
dies (10s session timeout after a hard kill — the coordinator waits to
distinguish "GC pause" from "gone"), topic gains partitions, subscription
changes. During a rebalance the whole group **stops consuming** — that's
the famous Kafka pain point, why longer rebalances hurt, and why modern
Kafka (and franz-go's default `cooperative-sticky`) introduced **incremental
cooperative rebalancing**: only the partitions that actually change hands
are revoked. This repo uses the classic `RangeBalancer` (eager, revoke
everything) on purpose — it makes the protocol loud and visible in logs.

> The commit-during-revoke subtlety: before handing a partition to another
> member you must have committed everything you processed, or the new owner
> will re-read (or skip) a chunk. Our commit-after-every-batch loop makes
> this a non-issue; the code comments show where you'd add it otherwise.

---

## 4. Lag — the number you watch

```
Lag = high watermark (latest offset) − committed offset
```

Lag 0 = caught up. Rising lag = producers outrun consumers (scale: add
members ≤ partition count, raise batch sizes, speed up processing). The UI
shows per-partition lag in Consumers → your group; `make load` + two
consumers makes the bars dance.

---

## 5. Replay — moving the cursor, not the data

```bash
# 1. stop every member of the group
# 2.
go run ./cmd/consumer -group billing -replay
```

`-replay` asks the admin API to commit each partition's **log start
offset** for the group, then re-consumes from 0. Nothing is re-sent by the
producer; nothing is deleted; the messages were always on disk (retention).
"Replay" is just **cursor movement** — the superpower of log-based systems.

Internally this is exactly what `kafka-consumer-groups.sh --reset-offsets
--to-earliest` does, with one shared gotcha you'll hit with kill -9:
the coordinator evicts dead members only after the **session timeout**
(10s here), and rejects outside commits until then — our `ResetGroupToStart`
retries through that window with a explanatory error if you get impatient.

---

## 6. Lab: try these in order

1. One consumer; produce 15 (`go run ./cmd/producer -n 15 -rate 5`) → it
   processes all 3 partitions, lag → 0 in UI.
2. Start a second consumer → REVOKED/ASSIGNED pair in both logs; UI shows
   p0,p1 / p2 split.
3. ctrl-c member 1 → within ~10s member 2 re-owns everything.
4. Stop all → `-replay` → watch offsets 0..N stream again; UI "Committed
   Offsets" restart from 0.
5. Start a `-group analytics` consumer → same data, second independent
   cursor, its own lag.

**Next:** [05-demos.md](05-demos.md) — load, replay, and ordering labs in one place.
