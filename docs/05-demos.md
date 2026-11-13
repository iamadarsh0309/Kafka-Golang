# 05 — Labs: Load, Replay & Ordering

One-command labs. Everything is observable live at http://localhost:8080.

```bash
make up          # start Kafka + Kafka UI
make help        # list every command
```

---

## Lab A — Partition distribution (load test)

```bash
make load N=500 RATE=100        # 500 messages, 100/s, keys o-0..o-499
```

**Do:** run it, then in the UI open Topics → `orders` → **Partitions** and
watch each partition's count climb. Then open partition 0's message list
(offsets climb in chunks — those chunks are producer *batches*, docs/03 §1).

**Observe:**
1. Partitions grow at slightly different rates — the murmur2 hash over
   sequential ids isn't perfectly uniform. Real-world clusters watch for
   "hot partitions" exactly like this.
2. Every offset per partition is contiguous 0,1,2,... — append-only logs.

**Variation:** `go run ./cmd/producer -key same-key -n 50 -rate 20`
(uses the same key for every message in one-shot mode → all land on ONE
partition: the hot-partition pathology on purpose).

---

## Lab B — Rebalancing (consumer groups)

```bash
# terminal 1                       # terminal 2
make consume                       make consume
```

**Do:** with both running, ctrl-c terminal 1. Terminal 2 logs:
```
↪️  REVOKED:  map[orders:[0 1]]
⚖️  ASSIGNED: map[orders:[0 1 2]]
```
In the UI: Consumers → `billing` → the members list drops to 1 and lag
stays 0 once the survivor re-reads nothing.

**Observe (the key lessons):**
- Ownership is exclusive per partition *per group* — that's the load split.
- A hard-killed member is detected only after the **session timeout**
  (10s here). Graceful leaves (ctrl-c into franz-go's leave-group) are
  near-instant; crashes are not. Production tuning note!

**Then:** start a third consumer — it goes **IDLE** (3 partitions, 3
members... actually try a 4th to see a member wait for work). The partition
count is the hard ceiling of group parallelism.

---

## Lab C — Replay (offsets are a movable cursor)

```bash
# 1. make sure no member of group "billing" is running
make replay
```

**Do:** watch the log: it resets offsets, then prints messages from
`p0 o0`, `p1 o0`, `p2 o0` — the very beginning of the log.

In the UI: Consumers → `billing` → **Offsets** column jumps backward;
`__consumer_offsets` gets a new commit record.

**Observe:** the old messages were never deleted, never re-sent by the
producer. Replay = the group's cursor moved backward. This is why Kafka
suits event sourcing, audit trails, and reprocessing after a bug fix.

> Internals recap (docs/01 §5): the cursor lives in the compacted
> `__consumer_offsets` topic. If the reset "mysteriously fails" right after
> a kill -9, that's the coordinator still counting the dead member until
> the session timeout — the tool retries for you and says so.

---

## Lab D — Ordering: keyed vs unkeyed

```bash
make ordering
go run ./cmd/consumer -group ordering-demo -from-beginning   # separate terminal
```

Producer output:
```
sent keyed   step-1 created    → partition=0 offset=35
sent keyed   step-2 paid       → partition=0 offset=36
...                                ← one partition, sequential offsets: ORDERED
sent UNKEYED step-1 created    → partition=0 offset=40
sent UNKEYED step-4 shipped    → partition=0 offset=41
sent UNKEYED step-2 paid       → partition=1 offset=10
sent UNKEYED step-5 delivered  → partition=1 offset=11
sent UNKEYED step-3 picked     → partition=2 offset=15
                                   ← scattered partitions: SHUFFLED
```

The consumer prints in partition-offset order: the keyed lifecycle reads
`created → paid → picked → shipped → delivered` in every consumer; the
unkeyed one reads interleaved across partitions and can deliver "paid"
before "created".

**Observe / remember:**
- Kafka's ordering guarantee is **per partition only**.
- The **key choice** is a design decision: all events that must be ordered
  together (an order's lifecycle, a user's session, an account's ledger)
  must share one key.
- Cross-partition order can only be recovered with application-level
  sequencing (timestamps/versions + a reordering step downstream) — or by
  not needing cross-partition order in the first place.

---

## Lab E — Independent groups (fan-out)

```bash
make consume                      # group "billing"
make consume-analytics            # group "analytics" — same data, own cursor
```

Two groups, each with its own offsets and lag, each seeing the full
stream. Kafka's fan-out mechanism is "use another group", not "send
copies".

---

## Bonus — from the shell, no Go

```bash
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group billing
```
This is the same data Kafka UI shows: CURRENT-OFFSET (committed),
LOG-END-OFFSET (high watermark), LAG, CONSUMER-ID (the member), PARTITION
ASSIGNMENT.
