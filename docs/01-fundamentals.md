# 01 — Kafka Fundamentals: The Internals Guide

> This is the mental model you should build **before** writing a line of Go code.
> Everything in this repo demos one of these concepts.

---

## 1. What is Kafka?

Apache Kafka is a **distributed, partitioned, replicated commit log**.
It is not a queue, not a database — it is an **append-only event log** that
clients read at any speed, in any order (within a partition), and as many
times as they want.

Compare the two mental models:

| | Queue (RabbitMQ, SQS) | Kafka |
|---|---|---|
| After a message is consumed | It is **deleted** | It is **kept** (until retention) |
| Who "knows" what was read | The broker (push, ack) | The **consumer** (it commits offsets) |
| Re-reading history | Impossible | Trivial — just rewind the offset |
| Scaling consumers | Compete on one queue | Each consumer group has its own view |

The single most important insight:

> **Kafka does not track "read" messages. The consumer does.**

A consumer asks: *"give me messages starting at offset 42 of partition 3"*.
Kafka serves them and forgets. If the consumer wants to replay from offset 0
tomorrow, it just asks again.

---

## 2. Brokers & the Cluster

```
                    Kafka Cluster
┌────────────────────────────────────────────────────┐
│  ┌─────────────┐   ┌─────────────┐   ┌──────────┐  │
│  │ Broker 0    │   │ Broker 1    │   │ Broker 2 │  │
│  │ (leader for │   │ (follower   │   │          │  │
│  │  some parts)│   │  for others)│   │          │  │
│  └─────────────┘   └─────────────┘   └──────────┘  │
└────────────────────────────────────────────────────┘
```

- A **broker** is one Kafka server (one Docker container in this repo).
- A **cluster** is several brokers working together. It provides:
  - **Horizontal scale** — partitions are spread across brokers.
  - **Fault tolerance** — each partition is replicated to multiple brokers.
- One broker is elected **controller**: it decides who leads each partition.
- In this starter we run **one** broker in **KRaft mode** (Kafka without
  ZooKeeper — since Kafka 3.3 the metadata layer is built-in). Single node is
  fine for learning; the concepts map 1:1 to a 3-broker production cluster.

### What actually happens when a client connects

A producer/consumer does NOT need to know which broker has the data. It:

1. Bootstraps against any broker (`localhost:9092` for us).
2. Receives **cluster metadata**: "topic `orders` has 3 partitions;
   partition 0 is led by broker 0, partition 1 by broker 0, ..."
3. Talks **directly** to the leader broker of the target partition.

> ⚠️ The classic beginner trap: the broker advertises its address to clients
> (`advertised.listeners`). If it advertises an internal Docker hostname, your
> host program can't reach it. That's why our compose file advertises
> `localhost:9092`. See `docs/02-infrastructure.md`.

---

## 3. Topics

A **topic** is a named channel (e.g. `orders`, `payments`). It exists purely
for organization and access control. A topic itself stores nothing — its
**partitions** do.

Think of a topic as a folder of files:

```
Topic: orders
├── partition 0  →  append-only log file(s) on disk
├── partition 1  →  append-only log file(s) on disk
└── partition 2  →  append-only log file(s) on disk
```

---

## 4. Partitions — the heart of Kafka

A **partition** is a single, strictly ordered, append-only log.

```
Partition 2 of topic "orders":
 offset:  0   1   2   3   4   5   6
        ┌───┬───┬───┬───┬───┬───┬───┐
        │ m │ m │ m │ m │ m │ m │ m │   ← append only!
        └───┴───┴───┴───┴───┴───┴───┘
                                    ↑ new writes land here
```

### Why partitions exist

1. **Parallelism.** One partition can be read by only one consumer in a group
   at a time. More partitions = more consumers working concurrently.
2. **Ordering.** Kafka guarantees order **only within a single partition**.
   Across partitions, there is NO order.
3. **Distribution.** Partitions of a topic are spread across brokers, which
   is how Kafka scales beyond one machine.

### How a message lands in a partition

When a producer sends a message with a **key**:

```
partition = hash(key) % number_of_partitions
```

- Same key ⇒ same partition ⇒ **ordering is preserved for that key**.
  (e.g. all events for `order-123` are processed in order.)
- No key ⇒ round-robin/sticky partitioning (max batching throughput, no
  ordering guarantee).

This is THE design decision you make as a Kafka developer:
> **Choose keys so that all events that must be ordered together share a key.**

Examples: `user_id`, `order_id`, `account_id`. Never use something random
if you care about ordering; never use the same key for everything or one
partition does all the work (a "hot partition").

### Offsets inside a partition

Each message in a partition gets a sequential 64-bit number: its **offset**.

- Offsets are assigned by the broker, per partition, starting at 0.
- An offset is only meaningful **together with** its (topic, partition).
- `(topic, partition, offset)` uniquely identifies one message in the cluster.

---

## 5. Offsets — Kafka's cursor

The offset system is what makes Kafka "rewindable".

### Who tracks progress? The consumer.

A consumer group stores its position — the **committed offset** — in an
internal Kafka topic called `__consumer_offsets` (yes, Kafka uses Kafka to
track consumers. It's turtles all the way down).

```
Partition 0 of "orders":        Group "billing" state:
 0 1 2 3 4 5 6 7                committed offset = 5
           ↑     ↑
        latest   committed
                 (0..4 delivered, 5..7 pending)
```

- **High watermark** — the latest offset visible to consumers
  (messages written but not yet fully replicated are not visible yet).
- **Committed offset** — where the group says it is.
- **Lag** = `high watermark − committed offset`. This is the number you
  watch in Kafka UI. High lag = consumers falling behind.

### Committing

After processing a batch, the consumer *commits* the next-to-read offset.
Three delivery semantics fall out of WHEN you commit:

| Strategy | If consumer crashes... | Semantics |
|---|---|---|
| Commit **before** processing | Messages may be **skipped** | at-most-once |
| Commit **after** processing | Messages may be **re-processed** | at-least-once (most common) |
| Idempotent/transactional writes | Neither | exactly-once |

Default in this repo: **at-least-once** — we commit after processing, so a
crash may re-deliver, never lose.

### Rewinding / replay

Because history is retained, "replay" is just: set the group's committed
offset to an older value (earliest, a timestamp, or any offset). Our
`make replay` demo does exactly this and you watch old messages flow again.

---

## 6. Consumer Groups

```
Topic "orders" (3 partitions)          Group "billing"
p0 ──────────────┐
p1 ──────────────┼──→  ┌──────────────┐
p2 ──────────────┘     │ Consumer A   │ ← owns p0, p1
                       │ Consumer B   │ ← owns p2
                       └──────────────┘
```

Rules:

1. A **group** is a set of consumers sharing a `group.id` that cooperate.
2. Each **partition is owned by exactly one consumer** in the group at any
   time — this is why you get load balancing without duplicate work.
3. A consumer may own **several** partitions (3 partitions, 2 consumers ⇒
   one owns 2). Fewer consumers than partitions is fine; more consumers than
   partitions means the extras sit **idle**.
4. **Different groups are independent** — group `analytics` also reads
   `orders` from offset 0. Kafka "fan-outs" via groups, not via pushing copies.
5. **Rebalancing**: when a member joins or leaves (crash, scale up/down),
   the group's coordinator reassigns partitions. During a rebalance the whole
   group briefly stops consuming. You will watch this happen live in Kafka UI.

---

## 7. Replication (why Kafka doesn't lose data)

Each partition has **one leader** and **N−1 followers** on other brokers.

```
Partition 2:
  Broker 0: leader   ← producers & consumers talk to this one
  Broker 1: follower (in-sync)
  Broker 2: follower (in-sync)
```

- The set of followers that are caught up is the **ISR** (in-sync replicas).
- With `acks=all` (our default), a write is acknowledged only after **all
  ISR replicas** have it — a leader crash loses nothing.
- The leader is re-elected from the ISR if it dies.

On a single-broker setup every partition has ISR size 1 — fine for learning.

---

## 8. Retention & Compaction

Data is kept **by time/size**, not by consumption:

- `retention.ms` (default 7 days) — delete segments older than this.
- Segments are deleted, never individual messages: the log is cut into
  ~1 GB **segment files**; whole segments are dropped.
- **Log compaction** (`cleanup.policy=compact`) — instead of deleting by
  time, keep only the **latest value per key**. Used for state (e.g. the
  latest balance per account). Our `orders-compacted` demo topic uses it.
- `__consumer_offsets` is internally compacted for the same reason.

---

## 9. The full journey — putting it together

```
Producer (Go)                Broker                        Consumer Group (Go)
┌──────────────┐   batch    ┌──────────────────────┐  poll  ┌──────────────┐
│ 1. pick part │──(key hash)│ 2. append to leader  │──────→ │ 4. process   │
│ 2. batch up  │  acks=all  │    log               │        │ 5. commit    │
│ 3. wait acks │←───────────│ 3. replicate to ISR  │        │    offset    │
└──────────────┘            │ 4. bump high watermark│       └──────────────┘
                            └──────────────────────┘
                                    │
                                    └─→ retained until retention.ms
                                        (replayable at any time)
```

1. Producer hashes the key → picks a partition → batches → sends.
2. Leader appends to its log, replicates to ISR, acks.
3. Consumer (assigned that partition) polls, gets the new records.
4. Consumer processes, then commits the new offset (lag shrinks in UI).
5. Message stays on disk; replay is always one offset-reset away.

---

## 10. Glossary cheat-sheet

| Term | One-liner |
|---|---|
| Broker | One Kafka server; stores partition replicas |
| Topic | Named channel; container for partitions |
| Partition | Ordered append-only log; unit of parallelism & ordering |
| Offset | Sequential position of a message within a partition |
| Producer | Writes messages to topic partitions |
| Consumer | Reads a partition sequentially from an offset |
| Consumer group | Set of consumers dividing a topic's partitions |
| Rebalance | Reassignment of partitions when the group membership changes |
| Commit | Consumer saving its current offset to `__consumer_offsets` |
| Lag | Latest available offset − committed offset |
| ISR | Replicas fully caught up with a partition's leader |
| High watermark | Highest offset safe for consumers to read |
| Retention | Time/size-based deletion of old segments |
| Compaction | Keeping only the latest message per key |
| KRaft | Kafka's built-in metadata layer (no ZooKeeper) |

---

**Next:** [02-infrastructure.md](02-infrastructure.md) — run Kafka + Kafka UI locally and understand every compose flag.
