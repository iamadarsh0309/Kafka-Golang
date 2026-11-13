# 03 — Producing: Writing to Kafka from Go

The producer is `cmd/producer`. Run it while watching
http://localhost:8080 → Topics → `orders` → **Messages** (live mode).

```bash
# one message, explicit key
go run ./cmd/producer -key o-100 -msg "laptop 1 999.9"

# mini load test: 200 msgs @ 50/s, varied keys
go run ./cmd/producer -n 200 -rate 50

# interactive (type: key product quantity price)
go run ./cmd/producer -i
```

Every produce prints **where the message landed**:

```
✅ topic=orders partition=0 offset=12 key=o-test value={"id":"o-test",...}
```

---

## 1. Client construction — the options that matter

```go
cl, err := kgo.NewClient(
    kgo.SeedBrokers("localhost:9092"),
    kgo.RequiredAcks(kgo.AllISRAcks()),
    kgo.ProducerLinger(0),
    kgo.ClientID("producer-demo"),
)
```

### `SeedBrokers` — bootstrap, not destination
This address is used **once** to fetch cluster metadata ("topic `orders` has
3 partitions; their leaders live on broker 1 at `localhost:9092`"). After
that, records go **directly to the leader of the target partition**. That's
why `KAFKA_ADVERTISED_LISTENERS` in docker-compose.yml must be reachable —
if it advertises a hostname your machine can't resolve, you'd get
"metadata OK but messages never send" (the classic Docker-Kafka trap).

### `RequiredAcks(kgo.AllISRAcks())` — the durability dial
This maps 1:1 to Kafka's `acks` setting:

| Acks | Broker behavior | Risk |
|---|---|---|
| `0` (`kgo.NoAck()`) | Don't even reply | Message can vanish silently |
| `1` (`kgo.LeaderAck()`) | Leader has it in memory, not replicated | Leader crash loses it |
| `all` (`kgo.AllISRAcks()`) | **Every in-sync replica** has it | Only lost if the *entire ISR* dies |

`acks=all` is the production default — and the reason Kafka is considered
durable. On our single-broker cluster ISR = {broker 1} only, so this is
effectively acks=1 today; on a 3-broker cluster it becomes a real guarantee.

### Idempotent producer (ON by default in franz-go ≥ v1.15)
Retries + timeouts can cause **duplicate writes** (broker wrote the record,
but the ack never reached the producer, so it retries). Kafka's idempotent
producer fixes this: each producer session gets a *PID*, each batch a
*sequence number*, and the broker **rejects duplicates**. Result: no dupes,
no loss, per partition. (Full exactly-once across *consumers* needs
transactions — see docs/06-next-steps.md.)

### Batching — the throughput lever
The producer does not send record-by-record. It appends each record to an
in-memory batch **per partition** and ships whole batches:

```
app code ─▶ record queue (per partition) ─▶ one network request = many records
                  ↑ linger.ms / batch size decide when to cut over
```

- `kgo.ProducerLinger(0)` (ours): send the moment a record arrives → lowest
  latency, smaller batches.
- `kgo.ProducerLinger(10*time.Millisecond)`: wait up to 10ms to collect more
  records → bigger batches, far higher throughput, +10ms latency.

This is the same latency-vs-throughput tradeoff you'll see on the consumer
side with fetch settings. Watch it: run `-n 200 -rate 50` and the UI's
partition detail shows records arriving in offset chunks, not one by one.

---

## 2. Choosing a partition — the key decision

```
partition = murmur2(key) % numPartitions
```

Demonstrate it:

```bash
go run ./cmd/producer -key o-100 -msg "a 1 1"     # → partition 0
go run ./cmd/producer -key o-100 -msg "b 1 1"     # → partition 0 again!
go run ./cmd/producer -key o-200 -msg "c 1 1"     # → partition 1 (probably)
```

Same key **always** lands on the same partition (until you add partitions —
then the hash splits, which is why key-based ordering breaks when topics
scale out. Remember that).

Why this matters (docs/01-fundamentals.md §4):
- All events for `order o-100` (created → paid → shipped) must be processed
  in order → use `order_id` as key.
- If you send **no key**, franz-go round-robins/stickies records across
  partitions → max parallelism, zero ordering guarantees.

Also note in the burst output: partition 0 reached offset 26 while others
were at ~17 — keys weren't perfectly uniform. Real clusters fight **hot
partitions** all the time; the UI's partition bars make this visible.

---

## 3. The async produce + callback

```go
cl.Produce(ctx, rec, func(r *kgo.Record, err error) { ... })
```

`Produce` is **asynchronous**: it buffers the record and returns immediately.
The callback fires when the broker acks (or retries exhaust). Printing the
partition/offset in the callback is the honest way — you're printing what
the *broker actually assigned*, not what you hoped for.

Two lifecycle rules every producer must follow:

```go
defer cl.Close()                    // 1. release network resources
if err := cl.Flush(context.Background()); err != nil { ... }   // 2. BEFORE exiting
```

Without `Flush`, the program can exit while records are still queued —
they're never sent. (A classic "my producer loses the last messages" bug.)

---

## 4. Error handling taxonomy

`crt.Err` / produce callback errors fall into three families:

1. **Retriable** (`UNKNOWN_TOPIC_OR_PARTITION` right after creating a topic,
   leader elections, timeouts) — franz-go retries these automatically.
2. **Fatal** (record too large, auth failed) — fail fast, don't retry.
3. **Unknown topic** — in this repo we create topics with `kadm` before
   producing (`ensureTopic`), so you never hit a race with auto-creation.

> Teaching moment baked into the code: `kadm.ListTopics` returns an entry
> for missing topics with the error *inside the entry* — hence the
> `found && t.Err == nil` check in `ensureTopic`.

---

## 5. Lab: try these in order

1. Send 3 messages with the same key → same partition, offsets 0,1,2.
2. Send 3 messages with different keys → likely different partitions.
3. Run `-n 500 -rate 100`, then in the UI open
   Topics → orders → **Partitions** → compare per-partition counts.
4. (Conceptual) change `ProducerLinger(0)` → `ProducerLinger(50ms)` and
   re-run the burst. Offset gaps arrive in bigger chunks = bigger batches.

**Next:** [04-consumers.md](04-consumers.md) — reading it all back.
