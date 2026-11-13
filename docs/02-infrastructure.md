# 02 — Infrastructure: Running Kafka + Kafka UI Locally

Our stack is two Docker containers:

```
┌────────────────────── your machine ──────────────────────┐
│                                                          │
│  Go CLIs ──:9092──▶ ┌──────────┐                          │
│                     │  kafka   │ (broker + KRaft ctrl)    │
│  Browser ──:8080──▶ │          │◀──:29092──┐              │
│                     └──────────┘           │              │
│                     ┌──────────┐           │              │
│                     │ kafka-ui │───────────┘              │
│                     └──────────┘                          │
└──────────────────────────────────────────────────────────┘
```

- **kafka** — an Apache Kafka 3.9 broker in **KRaft mode** (Kafka's built-in
  metadata layer — no ZooKeeper; that's the modern default since 3.3).
- **kafka-ui** — [Provectus Kafka UI](https://github.com/provectus/kafka-ui),
  our window into the cluster: topics, partitions, live messages, consumer
  groups and lag.

```bash
docker compose up -d          # start
docker compose ps             # wait until both are Up/healthy
open http://localhost:8080    # the dashboard
```

You should see cluster `local` with 1 broker. Done — Kafka is running.

---

## Why the config looks the way it does

### 1. KRaft mode (no ZooKeeper)

Older tutorials run a separate ZooKeeper container. Modern Kafka doesn't need
it: metadata (which brokers exist, which partition lives where, leader
election) is managed by the brokers themselves in a KRaft quorum
(`KAFKA_PROCESS_ROLES: broker,controller` — one node playing both roles here).

### 2. Listeners & advertised listeners — the classic trap ⚠️

Kafka doesn't just *listen* on ports; it also **tells clients where to find
its partitions**. When a client bootstraps, the broker replies with the
*advertised* address of each broker. If the broker advertises a hostname the
client can't resolve (like a Docker-internal name), the client gets metadata
fine and then **hangs forever** trying to fetch messages. This is the #1
Kafka-Docker problem.

Our setup has **three listeners**:

| Listener | Port | Who uses it | Advertised as |
|---|---|---|---|
| `PLAINTEXT` | 9092 | Your host (Go CLIs) | `localhost:9092` |
| `INTERNAL` | 29092 | Containers (kafka-ui) | `kafka:29092` |
| `CONTROLLER` | 29093 | KRaft metadata traffic | (not advertised to clients) |

- `KAFKA_LISTENERS` = where we bind sockets.
- `KAFKA_ADVERTISED_LISTENERS` = what we *tell clients* the address is.
  Host clients are told `localhost:9092` (works via the port mapping);
  in-network clients are told `kafka:29092` (Docker DNS).
- `KAFKA_LISTENER_SECURITY_PROTOCOL_MAP` maps listener names → protocol
  (`PLAINTEXT` = no TLS; fine for local learning).

### 3. Replication factors set to 1

On a single broker, internal topics (`__consumer_offsets`, transaction state)
can only have 1 replica. A production cluster would use 3. See
[01-fundamentals.md](01-fundamentals.md) §7 for what replication means.

### 4. Healthcheck

`kafka` reports healthy only when the broker actually answers protocol
requests (`kafka-broker-api-versions.sh`), and `kafka-ui` waits for that —
so the UI never starts against a half-initialized broker.

---

## Common operations

```bash
docker compose logs -f kafka      # broker logs (very educational)
docker compose down               # stop (data kept in anonymous volume? no -)
docker compose down -v            # stop AND wipe all data (fresh start)
```

> Note: we use the image's default ephemeral storage, so `docker compose down`
> effectively resets the cluster too. Add named volumes yourself if you want
> data to survive restarts.

### CLI inside the container (optional but great for learning)

```bash
# List topics
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list

# Create a topic with 3 partitions
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --create --topic orders --partitions 3 --replication-factor 1

# Describe it (partition → leader, ISR, offset ranges)
docker exec kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 \
  --describe --topic orders

# Consume raw from the earliest offset
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders --from-beginning

# Peek at what consumer groups have committed
docker exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --all-groups --describe
```

But in this repo the Go CLIs replace most of these — and the UI replaces all
of them.

---

**Next:** [03-producer.md](03-producer.md) — writing to Kafka from Go.
