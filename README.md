# Kafka-Golang — Learn Kafka by Building It With Go

A hands-on starter that teaches **Apache Kafka internals** by building each
concept in Go and watching it happen live in a Kafka UI.

- **Stack:** Go 1.25 · [franz-go](https://github.com/twmb/franz-go) · Kafka 3.9 (KRaft, Docker) · [Kafka UI](https://github.com/provectus/kafka-ui)
- **Docs:** [01 Fundamentals](docs/01-fundamentals.md) — the internals mental model (read first!)

## Quickstart

```bash
# 1. Start Kafka + Kafka UI
docker compose up -d
open http://localhost:8080        # the dashboard

# 2. Produce messages (partitions & keys lesson)
go run ./cmd/producer -key o-1 -msg "laptop 1 999.9"
go run ./cmd/producer -n 200 -rate 50

# 3. Consume them (consumer groups lesson)
go run ./cmd/consumer -group billing
#   ...in a second terminal, watch them rebalance:
go run ./cmd/consumer -group billing
```

Full walkthrough with per-concept explanations:

| Doc | Concept | Command |
|---|---|---|
| [02 Infrastructure](docs/02-infrastructure.md) | listeners, KRaft, advertised addresses | `docker compose up -d` |
| [03 Producer](docs/03-producer.md) | acks, batching, key→partition, idempotence | `go run ./cmd/producer ...` |
| [04 Consumers](docs/04-consumers.md) | poll loop, commits, groups, rebalancing, lag | `go run ./cmd/consumer ...` |
| [05 Demos](docs/05-demos.md) | load, replay, ordering labs | `make load` `make replay` |

## Layout

```
docker-compose.yml       Kafka (KRaft) + Kafka UI
docs/                    the lessons — read in order
internal/config/         shared brokers/topics/groups config
internal/common/         message types
cmd/producer/            producing CLI (one-shot, burst, interactive)
cmd/consumer/            consuming CLI (groups, offsets, replay)
```
