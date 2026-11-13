# GoKafka make targets — one command per lab.
# Run `make` to list everything.

BROKERS    ?= localhost:9092
TOPIC      ?= orders
GROUP      ?= billing
LOAD_N     ?= 200
LOAD_RATE  ?= 50
ARGS       ?= o-1 "laptop 1 999.9"

.DEFAULT_GOAL := help
.PHONY: help up down nuke ui produce send load consume consume-analytics replay ordering

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

# ── infrastructure ────────────────────────────────────────────────────────────
up: ## start Kafka + Kafka UI (docs/02)
	docker compose up -d

down: ## stop Kafka + Kafka UI
	docker compose down

nuke: ## stop AND wipe all Kafka data (fresh cluster)
	docker compose down -v && docker compose up -d

ui: ## open the Kafka UI dashboard
	open http://localhost:8080

# ── producing ────────────────────────────────────────────────────────────────
produce: ## interactive producer (type: key product qty price)
	go run ./cmd/producer -i

send: ## send one message: make send ARGS="o-1 \"laptop 1 999.9\""
	go run ./cmd/producer -key "$(word 1,$(ARGS))" -msg "$(word 2,$(ARGS))"

load: ## burst load test: make load N=200 RATE=50 (watch partitions in UI)
	go run ./cmd/producer -n $(LOAD_N) -rate $(LOAD_RATE)

# ── consuming ────────────────────────────────────────────────────────────────
consume: ## run a consumer in group billing (run 2+ copies to see rebalancing)
	go run ./cmd/consumer -group $(GROUP)

consume-analytics: ## independent second group reading the same data
	go run ./cmd/consumer -group analytics

replay: ## reset group billing to earliest and re-read history
	go run ./cmd/consumer -group $(GROUP) -replay

# ── ordering ─────────────────────────────────────────────────────────────────
ordering: ## ordering demo: keyed (ordered) vs unkeyed (shuffled) events
	go run ./cmd/demos/ordering
	@echo "then run:  go run ./cmd/consumer -group ordering-demo -from-beginning"
