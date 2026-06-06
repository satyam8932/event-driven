.PHONY: up down build logs migrate test lint fmt type-check scale-workers seed kill-worker

COMPOSE = docker compose

up:
	cp -n .env.example .env 2>/dev/null || true
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down -v

build:
	$(COMPOSE) build

logs:
	$(COMPOSE) logs -f

migrate:
	$(COMPOSE) run --rm migrate

scale-workers:
	$(COMPOSE) up -d --scale worker=$(n)

seed:
	@echo "Submitting test job..."
	curl -s -X POST http://localhost:8000/jobs \
		-H "Content-Type: application/json" \
		-d '{"manuscript": "Act 1. The storm arrived at midnight. Thunder cracked the sky open. Scene 2. Old Elara lit a candle. Its flame held steady against the dark."}' | python3 -m json.tool

seed-poison:
	@echo "Submitting poison-pill job..."
	curl -s -X POST http://localhost:8000/jobs \
		-H "Content-Type: application/json" \
		-d '{"manuscript": "__POISON_PILL__ This manuscript will always fail TTS."}' | python3 -m json.tool

kill-worker:
	@WORKER_ID=$$($(COMPOSE) ps -q worker | head -1); \
	if [ -z "$$WORKER_ID" ]; then echo "No worker running"; exit 1; fi; \
	echo "Killing worker $$WORKER_ID..."; \
	docker kill $$WORKER_ID

test:
	$(COMPOSE) --profile test run --rm test

test-unit:
	$(COMPOSE) --profile test run --rm test pytest tests/unit/ -v --tb=short

test-integration:
	$(COMPOSE) --profile test run --rm test pytest tests/integration/ -v --tb=short

lint:
	$(COMPOSE) run --rm --no-deps api sh -c "ruff check src/ tests/ && echo OK"

fmt:
	$(COMPOSE) run --rm --no-deps api sh -c "black src/ tests/ && ruff check --fix src/ tests/"

type-check:
	$(COMPOSE) run --rm --no-deps api mypy src/

status:
	curl -s http://localhost:8000/health | python3 -m json.tool
