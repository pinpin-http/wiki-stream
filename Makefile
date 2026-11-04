PY      ?= .venv/bin/python
TOPIC   ?= wiki.recentchange
DURATION ?= 2700
BROKER  ?= kafka

.PHONY: install up topic run run-offline test lint report down clean

install:
	uv venv --python 3.12 .venv
	uv pip install --python $(PY) -r requirements.lock
	uv pip install --python $(PY) --no-deps -e .

up:
	docker compose up -d --wait
	$(MAKE) topic

topic:
	docker compose exec -T redpanda rpk topic create $(TOPIC) -p 3 -r 1 \
		-c retention.ms=86400000 2>&1 | grep -v "TOPIC_ALREADY_EXISTS" || true

# Pipeline complet sur le flux réel (DURATION secondes, 45 min par défaut)
run:
	WIKISTREAM_BROKER=$(BROKER) $(PY) -m wikistream run --duration $(DURATION)

# Même chose sans Docker : broker de repli sur fichiers journalisés
run-offline:
	$(MAKE) run BROKER=file

test:
	$(PY) -m pytest

lint:
	.venv/bin/ruff check src tests

report:
	MPLBACKEND=Agg $(PY) -m wikistream report

down:
	docker compose down -v

clean:
	rm -rf data
