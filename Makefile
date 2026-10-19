.PHONY: up down reset logs live seed batch train test fe-dev smoke bench tools shell-hbase b01 b02 b03 b04 b05

COMPOSE ?= docker compose
s ?= spark-streaming
p ?= adsblol

up:
	@test -f .env || cp .env.example .env
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) --profile tools --profile batch down

reset:
	@read -p "Delete data/ and all volumes? [y/N] " a; \
	if [ "$$a" = "y" ]; then \
	  $(COMPOSE) --profile tools --profile batch down -v; \
	  find data -mindepth 1 ! -name .gitkeep -delete; \
	fi

logs:
	$(COMPOSE) logs -f --tail=100 $(s)

live:
	ADSB_PROVIDER=$(p) $(COMPOSE) up -d --force-recreate adsb-producer metar-producer

seed:
	bash scripts/seed_history.sh

batch: b01 b02 b03 b04 b05

b01 b02 b03 b04 b05:
	$(COMPOSE) --profile batch run --rm spark-batch batch.$(shell echo $@ | sed \
	  -e 's/^b01$$/b01_build_gold/' -e 's/^b02$$/b02_rdd_flight_efficiency/' \
	  -e 's/^b03$$/b03_build_training_set/' -e 's/^b04$$/b04_train_delay_model/' \
	  -e 's/^b05$$/b05_cluster_approaches/')

train: b03 b04

test:
	$(COMPOSE) run --rm --no-deps --entrypoint sh backend -c \
	  "pip install -q -r requirements/dev.txt && ruff check . && mypy vimana_common && pytest -q"

fe-dev:
	cd frontend && npm install && npm run dev

smoke:
	bash scripts/smoke_test.sh

bench:
	python scripts/benchmark.py

tools:
	$(COMPOSE) --profile tools up -d kafka-ui mlflow

shell-hbase:
	$(COMPOSE) exec hbase hbase shell
