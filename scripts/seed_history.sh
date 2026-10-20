#!/usr/bin/env bash
# Build enough synthetic history to train the delay model, then run the batch jobs.
#
# Runs the stack with the synthetic simulator at SIM_SPEED (default 30x) for
# SEED_MINUTES wall-clock minutes (default 45, about 22 simulated hours), with a
# fog episode at VIDP and a load spike at VABB injected part-way through.
set -euo pipefail

SIM_SPEED="${SIM_SPEED:-30}"
SEED_MINUTES="${SEED_MINUTES:-45}"
COMPOSE="${COMPOSE:-docker compose}"
API="${API:-http://localhost:8000}"

[ -f .env ] || cp .env.example .env

echo "starting stack at ${SIM_SPEED}x for ${SEED_MINUTES} min"
ADSB_PROVIDER=synthetic SIM_SPEED="$SIM_SPEED" $COMPOSE up -d --build
bash scripts/wait_for.sh "$API/api/health" 300

inject() {
  curl -fsS -X POST "$API/api/sim/control" -H 'Content-Type: application/json' -d "$1" >/dev/null \
    && echo "sent: $1"
}

third=$((SEED_MINUTES * 60 / 3))
sleep "$third"
inject '{"action":"set_fog","airport":"VIDP","visibility_m":200,"duration_min":240}'
sleep "$third"
inject '{"action":"set_load","airport":"VABB","load_factor":1.8}'
sleep "$third"
inject '{"action":"reset"}'

echo "running batch jobs"
make batch
echo "done: open http://localhost:8080/model"
