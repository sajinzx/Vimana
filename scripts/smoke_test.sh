#!/usr/bin/env bash
# End-to-end check of a fresh stack (spec 14.4). Exits non-zero on the first failure.
set -euo pipefail

API="${API:-http://localhost:8000}"
WEB="${WEB:-http://localhost:8080}"

pass() { echo "ok   $1"; }
fail() { echo "FAIL $1" >&2; exit 1; }

# jq-free JSON helpers (the python image has no jq; python is always present).
py() { python3 -c "import sys,json; d=json.load(sys.stdin); $1"; }
wait_until() { # description, timeout_s, command...
  local what="$1" timeout="$2"; shift 2
  local end=$((SECONDS + timeout))
  until "$@" >/dev/null 2>&1; do
    [ "$SECONDS" -ge "$end" ] && fail "$what (timed out after ${timeout}s)"
    sleep 5
  done
  pass "$what"
}

health_ok() { curl -fsS "$API/api/health" | py "sys.exit(0 if d['kafka'] and d['hbase'] else 1)"; }
enough_aircraft() { curl -fsS "$API/api/aircraft/live" | py "sys.exit(0 if len(d) >= 20 else 1)"; }
airports_ok() {
  curl -fsS "$API/api/airports" | py \
    "sys.exit(0 if len(d)==6 and all(a['metrics']['traffic_40nm'] is not None for a in d) else 1)"
}
holding_seen() {
  curl -fsS "$API/api/events?airport=VIDP&type=HOLDING_START" | py "sys.exit(0 if len(d) >= 1 else 1)"
}
pipeline_ok() {
  curl -fsS "$API/api/pipeline" | py "
q=d['queries']
need=['j1_clean','j2_flight_state','j3_traffic','j4_predict']
ok=all(q.get(n) and (q[n][-1].get('processedRowsPerSecond') or 0) > 0 for n in need)
sys.exit(0 if ok and d['dlq_count'] > 0 else 1)"
}

bash scripts/wait_for.sh "$API/api/health" 240 || fail "backend did not start"
wait_until "health: kafka and hbase reachable" 240 health_ok
wait_until ">= 20 aircraft tracked" 180 enough_aircraft
wait_until "6 airports with traffic_40nm" 180 airports_ok

curl -fsS -X POST "$API/api/sim/control" -H 'Content-Type: application/json' \
  -d '{"action":"set_fog","airport":"VIDP","visibility_m":200,"duration_min":90}' >/dev/null \
  || fail "fog injection"
pass "fog injected at VIDP"
wait_until "HOLDING_START at VIDP after fog" 600 holding_seen
wait_until "J1-J4 processing and DLQ populated" 240 pipeline_ok

curl -fsS "$WEB" | grep -qi '<div id="root">' || fail "frontend did not return the SPA"
pass "frontend serves the SPA"
echo "smoke test passed"
