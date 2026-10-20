#!/bin/bash
# Start HBase standalone, pre-split flight_track, then run Thrift1 and supervise both.
set -euo pipefail

HB="$HBASE_HOME/bin/hbase"

"$HBASE_HOME/bin/start-hbase.sh"

echo "Waiting for HBase master..."
for _ in $(seq 1 90); do
  if curl -sf -o /dev/null http://localhost:16010/master-status; then
    break
  fi
  sleep 2
done

# Thrift1 createTable (used by happybase) cannot take split keys, so the
# pre-split table is created here with the local shell. Standalone ZooKeeper
# only listens on localhost, so this cannot run from another container.
# Capture first: with pipefail, grep -q closing the pipe early would fail the check.
EXISTS_OUT=$(echo "exists 'flight_track'" | "$HB" shell -n 2>&1 || true)
if grep -q "does exist" <<<"$EXISTS_OUT"; then
  echo "flight_track already exists"
else
  SPLITS=$(seq -f "'%02g'" 1 15 | paste -sd, -)
  echo "create 'flight_track', {NAME=>'pos',TTL=>604800}, {NAME=>'ctx',TTL=>604800}, {NAME=>'wx',TTL=>604800}, SPLITS => [${SPLITS}]" \
    | "$HB" shell -n
  echo "flight_track created with 16 regions"
fi

"$HB" thrift start -p 9090 --infoport 9095 &
THRIFT_PID=$!

# Exit when either the master or Thrift dies so Docker restarts the container
# instead of leaving a Thrift server in front of a dead master.
while kill -0 "$THRIFT_PID" 2>/dev/null && curl -sf -o /dev/null http://localhost:16010/master-status; do
  sleep 5
done
echo "HBase master or Thrift stopped; exiting" >&2
exit 1
