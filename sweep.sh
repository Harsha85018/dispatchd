#!/bin/bash
# Sweeps lease durations against a fixed load to find where the
# reap-spiral starts. Restarts the server and workers for each run
# so every trial begins from a clean WAL.

set -e
cd "$(dirname "$0")"

LEASES=("5s" "10s" "15s" "30s" "60s")
JOBS=1000

cleanup() {
  pkill -9 -f "bin/server" 2>/dev/null || true
  pkill -9 -f "bin/worker" 2>/dev/null || true
  sleep 1
}
trap cleanup EXIT

for lease in "${LEASES[@]}"; do
  echo ""
  echo "############################################"
  echo "### lease = $lease"
  echo "############################################"

  cleanup
  rm -f dispatchd.wal

  ./bin/server -lease "$lease" > "sweep-server-$lease.log" 2>&1 &
  sleep 1

  ./bin/worker -id worker-a > /dev/null 2>&1 &
  ./bin/worker -id worker-b > /dev/null 2>&1 &
  sleep 1

  ./bin/loadtest -jobs "$JOBS" 2>/dev/null | grep -E "jobs:|total attempts:|drain time:|p99:"

  reaped=$(grep -c "reaper: requeued" "sweep-server-$lease.log" || true)
  echo "reaper requeues: $reaped"
done

echo ""
echo "sweep complete"