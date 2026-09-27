#!/bin/bash
# Runs the same load repeatedly against ONE long-lived server without
# clearing the WAL, so the store grows. If Lease's full-store scan is
# the bottleneck, throughput should fall as the job count climbs.

set -e
cd "$(dirname "$0")"

ROUNDS=6
JOBS=1000
LEASE=10s

cleanup() {
  pkill -9 -f "bin/server" 2>/dev/null || true
  pkill -9 -f "bin/worker" 2>/dev/null || true
  sleep 1
}
trap cleanup EXIT

cleanup
rm -f dispatchd.wal growth-server.log

./bin/server -lease "$LEASE" > growth-server.log 2>&1 &
sleep 1
./bin/worker -id worker-a > /dev/null 2>&1 &
./bin/worker -id worker-b > /dev/null 2>&1 &
sleep 1

for round in $(seq 1 $ROUNDS); do
  echo ""
  echo "### round $round  (store holds ~$(( (round-1) * JOBS )) jobs before this run)"
  ./bin/loadtest -jobs "$JOBS" 2>/dev/null | grep -E "jobs:|total attempts:|drain time:|p99:"
  echo "reaper requeues so far: $(grep -c 'reaper: requeued' growth-server.log || true)"
done

echo ""
echo "growth test complete"