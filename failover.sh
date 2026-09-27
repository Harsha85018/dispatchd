#!/bin/bash
# Kills two of three server replicas mid-run and checks every job still
# completes. Submission goes to server1 throughout, so only the workers'
# path is disrupted — this measures availability, not throughput.

set -e
cd "$(dirname "$0")"

docker compose down -v >/dev/null 2>&1 || true
docker compose up -d >/dev/null 2>&1
sleep 15

echo "submitting 30k jobs; killing server2 and server3 once draining starts"
./bin/loadtest -server http://localhost:8081 -jobs 30000 &
LOAD=$!

sleep 8
echo ""
echo ">>> killing server2 and server3"
docker compose kill server2 server3 >/dev/null 2>&1
echo ">>> two of three replicas are down"
echo ""

wait $LOAD

docker compose down -v >/dev/null 2>&1 || true