#!/bin/bash
# Compares 1 vs 3 server replicas at 10k jobs, each starting from a clean
# database and hitting a replica directly, so neither store size nor the
# proxy is part of the measurement.

set -e
cd "$(dirname "$0")"

run() {
  local label="$1"; shift

  echo ""
  echo "############################################"
  echo "### $label"
  echo "############################################"

  docker compose down -v >/dev/null 2>&1 || true
  docker compose up -d "$@" >/dev/null 2>&1
  sleep 12

  ./bin/loadtest -server http://localhost:8081 -jobs 10000 \
    | grep -E "jobs:|total attempts:|submit time:|sustained:|p99:"
}

run "1 server replica, 6 workers"  server1 worker
run "3 server replicas, 6 workers" server1 server2 server3 worker

docker compose down -v >/dev/null 2>&1 || true
echo ""
echo "comparison complete"