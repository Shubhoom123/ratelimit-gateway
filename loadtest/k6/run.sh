#!/usr/bin/env bash
# Runs a k6 script in Docker against the local Compose stack.
#   make up                       # start the stack first
#   bash loadtest/k6/run.sh smoke
#   bash loadtest/k6/run.sh accuracy
#   bash loadtest/k6/run.sh slow_backend
set -euo pipefail
cd "$(dirname "$0")"
name="${1:?usage: run.sh smoke|accuracy|slow_backend}"
[ -f "$name.js" ] || { echo "no such script: $name.js"; exit 1; }
# host.docker.internal reaches the host's published ports from inside Docker Desktop.
docker run --rm -i \
  --add-host=host.docker.internal:host-gateway \
  -e GATEWAYS -e ROUTE -e BACKEND2_CONTROL \
  grafana/k6:latest run - < "$name.js"
