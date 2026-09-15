#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:3000}"
COUNT="${COUNT:-20000}"
WAIT_SECONDS="${WAIT_SECONDS:-180}"

json_number() {
  node -e 'const x=JSON.parse(process.argv[1]); const path=process.argv[2].split(".").filter(Boolean); let value=x; for (const key of path) value=value?.[key]; console.log(Number(value ?? 0));' "$1" "$2"
}

wait_until() {
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    local status
    status=$(curl -fsS "$API_URL/api/status")
    if [ "$(json_number "$status" '.checkpoints.search')" -ge "$1" ] && [ "$(json_number "$status" '.checkpoints.events')" -ge "$1" ]; then return 0; fi
    sleep 1
  done
  return 1
}

curl -fsS "$API_URL/api/health" >/dev/null
curl -fsS -X POST "$API_URL/api/pipeline/pause" >/dev/null
docker compose stop worker >/dev/null
curl -sS -o /dev/null -X DELETE http://localhost:9200/replicated-records || true
docker compose exec -T rabbitmq rabbitmqctl purge_queue replication.consumer >/dev/null 2>&1 || true
curl -fsS -X POST "$API_URL/api/simulation/seed" -H 'content-type: application/json' -d "{\"count\":$COUNT,\"reset\":true}" >/dev/null
expected=$(json_number "$(curl -fsS "$API_URL/api/status")" '.sourceCount')
docker compose up -d worker >/dev/null
start_ms=$(node -e 'console.log(Date.now())')
curl -fsS -X POST "$API_URL/api/pipeline/resume" >/dev/null
if ! wait_until "$expected"; then
  echo "Capacity run did not complete within ${WAIT_SECONDS}s" >&2
  exit 1
fi
end_ms=$(node -e 'console.log(Date.now())')
elapsed_ms=$((end_ms - start_ms))
elapsed_seconds=$(node -e 'console.log((Number(process.argv[1]) / 1000).toFixed(2))' "$elapsed_ms")
throughput=$(node -e 'const count=Number(process.argv[1]); const seconds=Math.max(0.001,Number(process.argv[2])/1000); console.log((count/seconds).toFixed(1))' "$expected" "$elapsed_ms")
search_count=$(curl -fsS http://localhost:9200/replicated-records/_count | node -e 'let b="";process.stdin.on("data",d=>b+=d).on("end",()=>console.log(Number(JSON.parse(b).count)))')
echo "Capacity: records=$expected elapsed_seconds=$elapsed_seconds records_per_second=$throughput search_count=$search_count"
