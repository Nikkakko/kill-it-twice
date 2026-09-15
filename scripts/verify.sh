#!/usr/bin/env bash
set -euo pipefail

API_URL="${API_URL:-http://localhost:3000}"
WORKER_SERVICE="${WORKER_SERVICE:-worker}"
WAIT_SECONDS="${WAIT_SECONDS:-90}"
failures=0

say() { printf '%s\n' "$1"; }
pass() { say "$1 PASS${2:+ ($2)}"; }
fail() { say "$1 FAIL${2:+ ($2)}"; failures=$((failures + 1)); }
wait_for_api() {
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    if curl -fsS "$API_URL/api/health" >/dev/null 2>&1; then return 0; fi
    sleep 1
  done
  return 1
}
status_json() { curl -fsS "$API_URL/api/status"; }
json_number() {
  node -e 'const x=JSON.parse(process.argv[1]); const path=process.argv[2].split(".").filter(Boolean); let value=x; for (const key of path) value=value?.[key]; console.log(Number(value ?? 0));' "$1" "$2"
}
wait_until() {
  local field="$1" expected="$2"
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    local value
    value=$(json_number "$(status_json)" "$field")
    if [ "$value" -ge "$expected" ]; then return 0; fi
    sleep 1
  done
  return 1
}
wait_until_mid_backfill() {
  local expected="$1"
  for _ in $(seq 1 "$WAIT_SECONDS"); do
    local checkpoint
    checkpoint=$(json_number "$(status_json)" '.checkpoints.search')
    if [ "$checkpoint" -gt 0 ] && [ "$checkpoint" -lt "$expected" ]; then return 0; fi
    sleep 1
  done
  return 1
}

say "Kill It Twice integration verification"
if ! wait_for_api; then fail "Prerequisites" "API did not become ready"; exit 1; fi
curl -fsS -X POST "$API_URL/api/pipeline/pause" >/dev/null
curl -sS -o /dev/null -X DELETE http://localhost:9200/replicated-records || true
docker compose exec -T rabbitmq rabbitmqctl purge_queue replication.consumer >/dev/null 2>&1 || true
curl -fsS -X POST "$API_URL/api/simulation/seed" -H 'content-type: application/json' -d '{"count":5000,"reset":true}' >/dev/null

expected=$(json_number "$(status_json)" '.sourceCount')
curl -fsS -X POST "$API_URL/api/pipeline/resume" >/dev/null
if ! wait_until_mid_backfill "$expected"; then fail "G1 resume after kill" "backfill completed before a mid-run kill"; fi
docker compose kill "$WORKER_SERVICE" >/dev/null
docker compose up -d "$WORKER_SERVICE" >/dev/null
if wait_until '.checkpoints.search' "$expected"; then pass "G1 resume after kill" "checkpoint reached $expected"; else fail "G1 resume after kill" "checkpoint did not recover"; fi

if wait_until '.consumedEventCount' "$expected"; then
  search_count=$(curl -fsS http://localhost:9200/replicated-records/_count | node -e 'let b="";process.stdin.on("data",d=>b+=d).on("end",()=>console.log(Number(JSON.parse(b).count)))')
  consumed_count=$(json_number "$(status_json)" '.consumedEventCount')
  if [ "$search_count" -eq "$expected" ] && [ "$consumed_count" -eq "$expected" ]; then
    pass "G2 no duplicates" "source=$expected, search=$search_count, consumer_unique=$consumed_count"
  else
    fail "G2 no duplicates" "source=$expected, search=$search_count, consumer_unique=$consumed_count"
  fi
else fail "G2 no duplicates" "independent consumer did not receive all events"; fi

curl -fsS -X POST "$API_URL/api/simulation/sink/search/down" >/dev/null
curl -fsS -X POST "$API_URL/api/simulation/change" -H 'content-type: application/json' -d '{}' >/dev/null
sleep 3
stalled=$(json_number "$(status_json)" '.pending.search')
curl -fsS -X POST "$API_URL/api/simulation/sink/search/up" >/dev/null
if wait_until '.pending.search' 0; then pass "G3 sink outage" "pending=$stalled, recovered"; else fail "G3 sink outage" "sink did not recover"; fi

curl -fsS -X POST "$API_URL/api/simulation/partial" -H 'content-type: application/json' -d '{"count":500}' >/dev/null
if wait_until '.dlqCount' 3; then
  actual_dlq=$(json_number "$(status_json)" '.dlqCount')
  if [ "$actual_dlq" -eq 3 ]; then pass "G4 partial batch failure" "497 written, 3 in DLQ"; else fail "G4 partial batch failure" "expected 3 DLQ records, got $actual_dlq"; fi
else fail "G4 partial batch failure" "DLQ did not receive invalid records"; fi

if curl -fsS "$API_URL/api/status" | node -e "let b='';process.stdin.on('data',d=>b+=d).on('end',()=>{const x=JSON.parse(b); if(!x.health||x.incrementalLag===undefined||x.throughputPerSecond===undefined||x.dlqCount===undefined) process.exit(1)})"; then pass "G5 observability" "status API exposes health, throughput, lag, and DLQ"; else fail "G5 observability" "status API incomplete"; fi

if [ "$failures" -gt 0 ]; then say "$failures gate(s) failed"; exit 1; fi
say "All verification gates passed"
