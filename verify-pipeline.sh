#!/bin/bash
###############################################################################
# verify-pipeline.sh
# Runs end-to-end diagnostics on the CDC pipeline.
# Usage: chmod +x verify-pipeline.sh && ./verify-pipeline.sh
# Run from the cdc-flink-poc/ directory (where docker-compose.yml lives).
###############################################################################

set -o pipefail

PASS=0
FAIL=0
WARN=0

GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

pass() { ((PASS++)); echo -e "  ${GREEN}✔ PASS${NC} $1"; }
fail() { ((FAIL++)); echo -e "  ${RED}✘ FAIL${NC} $1"; }
warn() { ((WARN++)); echo -e "  ${YELLOW}⚠ WARN${NC} $1"; }
info() { echo -e "  ${CYAN}ℹ${NC} $1"; }
header() { echo ""; echo -e "${BOLD}[$1] $2${NC}"; }

###############################################################################
header "STEP 0" "Docker Compose services"
###############################################################################

SERVICES_JSON=$(docker compose ps --format json 2>/dev/null)
if [ $? -ne 0 ] || [ -z "$SERVICES_JSON" ]; then
    fail "docker compose ps failed — are you in the project directory?"
    echo ""
    echo -e "${RED}Cannot continue. Exiting.${NC}"
    exit 1
fi

EXPECTED_RUNNING=("postgres" "zookeeper" "kafka" "kafka-connect" "flink-jobmanager" "flink-taskmanager" "pgadmin")

for svc in "${EXPECTED_RUNNING[@]}"; do
    state=$(echo "$SERVICES_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data:
    if s['Service'] == '$svc':
        print(s['State'])
        break
" 2>/dev/null)
    if [ -z "$state" ]; then
        fail "$svc — service not found"
    elif [[ "$state" == *"running"* ]]; then
        pass "$svc is running"
    else
        fail "$svc state: $state"
    fi
done

# Setup container should have exited 0
setup_info=$(echo "$SERVICES_JSON" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for s in data:
    if s['Service'] == 'setup':
        print(s['State'], s.get('ExitCode', ''))
        break
" 2>/dev/null)
if [ -n "$setup_info" ]; then
    setup_state=$(echo "$setup_info" | awk '{print $1}')
    setup_exit=$(echo "$setup_info" | awk '{print $2}')
    if [[ "$setup_state" == *"exited"* ]] && [[ "$setup_exit" == "0" ]]; then
        pass "setup exited cleanly (code 0)"
    else
        fail "setup state: $setup_state, exit code: $setup_exit"
        info "Run: docker compose logs setup"
    fi
else
    warn "setup service not found (may be named differently)"
fi

###############################################################################
header "STEP 1" "PostgreSQL source — seed data"
###############################################################################

SEED_COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
    "SELECT count(*) FROM policy;" 2>/dev/null | tr -d '[:space:]')

if [ -z "$SEED_COUNT" ] || [ "$SEED_COUNT" = "" ]; then
    fail "Could not query policy table"
    info "Run: docker compose logs postgres"
elif [ "$SEED_COUNT" -ge 3 ]; then
    pass "policy table has $SEED_COUNT seed records"
else
    warn "policy table has $SEED_COUNT records (expected >= 3)"
fi

###############################################################################
header "STEP 2" "Debezium connector status"
###############################################################################

CONNECTOR_STATUS=$(curl -s http://localhost:8083/connectors/policy-connector/status 2>/dev/null)

if [ -z "$CONNECTOR_STATUS" ]; then
    fail "Kafka Connect API not reachable at localhost:8083"
    info "Run: docker compose logs kafka-connect"
else
    CONN_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
    TASK_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; tasks=json.load(sys.stdin)['tasks']; print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null)

    if [ "$CONN_STATE" = "RUNNING" ]; then
        pass "Connector state: RUNNING"
    else
        fail "Connector state: ${CONN_STATE:-UNKNOWN}"
        info "Run: curl -s http://localhost:8083/connectors/policy-connector/status | python3 -m json.tool"
    fi

    if [ "$TASK_STATE" = "RUNNING" ]; then
        pass "Task state: RUNNING"
    else
        fail "Task state: ${TASK_STATE:-UNKNOWN}"
        TASK_TRACE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; tasks=json.load(sys.stdin)['tasks']; print(tasks[0].get('trace','')[:200] if tasks else '')" 2>/dev/null)
        if [ -n "$TASK_TRACE" ]; then
            info "Error trace: $TASK_TRACE"
        fi
    fi
fi

###############################################################################
header "STEP 3" "Kafka topic — CDC events"
###############################################################################

TOPIC_EXISTS=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep "cdc.public.policy")

if [ -z "$TOPIC_EXISTS" ]; then
    fail "Topic cdc.public.policy does not exist"
    info "Debezium may not have captured any changes yet."
    info "Available topics:"
    docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | sed 's/^/       /'
else
    pass "Topic cdc.public.policy exists"

    MSG_COUNT=$(docker compose exec -T kafka kafka-run-class kafka.tools.GetOffsetShell \
        --broker-list localhost:9092 \
        --topic cdc.public.policy \
        --time -1 2>/dev/null | awk -F: '{sum+=$3} END {print sum}')

    if [ -n "$MSG_COUNT" ] && [ "$MSG_COUNT" -gt 0 ]; then
        pass "Topic has $MSG_COUNT message(s)"
    else
        warn "Topic exists but appears empty"
        info "Debezium snapshot may not have completed yet. Wait 30s and retry."
    fi

    # Peek at one message to verify shape
    SAMPLE=$(docker compose exec -T kafka kafka-console-consumer \
        --bootstrap-server localhost:9092 \
        --topic cdc.public.policy \
        --from-beginning \
        --max-messages 1 \
        --timeout-ms 5000 2>/dev/null)

    if echo "$SAMPLE" | python3 -c "import sys,json; d=json.load(sys.stdin); assert 'after' in d" 2>/dev/null; then
        pass "Message has expected Debezium envelope (after field present)"
    elif [ -n "$SAMPLE" ]; then
        warn "Message received but unexpected shape"
        info "First 200 chars: ${SAMPLE:0:200}"
    fi
fi

###############################################################################
header "STEP 4" "Flink — job status"
###############################################################################

FLINK_OVERVIEW=$(curl -s http://localhost:8081/overview 2>/dev/null)

if [ -z "$FLINK_OVERVIEW" ]; then
    fail "Flink JobManager API not reachable at localhost:8081"
    info "Run: docker compose logs flink-jobmanager"
else
    TASKMANAGERS=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" 2>/dev/null)
    if [ "$TASKMANAGERS" -ge 1 ] 2>/dev/null; then
        pass "Flink has $TASKMANAGERS TaskManager(s) registered"
    else
        fail "No TaskManagers registered"
        info "Run: docker compose logs flink-taskmanager"
    fi

    JOBS_RUNNING=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobs-running',0))" 2>/dev/null)
    JOBS_FAILED=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobs-failed',0))" 2>/dev/null)

    if [ "$JOBS_RUNNING" -ge 1 ] 2>/dev/null; then
        pass "Flink has $JOBS_RUNNING running job(s)"
    else
        fail "No running Flink jobs (running=$JOBS_RUNNING, failed=$JOBS_FAILED)"
        info "Check SQL submission: docker compose logs flink-jobmanager | grep -i 'error\|exception'"
    fi

    if [ "$JOBS_FAILED" -gt 0 ] 2>/dev/null; then
        warn "$JOBS_FAILED failed Flink job(s) detected"
        # Get failed job details
        JOBS_LIST=$(curl -s http://localhost:8081/jobs/overview 2>/dev/null)
        FAILED_IDS=$(echo "$JOBS_LIST" | python3 -c "
import sys, json
data = json.load(sys.stdin)
for j in data.get('jobs', []):
    if j.get('state') == 'FAILED':
        print(j['jid'])
" 2>/dev/null)
        for jid in $FAILED_IDS; do
            EXCEPTIONS=$(curl -s "http://localhost:8081/jobs/$jid/exceptions" 2>/dev/null)
            ROOT=$(echo "$EXCEPTIONS" | python3 -c "
import sys, json
data = json.load(sys.stdin)
exc = data.get('root-exception', '')
print(exc[:300])
" 2>/dev/null)
            if [ -n "$ROOT" ]; then
                info "Job $jid exception: $ROOT"
            fi
        done
    fi
fi

###############################################################################
header "STEP 5" "Output tables — flattened data"
###############################################################################

OUTPUT_TABLES=("output_policy" "output_coverage" "output_vehicle" "output_driver" "output_claim")

for tbl in "${OUTPUT_TABLES[@]}"; do
    COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM $tbl;" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$COUNT" ]; then
        fail "$tbl — could not query"
    elif [ "$COUNT" -gt 0 ]; then
        pass "$tbl has $COUNT row(s)"
    else
        fail "$tbl is empty"
    fi
done

###############################################################################
header "STEP 6" "Live CDC test — insert and verify"
###############################################################################

LIVE_TAG="POL-VERIFY-$(date +%s)"

info "Inserting test record: $LIVE_TAG"

docker compose exec -T postgres psql -U cdc_user -d cdc_db -c "
INSERT INTO policy (data) VALUES ('{
  \"policy_number\": \"$LIVE_TAG\",
  \"status\": \"active\",
  \"effective_date\": \"2024-06-01\",
  \"expiration_date\": \"2025-06-01\",
  \"policyholder\": {
    \"first_name\": \"Verify\",
    \"last_name\": \"Script\",
    \"date_of_birth\": \"1990-01-01\",
    \"contact\": {
      \"email\": \"verify@test.com\",
      \"phone\": \"+1-555-0000\",
      \"address\": {
        \"street\": \"1 Verify Lane\",
        \"city\": \"Testburg\",
        \"state\": \"NY\",
        \"zip\": \"10001\"
      }
    }
  },
  \"coverages\": [
    {\"type\": \"liability\", \"limit\": 100000, \"deductible\": 500, \"premium\": 600.00}
  ],
  \"vehicles\": [
    {
      \"vin\": \"VERIFY12345678901\",
      \"year\": 2024,
      \"make\": \"Toyota\",
      \"model\": \"Camry\",
      \"drivers\": [
        {\"name\": \"Verify Script\", \"license_number\": \"V000-0000-0001\", \"is_primary\": true}
      ]
    }
  ],
  \"claims_history\": []
}'::jsonb);
" > /dev/null 2>&1

info "Waiting for pipeline to process (polling up to 60s)..."

FOUND=false
for i in $(seq 1 12); do
    sleep 5
    LIVE_COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM output_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')

    if [ "$LIVE_COUNT" = "1" ]; then
        FOUND=true
        pass "Live CDC test passed — record appeared in output_policy after $((i * 5))s"

        # Check downstream tables too
        COV=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_coverage oc JOIN output_policy op ON oc.policy_id = op.policy_id WHERE op.policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
        VEH=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_vehicle ov JOIN output_policy op ON ov.policy_id = op.policy_id WHERE op.policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
        DRV=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_driver od JOIN output_policy op ON od.policy_id = op.policy_id WHERE op.policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')

        [ "${COV:-0}" -ge 1 ] && pass "output_coverage: $COV row(s) for live test" || warn "output_coverage: 0 rows for live test"
        [ "${VEH:-0}" -ge 1 ] && pass "output_vehicle: $VEH row(s) for live test" || warn "output_vehicle: 0 rows for live test"
        [ "${DRV:-0}" -ge 1 ] && pass "output_driver: $DRV row(s) for live test" || warn "output_driver: 0 rows for live test"
        break
    fi
done

if [ "$FOUND" = false ]; then
    fail "Live CDC test — record did not appear in output_policy within 60s"
    info "Debug: check each step above to find where the pipeline stalled"
fi

###############################################################################
header "STEP 7" "PGAdmin"
###############################################################################

PGADMIN_HTTP=$(curl -s -o /dev/null -w "%{http_code}" http://localhost:5050/login 2>/dev/null)

if [ "$PGADMIN_HTTP" = "200" ]; then
    pass "PGAdmin is reachable at http://localhost:5050"
else
    fail "PGAdmin not reachable (HTTP $PGADMIN_HTTP)"
fi

###############################################################################
# Summary
###############################################################################

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "${BOLD}RESULTS${NC}"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo -e "  ${GREEN}✔ Passed: $PASS${NC}"
echo -e "  ${YELLOW}⚠ Warnings: $WARN${NC}"
echo -e "  ${RED}✘ Failed: $FAIL${NC}"
echo ""

if [ "$FAIL" -eq 0 ]; then
    echo -e "${GREEN}${BOLD}Pipeline is fully operational.${NC}"
    exit 0
elif [ "$FAIL" -le 2 ]; then
    echo -e "${YELLOW}${BOLD}Pipeline is partially working. Check failures above.${NC}"
    exit 1
else
    echo -e "${RED}${BOLD}Pipeline has significant issues. Debug from Step 0 downward.${NC}"
    exit 2
fi
