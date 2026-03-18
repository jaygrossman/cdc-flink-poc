#!/bin/bash
###############################################################################
# verify-pipeline.sh (dbt_event_trigger variant)
# Runs end-to-end diagnostics on the CDC pipeline with event-driven dbt.
# Usage: chmod +x verify-pipeline.sh && ./verify-pipeline.sh
# Run from the dbt_event_trigger/ directory (where docker-compose.yml lives).
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

EXPECTED_RUNNING=("postgres" "zookeeper" "kafka" "kafka-connect" "flink-jobmanager" "flink-taskmanager" "pgadmin" "dbt")

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
else
    CONN_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null)
    TASK_STATE=$(echo "$CONNECTOR_STATUS" | python3 -c "import sys,json; tasks=json.load(sys.stdin)['tasks']; print(tasks[0]['state'] if tasks else 'NO_TASKS')" 2>/dev/null)

    if [ "$CONN_STATE" = "RUNNING" ]; then
        pass "Connector state: RUNNING"
    else
        fail "Connector state: ${CONN_STATE:-UNKNOWN}"
    fi

    if [ "$TASK_STATE" = "RUNNING" ]; then
        pass "Task state: RUNNING"
    else
        fail "Task state: ${TASK_STATE:-UNKNOWN}"
    fi
fi

###############################################################################
header "STEP 3" "Kafka topic — CDC events"
###############################################################################

TOPIC_EXISTS=$(docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list 2>/dev/null | grep "cdc.public.policy")

if [ -z "$TOPIC_EXISTS" ]; then
    fail "Topic cdc.public.policy does not exist"
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
    fi
fi

###############################################################################
header "STEP 4" "Flink — job status"
###############################################################################

FLINK_OVERVIEW=$(curl -s http://localhost:8081/overview 2>/dev/null)

if [ -z "$FLINK_OVERVIEW" ]; then
    fail "Flink JobManager API not reachable at localhost:8081"
else
    TASKMANAGERS=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('taskmanagers',0))" 2>/dev/null)
    if [ "$TASKMANAGERS" -ge 1 ] 2>/dev/null; then
        pass "Flink has $TASKMANAGERS TaskManager(s) registered"
    else
        fail "No TaskManagers registered"
    fi

    JOBS_RUNNING=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobs-running',0))" 2>/dev/null)
    JOBS_FAILED=$(echo "$FLINK_OVERVIEW" | python3 -c "import sys,json; print(json.load(sys.stdin).get('jobs-failed',0))" 2>/dev/null)

    if [ "$JOBS_RUNNING" -ge 1 ] 2>/dev/null; then
        pass "Flink has $JOBS_RUNNING running job(s)"
    else
        fail "No running Flink jobs (running=$JOBS_RUNNING, failed=$JOBS_FAILED)"
    fi
fi

###############################################################################
header "STEP 5" "Staging tables — Flink output"
###############################################################################

STG_TABLES=("stg_policy" "stg_coverage" "stg_vehicle" "stg_driver" "stg_claim")

for tbl in "${STG_TABLES[@]}"; do
    COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM $tbl;" 2>/dev/null | tr -d '[:space:]')

    if [ -z "$COUNT" ]; then
        fail "$tbl — could not query"
    elif [ "$COUNT" -gt 0 ]; then
        pass "$tbl has $COUNT row(s)"
    else
        fail "$tbl is empty (Flink not writing to staging)"
    fi
done

OP_CHECK=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
    "SELECT DISTINCT op FROM stg_policy ORDER BY op;" 2>/dev/null | tr -d '[:space:]')
if [ -n "$OP_CHECK" ]; then
    pass "stg_policy has op values: $OP_CHECK"
else
    warn "Could not verify op column in stg_policy"
fi

###############################################################################
header "STEP 6" "dbt — event-trigger status"
###############################################################################

DBT_LOG=$(docker compose logs dbt 2>/dev/null | tail -80)
if echo "$DBT_LOG" | grep -q "Completed successfully"; then
    pass "dbt has completed at least one successful run"
else
    warn "dbt may not have completed a run yet (check: docker compose logs dbt)"
fi

if echo "$DBT_LOG" | grep -q "Listening on channel"; then
    pass "Event trigger is listening on 'stg_data_arrived' channel"
else
    warn "Event trigger may not be active yet"
fi

if echo "$DBT_LOG" | grep -q "Notification(s) received"; then
    pass "Event trigger has received at least one notification"
else
    warn "No notifications received yet (data may not have arrived)"
fi

###############################################################################
header "STEP 7" "Output tables — dbt merged data"
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
        fail "$tbl is empty (dbt may not have run yet)"
    fi
done

###############################################################################
header "STEP 8" "Live CDC test — insert and verify via event-triggered dbt"
###############################################################################

LIVE_TAG="POL-VERIFY-$(date +%s)"

info "Inserting test record: $LIVE_TAG"
INSERT_START=$(python3 -c "import time; print(int(time.time()*1000))")

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

# First check staging (should appear in ~15s)
info "Waiting for record in staging tables (up to 30s)..."
STG_FOUND=false
for i in $(seq 1 6); do
    sleep 5
    STG_COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM stg_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
    if [ "$STG_COUNT" = "1" ]; then
        STG_FOUND=true
        STG_ARRIVED=$(python3 -c "import time; print(int(time.time()*1000))")
        STG_ELAPSED=$(( STG_ARRIVED - INSERT_START ))
        pass "Record appeared in stg_policy after ${STG_ELAPSED}ms (source -> staging)"
        break
    fi
done
if [ "$STG_FOUND" = false ]; then
    fail "Record did not appear in stg_policy within 30s"
fi

# Wait for event-triggered dbt run to auto-merge (NOTIFY + debounce + dbt run)
info "Waiting for event-triggered dbt to auto-merge (up to 30s)..."
FOUND=false
for i in $(seq 1 6); do
    sleep 5
    LIVE_COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM output_policy WHERE policy_number = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')
    if [ "$LIVE_COUNT" = "1" ]; then
        FOUND=true
        OUTPUT_ARRIVED=$(python3 -c "import time; print(int(time.time()*1000))")
        E2E_ELAPSED=$(( OUTPUT_ARRIVED - INSERT_START ))
        DBT_ELAPSED=$(( OUTPUT_ARRIVED - STG_ARRIVED ))
        pass "Live CDC test passed — record auto-merged into output_policy (event-triggered)"
        info "Timing breakdown:"
        info "  Source -> Staging (Debezium+Kafka+Flink): ${STG_ELAPSED}ms"
        info "  Staging -> Output (NOTIFY+debounce+dbt):  ${DBT_ELAPSED}ms"
        info "  End-to-end total:                         ${E2E_ELAPSED}ms"

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
    fail "Live CDC test — record did not auto-merge into output_policy within 30s"
    info "Check event trigger logs: docker compose logs dbt --tail 30"
fi

###############################################################################
header "STEP 8b" "Live CDC update test — add vehicle + coverage to existing policy"
###############################################################################

if [ "$FOUND" = true ]; then
    LIVE_POLICY_ID=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT id FROM policy WHERE data->>'policy_number' = '$LIVE_TAG';" 2>/dev/null | tr -d '[:space:]')

    COV_BEFORE=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
    VEH_BEFORE=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
        "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')

    info "Policy $LIVE_TAG (id=$LIVE_POLICY_ID) has $COV_BEFORE coverage(s) and $VEH_BEFORE vehicle(s)"
    info "Updating: adding collision coverage + second vehicle with driver..."

    UPDATE_START=$(python3 -c "import time; print(int(time.time()*1000))")

    docker compose exec -T postgres psql -U cdc_user -d cdc_db -c "
    UPDATE policy SET data = data
        || '{\"coverages\": [
            {\"type\": \"liability\", \"limit\": 100000, \"deductible\": 500, \"premium\": 600.00},
            {\"type\": \"collision\", \"limit\": 50000, \"deductible\": 1000, \"premium\": 475.00}
        ]}'::jsonb
        || '{\"vehicles\": [
            {\"vin\": \"VERIFY12345678901\", \"year\": 2024, \"make\": \"Toyota\", \"model\": \"Camry\",
             \"drivers\": [{\"name\": \"Verify Script\", \"license_number\": \"V000-0000-0001\", \"is_primary\": true}]},
            {\"vin\": \"UPDATE98765432100\", \"year\": 2025, \"make\": \"Subaru\", \"model\": \"Outback\",
             \"drivers\": [{\"name\": \"Verify Partner\", \"license_number\": \"V000-0000-0002\", \"is_primary\": true}]}
        ]}'::jsonb,
        updated_at = NOW()
    WHERE id = $LIVE_POLICY_ID;
    " > /dev/null 2>&1

    # Wait for update to reach staging
    info "Waiting for update in staging tables (up to 30s)..."
    STG_UPDATE_FOUND=false
    for i in $(seq 1 6); do
        sleep 5
        STG_UPDATE_COUNT=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM stg_policy WHERE policy_id = $LIVE_POLICY_ID AND op = 'u';" 2>/dev/null | tr -d '[:space:]')
        if [ "${STG_UPDATE_COUNT:-0}" -ge 1 ]; then
            STG_UPDATE_FOUND=true
            STG_UPDATE_ARRIVED=$(python3 -c "import time; print(int(time.time()*1000))")
            STG_UPDATE_ELAPSED=$(( STG_UPDATE_ARRIVED - UPDATE_START ))
            pass "Update arrived in staging after ${STG_UPDATE_ELAPSED}ms (op='u' event captured)"
            break
        fi
    done
    if [ "$STG_UPDATE_FOUND" = false ]; then
        fail "Update did not appear in staging within 30s"
    fi

    # Wait for event-triggered dbt to auto-merge the update
    info "Waiting for event-triggered dbt to auto-merge update (up to 30s)..."
    UPDATE_MERGED=false
    for i in $(seq 1 6); do
        sleep 5
        COV_AFTER=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_coverage WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        VEH_AFTER=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_vehicle WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')
        if [ "${COV_AFTER:-0}" -ge 2 ] && [ "${VEH_AFTER:-0}" -ge 2 ]; then
            UPDATE_MERGED=true
            UPDATE_DONE=$(python3 -c "import time; print(int(time.time()*1000))")
            UPDATE_E2E=$(( UPDATE_DONE - UPDATE_START ))
            break
        fi
    done

    if [ "$UPDATE_MERGED" = true ]; then
        DRV_AFTER=$(docker compose exec -T postgres psql -U cdc_user -d cdc_db -tAc \
            "SELECT count(*) FROM output_driver WHERE policy_id = $LIVE_POLICY_ID;" 2>/dev/null | tr -d '[:space:]')

        pass "output_coverage: $COV_BEFORE -> $COV_AFTER (collision coverage added)"
        pass "output_vehicle: $VEH_BEFORE -> $VEH_AFTER (Subaru Outback added)"
        [ "${DRV_AFTER:-0}" -ge 2 ] && pass "output_driver: 1 -> $DRV_AFTER (Verify Partner added)" || fail "output_driver: expected >= 2, got $DRV_AFTER"

        if [ "$STG_UPDATE_FOUND" = true ]; then
            DBT_UPDATE_ELAPSED=$(( UPDATE_DONE - STG_UPDATE_ARRIVED ))
            info "Update timing breakdown:"
            info "  Source -> Staging (Debezium+Kafka+Flink): ${STG_UPDATE_ELAPSED}ms"
            info "  Staging -> Output (NOTIFY+debounce+dbt):  ${DBT_UPDATE_ELAPSED}ms"
            info "  End-to-end total:                         ${UPDATE_E2E}ms"
        fi
    else
        fail "Update did not auto-merge into output tables within 30s"
        info "Check event trigger logs: docker compose logs dbt --tail 30"
    fi
else
    info "Skipping update test — insert test did not pass"
fi

###############################################################################
header "STEP 9" "PGAdmin"
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
