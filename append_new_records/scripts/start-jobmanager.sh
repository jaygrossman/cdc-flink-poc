#!/bin/bash
set -e

echo "=== Starting Flink JobManager ==="

# Start the JobManager in the background using the standard entrypoint
/docker-entrypoint.sh jobmanager &
FLINK_PID=$!

echo "Waiting for JobManager REST API to be available..."
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/overview 2>/dev/null | grep -q "200"; do
    sleep 2
done
echo "JobManager REST API is ready."

echo "Waiting 20s for TaskManager(s) to register..."
sleep 20

# Verify at least one TaskManager is registered
TM_COUNT=$(curl -s http://localhost:8081/taskmanagers | grep -o '"id"' | wc -l)
echo "Registered TaskManagers: $TM_COUNT"

if [ "$TM_COUNT" -lt 1 ]; then
    echo "WARNING: No TaskManagers registered yet. Waiting 15 more seconds..."
    sleep 15
fi

echo ""
echo "Submitting Flink SQL jobs..."
/opt/flink/bin/sql-client.sh -f /opt/flink/usrlib/sql/submit-jobs.sql
SQL_EXIT=$?

if [ $SQL_EXIT -eq 0 ]; then
    echo "Flink SQL jobs submitted successfully!"
else
    echo "ERROR: Flink SQL job submission failed with exit code $SQL_EXIT"
fi

echo ""
echo "Flink JobManager is running. Waiting for process..."
wait $FLINK_PID
