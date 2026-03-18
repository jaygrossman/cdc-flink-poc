#!/usr/bin/env python3
"""
Event-driven dbt trigger service.

Connects to PostgreSQL, LISTENs on the 'stg_data_arrived' channel,
and runs `dbt run` when new staging data arrives. Debounces notifications
to batch nearby events (e.g., Flink writes to all 5 stg_* tables within
a few seconds of each other).
"""

import os
import sys
import time
import select
import subprocess
import logging

import psycopg2
import psycopg2.extensions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("event-trigger")

PGHOST = os.environ.get("PGHOST", "postgres")
PGPORT = os.environ.get("PGPORT", "5432")
PGUSER = os.environ.get("PGUSER", "cdc_user")
PGPASSWORD = os.environ.get("PGPASSWORD", "cdc_pass")
PGDATABASE = os.environ.get("PGDATABASE", "cdc_db")
DEBOUNCE_SECONDS = float(os.environ.get("DEBOUNCE_SECONDS", "3"))
INITIAL_WAIT_SECONDS = int(os.environ.get("INITIAL_WAIT_SECONDS", "60"))
DBT_PROJECT_DIR = os.environ.get("DBT_PROJECT_DIR", "/usr/app/dbt_project")


def wait_for_postgres():
    log.info("Waiting for PostgreSQL at %s:%s ...", PGHOST, PGPORT)
    while True:
        try:
            conn = psycopg2.connect(
                host=PGHOST, port=PGPORT,
                user=PGUSER, password=PGPASSWORD,
                dbname=PGDATABASE,
            )
            conn.close()
            log.info("PostgreSQL is ready.")
            return
        except psycopg2.OperationalError:
            time.sleep(2)


def run_dbt():
    log.info("Running dbt run ...")
    start = time.time()
    result = subprocess.run(
        ["dbt", "run", "--profiles-dir", DBT_PROJECT_DIR],
        cwd=DBT_PROJECT_DIR,
        capture_output=True,
        text=True,
    )
    elapsed = time.time() - start
    if result.returncode == 0:
        log.info("dbt run completed successfully in %.1fs", elapsed)
    else:
        log.error("dbt run FAILED (exit %d) in %.1fs", result.returncode, elapsed)
        log.error("stdout: %s", result.stdout[-2000:] if result.stdout else "")
        log.error("stderr: %s", result.stderr[-2000:] if result.stderr else "")


def listen_loop():
    conn = psycopg2.connect(
        host=PGHOST, port=PGPORT,
        user=PGUSER, password=PGPASSWORD,
        dbname=PGDATABASE,
    )
    conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)

    cur = conn.cursor()
    cur.execute("LISTEN stg_data_arrived;")
    log.info("Listening on channel 'stg_data_arrived' ...")

    while True:
        # Block until a notification arrives (60s timeout as heartbeat)
        if select.select([conn], [], [], 60) == ([], [], []):
            continue

        conn.poll()
        if not conn.notifies:
            continue

        # Drain first batch of notifications
        tables_seen = set()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            tables_seen.add(notify.payload)

        log.info(
            "Notification(s) received from: %s. Debouncing %.1fs ...",
            ", ".join(sorted(tables_seen)),
            DEBOUNCE_SECONDS,
        )

        # Debounce: wait a short period to batch nearby writes
        debounce_deadline = time.time() + DEBOUNCE_SECONDS
        while time.time() < debounce_deadline:
            remaining = debounce_deadline - time.time()
            if remaining <= 0:
                break
            if select.select([conn], [], [], remaining) != ([], [], []):
                conn.poll()
                while conn.notifies:
                    notify = conn.notifies.pop(0)
                    tables_seen.add(notify.payload)

        log.info(
            "Debounce complete. Tables with new data: %s",
            ", ".join(sorted(tables_seen)),
        )

        run_dbt()


def main():
    wait_for_postgres()

    log.info(
        "Waiting %ds for Flink to populate staging tables...",
        INITIAL_WAIT_SECONDS,
    )
    time.sleep(INITIAL_WAIT_SECONDS)

    log.info("Running initial dbt run...")
    run_dbt()

    log.info("Entering event-driven listen loop (debounce=%.1fs)", DEBOUNCE_SECONDS)
    while True:
        try:
            listen_loop()
        except psycopg2.OperationalError as e:
            log.warning("PostgreSQL connection lost: %s. Reconnecting in 5s...", e)
            time.sleep(5)
        except Exception as e:
            log.exception("Unexpected error in listen loop: %s", e)
            time.sleep(5)


if __name__ == "__main__":
    main()
