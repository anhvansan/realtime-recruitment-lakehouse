#!/usr/bin/env bash
# Lightweight dbt scheduler replacing Airflow for local 16GB RAM discipline (Task 4.6).
# Airflow remains optional in Task 7.1. Uses flock to prevent overlapping runs and
# writes s3://<bucket>/gold/_last_run.json after each cycle.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_ROOT}"

INTERVAL_SEC="${INTERVAL_SEC:-60}"
RUN_ONCE=0
DBT_LOCK_FILE="${DBT_LOCK_FILE:-/tmp/dbt_cpc_loop.lock}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --once)
            RUN_ONCE=1
            shift
            ;;
        --interval)
            INTERVAL_SEC="${2:?Missing value for --interval}"
            shift 2
            ;;
        *)
            echo "Unknown argument: $1" >&2
            exit 2
            ;;
    esac
done

# Safely load .env into environment without echoing secrets (Rule R6)
if [[ -f ".env" ]]; then
    while IFS= read -r line || [[ -n "$line" ]]; do
        line="${line%$'\r'}"
        [[ -z "$line" || "$line" =~ ^[[:space:]]*# ]] && continue
        if [[ "$line" == *=* ]]; then
            key="${line%%=*}"
            val="${line#*=}"
            key="${key//[[:space:]]/}"
            val="${val#\"}"
            val="${val%\"}"
            val="${val#\'}"
            val="${val%\'}"
            export "${key}=${val}"
        fi
    done < ".env"
fi

resolve_bin() {
    for candidate in "$@"; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            echo "${candidate}"
            return 0
        fi
    done
    return 1
}

resolve_python_with_boto3() {
    for candidate in "$@"; do
        if command -v "${candidate}" >/dev/null 2>&1; then
            if "${candidate}" -c "import boto3" >/dev/null 2>&1; then
                echo "${candidate}"
                return 0
            fi
        fi
    done
    resolve_bin "$@" || true
}

DBT_CMD="${DBT_BIN:-$(resolve_bin dbt dbt.exe || true)}"
if [[ -n "${PYTHON_BIN:-}" ]]; then
    PYTHON_CMD="${PYTHON_BIN}"
elif [[ "${DBT_CMD}" == *.exe ]]; then
    PYTHON_CMD="$(resolve_python_with_boto3 python.exe python3 python)"
else
    PYTHON_CMD="$(resolve_python_with_boto3 python3 python python.exe)"
fi

if [[ -z "${DBT_CMD}" ]]; then
    echo "[dbt-loop] ERROR: dbt executable not found (set DBT_BIN)." >&2
    exit 1
fi
if [[ -z "${PYTHON_CMD}" ]]; then
    echo "[dbt-loop] ERROR: python executable not found (set PYTHON_BIN)." >&2
    exit 1
fi

# Bridge env vars across WSL2 -> Windows .exe boundary when using dbt.exe / python.exe
if [[ "${DBT_CMD}" == *.exe || "${PYTHON_CMD}" == *.exe ]]; then
    _S3_WSLENV="S3_ACCESS_KEY:S3_SECRET_KEY:S3_ENDPOINT:S3_ENDPOINT_EXTERNAL:S3_ENDPOINT_HOST_PORT:S3_BUCKET:S3_REGION"
    export WSLENV="${WSLENV:+${WSLENV}:}${_S3_WSLENV}"
fi

run_one_cycle() {
    (
        if ! flock -n 200; then
            echo "[dbt-loop] $(date -u +%Y-%m-%dT%H:%M:%SZ) Lock busy (${DBT_LOCK_FILE}) — skipping overlapping run."
            return 0
        fi

        local start_epoch end_epoch duration_sec status models_built rc
        start_epoch=$("${PYTHON_CMD}" -c "import time; print(f'{time.monotonic():.3f}')")
        rc=0
        "${DBT_CMD}" build --project-dir dbt_cpc --profiles-dir dbt_cpc || rc=$?
        end_epoch=$("${PYTHON_CMD}" -c "import time; print(f'{time.monotonic():.3f}')")
        duration_sec=$("${PYTHON_CMD}" -c "print(f'{max(0.0, float(${end_epoch}) - float(${start_epoch})):.3f}')")

        if [[ ${rc} -eq 0 ]]; then
            status="success"
            models_built=3
        else
            status="failed"
            models_built=0
        fi

        echo "[dbt-loop] $(date -u +%Y-%m-%dT%H:%M:%SZ) status=${status} duration_sec=${duration_sec}"
        "${PYTHON_CMD}" scripts/write_gold_last_run.py \
            --status "${status}" \
            --duration-sec "${duration_sec}" \
            --models-built "${models_built}"
        return "${rc}"
    ) 200>"${DBT_LOCK_FILE}"
}

if [[ "${RUN_ONCE}" -eq 1 ]]; then
    run_one_cycle
    exit $?
fi

echo "[dbt-loop] Starting periodic dbt build every ${INTERVAL_SEC}s (lock=${DBT_LOCK_FILE})..."
while true; do
    run_one_cycle || echo "[dbt-loop] Cycle finished with non-zero status; retrying next interval." >&2
    sleep "${INTERVAL_SEC}"
done
