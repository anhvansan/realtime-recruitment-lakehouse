#!/usr/bin/env bash
# ==============================================================================
# Idempotent Redpanda Topic Provisioning Script (Task 2.4)
# Runs inside the running `redpanda` container via `rpk`.
# Safe to execute multiple times (`make topics` x2 passes with zero errors).
# ==============================================================================
set -euo pipefail

RETENTION_MS="${TOPIC_RETENTION_MS:-3600000}"          # 1 hour
SEGMENT_MS="${TOPIC_SEGMENT_MS:-600000}"               # 10 minutes (avoid active segment trap)
RAW_RETENTION_BYTES="${RAW_RETENTION_BYTES:-134217728}" # 128 MiB per partition
DLQ_RETENTION_BYTES="${DLQ_RETENTION_BYTES:-67108864}"  # 64 MiB per partition

RAW_TOPIC="${KAFKA_RAW_TOPIC:-cpc_events_raw}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-cpc_events_dlq}"

rpk_exec() {
    docker compose --profile core exec -T redpanda rpk "$@"
}

ensure_topic() {
    local topic="$1"
    local partitions="$2"
    local replicas="$3"
    local retention_bytes="$4"

    if rpk_exec topic describe "${topic}" >/dev/null 2>&1; then
        echo "ℹ️  Topic '${topic}' already exists — syncing retention & segment configs..."
        rpk_exec topic alter-config "${topic}" \
            --set "retention.ms=${RETENTION_MS}" \
            --set "segment.ms=${SEGMENT_MS}" \
            --set "retention.bytes=${retention_bytes}"
    else
        echo "🚀 Creating topic '${topic}' (partitions=${partitions}, replicas=${replicas})..."
        rpk_exec topic create "${topic}" \
            -p "${partitions}" \
            -r "${replicas}" \
            -c "retention.ms=${RETENTION_MS}" \
            -c "segment.ms=${SEGMENT_MS}" \
            -c "retention.bytes=${retention_bytes}"
    fi
}

echo "================================================================================"
echo "🔧 Provisioning Redpanda Topics (Task 2.4 - Idempotent)"
echo "================================================================================"

ensure_topic "${RAW_TOPIC}" 3 1 "${RAW_RETENTION_BYTES}"
ensure_topic "${DLQ_TOPIC}" 1 1 "${DLQ_RETENTION_BYTES}"

echo ""
echo "📋 Current Redpanda topics:"
rpk_exec topic list
echo "✅ Topic provisioning completed successfully."
