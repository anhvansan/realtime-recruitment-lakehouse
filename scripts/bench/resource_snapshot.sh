#!/usr/bin/env bash
# ==============================================================================
# Resource Snapshot & Baseline Profiler (Task 1.6 - Đo baseline tài nguyên)
# ==============================================================================
set -euo pipefail

# Ensure standard POSIX tools are discoverable in any bash environment (WSL, Git Bash)
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Arguments with sensible defaults
SAMPLES="${1:-12}"          # Default 12 samples (1 minute at 5s interval)
INTERVAL="${2:-5}"          # Default 5 seconds interval
OUTPUT_CSV="${3:-${PROJECT_ROOT}/docs/benchmarks/resource_snapshot.csv}"

mkdir -p "$(dirname "${OUTPUT_CSV}")"

echo "================================================================================"
echo "📈 BẮT ĐẦU ĐO BASELINE TÀI NGUYÊN DOCKER (Task 1.6)"
echo "   Số mẫu:      ${SAMPLES}"
echo "   Chu kỳ:      ${INTERVAL} giây / mẫu"
echo "   Tổng time:   $((SAMPLES * INTERVAL)) giây (~$(( (SAMPLES * INTERVAL) / 60 )) phút)"
echo "   Xuất file:   ${OUTPUT_CSV}"
echo "================================================================================"
echo ""

# Write CSV Header
echo "timestamp,container_id,name,cpu_perc,mem_usage,mem_limit,mem_perc,net_io,block_io,pids" > "${OUTPUT_CSV}"

for i in $(seq 1 "${SAMPLES}"); do
    TS="$(date '+%Y-%m-%d %H:%M:%S')"
    echo "⏱️  [Mẫu ${i}/${SAMPLES}] Thu thập dữ liệu tại ${TS}..."

    # Extract raw docker stats snapshot
    # Format: ID, Name, CPUPerc, MemUsage (usage / limit), MemPerc, NetIO, BlockIO, PIDs
    STATS=$(docker stats --no-stream --format "{{.ID}},{{.Name}},{{.CPUPerc}},{{.MemUsage}},{{.MemPerc}},{{.NetIO}},{{.BlockIO}},{{.PIDs}}")

    while IFS= read -r line; do
        if [ -n "${line}" ]; then
            # Split line: ID, Name, CPU, MemRaw, MemPerc, NetIO, BlockIO, PIDs
            IFS=',' read -r CID CNAME CCPU CMEMRAW CMEMPERC CNET CBLOCK CPIDS <<< "${line}"
            # MemRaw contains e.g. "80.5MiB / 448MiB" -> split into usage and limit
            CMEM_USAGE="$(echo "${CMEMRAW}" | awk -F '/' '{print $1}' | tr -d ' ')"
            CMEM_LIMIT="$(echo "${CMEMRAW}" | awk -F '/' '{print $2}' | tr -d ' ')"

            echo "${TS},${CID},${CNAME},${CCPU},${CMEM_USAGE},${CMEM_LIMIT},${CMEMPERC},${CNET},${CBLOCK},${CPIDS}" >> "${OUTPUT_CSV}"
        fi
    done <<< "${STATS}"

    if [ "${i}" -lt "${SAMPLES}" ]; then
        sleep "${INTERVAL}"
    fi
done

echo ""
echo "================================================================================"
echo "✅ ĐÃ HOÀN TẤT THU THẬP BASELINE (${SAMPLES} MẪU)!"
echo "   File kết quả lưu tại: ${OUTPUT_CSV}"
echo "================================================================================"
echo ""
echo "📊 BẢNG TỔNG HỢP MẪU GẦN NHẤT:"
echo "--------------------------------------------------------------------------------"
docker stats --no-stream
echo "================================================================================"
