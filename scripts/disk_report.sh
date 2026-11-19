#!/usr/bin/env bash
# ==============================================================================
# Disk & Resource Governance Report (Task 1.5 - Kỷ luật ổ đĩa WSL2)
# ==============================================================================
set -euo pipefail

# Ensure standard POSIX tools are discoverable in any bash environment (WSL, Git Bash)
export PATH="/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:$PATH"

# 1. Resolve Project Root and .env
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Load DATA_ROOT from .env if present, otherwise default
if [ -f "${PROJECT_ROOT}/.env" ]; then
    # shellcheck disable=SC1091
    DATA_ROOT=$(grep -E '^DATA_ROOT=' "${PROJECT_ROOT}/.env" | cut -d '=' -f2- | tr -d '"' | tr -d "'")
fi
DATA_ROOT="${DATA_ROOT:-/mnt/d/docker_data/cpc_lakehouse}"

echo "================================================================================"
echo "📊 BÁO CÁO KỶ LUẬT Ổ ĐĨA & TÀI NGUYÊN (WSL2 / Docker Engine)"
echo "   Thời gian: $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "   Project Root: ${PROJECT_ROOT}"
echo "   DATA_ROOT:    ${DATA_ROOT}"
echo "================================================================================"
echo ""

# 2. Disk Space on Drives (C, D, WSL root)
echo "📁 1. DUNG LƯỢNG TRỐNG TRÊN CÁC PHÂN VÙNG:"
echo "--------------------------------------------------------------------------------"
df -h /mnt/c /mnt/d / 2>/dev/null || df -h /
echo ""

# 3. Docker Storage Breakdown
echo "🐳 2. DUNG LƯỢNG SỬ DỤNG CỦA DOCKER ENGINE (docker system df):"
echo "--------------------------------------------------------------------------------"
if command -v docker >/dev/null 2>&1; then
    docker system df
else
    echo "⚠️ Docker command not available in current shell."
fi
echo ""

# 4. DATA_ROOT Breakdown on Drive D
echo "💾 3. CHI TIẾT DỮ LIỆU TẠI DATA_ROOT (${DATA_ROOT}):"
echo "--------------------------------------------------------------------------------"
if [ -d "${DATA_ROOT}" ]; then
    echo "Tổng dung lượng DATA_ROOT: $(du -sh "${DATA_ROOT}" 2>/dev/null | cut -f1)"
    echo "Chi tiết từng dịch vụ:"
    du -sh "${DATA_ROOT}"/* 2>/dev/null || echo "   (Chưa có thư mục con)"
else
    echo "⚠️ Thư mục DATA_ROOT (${DATA_ROOT}) chưa tồn tại trên ổ D."
fi
echo ""

# 5. Lakehouse / Delta Lake Storage Check
echo "🧊 4. TRẠNG THÁI DỮ LIỆU MINIO & DELTA LAKE:"
echo "--------------------------------------------------------------------------------"
MINIO_DIR="${DATA_ROOT}/minio/lakehouse"
if [ -d "${MINIO_DIR}" ]; then
    echo "Bucket 'lakehouse' size: $(du -sh "${MINIO_DIR}" 2>/dev/null | cut -f1)"
    echo "Tổng số objects/files trong bucket: $(find "${MINIO_DIR}" -type f 2>/dev/null | wc -l)"
else
    echo "Bucket 'lakehouse' chưa có dữ liệu hoặc MinIO lưu theo cấu trúc khác."
fi
echo ""

# 6. WSL2 ext4.vhdx check advice
echo "💡 5. LƯU Ý BẢO VỆ Ổ C (WSL2 Virtual Hard Disk):"
echo "--------------------------------------------------------------------------------"
echo "- File đĩa ảo của WSL (ext4.vhdx) nằm tại: %LOCALAPPDATA%\\Packages\\...\\LocalState\\ext4.vhdx"
echo "- Toàn bộ dữ liệu nặng đã được bind mount sang ổ D: ${DATA_ROOT}"
echo "- Nếu cần tự động thu hồi dung lượng khi xoá file trong WSL2, đảm bảo đã bật"
echo "  [experimental] sparseVhd=true trong file %USERPROFILE%\\.wslconfig"
echo "================================================================================"
