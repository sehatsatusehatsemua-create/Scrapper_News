#!/bin/bash
set -euo pipefail

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SOURCE="${BASE_DIR}/data"
REMOTE="${RCLONE_REMOTE:-gdrive_corpus}:${RCLONE_DEST:-research/corpus_indonesia/politik}"
LOG_FILE="${BASE_DIR}/logs/rclone.log"

mkdir -p "${BASE_DIR}/logs"

# move only files that are old enough to avoid race with writer
rclone move "$SOURCE" "$REMOTE" \
  --include "*.jsonl" \
  --include "*.md5" \
  --min-age 15m \
  --progress \
  --log-file "$LOG_FILE"
