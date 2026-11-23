#!/usr/bin/env bash
set -euo pipefail
BASE_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DATES=("$@")
if [[ ${#DATES[@]} -eq 0 ]]; then DATES=(2026-03-05 2026-03-06); fi
FAILED=0
for LOAD_DATE in "${DATES[@]}"; do
  echo "=== Проверка $LOAD_DATE ==="
  if [[ -f /.dockerenv ]]; then
    bash "$BASE_DIR/spark_check/run_check.sh" --load-date "$LOAD_DATE" --log-file "$BASE_DIR/spark_check/checks.log" || FAILED=1
  else
    docker exec ubuntu-vm bash /home/student/study/store_hse/spark_check/run_check.sh --load-date "$LOAD_DATE" --log-file /home/student/study/store_hse/spark_check/checks.log || FAILED=1
  fi
done
exit "$FAILED"
