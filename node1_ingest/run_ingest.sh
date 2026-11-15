#!/usr/bin/env bash
# Node 1: upload a phase file to HDFS and run the ingestion job.
#
#   node1_ingest/run_ingest.sh <phase> [file]
#
#   file defaults to $DATA_DIR/pages.jsonl (phase 0) or $DATA_DIR/changes.jsonl (phase >= 1),
#   DATA_DIR defaults to ~/data (same as NODE1_DATA in scripts/run_phase.sh).
#
#   LOCAL=1 node1_ingest/run_ingest.sh 0 node1_ingest/toy_data/pages.jsonl
#       -> no HDFS upload, Spark local mode, warehouse file:///tmp/searchengine/warehouse
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/spark_args.sh"

PHASE="${1:?usage: $0 <phase> [file]}"
DATA_DIR="${DATA_DIR:-$HOME/data}"
if [[ $# -ge 2 ]]; then FILE="$2"
elif [[ "$PHASE" == "0" ]]; then FILE="$DATA_DIR/pages.jsonl"
else FILE="$DATA_DIR/changes.jsonl"; fi
[[ -f "$FILE" ]] || { echo "input file not found: $FILE" >&2; exit 1; }

if [[ "${LOCAL:-0}" == "1" ]]; then
  INPUT="file://$(realpath "$FILE")"
else
  RAW_DIR="/searchengine/raw/phase${PHASE}"
  echo "[run_ingest] uploading $FILE -> hdfs://$RAW_DIR/"
  hdfs dfs -mkdir -p "$RAW_DIR"
  hdfs dfs -put -f "$FILE" "$RAW_DIR/"
  INPUT="$RAW_DIR/$(basename "$FILE")"
fi

START=$(date +%s)
spark-submit "${SPARK_ARGS[@]}" "$HERE/ingest.py" --phase "$PHASE" --input "$INPUT"
echo "[run_ingest] phase $PHASE done in $(( $(date +%s) - START ))s"
