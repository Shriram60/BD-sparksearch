#!/usr/bin/env bash
# Drop the Iceberg tables to start again from Phase 0 (e.g. toy data -> real pages.jsonl).
#   node1_ingest/reset_tables.sh [--all] [--yes]
#   LOCAL=1 node1_ingest/reset_tables.sh --all      # local warehouse
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$HERE/spark_args.sh"
spark-submit "${SPARK_ARGS[@]}" "$HERE/reset_tables.py" "$@" 2> >(grep -v -E " INFO |WARN" >&2)
