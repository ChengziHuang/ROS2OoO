#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
WORKERS=${1:-1,32}
REPEAT=${2:-3}
OUT=${OUT:-results_model}
CARS=${CARS:-8}
STEP=${STEP:-37ms}
HORIZONS=${HORIZONS:-10s 40s}
ENGINES=${ENGINES:-seq,ooo}
BIN=$(pkg_prefix let_ros)/lib/let_ros
mkdir -p "$OUT"
echo "== bench_model.sh $(date -u +%FT%TZ) CARS=$CARS STEP=$STEP HORIZONS=$HORIZONS" \
  "WORKERS=$WORKERS REPEAT=$REPEAT ENGINES=$ENGINES" | tee -a "$OUT/summary.txt"
for h in $HORIZONS; do
  for model in streams calendar; do
    f="$OUT/model_${model}_$h.jsonl"
    echo "== $model model, horizon $h" | tee -a "$OUT/summary.txt"
    if ! "$BIN/let_ros_fleet" --cars "$CARS" --offset-step "$STEP" --model "$model" \
        --engine "$ENGINES" --workers "$WORKERS" --repeat "$REPEAT" --horizon "$h" --pin \
        --check > "$f"; then
      bench_fail "$OUT" "$model model, horizon $h (see $f)"
      continue
    fi
    summarize "$f" "$OUT"
  done
  a=$(grep -o '"hash":"[0-9a-f]*"' "$OUT/model_streams_$h.jsonl" 2>/dev/null | sort -u)
  b=$(grep -o '"hash":"[0-9a-f]*"' "$OUT/model_calendar_$h.jsonl" 2>/dev/null | sort -u)
  if [ -n "$a" ] && [ -n "$b" ] && [ "$a" != "$b" ]; then
    bench_fail "$OUT" "horizon $h: the streams and the calendar model give different hashes"
  fi
done
bench_exit "$OUT"
