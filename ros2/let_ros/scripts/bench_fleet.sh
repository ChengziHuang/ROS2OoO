#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
CARS=${1:-1,2,4,8}
WORKERS=${2:-1,2,4,8,16,32}
HORIZON=${3:-10s}
REPEAT=${4:-3}
OUT=${OUT:-results_fleet}
BIN=$(pkg_prefix let_ros)/lib/let_ros
mkdir -p "$OUT"
echo "== bench_fleet.sh $(date -u +%FT%TZ) CARS=$CARS WORKERS=$WORKERS HORIZON=$HORIZON" \
  "REPEAT=$REPEAT STEPS=${STEPS:-0ms 37ms} ENGINES=${ENGINES:-seq,barrier,ooo}" \
  "RING_INF=${RING_INF:-0} FRONTIER_LEAD=${FRONTIER_LEAD:-}" | tee -a "$OUT/summary.txt"
for n in ${CARS//,/ }; do
  for step in ${STEPS:-0ms 37ms}; do
    f="$OUT/cars${n}_step${step}.jsonl"
    echo "== $n cars, offset step $step" | tee -a "$OUT/summary.txt"
    if ! "$BIN/let_ros_fleet" --cars "$n" --offset-step "$step" --engine "${ENGINES:-seq,barrier,ooo}" \
        --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" --pin --check > "$f"; then
      bench_fail "$OUT" "$n cars, offset step $step (see $f)"
      continue
    fi
    if [ "${RING_INF:-0}" = 1 ] && ! "$BIN/let_ros_fleet" --cars "$n" --offset-step "$step" \
        --engine ooo --ring inf --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" \
        --pin --check >> "$f"; then
      bench_fail "$OUT" "$n cars, offset step $step, ring inf (see $f)"
      continue
    fi
    if [ "${FRONTIER_LEAD:-none}" != none ] && ! "$BIN/let_ros_fleet" --cars "$n" \
        --offset-step "$step" --engine frontier --lead "$FRONTIER_LEAD" --workers "$WORKERS" \
        --repeat "$REPEAT" --horizon "$HORIZON" --pin --check >> "$f"; then
      bench_fail "$OUT" "$n cars, offset step $step, frontier lead (see $f)"
      continue
    fi
    summarize "$f" "$OUT"
  done
done
bench_exit "$OUT"
