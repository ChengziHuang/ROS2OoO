#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
CARS=${1:-1,2,4,8,16}
HORIZON=${2:-10s}
OUT=${OUT:-results_bounds}
STEPS=${STEPS:-37ms 0ms}
PROCS=${PROCS:-1,2,4,8,16,32}
WINDOW=${WINDOW:-25000000}
BIN=$(pkg_prefix let_ros)/lib/let_ros
SETS=$(pkg_prefix let_ros)/share/let_ros/config/autoware_publish_sets.txt
mkdir -p "$OUT"
echo "== bench_bounds.sh $(date -u +%FT%TZ) CARS=$CARS STEPS=$STEPS HORIZON=$HORIZON" \
  "PROCS=$PROCS WINDOW=$WINDOW" | tee -a "$OUT/summary.txt"
bound() {
  local name=$1 label=$2
  shift 2
  local dag="$OUT/$name.dag" costs="$OUT/$name.costs"
  echo "== $name" | tee -a "$OUT/summary.txt"
  if ! LET_ROS_DAG="$dag" "$@" --model calendar --engine seq --horizon "$HORIZON" \
      --record-costs "$costs" > "$OUT/$name.jsonl" 2> "$OUT/$name.err"; then
    bench_fail "$OUT" "$name (see $OUT/$name.err)"
    return
  fi
  if ! python3 "$BIN/schedule_bound.py" "$dag" "$costs" "$WINDOW" --procs "$PROCS" \
      --json "$OUT/bounds.jsonl" --label "$label horizon=$HORIZON" | tee -a "$OUT/summary.txt"; then
    bench_fail "$OUT" "$name: schedule_bound.py failed"
  fi
  rm -f "$dag" "$costs"
}
bound rs "workload=RS cars=0 step=-" "$BIN/let_ros_autoware" --publish-sets "$SETS"
for n in ${CARS//,/ }; do
  for step in $STEPS; do
    bound "cars${n}_step$step" "workload=Fleet-$n cars=$n step=$step" \
      "$BIN/let_ros_fleet" --cars "$n" --offset-step "$step"
  done
done
bench_exit "$OUT"
