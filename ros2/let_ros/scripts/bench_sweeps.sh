#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
WORKERS=${1:-8,32}
HORIZON=${2:-5s}
REPEAT=${3:-3}
OUT=${OUT:-results_sweeps}
CARS=${CARS:-8}
STEP=${STEP:-37ms}
ENGINES=${ENGINES:-seq,barrier,tag,frontier,static,ooo}
BIN=$(pkg_prefix let_ros)/lib/let_ros
extra=()
if [ -n "${FRONTIER_LEADS:-}" ]; then extra+=(--lead "$FRONTIER_LEADS"); fi
if [ "${INTERLEAVE:-0}" = 1 ]; then extra+=(--interleave); fi
mkdir -p "$OUT"
echo "== bench_sweeps.sh $(date -u +%FT%TZ) CARS=$CARS STEP=$STEP WORKERS=$WORKERS" \
  "HORIZON=$HORIZON REPEAT=$REPEAT ENGINES=$ENGINES" \
  "FRONTIER_LEADS=${FRONTIER_LEADS:-} INTERLEAVE=${INTERLEAVE:-0}" | tee -a "$OUT/summary.txt"
RUN_OK=0
run() {
  local name=$1
  shift
  local f="$OUT/$name.jsonl"
  RUN_OK=0
  echo "== $name" | tee -a "$OUT/summary.txt"
  if ! "$BIN/let_ros_fleet" --cars "$CARS" --offset-step "$STEP" --engine "$ENGINES" \
      --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" --pin --check \
      "${extra[@]}" "$@" > "$f"; then
    bench_fail "$OUT" "$name (see $f)"
    return
  fi
  summarize "$f" "$OUT"
  RUN_OK=$SUMMARY_OK
}
BASE_RUN=""
base() {
  local name=$1
  if [ -n "$BASE_RUN" ]; then
    echo "== $name (the default configuration, a copy of $BASE_RUN from this call)" |
      tee -a "$OUT/summary.txt"
    cp "$OUT/$BASE_RUN.jsonl" "$OUT/$name.jsonl"
    summarize "$OUT/$name.jsonl" "$OUT"
  else
    run "$name"
    if [ "$RUN_OK" = 1 ]; then
      BASE_RUN=$name
    fi
  fi
}
for sweep in ${SWEEPS:-d timing sets}; do
  case "$sweep" in
    d) for s in ${SCALES:-0.5 1 2 4}; do
         if [ "$s" = 1 ]; then base "dscale_1"; else run "dscale_$s" --d-scale "$s"; fi
       done ;;
    timing) for t in ${TIMINGS:-default cpu}; do
              if [ "$t" = default ]; then base "timing_default"; else run "timing_$t" --timing "$t"; fi
            done ;;
    sets) base "sets_declared"; run "sets_none" --no-publish-sets ;;
    v2x) for d in ${V2X_DELAYS:-0ms 10ms 50ms 100ms}; do
           if [ "$d" = 0ms ] || [ "$d" = 0 ]; then base "v2x_0ms"; else run "v2x_$d" --v2x-delay "$d"; fi
         done ;;
    *) echo "unknown sweep $sweep" >&2; exit 1 ;;
  esac
done
bench_exit "$OUT"
