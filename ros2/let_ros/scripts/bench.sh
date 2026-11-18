#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
WORKERS=${1:-1,2,4}
HORIZON=${2:-10s}
REPEAT=${3:-3}
OUT=${OUT:-results}
BIN=$(pkg_prefix let_ros)/lib/let_ros
SETS=$(pkg_prefix let_ros)/share/let_ros/config/autoware_publish_sets.txt
GDIR=$(pkg_prefix let_ros_test_nodes)/share/let_ros_test_nodes/graphs
mkdir -p "$OUT"

ENGINES=${ENGINES:-seq,barrier,tag,frontier,static,ooo}
echo "== bench.sh $(date -u +%FT%TZ) WORKERS=$WORKERS HORIZON=$HORIZON REPEAT=$REPEAT" \
  "ENGINES=$ENGINES RING_INF=${RING_INF:-0} FRONTIER_LEAD=${FRONTIER_LEAD:-none}" |
  tee -a "$OUT/summary.txt"
RUN_OK=0
run_all() {
  local out="$OUT/$1.jsonl" label=$2
  shift 2
  RUN_OK=0
  echo "== $label" | tee -a "$OUT/summary.txt"
  if ! "$@" --engine "$ENGINES" --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" \
      --pin --check > "$out"; then
    bench_fail "$OUT" "$label (see $out)"
    return
  fi
  if [ "${RING_INF:-0}" = 1 ] && ! "$@" --engine ooo --ring inf --workers "$WORKERS" \
      --repeat "$REPEAT" --horizon "$HORIZON" --pin --check >> "$out"; then
    bench_fail "$OUT" "$label, ring inf (see $out)"
    return
  fi
  if [ "${FRONTIER_LEAD:-none}" != none ] && ! "$@" --engine frontier --lead "$FRONTIER_LEAD" \
      --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" --pin --check >> "$out"; then
    bench_fail "$OUT" "$label, frontier lead (see $out)"
    return
  fi
  RUN_OK=1
}
run_all autoware "Autoware reference system" "$BIN/let_ros_autoware" --publish-sets "$SETS"
[ "$RUN_OK" = 1 ] && summarize "$OUT/autoware.jsonl" "$OUT"
run_all mixed "mixed test graph" "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" \
  --publish-sets "$GDIR/mixed_publish_sets.txt"
[ "$RUN_OK" = 1 ] && summarize "$OUT/mixed.jsonl" "$OUT"
run_all mixed_heavy "mixed test graph, heavy callbacks" "$BIN/let_ros_run" \
  --nodes "$GDIR/mixed_heavy.txt" --publish-sets "$GDIR/mixed_publish_sets.txt"
[ "$RUN_OK" = 1 ] && summarize "$OUT/mixed_heavy.jsonl" "$OUT"
run_all mixed_jitter "mixed test graph, heavy callbacks with data-dependent costs" \
  "$BIN/let_ros_run" --nodes "$GDIR/mixed_jitter.txt" --publish-sets "$GDIR/mixed_publish_sets.txt"
if [ "$RUN_OK" = 1 ] && [[ ",$ENGINES," == *",static,"* ]]; then
  if ! "$BIN/let_ros_run" --nodes "$GDIR/mixed_jitter.txt" \
      --publish-sets "$GDIR/mixed_publish_sets.txt" --engine static --cost-model callback \
      --workers "$WORKERS" --repeat "$REPEAT" --horizon "$HORIZON" --pin --check \
      >> "$OUT/mixed_jitter.jsonl"; then
    bench_fail "$OUT" "mixed test graph with data-dependent costs, static callback"
    RUN_OK=0
  fi
fi
[ "$RUN_OK" = 1 ] && summarize "$OUT/mixed_jitter.jsonl" "$OUT"
bench_exit "$OUT"
