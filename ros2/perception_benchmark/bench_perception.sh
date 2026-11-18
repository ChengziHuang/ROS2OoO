#!/usr/bin/env bash
set -euo pipefail
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
DATASET=${1:?usage: bench_perception.sh DATASET_ROOT [single|r2b-four] [workers] [repeat]}
MODE=${2:-single}
WORKERS=${3:-1,2,4}
REPEAT=${4:-3}
OUT=${OUT:-results_perception}
ENGINES=${ENGINES:-seq,barrier,tag,frontier,static,ooo}
MODEL=${MODEL:-streams}
MAX_FRAMES=${MAX_FRAMES:-0}
DEADLINE_NS=${DEADLINE_NS:-1000000}
LET_PREFIX=""
for candidate in ${AMENT_PREFIX_PATH//:/ }; do
  if [[ -e "$candidate/share/ament_index/resource_index/packages/let_ros" ]]; then
    LET_PREFIX=$candidate
    break
  fi
done
if [[ -z "$LET_PREFIX" ]]; then
  echo "let_ros not found; source the built workspace first" >&2
  exit 2
fi
BIN="$LET_PREFIX/lib/let_ros"
source "$BIN/common.sh"
if ! pkg_prefix let_ros_perception >/dev/null; then exit 2; fi
if ! pkg_prefix image_proc >/dev/null; then exit 2; fi
if ! pkg_prefix stereo_image_proc >/dev/null; then exit 2; fi
if [[ -e "$OUT" ]]; then
  if [[ ! -d "$OUT" || -n "$(find "$OUT" -mindepth 1 -maxdepth 1 -print -quit)" ]]; then
    echo "OUT must be a new or empty directory; refusing to overwrite prior results: $OUT" >&2
    exit 2
  fi
else
  mkdir -p "$OUT"
fi
GRAPH="$OUT/graph_$MODE"
GEN_ARGS=(--dataset-root "$DATASET" --mode "$MODE" --output "$GRAPH"
  --max-frames "$MAX_FRAMES" --deadline-ns "$DEADLINE_NS")
if [[ -n "${PERIOD_NS:-}" ]]; then GEN_ARGS+=(--period-ns "$PERIOD_NS"); fi
if [[ "${OBSERVE_INTERMEDIATES:-0}" == 1 ]]; then GEN_ARGS+=(--observe-intermediates); fi
if ! python3 "$HERE/gen_graph.py" "${GEN_ARGS[@]}"; then
  bench_fail "$OUT" "cannot prepare real input graph ($DATASET)"
  bench_exit "$OUT"
fi
CONFIG="$GRAPH/run_config.json"
mapfile -t DRIVER_ARGS < <(python3 - "$CONFIG" <<'PY'
import json, sys
for value in json.load(open(sys.argv[1], encoding="utf-8"))["driver_args"]:
    if "\n" in value or "\r" in value:
        raise SystemExit("newlines are not allowed in driver arguments")
    print(value)
PY
)
if [[ ${#DRIVER_ARGS[@]} -eq 0 ]]; then
  bench_fail "$OUT" "run_config contains no driver arguments"
  bench_exit "$OUT"
fi
echo "== perception $(date -u +%FT%TZ) DATASET=$DATASET MODE=$MODE MODEL=$MODEL" \
  "ENGINES=$ENGINES WORKERS=$WORKERS REPEAT=$REPEAT" | tee -a "$OUT/summary.txt"
RESULT="$OUT/${MODE}_${MODEL}.jsonl"
LOG="$OUT/${MODE}_${MODEL}.stderr.log"
python3 - "$OUT" "$MODE" "$MODEL" "$ENGINES" "$WORKERS" "$REPEAT" <<'PY'
from datetime import datetime, timezone
import json, os, pathlib, sys
output = pathlib.Path(sys.argv[1])
mode, model = sys.argv[2:4]
engines = [value for value in sys.argv[4].split(',') if value]
workers = [int(value) for value in sys.argv[5].split(',') if value]
repeat = int(sys.argv[6])
reported = repeat * sum(1 if value == 'seq' else len(workers) for value in engines)
manifest = {
    'schema_version': 1, 'workload': os.environ.get('WORKLOAD', output.resolve().name),
    'created_utc': datetime.now(timezone.utc).isoformat(),
    'config': f'graph_{mode}/run_config.json', 'results': f'{mode}_{model}.jsonl',
    'stderr': f'{mode}_{model}.stderr.log', 'engines': engines, 'workers': workers,
    'repeat': repeat, 'model': model,
    'expected_sink_runs': reported + int('static' in engines),
}
with (output / 'run_manifest.json').open('x', encoding='utf-8') as stream:
    json.dump(manifest, stream, indent=2)
    stream.write('\n')
PY
EXTRA_ARGS=()
if [[ "${PIN:-1}" == 1 ]]; then EXTRA_ARGS+=(--pin); fi
if [[ -n "${LEAD:-}" ]]; then EXTRA_ARGS+=(--lead "$LEAD"); fi
if ! RCUTILS_LOGGING_USE_STDOUT=0 RCUTILS_COLORIZED_OUTPUT=0 \
    "$BIN/let_ros_run" "${DRIVER_ARGS[@]}" --model "$MODEL" --engine "$ENGINES" \
    --workers "$WORKERS" --repeat "$REPEAT" --check "${EXTRA_ARGS[@]}" \
    >"$RESULT" 2>"$LOG"; then
  bench_fail "$OUT" "$MODE/$MODEL run failed (see $LOG)"
else
  summarize "$RESULT" "$OUT"
  EXPECTED_RUNS=$(python3 - "$ENGINES" "$WORKERS" "$REPEAT" <<'PY'
import sys
engines = [value for value in sys.argv[1].split(',') if value]
workers = [value for value in sys.argv[2].split(',') if value]
reported = int(sys.argv[3]) * sum(1 if value == 'seq' else len(workers) for value in engines)
print(reported + int('static' in engines))
PY
)
  if ! python3 "$HERE/check_sinks.py" "$CONFIG" "$LOG" --expected-runs "$EXPECTED_RUNS" \
      | tee -a "$OUT/summary.txt"; then
    bench_fail "$OUT" "$MODE/$MODEL did not consume all real frames (see $LOG)"
  fi
  if ! python3 "$HERE/summarize_perception.py" "$OUT" | tee -a "$OUT/summary.txt"; then
    bench_fail "$OUT" "$MODE/$MODEL strict timing/RSS summary failed"
  fi
fi
bench_exit "$OUT"
