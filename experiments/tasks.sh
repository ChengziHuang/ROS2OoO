BIN=$LETROS_PREFIX/ws/install/let_ros/lib/let_ros

task_fleet() {
  export STEPS=$2 ENGINES RING_INF FRONTIER_LEAD
  run_bench bash "$BIN/bench_fleet.sh" "$1" "$WORKERS" "$HORIZON" "$REPEAT"
}

task_reference() {
  export ENGINES RING_INF FRONTIER_LEAD
  run_bench bash "$BIN/bench.sh" "$WORKERS" "$HORIZON" "$REPEAT"
}

task_sweeps() {
  export ENGINES CARS=$SWEEP_CARS STEP=$SWEEP_STEP
  export SWEEPS=${SWEEP_KINDS:-d timing sets} V2X_DELAYS=${SWEEP_V2X_DELAYS:-0ms 10ms 50ms 100ms}
  export FRONTIER_LEADS=${SWEEP_FRONTIER_LEADS:-} INTERLEAVE=${SWEEP_INTERLEAVE:-0}
  if [[ ${SWEEP_V2X_BOUNDS:-0} == 1 ]]; then
    bounds_v2x
  fi
  run_bench bash "$BIN/bench_sweeps.sh" "$SWEEP_WORKERS" "$SWEEP_HORIZON" "$SWEEP_REPEAT"
}

bounds_v2x() {
  local d name
  mkdir -p "$OUT"
  for d in ${SWEEP_V2X_DELAYS:-0ms 10ms 50ms 100ms}; do
    name=bound_v2x_$d
    echo "== V2X sweep bound, delay $d" | tee -a "$OUT/summary.txt"
    if run_step env LET_ROS_DAG="$OUT/$name.dag" "$BIN/let_ros_fleet" --cars "${SWEEP_CARS:-8}" \
          --offset-step "${SWEEP_STEP:-37ms}" --v2x-delay "$d" --model calendar --engine seq \
          --horizon "${SWEEP_HORIZON:-5s}" --record-costs "$OUT/$name.costs" \
          > "$OUT/$name.out" 2> "$OUT/$name.err" &&
       python3 "$BIN/schedule_bound.py" "$OUT/$name.dag" "$OUT/$name.costs" 25000000 \
          --procs "${SWEEP_WORKERS:-8,32}" --json "$OUT/bounds.jsonl" \
          --label "workload=Fleet-${SWEEP_CARS:-8} cars=${SWEEP_CARS:-8} step=${SWEEP_STEP:-37ms} horizon=${SWEEP_HORIZON:-5s} v2x=$d" \
          | tee -a "$OUT/summary.txt"; then
      rm -f "$OUT/$name.dag" "$OUT/$name.costs"
    else
      echo "V2X sweep bound, delay $d (see $OUT/$name.err)" >> "$OUT/FAILED"
    fi
  done
}

bounds_dscale() {
  local s win name
  mkdir -p "$OUT"
  for s in ${SCALES:-0.5 1 2 4}; do
    name=dscale_$s
    win=$(awk -v s="$s" 'BEGIN { printf "%d", 25000000 * s }')
    echo "== D sweep bound, factor $s" | tee -a "$OUT/summary.txt"
    if run_step env LET_ROS_DAG="$OUT/$name.dag" "$BIN/let_ros_fleet" --cars "${SWEEP_CARS:-8}" \
          --offset-step "${SWEEP_STEP:-37ms}" --d-scale "$s" --model calendar --engine seq \
          --horizon "${SWEEP_HORIZON:-5s}" --record-costs "$OUT/$name.costs" \
          > "$OUT/$name.jsonl" 2> "$OUT/$name.err" &&
       python3 "$BIN/schedule_bound.py" "$OUT/$name.dag" "$OUT/$name.costs" "$win" \
          --procs "${PROCS:-1,2,4,8,16,32}" --json "$OUT/bounds.jsonl" \
          --label "workload=Fleet-${SWEEP_CARS:-8} cars=${SWEEP_CARS:-8} step=${SWEEP_STEP:-37ms} horizon=${SWEEP_HORIZON:-5s} dscale=$s" \
          | tee -a "$OUT/summary.txt"; then
      rm -f "$OUT/$name.dag" "$OUT/$name.costs"
    else
      echo "D sweep bound, factor $s (see $OUT/$name.err)" >> "$OUT/FAILED"
    fi
  done
}

task_bounds() {
  export STEPS=$FLEET_STEPS PROCS=${BOUNDS_PROCS:-1,2,4,8,16,32}
  if [[ ${BOUNDS_DSCALE:-1} == 1 ]]; then
    bounds_dscale
  fi
  run_bench bash "$BIN/bench_bounds.sh" "$BOUNDS_CARS" "$BOUNDS_HORIZON"
}

task_model() {
  export CARS=$MODEL_CARS STEP=37ms HORIZONS=$MODEL_HORIZONS ENGINES=seq,ooo
  run_bench bash "$BIN/bench_model.sh" "$MODEL_WORKERS" "$MODEL_REPEAT"
}

script_of() {
  if [[ -f $BIN/$1 ]]; then echo "$BIN/$1"; else echo "$PWD/ros2/let_ros/scripts/$1"; fi
}

items() {
  if [[ ${2:-0} != 0 ]]; then
    grep -v '^\s*#' "$1" | grep . | head -n "$2"
  else
    cat "$1"
  fi
}

task_leads() {
  local list=experiments/leads_$1.txt
  [[ -f $list ]] || { echo "ERROR: no list $list" >&2; exit 2; }
  export SEQ_REPEAT=${LEAD_SEQ_REPEAT:-3} LEADS=${LEAD_LEADS:-5s,2s,1s,500ms,200ms,100ms,50ms,25ms,10ms}
  items "$list" "${LEAD_LIMIT:-0}" > "$OUT/items.txt"
  run_bench bash "$(script_of bench_leads.sh)" "$OUT/items.txt" "${LEAD_WORKERS:-8,16,32}" \
    "${LEAD_REPEAT:-3}"
}

task_node() {
  local list=experiments/node_$1.txt lead=NODE_FRONTIER_LEAD_$1 workers=NODE_WORKERS_$1
  [[ -f $list ]] || { echo "ERROR: no list $list" >&2; exit 2; }
  export SEQ_REPEAT=${NODE_SEQ_REPEAT:-3} ENGINES=${NODE_ENGINES:-ooo,barrier}
  export FRONTIER_LEAD=${!lead:-none} SPREAD_WORKERS=${NODE_SPREAD_WORKERS:-8,16,32,64}
  items "$list" "${NODE_LIMIT:-0}" > "$OUT/items.txt"
  run_bench bash "$(script_of bench_node.sh)" "$OUT/items.txt" \
    "${!workers:-${NODE_WORKERS:-2,4,8,16,32,64,128,256}}" "${NODE_REPEAT:-3}"
}

task_stereo() {
  local base=$OUT parent prefix bag data rc d
  parent=$(dirname "$LETROS_PREFIX")
  prefix=${STEREO_PREFIX:-$parent/letros-perception}
  bag=${STEREO_BAG:-$parent/r2b/r2b_galileo}
  data=${STEREO_DATA:-$parent/prepared/galileo-four-$STEREO_FRAMES}
  [[ -d "$prefix/env/conda-meta" && -f "$prefix/activate.sh" ]] || {
    echo "ERROR: no perception environment in $prefix. Run" >&2
    echo "  bash ros2/perception_benchmark/install.sh $prefix env" >&2; exit 2; }
  echo "stereo: $prefix, data $data, deadlines $STEREO_DEADLINES" >> "$base/environment.txt"
  set +u
  source "$prefix/activate.sh"
  set -u
  if [[ ! -f "$data/front/frames.tsv" ]]; then
    [[ -d "$bag" ]] || { echo "ERROR: neither prepared data $data nor the bag $bag. Run" >&2
      echo "  source $prefix/activate.sh; python3 ros2/perception_data/fetch_r2b.py --sequence r2b_galileo --out $(dirname "$bag")" >&2
      exit 2; }
    echo "== prepare $STEREO_FRAMES frames per rig from $bag (log in $base/prepare.log)"
    mkdir -p "$(dirname "$data")"
    if ! python3 ros2/perception_data/prepare.py "$bag" --all-rigs --max-frames "$STEREO_FRAMES" \
        --max-unmatched "${STEREO_MAX_UNMATCHED:-64}" --out "$data" > "$base/prepare.log" 2>&1; then
      echo "ERROR: preparing the stereo data failed, see $base/prepare.log" >&2; exit 1
    fi
  fi
  echo "== build the perception workspace in $prefix (log in $base/build.log)"
  if ! CMAKE_BUILD_PARALLEL_LEVEL=${CPUS_PER_TASK:-8} \
      LET_ROS_NATIVE_OPTIONAL=${LET_ROS_NATIVE_OPTIONAL:-1} bash ros2/perception_benchmark/install.sh \
      "$prefix" build > "$base/build.log" 2>&1; then
    echo "ERROR: the perception build failed, see $base/build.log" >&2; exit 1
  fi
  set +u
  source "$prefix/activate.sh"
  set -u
  rc=0
  for d in $STEREO_DEADLINES; do
    echo "== stereo, four rigs, D = $d ns"
    run_step env OUT="$base/d$d" ENGINES="$STEREO_ENGINES" MAX_FRAMES="$STEREO_FRAMES" \
      DEADLINE_NS="$d" PIN=1 WORKLOAD="stereo4-d$d" \
      bash ros2/perception_benchmark/bench_perception.sh "$data" r2b-four \
      "$STEREO_WORKERS" "$STEREO_REPEAT" || rc=1
    [[ -f "$base/d$d/FAILED" ]] && rc=1
  done
  if [[ $rc -ne 0 ]]; then
    echo "ERROR: a stereo run failed, see $base/d*/FAILED and the log above" >&2; exit 1
  fi
  date -u +%FT%TZ > "$base/DONE"
  echo "done: $base"
}
