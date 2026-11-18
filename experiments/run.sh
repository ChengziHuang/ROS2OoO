#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
if [[ ${1:-} == --list && -f ${2:-} ]]; then
  set -- --list "$(realpath "$2")"
elif [[ -n ${1:-} && ${1:0:2} != -- && -f $1 ]]; then
  conf=$(realpath "$1")
  shift
  set -- "$conf" "$@"
fi
cd "$ROOT"
export LETROS_PREFIX=${LETROS_PREFIX:-$HOME/letros}

usage() {
  echo "usage: experiments/run.sh CONF [TASK ...]   run the tasks of CONF, all of them by default" >&2
  echo "       experiments/run.sh --list CONF        list the tasks of CONF" >&2
  exit 2
}

make_tasks() {
  (
    source "$1"
    kinds=" ${TASK_KINDS:-fleet reference sweeps model bounds} "
    want() { [[ $kinds == *" $1 "* ]]; }
    i=0
    if want fleet; then
      for n in $(tr ' ' '\n' <<< "$FLEET_CARS" | sort -rn); do
        for s in $FLEET_STEPS; do
          echo "$i fleet-c$n-s$s fleet $n $s"; i=$((i + 1))
        done
      done
    fi
    if want reference; then echo "$i reference reference"; i=$((i + 1)); fi
    if want stereo && [[ -n ${STEREO_FRAMES:-} ]]; then echo "$i stereo stereo"; i=$((i + 1)); fi
    if want sweeps; then echo "$i sweeps sweeps"; i=$((i + 1)); fi
    if want model; then echo "$i model model"; i=$((i + 1)); fi
    if want bounds; then echo "$i bounds bounds"; i=$((i + 1)); fi
    if want leads; then
      for l in ${LEAD_LISTS:-fleet}; do echo "$i leads-$l leads $l"; i=$((i + 1)); done
    fi
    if want node; then
      for l in ${NODE_LISTS:-fleet}; do echo "$i node-$l node $l"; i=$((i + 1)); done
    fi
  )
}

record_environment() {
  mkdir -p "$1"
  {
    echo "date: $(date -u +%FT%TZ)"
    echo "cpus_per_task: ${CPUS_PER_TASK:-?}"
    echo "affinity: $(taskset -cp $$ 2>/dev/null || true)"
    echo "reference-system: $(git -C "$LETROS_PREFIX/ws/src/reference-system" rev-parse HEAD 2>/dev/null || echo ?)"
    echo
    lscpu 2>/dev/null || true
  } > "$1/environment.txt"
}

run_step() {
  if [[ -n ${CORES:-} ]]; then
    taskset -c "$CORES" "$@"
  else
    "$@"
  fi
}

run_bench() {
  local rc=0
  run_step "$@" || rc=$?
  if [[ -f "$OUT/FAILED" ]]; then
    echo "ERROR: some configurations failed, see $OUT/FAILED" >&2
    exit 1
  fi
  if [[ $rc -ne 0 ]]; then
    echo "ERROR: the benchmark script exited with $rc, see the log above" >&2
    exit "$rc"
  fi
  date -u +%FT%TZ > "$OUT/DONE"
  echo "done: $OUT"
}

run_one() {
  local root=$1 name=$2 idx="" kind="" args=""
  read -r idx _ kind args < <(awk -v n="$name" '$2 == n' "$root/tasks.txt") || true
  [[ -n $kind ]] || { echo "ERROR: no task $name in $root/tasks.txt" >&2; exit 2; }
  export OUT=$root/$name
  if [[ -f "$OUT/DONE" ]]; then
    echo "$name is done already"
    return 0
  fi
  if [[ -e "$OUT" ]]; then
    mv "$OUT" "$OUT.attempt-$(date +%Y%m%d-%H%M%S)"
  fi
  set +u
  source "$LETROS_PREFIX/activate.sh"
  set -u
  export ROS_LOG_DIR=${TMPDIR:-/tmp}/roslog-$$
  source "$root/campaign.conf"
  record_environment "$OUT"
  echo "task: $idx $name $kind $args" >> "$OUT/environment.txt"
  source experiments/tasks.sh
  "task_$kind" $args
}

case ${1:-} in
  --list) [[ -f ${2:-} ]] || usage; make_tasks "$2"; exit 0 ;;
  --task) [[ $# -eq 3 ]] || usage; run_one "$2" "$3"; exit 0 ;;
  ""|-h|--help) usage ;;
esac

CONF=$1
shift
[[ -f $CONF ]] || usage
[[ -x $LETROS_PREFIX/ws/install/let_ros/lib/let_ros/let_ros_fleet ]] || {
  echo "ERROR: let_ros is not built in $LETROS_PREFIX, run experiments/install.sh first" >&2; exit 2; }
tasks=$(make_tasks "$CONF")
if [[ $# -gt 0 ]]; then
  sel=""
  for t in "$@"; do
    line=$(awk -v n="$t" '$2 == n' <<< "$tasks")
    [[ -n $line ]] || { echo "ERROR: no task $t in $CONF, see experiments/run.sh --list $CONF" >&2; exit 2; }
    sel+=$line$'\n'
  done
  tasks=${sel%$'\n'}
fi
[[ -n $tasks ]] || { echo "ERROR: TASK_KINDS in $CONF leaves no task" >&2; exit 2; }

tag=$(basename "$CONF" .conf)
tag=${tag#campaign}
tag=${tag#_}
root=$ROOT/results/ros2${tag:+-$tag}-$(date +%Y%m%d-%H%M%S)
mkdir -p "$root"
cp "$CONF" "$root/campaign.conf"
echo "$tasks" > "$root/tasks.txt"
echo "Campaign ${root#$ROOT/}, $(wc -l <<< "$tasks") tasks:"
sed 's/^/  /' <<< "$tasks"

failed=()
while read -r _ name _; do
  echo "== $name (log in ${root#$ROOT/}/$name.log)"
  bash "$ROOT/experiments/run.sh" --task "$root" "$name" < /dev/null 2>&1 | tee "$root/$name.log" ||
    failed+=("$name")
done <<< "$tasks"

echo
if [[ ${#failed[@]} -gt 0 ]]; then
  echo "failed: ${failed[*]}" >&2
  exit 1
fi
echo "done. CSV files: python3 analysis/collect.py ${root#$ROOT/}"
