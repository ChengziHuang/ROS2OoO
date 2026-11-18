#!/usr/bin/env bash
set -euo pipefail
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
LETROS_PREFIX=${LETROS_PREFIX:-$HOME/letros}
parent=$(dirname "$LETROS_PREFIX")
prefix=${STEREO_PREFIX:-$parent/letros-perception}
data=${STEREO_DATA:-$parent/prepared/galileo-four-60}
[[ -f "$prefix/activate.sh" ]] || {
  echo "ERROR: no perception environment in $prefix, see the task stereo of experiments/campaign.conf" >&2; exit 2; }
[[ -f "$data/front/frames.tsv" ]] || {
  echo "ERROR: no prepared stereo data in $data, the task stereo of experiments/campaign.conf prepares it" >&2; exit 2; }
set +u
source "$prefix/activate.sh"
set -u
export ROS_LOG_DIR=${TMPDIR:-/tmp}/roslog-$$
root=$ROOT/results/ros2-stereo-lead-$(date +%Y%m%d-%H%M%S)
mkdir -p "$root"
env OUT="$root/d33333333" ENGINES=frontier,ooo LEAD="${LEAD:-200ms}" MAX_FRAMES=60 DEADLINE_NS=33333333 \
  PIN=1 WORKLOAD=stereo4-d33333333 \
  bash ros2/perception_benchmark/bench_perception.sh "$data" r2b-four "${WORKERS:-32}" "${REPEAT:-3}"
echo "results in ${root#$ROOT/}"
