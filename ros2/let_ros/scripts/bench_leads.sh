#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
LIST=${1:?usage: bench_leads.sh LIST [WORKERS] [REPEAT]}
WORKERS=${2:-8,32}
REPEAT=${3:-3}
OUT=${OUT:-results_leads}
LEADS=${LEADS:-5s,2s,1s,500ms,200ms,100ms,50ms,25ms,10ms}
BIN=$(pkg_prefix let_ros)/lib/let_ros
mkdir -p "$OUT"
echo "== bench_leads.sh $(date -u +%FT%TZ) LIST=$LIST WORKERS=$WORKERS REPEAT=$REPEAT" \
  "LEADS=$LEADS SEQ_REPEAT=${SEQ_REPEAT:-3}" \
  | tee -a "$OUT/summary.txt"

one_item() {
  local name=$1 leads=$2 f=$OUT/$1.jsonl e=$OUT/$1.err
  shift 2
  echo "== $name, leads $leads $(date -u +%T)" | tee -a "$OUT/summary.txt"
  if ! "$@" --engine seq --workers 1 --repeat "${SEQ_REPEAT:-3}" --pin --check \
      > "$f" 2> "$e" < /dev/null; then
    bench_fail "$OUT" "$name: seq failed: $(tail -1 "$e")"
    return
  fi
  if ! "$@" --engine ooo,frontier --lead "$leads" --workers "$WORKERS" --repeat "$REPEAT" \
      --interleave --pin --check >> "$f" 2>> "$e" < /dev/null; then
    bench_fail "$OUT" "$name: ooo or frontier failed: $(tail -1 "$e")"
    return
  fi
  summarize "$f" "$OUT"
}

while read -r -u 3 a b c d _; do
  [[ -z ${a:-} || $a == \#* ]] && continue
  if [[ $a == fleet ]]; then
    one_item "fleet-c$b-s$c" "inf,$LEADS" "$BIN/let_ros_fleet" --cars "$b" --offset-step "$c" \
      --horizon "$d"
  else
    bench_fail "$OUT" "$a $b: unsupported workload; this script accepts only fleet entries"
  fi
done 3< "$LIST"
bench_exit "$OUT"
