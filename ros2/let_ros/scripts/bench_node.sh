#!/usr/bin/env bash
set -euo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
LIST=${1:?usage: bench_node.sh LIST [WORKERS] [REPEAT]}
WORKERS=${2:-2,4,8,16,32,64}
REPEAT=${3:-3}
OUT=${OUT:-results_node}
ENGINES=${ENGINES:-ooo,barrier}
SPREAD_WORKERS=${SPREAD_WORKERS:-8,16,32,64}
BIN=$(pkg_prefix let_ros)/lib/let_ros
ENG=$ENGINES
LEAD_ARGS=()
if [[ ${FRONTIER_LEAD:-none} != none && -n ${FRONTIER_LEAD:-} ]]; then
  ENG+=,frontier
  LEAD_ARGS=(--lead "$FRONTIER_LEAD")
fi
mkdir -p "$OUT"
echo "== bench_node.sh $(date -u +%FT%TZ) LIST=$LIST WORKERS=$WORKERS REPEAT=$REPEAT" \
  "SPREAD_WORKERS=$SPREAD_WORKERS ENGINES=$ENG FRONTIER_LEAD=${FRONTIER_LEAD:-none}" \
  "SEQ_REPEAT=${SEQ_REPEAT:-3}" | tee -a "$OUT/summary.txt"

cores() {
  python3 - "$@" <<'PY'
import os, sys

def read(path):
    try:
        with open(path) as f:
            return f.read().strip()
    except OSError:
        return None

seen, groups = set(), {}
for c in sorted(os.sched_getaffinity(0)):
    base = f"/sys/devices/system/cpu/cpu{c}"
    core = (read(base + "/topology/physical_package_id"), read(base + "/topology/core_id"))
    if None not in core:
        if core in seen:
            continue  # a second hardware thread of a core
        seen.add(core)
    l3 = read(base + "/cache/index3/shared_cpu_list") or "all"
    groups.setdefault(l3, []).append(c)
order = sorted(groups.values(), key=lambda g: g[0])
if len(sys.argv) < 3:
    for g in order:
        print(f"L3 group of {len(g)} cores: {','.join(map(str, g))}")
    sys.exit(0)
mode, p = sys.argv[1], int(sys.argv[2])
if mode == "compact":
    cs = [c for g in order for c in g]
else:
    cs = [g[k] for k in range(max(map(len, order))) for g in order if k < len(g)]
if p > len(cs):
    sys.exit(f"{p} workers but only {len(cs)} allowed cores")
print(",".join(map(str, cs[:p])))
PY
}
cores > "$OUT/topology.txt"
awk '{n += $4} END {print NR " L3 groups, " n " allowed cores"}' "$OUT/topology.txt" | tee -a "$OUT/summary.txt"

one_item() {
  local name=$1 maxw=$2 fc=$OUT/$1.compact.jsonl fs=$OUT/$1.spread.jsonl e=$OUT/$1.err
  local configs=() p r k n c mode f cs
  shift 2
  [[ $maxw =~ ^[0-9]+$ ]] || maxw=0
  for p in ${WORKERS//,/ }; do
    if ((maxw == 0 || p <= maxw)); then configs+=("compact:$p"); fi
  done
  for p in ${SPREAD_WORKERS//,/ }; do
    if ((maxw == 0 || p <= maxw)); then configs+=("spread:$p"); fi
  done
  n=${#configs[@]}
  echo "== $name, ${configs[*]} $(date -u +%T)" | tee -a "$OUT/summary.txt"
  : > "$fs"
  if ! cs=$(cores compact 1 2>> "$e") || ! taskset -c "$cs" "$@" --engine seq --workers 1 \
      --repeat "${SEQ_REPEAT:-3}" --pin --check > "$fc" 2>> "$e" < /dev/null; then
    bench_fail "$OUT" "$name: seq failed: $(tail -1 "$e")"
    return
  fi
  for ((r = 0; r < REPEAT; r++)); do
    for ((k = 0; k < n; k++)); do
      c=${configs[$(( r % 2 == 0 ? k : n - 1 - k ))]}
      mode=${c%%:*}
      p=${c#*:}
      f=$fc
      [[ $mode == spread ]] && f=$fs
      if ! cs=$(cores "$mode" "$p" 2>> "$e") || ! taskset -c "$cs" "$@" --engine "$ENG" \
          ${LEAD_ARGS[@]+"${LEAD_ARGS[@]}"} --workers "$p" --pin --check >> "$f" 2>> "$e" < /dev/null; then
        bench_fail "$OUT" "$name: $mode on $p workers failed: $(tail -1 "$e")"
        return
      fi
    done
  done
  summarize "$fc" "$OUT"
  [[ -s $fs ]] && summarize "$fs" "$OUT"
  if [[ $(cat "$fc" "$fs" | grep -o '"hash":"[0-9a-f]*"' | sort -u | wc -l) != 1 ]]; then
    bench_fail "$OUT" "$name: compact and spread give different hashes"
  fi
}

while read -r -u 3 a b c d e _; do
  [[ -z ${a:-} || $a == \#* ]] && continue
  if [[ $a == fleet ]]; then
    one_item "fleet-c$b-s$c" "${e:-0}" "$BIN/let_ros_fleet" --cars "$b" --offset-step "$c" --horizon "$d"
  else
    bench_fail "$OUT" "$a $b: unsupported workload; this script accepts only fleet entries"
  fi
done 3< "$LIST"
bench_exit "$OUT"
