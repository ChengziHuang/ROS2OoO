pkg_prefix() {
  local p
  for p in ${AMENT_PREFIX_PATH//:/ }; do
    if [ -e "$p/share/ament_index/resource_index/packages/$1" ]; then
      echo "$p"
      return 0
    fi
  done
  echo "package $1 not found; source the workspace first" >&2
  return 1
}

BENCH_FAILURES=0
bench_fail() {
  echo "FAILED: $2" | tee -a "$1/FAILED"
  BENCH_FAILURES=$((BENCH_FAILURES + 1))
}
bench_exit() {
  if [ "$BENCH_FAILURES" -gt 0 ]; then
    echo "$BENCH_FAILURES configuration(s) failed in this call, see $1/FAILED" >&2
    exit 1
  fi
}
SUMMARY_OK=0
summarize() {
  local text
  SUMMARY_OK=1
  if ! text=$(python3 "$BIN/summarize.py" "$1" 2>&1); then
    SUMMARY_OK=0
  fi
  printf '%s\n' "$text" | tee -a "$2/summary.txt"
  if [ "$SUMMARY_OK" = 0 ]; then
    bench_fail "$2" "$(basename "$1"): no results or different hashes"
  fi
}
