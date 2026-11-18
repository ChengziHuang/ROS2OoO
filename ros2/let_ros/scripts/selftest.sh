#!/usr/bin/env bash
set -uo pipefail
source "$(dirname "$(readlink -f "$0")")/common.sh"
BIN=$(pkg_prefix let_ros)/lib/let_ros || exit 1
GDIR=$(pkg_prefix let_ros_test_nodes)/share/let_ros_test_nodes/graphs || exit 1
SETS=$(pkg_prefix let_ros)/share/let_ros/config/autoware_publish_sets.txt
TMP=$(mktemp -d)
trap 'rm -rf "$TMP"' EXIT
export ROS_LOG_DIR=${ROS_LOG_DIR:-$TMP/log}
failed=0

ok() { echo "PASS  $1"; }
bad() { echo "FAIL  $1"; failed=1; }

field() { grep -o "\"$1\":[^,}]*" "$2" | sort -u | tr '\n' ' '; }

run() {
  local name=$1
  shift
  "$@" > "$TMP/$name.jsonl" 2> "$TMP/$name.err"
}

same() {
  local ha hb ma mb ca cb
  ha=$(field hash "$TMP/$1.jsonl"); hb=$(field hash "$TMP/$2.jsonl")
  ma=$(field messages "$TMP/$1.jsonl"); mb=$(field messages "$TMP/$2.jsonl")
  ca="$(field timer_callbacks "$TMP/$1.jsonl")$(field sub_callbacks "$TMP/$1.jsonl")"
  cb="$(field timer_callbacks "$TMP/$2.jsonl")$(field sub_callbacks "$TMP/$2.jsonl")"
  if [ -n "$ha" ] && [ "$ha" = "$hb" ] && [ "$ma" = "$mb" ] && [ "$ca" = "$cb" ] &&
      [ "$(wc -w <<< "$ha$ca")" = 3 ]; then
    ok "$3 ($ha$ma)"
  else
    bad "$3 (hashes $ha/ $hb, messages $ma/ $mb, callbacks $ca/ $cb)"
  fi
}

ALL=(--engine seq,barrier,tag,frontier,static,ooo --workers 1,2 --horizon 2s --check)

run lazy_connections "$BIN/let_ros_run" --nodes "$GDIR/lazy_connections.txt" \
  --initialize-components --require-output-topic /result "${ALL[@]}" ||
  bad "lazy subscriptions: $(tail -1 "$TMP/lazy_connections.err")"
if run lazy_uninitialized "$BIN/let_ros_run" --nodes "$GDIR/lazy_connections.txt" \
    --require-output-topic /result --default-deadline 10ms --engine seq --horizon 200ms; then
  bad "uninitialized lazy graph must fail its output requirement"
elif grep -q 'required output topic' "$TMP/lazy_uninitialized.err"; then
  ok "uninitialized lazy graph is rejected"
else
  bad "unexpected lazy graph failure: $(tail -1 "$TMP/lazy_uninitialized.err")"
fi
if [ -s "$TMP/lazy_connections.jsonl" ] && grep -q 'CHECK OK' "$TMP/lazy_connections.err"; then
  ok "lazy subscriptions, all engines, nonempty output"
fi

sed 's/ipc:=true/ipc:=false/g' "$GDIR/hooks.txt" > "$TMP/hooks_plain.txt"
sed 's/ipc:=true/ipc:=false/g' "$GDIR/ipc_subscription.txt" > "$TMP/ipcsub_plain.txt"
for m in streams calendar; do
  run "hooks_$m" "$BIN/let_ros_run" --nodes "$GDIR/hooks.txt" --model $m "${ALL[@]}" ||
    bad "hooks graph, $m model: $(tail -1 "$TMP/hooks_$m.err")"
done
run hooks_plain "$BIN/let_ros_run" --nodes "$TMP/hooks_plain.txt" "${ALL[@]}" ||
  bad "hooks graph without intra-process: $(tail -1 "$TMP/hooks_plain.err")"
same hooks_streams hooks_calendar "hooks graph, streams = calendar, all engines"
same hooks_streams hooks_plain "hooks graph, intra-process on = off"
run ipcsub "$BIN/let_ros_run" --nodes "$GDIR/ipc_subscription.txt" "${ALL[@]}" ||
  bad "intra-process chain: $(tail -1 "$TMP/ipcsub.err")"
run ipcsub_plain "$BIN/let_ros_run" --nodes "$TMP/ipcsub_plain.txt" "${ALL[@]}" ||
  bad "chain without intra-process: $(tail -1 "$TMP/ipcsub_plain.err")"
same ipcsub ipcsub_plain "intra-process chain, on = off"

MIXED0=(--nodes "$GDIR/mixed.txt" --publish-sets "$GDIR/mixed_publish_sets.txt" --horizon 2s --check)
MIXED=("${MIXED0[@]}" "${ALL[@]}")
for m in streams calendar; do
  run "mixed_$m" "$BIN/let_ros_run" --model $m "${MIXED[@]}" ||
    bad "mixed graph, $m model: $(tail -1 "$TMP/mixed_$m.err")"
done
run mixed_ipc "$BIN/let_ros_run" --intra-process "${MIXED[@]}" ||
  bad "mixed graph, --intra-process: $(tail -1 "$TMP/mixed_ipc.err")"
same mixed_streams mixed_calendar "mixed graph, streams = calendar, all engines"
same mixed_streams mixed_ipc "mixed graph, intra-process on = off"

base_opts() {
  "$BIN/let_ros_run" "${MIXED0[@]}" --engine frontier --lead 5ms --workers 2,3 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" --engine static --cost-model callback --workers 3 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" --engine ooo,barrier --ring inf --workers 2 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" --engine ooo,barrier --ring-scale 3 --workers 2 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" --model calendar --engine ooo --ring-scale 4 --workers 2 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" --engine barrier --window 1ms --workers 2
}
run base_opts base_opts || bad "baseline options: $(tail -1 "$TMP/base_opts.err")"
same mixed_streams base_opts "mixed graph, lead, callback costs, ring inf, ring scale, window"
run leads "$BIN/let_ros_run" "${MIXED0[@]}" --engine ooo,frontier --lead inf,5ms --workers 2 \
  --repeat 2 --interleave || bad "frontier with two leads: $(tail -1 "$TMP/leads.err")"
if [ "$(grep -c '"engine":"frontier".*"lead_ns":-1,' "$TMP/leads.jsonl")" = 2 ] &&
    [ "$(grep -c '"engine":"frontier".*"lead_ns":5000000,' "$TMP/leads.jsonl")" = 2 ] &&
    [ "$(grep -c '"engine":"ooo"' "$TMP/leads.jsonl")" = 2 ]; then
  ok "--lead inf,5ms runs frontier once per lead"
else
  bad "--lead inf,5ms ($(grep -c '^{' "$TMP/leads.jsonl") lines)"
fi
same mixed_streams leads "mixed graph, frontier with two leads"
run sched "$BIN/let_ros_run" "${MIXED0[@]}" --engine ooo,frontier --sched old,new --lead inf,5ms \
  --ring-scale 1,4 --workers 1,3 --repeat 2 --interleave ||
  bad "two schedulers: $(tail -1 "$TMP/sched.err")"
if [ "$(grep -c '"engine":"ooo".*"sched":0' "$TMP/sched.jsonl")" = 8 ] &&
    [ "$(grep -c '"engine":"ooo".*"sched":1' "$TMP/sched.jsonl")" = 8 ] &&
    [ "$(grep -c '"engine":"ooo".*"ring_scale":4,.*"sched":1' "$TMP/sched.jsonl")" = 4 ] &&
    [ "$(grep -c '"engine":"frontier".*"ring_scale":1,.*"sched":1' "$TMP/sched.jsonl")" = 8 ]; then
  ok "--sched old,new and --ring-scale 1,4 run once per scheduler and scale"
else
  bad "--sched old,new --ring-scale 1,4 ($(grep -c '^{' "$TMP/sched.jsonl") lines)"
fi
same mixed_streams sched "mixed graph, old and new scheduler"
run sched_cal "$BIN/let_ros_run" "${MIXED0[@]}" --model calendar --engine ooo --sched new \
  --ring-scale 4 --workers 2,3 --repeat 3 || bad "new scheduler, calendar: $(tail -1 "$TMP/sched_cal.err")"
same mixed_streams sched_cal "mixed graph, new scheduler, calendar model, ring scale 4"
run calib "$BIN/let_ros_run" "${MIXED0[@]}" --engine seq,static --workers 2 \
  --record-costs "$TMP/costs.txt" || bad "calibration: $(tail -1 "$TMP/calib.err")"
if [ "$(grep -c '^{' "$TMP/calib.jsonl")" = 2 ] &&
    grep -q '"engine":"static".*"cost_model":"job","costs_source":"calibration"' "$TMP/calib.jsonl" &&
    grep -q "calibration run give the same hash" "$TMP/calib.err" && [ -s "$TMP/costs.txt" ]; then
  ok "static costs from a calibration run that is not reported"
else
  bad "calibration run ($(grep -c '^{' "$TMP/calib.jsonl") lines, $(tail -1 "$TMP/calib.err"))"
fi
rm -f "$TMP/costs.txt"
if "$BIN/let_ros_run" "${MIXED0[@]}" --engine ooo --workers 2 --record-costs "$TMP/costs.txt" \
    > /dev/null 2> "$TMP/rc.err" && [ -s "$TMP/costs.txt" ]; then
  ok "--record-costs without seq writes the costs"
else
  bad "--record-costs without seq: $(tail -1 "$TMP/rc.err")"
fi
run front_plan "$BIN/let_ros_run" "${MIXED0[@]}" --engine frontier --workers 2 --max-streams 1 \
  --describe || bad "frontier with --max-streams 1: $(tail -1 "$TMP/front_plan.err")"
same mixed_streams front_plan "frontier without the job plan (--max-streams 1, --describe)"
if grep -q '"rss_before_mb":[0-9.]*,"rss_peak_mb":[0-9.]*' "$TMP/front_plan.jsonl" &&
    python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; sys.exit(0 if all(x["rss_peak_mb"]>=x["rss_before_mb"]>0 for x in r) else 1)' \
    "$TMP/front_plan.jsonl"; then
  ok "resident memory before and at the peak of each run"
else
  bad "resident memory fields missing or wrong: $(grep -o '"rss[^,]*' "$TMP/front_plan.jsonl" | head -2 | tr '\n' ' ')"
fi
echo "earlier file" > "$TMP/dag.txt"
LET_ROS_DAG="$TMP/dag.txt" "$BIN/let_ros_run" "${MIXED0[@]}" --model calendar --engine frontier \
  --workers 2 > "$TMP/dag_front.jsonl" 2> "$TMP/dag_front.err"
if [ "$(cat "$TMP/dag.txt")" = "earlier file" ] && grep -q "LET_ROS_DAG is not written" "$TMP/dag_front.err" &&
    LET_ROS_DAG="$TMP/dag.txt" "$BIN/let_ros_run" "${MIXED0[@]}" --model calendar --engine seq \
      > "$TMP/dag_seq.jsonl" 2> /dev/null &&
    [ "$(wc -l < "$TMP/dag.txt")" = "$(field jobs "$TMP/dag_seq.jsonl" | grep -o '[0-9]*')" ]; then
  ok "LET_ROS_DAG written by a calendar run, left alone by frontier"
else
  bad "LET_ROS_DAG: $(tail -1 "$TMP/dag_front.err")"
fi
if [ -x "$BIN/let_ros_fleet" ]; then
  if OUT="$TMP/sweeps" CARS=1 ENGINES=seq SWEEPS="d timing" SCALES=1 TIMINGS=default \
        bash "$BIN/bench_sweeps.sh" 1 100ms 1 > /dev/null 2>&1 &&
      OUT="$TMP/sweeps" CARS=1 ENGINES=seq SWEEPS="d timing" SCALES=1 TIMINGS=default \
        bash "$BIN/bench_sweeps.sh" 1 200ms 1 > /dev/null 2>&1 &&
      [ "$(field horizon_ns "$TMP/sweeps/dscale_1.jsonl")" = '"horizon_ns":200000000 ' ] &&
      [ "$(field horizon_ns "$TMP/sweeps/timing_default.jsonl")" = '"horizon_ns":200000000 ' ]; then
    ok "bench_sweeps.sh shares the default configuration only within one call"
  else
    bad "bench_sweeps.sh reuses a run of an earlier call ($(field horizon_ns "$TMP/sweeps/dscale_1.jsonl" 2>/dev/null))"
  fi
  if OUT="$TMP/sweeps_bad" CARS=1 ENGINES=seq SWEEPS="d timing" SCALES=1 TIMINGS="default none" \
      bash "$BIN/bench_sweeps.sh" 1 100ms 1 > /dev/null 2> "$TMP/sweeps_bad.err"; then
    bad "bench_sweeps.sh with a failing configuration exits with 0"
  elif grep -q "timing_none" "$TMP/sweeps_bad/FAILED" 2>/dev/null &&
      [ -s "$TMP/sweeps_bad/dscale_1.jsonl" ]; then
    ok "a benchmark script with a failing configuration exits with 1 and runs the others"
  else
    bad "bench_sweeps.sh with a failing configuration: $(tail -1 "$TMP/sweeps_bad.err")"
  fi
  if OUT="$TMP/bounds" STEPS=37ms PROCS=1,4 bash "$BIN/bench_bounds.sh" 1 300ms > /dev/null 2>&1 &&
      python3 -c 'import json,sys; r=[json.loads(l) for l in open(sys.argv[1])]; sys.exit(0 if len(r)==2 and all(x["bound"]>1 and x["speedup"]["4"]["ooo"]>=1 for x in r) else 1)' \
        "$TMP/bounds/bounds.jsonl"; then
    ok "bench_bounds.sh, work over span of the reference system and of a fleet"
  else
    bad "bench_bounds.sh: $(tail -1 "$TMP/bounds/summary.txt" 2>/dev/null)"
  fi
  if OUT="$TMP/model" CARS=1 HORIZONS="200ms 400ms" bash "$BIN/bench_model.sh" 1,2 1 \
      > /dev/null 2>&1 && [ "$(ls "$TMP"/model/model_*.jsonl | wc -l)" = 4 ]; then
    ok "bench_model.sh, streams and calendar model at two horizons"
  else
    bad "bench_model.sh: $(tail -1 "$TMP/model/summary.txt" 2>/dev/null)"
  fi
else
  echo "SKIP  bench_sweeps.sh, bench_bounds.sh, bench_model.sh (need let_ros_fleet, built only with reference_system)"
fi
run cycle "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --horizon 1s --check \
  --engine seq,tag,frontier,static,ooo --workers 2 ||
  bad "mixed graph without publish sets: $(tail -1 "$TMP/cycle.err")"
run cycle_cal "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --horizon 1s --model calendar \
  --engine seq --workers 1 || bad "calendar: $(tail -1 "$TMP/cycle_cal.err")"
same cycle cycle_cal "mixed graph with its feedback cycle, all engines = calendar"

LINKS=(--random-link-delays 8ms:7)
for m in streams calendar; do
  run "links_$m" "$BIN/let_ros_run" --model $m "${MIXED[@]}" "${LINKS[@]}" ||
    bad "mixed graph with link delays, $m model: $(tail -1 "$TMP/links_$m.err")"
done
same links_streams links_calendar "mixed graph with random link delays, streams = calendar, all engines"
if [ -n "$(field hash "$TMP/links_streams.jsonl")" ] &&
    [ "$(field hash "$TMP/links_streams.jsonl")" != "$(field hash "$TMP/mixed_streams.jsonl")" ] &&
    grep -q '"delayed_channels":[1-9]' "$TMP/links_streams.jsonl"; then
  ok "random link delays change the result ($(field delayed_channels "$TMP/links_streams.jsonl"))"
else
  bad "random link delays have no effect ($(field delayed_channels "$TMP/links_streams.jsonl"))"
fi
link_opts() {
  "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --engine frontier --lead 5ms --workers 2,3 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --engine static --cost-model callback --workers 3 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --engine ooo,barrier --ring inf --workers 2 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --engine ooo,frontier --sched new --workers 2,3 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --engine barrier --window 1ms --workers 2 &&
    "$BIN/let_ros_run" "${MIXED0[@]}" "${LINKS[@]}" --model calendar --engine ooo --sched new --workers 3
}
run link_opts link_opts || bad "link delays, baseline options: $(tail -1 "$TMP/link_opts.err")"
same links_streams link_opts "random link delays, lead, callback costs, ring inf, new scheduler, window"
run link_zero "$BIN/let_ros_run" "${MIXED0[@]}" --link-delay 0ms --engine seq,ooo --workers 2 ||
  bad "link delay 0: $(tail -1 "$TMP/link_zero.err")"
same mixed_streams link_zero "a link delay of 0 equals no link delay"
run link_uni "$BIN/let_ros_run" "${MIXED[@]}" --link-delay 3ms ||
  bad "uniform link delay: $(tail -1 "$TMP/link_uni.err")"
run link_uni_cal "$BIN/let_ros_run" --model calendar "${MIXED0[@]}" --link-delay 3ms --engine seq ||
  bad "uniform link delay, calendar: $(tail -1 "$TMP/link_uni_cal.err")"
same link_uni link_uni_cal "uniform link delay 3ms, all engines = calendar"
cat > "$TMP/links.txt" <<'EOF'
# topic for every subscription, topic and fully qualified node, topic and node name
/c: 4ms
/f /gate: 2ms
/loop sampler: 7ms
EOF
run link_file "$BIN/let_ros_run" "${MIXED[@]}" --link-delays "$TMP/links.txt" ||
  bad "link delay file: $(tail -1 "$TMP/link_file.err")"
run link_file_cal "$BIN/let_ros_run" --model calendar "${MIXED0[@]}" --link-delays "$TMP/links.txt" \
  --engine seq || bad "link delay file, calendar: $(tail -1 "$TMP/link_file_cal.err")"
same link_file link_file_cal "link delay file, all engines = calendar"
cat > "$TMP/stamp_links.txt" <<'EOF'
/samples: 9ms
/samples fast_stamp: 0ms
/samples /slow_stamp: 35ms
EOF
run transport_stamps "$BIN/let_ros_run" --nodes "$GDIR/transport_stamps.txt" \
  --link-delays "$TMP/stamp_links.txt" --horizon 60ms --engine seq,barrier,tag,frontier,static,ooo \
  --workers 1,2 --require-output-topic /fast_stamps --require-output-topic /slow_stamps --check ||
  bad "transport timestamps: $(tail -1 "$TMP/transport_stamps.err")"
run transport_stamps_cal "$BIN/let_ros_run" --nodes "$GDIR/transport_stamps.txt" \
  --link-delays "$TMP/stamp_links.txt" --horizon 60ms --model calendar --engine seq ||
  bad "transport timestamps, calendar: $(tail -1 "$TMP/transport_stamps_cal.err")"
same transport_stamps transport_stamps_cal "transport timestamps and shadowed defaults, all engines = calendar"
if [ "$(field jobs "$TMP/transport_stamps.jsonl")" = '"jobs":10 ' ] &&
    [ "$(field messages "$TMP/transport_stamps.jsonl")" = '"messages":10 ' ] &&
    [ "$(field sub_callbacks "$TMP/transport_stamps.jsonl")" = '"sub_callbacks":5 ' ]; then
  ok "transport horizon excludes arrivals at H, with C greater than a period"
else
  bad "transport horizon: $(field jobs "$TMP/transport_stamps.jsonl")$(field messages "$TMP/transport_stamps.jsonl")$(field sub_callbacks "$TMP/transport_stamps.jsonl")"
fi
if grep -q '"delayed_channels":4,' "$TMP/link_file.jsonl" &&
    grep -q '"max_link_delay_ns":7000000,' "$TMP/link_file.jsonl"; then
  ok "link delay file sets the 4 matching connections, the largest 7ms"
else
  bad "link delay file: $(field delayed_channels "$TMP/link_file.jsonl")$(field max_link_delay_ns "$TMP/link_file.jsonl")"
fi
run link_cycle "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --horizon 1s --check "${LINKS[@]}" \
  --engine seq,tag,frontier,static,ooo --workers 2 ||
  bad "feedback cycle with link delays: $(tail -1 "$TMP/link_cycle.err")"
run link_cycle_cal "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --horizon 1s --model calendar \
  "${LINKS[@]}" --engine seq || bad "feedback cycle with link delays, calendar: $(tail -1 "$TMP/link_cycle_cal.err")"
same link_cycle link_cycle_cal "feedback cycle with random link delays, all engines = calendar"

run jitter "$BIN/let_ros_run" --nodes "$GDIR/mixed_jitter.txt" \
  --publish-sets "$GDIR/mixed_publish_sets.txt" --horizon 300ms --check \
  --engine seq,frontier,static,ooo --workers 2 ||
  bad "data-dependent costs: $(tail -1 "$TMP/jitter.err")"
run jitter_cb "$BIN/let_ros_run" --nodes "$GDIR/mixed_jitter.txt" \
  --publish-sets "$GDIR/mixed_publish_sets.txt" --horizon 300ms --check \
  --engine static --cost-model callback --workers 2 ||
  bad "data-dependent costs, callback costs: $(tail -1 "$TMP/jitter_cb.err")"
same jitter jitter_cb "data-dependent costs, all engines, both static cost models"
run dscale "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" \
  --publish-sets "$GDIR/mixed_publish_sets.txt" --horizon 500ms --check --d-scale 2.5 \
  --engine seq,barrier,frontier,static,ooo --workers 2 ||
  bad "D sweep: $(tail -1 "$TMP/dscale.err")"
run dscale_cal "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --model calendar \
  --publish-sets "$GDIR/mixed_publish_sets.txt" --horizon 500ms --d-scale 2.5 --engine seq ||
  bad "D sweep, calendar: $(tail -1 "$TMP/dscale_cal.err")"
same dscale dscale_cal "D times 2.5, all engines = calendar"

for a in "--repeat 0" "--workers ," "--workers 0" "--engine ," "--horizon 0s" \
  "--horizon -1s" "--max-streams 0" "--timer-phase Zero" "--default-deadline -5ms" \
  "--horizon nan" "--engine barrier --window 1s" "--ring two" "--ring-scale 0" "--lead 0ms" \
  "--lead inf,0ms" "--lead ," "--sched fast" "--sched ," "--ring-scale 1,0" \
  "--cost-model wcet" "--d-scale 0" "--d-scale -1" "--offsets /nonexistent" \
  "--link-delay -1ms" "--random-link-delays 0ms" "--random-link-delays 5ms:x" \
  "--link-delays /nonexistent"; do
  if "$BIN/let_ros_run" --nodes "$GDIR/hooks.txt" --check $a > /dev/null 2> "$TMP/arg.err"; then
    bad "argument $a is accepted"
  elif grep -q "CHECK OK" "$TMP/arg.err"; then
    bad "argument $a reports CHECK OK"
  else
    ok "argument $a is rejected: $(tail -1 "$TMP/arg.err")"
  fi
done
for line in "/a 1ms" ": 1ms"; do
  echo "$line" > "$TMP/links_malformed.txt"
  if "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --link-delays "$TMP/links_malformed.txt" \
    --engine seq --horizon 100ms > /dev/null 2> "$TMP/arg.err"; then
    bad "malformed link delay '$line' is accepted"
  else
    ok "malformed link delay '$line' is rejected: $(tail -1 "$TMP/arg.err")"
  fi
done
echo "nonode: 5ms" > "$TMP/deadlines.txt"
if "$BIN/let_ros_run" --nodes "$GDIR/hooks.txt" --deadlines "$TMP/deadlines.txt" \
  > /dev/null 2> "$TMP/arg.err"; then
  bad "deadline for an unknown node is accepted"
else
  ok "deadline for an unknown node is rejected: $(tail -1 "$TMP/arg.err")"
fi
for line in "/nosuchtopic: 1ms" "/a /gate: 1ms"; do
  echo "$line" > "$TMP/links_bad.txt"
  if "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" --link-delays "$TMP/links_bad.txt" \
    > /dev/null 2> "$TMP/arg.err"; then
    bad "link delay '$line' is accepted"
  else
    ok "link delay '$line' is rejected: $(tail -1 "$TMP/arg.err")"
  fi
done

if [ -x "$BIN/let_ros_fleet" ]; then
  FLEET=(--cars 3 --v2x 1 --engine seq,tag,frontier,static,ooo --workers 2 --horizon 1s --check)
  run fleet_neg "$BIN/let_ros_fleet" "${FLEET[@]}" --offset-step -37ms ||
    bad "fleet, offset step -37ms: $(tail -1 "$TMP/fleet_neg.err")"
  run fleet_pos "$BIN/let_ros_fleet" "${FLEET[@]}" --offset-step 63ms ||
    bad "fleet, offset step 63ms: $(tail -1 "$TMP/fleet_pos.err")"
  same fleet_neg fleet_pos "fleet, offset step -37ms = 63ms"
  run fleet_param "$BIN/let_ros_fleet" "${FLEET[@]}" --offset-step 63ms --param-services ||
    bad "fleet, --param-services: $(tail -1 "$TMP/fleet_param.err")"
  same fleet_pos fleet_param "fleet, lean nodes = nodes with parameter services"
  run fleet_v2x "$BIN/let_ros_fleet" "${FLEET[@]}" --offset-step 63ms --v2x-delay 50ms ||
    bad "fleet, --v2x-delay 50ms: $(tail -1 "$TMP/fleet_v2x.err")"
  run fleet_v2x_cal "$BIN/let_ros_fleet" --cars 3 --v2x 1 --engine seq --horizon 1s \
    --offset-step 63ms --v2x-delay 50ms --model calendar ||
    bad "fleet, --v2x-delay 50ms, calendar: $(tail -1 "$TMP/fleet_v2x_cal.err")"
  same fleet_v2x fleet_v2x_cal "fleet with V2X delay 50ms, all engines = calendar"
  if grep -q '"delayed_channels":6,' "$TMP/fleet_v2x.jsonl" &&
      [ "$(field hash "$TMP/fleet_v2x.jsonl")" != "$(field hash "$TMP/fleet_pos.jsonl")" ]; then
    ok "fleet V2X delay sets the 6 neighbour connections and changes the result"
  else
    bad "fleet V2X delay: $(field delayed_channels "$TMP/fleet_v2x.jsonl")"
  fi
  if "$BIN/let_ros_fleet" --cars 0 > /dev/null 2> "$TMP/arg.err"; then
    bad "fleet accepts --cars 0"
  else
    ok "fleet rejects --cars 0: $(tail -1 "$TMP/arg.err")"
  fi
  if [ -f "$SETS" ]; then
    if run autoware "$BIN/let_ros_autoware" --engine seq,barrier,tag,frontier,static,ooo \
      --workers 2 --horizon 1s --check --publish-sets "$SETS"; then
      ok "Autoware reference system, all engines ($(field hash "$TMP/autoware.jsonl"))"
    else
      bad "Autoware reference system: $(tail -1 "$TMP/autoware.err")"
    fi
  fi
else
  echo "SKIP  fleet and Autoware (reference_system was not found at build time)"
fi

if run interleave "$BIN/let_ros_run" --nodes "$GDIR/mixed.txt" \
    --publish-sets "$GDIR/mixed_publish_sets.txt" --engine seq,ooo --workers 1,2 --repeat 2 \
    --interleave --horizon 500ms --check && [ "$(grep -c '^{' "$TMP/interleave.jsonl")" = 6 ]; then
  ok "--interleave: 6 runs, same hash ($(field hash "$TMP/interleave.jsonl"))"
else
  bad "--interleave: $(tail -1 "$TMP/interleave.err")"
fi

NATIVE=$(pkg_prefix let_ros_test_nodes)/lib/let_ros_test_nodes/native_conformance
if [ -x "$NATIVE" ]; then
  if python3 "$BIN/check_native_conformance.py" --native "$NATIVE" \
      --simulator "$BIN/let_ros_run" --out "$TMP/native" > "$TMP/native.log" 2>&1; then
    ok "native rclcpp/RMW matching, payload order and lossless-model boundaries"
  elif [ "${LET_ROS_NATIVE_OPTIONAL:-0}" = 1 ]; then
    echo "WARN  native conformance failed, not counted (LET_ROS_NATIVE_OPTIONAL=1): $(tail -4 "$TMP/native.log" | tr '\n' ' ')"
  else
    bad "native conformance: $(tail -4 "$TMP/native.log" | tr '\n' ' ')"
  fi
  grep -E ': (PASS|MISMATCH),' "$TMP/native.log" | sed 's/^/      native /' || true
else
  bad "native conformance runner is missing; rebuild let_ros_test_nodes"
fi

if [ "$failed" = 0 ]; then
  echo "all checks passed"
else
  echo "some checks FAILED"
fi
exit "$failed"
