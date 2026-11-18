#!/usr/bin/env python3
import json
import statistics
import sys
from collections import defaultdict

def label(r):
    eng = r["engine"]
    if eng == "static" and r.get("cost_model") == "callback":
        eng = "static-cb"
    name = eng + ("-full" if r.get("ring_inf") else "")
    if not r.get("ring_inf") and r.get("ring_scale", 1) > 1:
        name += f"-x{r['ring_scale']}"
    if r["engine"] == "frontier" and r.get("lead_ns", -1) > 0:
        name += f"-L{r['lead_ns'] / 1e6:g}ms"
    if r.get("sched", 0) == 1:
        name += "-v2"
    return name

def med_or_dash(values, fmt):
    return fmt.format(statistics.median(values)) if values else "-"

def main(paths):
    rows = []
    for p in paths:
        with open(p) as f:
            rows += [json.loads(line) for line in f if line.startswith("{")]
    if not rows:
        print("no results")
        return 1
    times = defaultdict(list)
    setup = defaultdict(list)
    extra = defaultdict(list)
    mem = defaultdict(list)
    rss = defaultdict(list)
    hashes = set()
    for r in rows:
        key = (label(r), r["workers"])
        times[key].append(r["run_s"])
        setup[key].append(r.get("setup_s", 0.0))
        mem[key].append(r.get("payload_bytes", 0))
        if r.get("rss_peak_mb", -1) >= 0:
            rss[key].append(r["rss_peak_mb"] - r["rss_before_mb"])
        if r["engine"] == "frontier":
            extra[key].append(r.get("nulls", 0))
        elif r["engine"].startswith("static"):
            extra[key].append(r.get("est_s", 0.0))
        hashes.add(r["hash"])
    med = {k: statistics.median(v) for k, v in times.items()}
    seq = med.get(("seq", 1))
    one = [t for (eng, w), t in med.items() if w == 1]
    best1 = min(one) if one else None
    r0 = rows[0]
    cb = ""
    if "timer_callbacks" in r0:
        cb = f"  callbacks {r0['timer_callbacks']} timer + {r0['sub_callbacks']} subscription"
    print(f"tasks {r0['tasks']}  jobs {r0['jobs']}  active {r0['active_jobs']}{cb}  "
          f"messages {r0['messages']}  horizon {r0['horizon_ns'] / 1e9:g}s")
    print(f"{'engine':22} {'workers':>7} {'runs':>5} {'median s':>9} {'setup s':>8} "
          f"{'vs seq':>7} {'vs 1w':>6} {'/ ooo':>6} {'payload MB':>10} {'rss +MB':>8}  note")
    order = ["seq", "ooo", "ooo-x2", "ooo-x4", "ooo-x8", "ooo-x16", "ooo-full", "barrier", "tag",
             "frontier", "static", "static-cb"]

    def rank(k):
        base = k[0].split("-L")[0]
        return (order.index(base) if base in order else len(order), k[0], k[1])

    for (eng, w) in sorted(med, key=rank):
        t = med[(eng, w)]
        sp = f"{seq / t:.2f}" if seq else "-"
        sp1 = f"{best1 / t:.2f}" if best1 else "-"
        ratio = ""
        if eng not in ("seq", "ooo") and ("ooo", w) in med:
            ratio = f"{t / med[('ooo', w)]:.2f}"
        note = ""
        if eng.startswith("frontier"):
            note = f"nulls {statistics.median(extra[(eng, w)]):.0f}"
        elif eng.startswith("static"):
            note = f"est {statistics.median(extra[(eng, w)]):.3f} s"
        mb = statistics.median(mem[(eng, w)]) / 1e6
        print(f"{eng:22} {w:7d} {len(times[(eng, w)]):5d} {t:9.3f} "
              f"{med_or_dash(setup[(eng, w)], '{:.3f}'):>8} {sp:>7} {sp1:>6} {ratio:>6} "
              f"{mb:10.2f} {med_or_dash(rss[(eng, w)], '{:.1f}'):>8}  {note}")
    print("hash:", "all runs equal" if len(hashes) == 1 else f"{len(hashes)} DIFFERENT hashes")
    return 0 if len(hashes) == 1 else 1

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
