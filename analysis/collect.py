#!/usr/bin/env python3
"""Collect the results of a campaign into CSV files."""
import argparse
import csv
import datetime
import json
import re
import statistics
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from synth_data import Synth

RUN_FIELDS = [
    "task", "kind", "file", "suite", "workload", "cars", "step", "phase", "family", "work", "jitter",
    "sweep", "value", "model", "engine", "label", "workers", "horizon_s", "run_s", "setup_s",
    "node_setup_s",
    "tasks", "jobs", "active_jobs", "timer_callbacks", "sub_callbacks", "callbacks", "messages",
    "streams", "ring_slots", "payload_bytes", "rss_before_mb", "rss_peak_mb", "rss_delta_mb",
    "nulls", "est_s", "windows", "window_ns", "d_scale", "max_link_delay_ns", "delayed_channels",
    "cores", "pinned", "hash",
]
CONFIG_KEYS = ["task", "file", "kind", "suite", "workload", "cars", "step", "phase", "family", "work",
               "jitter", "sweep", "value", "model", "label", "workers"]

def label_of(r):
    eng = r.get("engine", "?")
    if eng == "static" and r.get("cost_model") == "callback":
        eng = "static-cb"
    name = eng + ("-full" if r.get("ring_inf") else "")
    if not r.get("ring_inf") and r.get("ring_scale", 1) > 1:
        name += f"-x{r['ring_scale']}"
    if r.get("engine") == "frontier" and r.get("lead_ns", -1) > 0:
        name += f"-L{r['lead_ns'] / 1e6:g}ms"
    if r.get("sched", 0) == 1:
        name += "-v2"
    return name

BATCH_FIELDS = ["task", "item", "workload", "strategy", "engine", "workers", "concurrent", "cores",
                "repeat", "scenarios", "passed", "makespan_s", "scenarios_per_hour",
                "median_latency_s", "max_latency_s", "run_s", "node_setup_s", "rss_peak_mb", "status"]
BATCH_RUN_FIELDS = ["task", "item", "workload", "strategy", "workers", "repeat", "scenario", "cpus",
                    "start_s", "end_s", "elapsed_s", "run_s", "setup_s", "node_setup_s", "rss_peak_mb",
                    "hash", "status"]

def batch_workload(item):
    m = re.fullmatch(r"fleet-c(\d+)-h.*", item)
    if m:
        return f"Fleet-{m[1]}"
    if item.startswith("autoware"):
        return "RS"
    m = re.fullmatch(r"(periodic|random|sweep)-(.+)", item)
    return m[2] if m else item

def read_batch(task, folder, problems):
    rows, runs = [], []
    for path in sorted(folder.glob("*/report.json")):
        item = path.parent.name
        try:
            report = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError) as exc:
            problems.append(f"{path}: {exc}")
            continue
        if report.get("status") != "passed":
            problems.append(f"{path}: status {report.get('status')}")
        wl = batch_workload(item)
        for b in report.get("batches", []):
            recs = b.get("records", [])
            results = [r.get("result") or {} for r in recs]

            def med(key):
                vals = [float(x[key]) for x in results if isinstance(x.get(key), (int, float))]
                return statistics.median(vals) if vals else ""

            rows.append({"task": task, "item": item, "workload": wl, "strategy": b.get("strategy_id"),
                         "engine": str(b.get("strategy_id", "")).split("-")[0],
                         "workers": b.get("workers_per_scenario"), "concurrent": b.get("max_concurrent"),
                         "cores": b.get("cpu_budget"), "repeat": b.get("repeat"),
                         "scenarios": b.get("scenarios_requested"), "passed": b.get("scenarios_passed"),
                         "makespan_s": b.get("makespan_s"), "scenarios_per_hour": b.get("scenarios_per_hour"),
                         "median_latency_s": b.get("median_process_latency_s"),
                         "max_latency_s": b.get("max_process_latency_s"), "run_s": med("run_s"),
                         "node_setup_s": med("node_setup_s"), "rss_peak_mb": med("rss_peak_mb"),
                         "status": b.get("status")})
            for r, x in zip(recs, results):
                runs.append({"task": task, "item": item, "workload": wl, "strategy": b.get("strategy_id"),
                             "workers": b.get("workers_per_scenario"), "repeat": b.get("repeat"),
                             "scenario": r.get("scenario_id"), "cpus": len(r.get("cpu_ids") or []),
                             "start_s": r.get("start_offset_s"), "end_s": r.get("completion_offset_s"),
                             "elapsed_s": r.get("elapsed_s"), "run_s": x.get("run_s", ""),
                             "setup_s": x.get("setup_s", ""), "node_setup_s": x.get("node_setup_s", ""),
                             "rss_peak_mb": x.get("rss_peak_mb", ""), "hash": x.get("hash", ""),
                             "status": r.get("status")})
            if b.get("status") != "passed":
                problems.append(f"{path}: batch {b.get('strategy_id')} round {b.get('repeat')}: "
                                f"{b.get('scenarios_passed')} of {b.get('scenarios_requested')} scenarios passed")
    return rows, runs

def task_kind(folder):
    kind = folder.split("-")[0]
    return {"baselines": "fleet"}.get(kind, kind)

def describe(kind, name, cars):
    stem = name[:-len(".jsonl")]
    d = {}
    if kind == "fleet":
        m = re.fullmatch(r"cars(\d+)_step(-?\d+(?:\.\d+)?m?s)", stem)
        if not m:
            return None
        d.update(workload=f"Fleet-{m[1]}", cars=int(m[1]), step=m[2])
    elif kind == "reference":
        names = {"autoware": "RS", "mixed": "Mixed", "mixed_heavy": "Mixed-heavy",
                 "mixed_jitter": "Mixed-jitter"}
        if stem not in names:
            return None
        d.update(workload=names[stem], cars=1 if stem == "autoware" else "")
    elif kind == "families":
        m = re.fullmatch(r"([a-z]+)_w(\d+)_j(\d+)", stem)
        if not m:
            return None
        d.update(workload=m[1], family=m[1], work=int(m[2]), jitter=int(m[3]))
    elif kind == "sweeps":
        m = re.fullmatch(r"(dscale|timing|sets|v2x)_(.+)", stem)
        if not m:
            return None
        d.update(workload=f"Fleet-{cars}", cars=cars,
                 sweep={"dscale": "d"}.get(m[1], m[1]), value=m[2])
    elif kind == "model":
        m = re.fullmatch(r"model_(streams|calendar)_(.+)", stem)
        if not m:
            return None
        d.update(workload=f"Fleet-{cars}", cars=cars, model_file=m[1], value=m[2])
    elif kind in ("rings", "leads", "node", "sched"):
        place = ""
        if kind == "node":
            m = re.fullmatch(r"(.+)\.(compact|spread)", stem)
            if not m:
                return None
            stem, place = m[1], m[2]
        m = re.fullmatch(r"fleet-c(\d+)-s(-?\d+(?:\.\d+)?m?s)", stem)
        if m:
            d.update(workload=f"Fleet-{m[1]}", cars=int(m[1]), step=m[2])
        elif stem == "autoware":
            d.update(workload="RS", cars=1)
        else:
            m = re.fullmatch(r"(periodic|random|sweep)-(.+)", stem)
            if not m:
                return None
            d.update(workload=m[2], suite=m[1])
        if place:
            d["value"] = place
    elif kind == "rslcpp":
        m = re.fullmatch(r"cars(\d+)", stem)
        if not m:
            return None
        n = int(m[1])
        d.update(workload="RS" if n == 0 else f"Fleet-{n}", cars=n)
    else:
        return None
    if d.get("step"):
        d["phase"] = "in phase" if re.fullmatch(r"-?0+(\.0+)?m?s", d["step"]) else "staggered"
    return d

def header_value(folder, key):
    p = folder / "summary.txt"
    if p.exists():
        with open(p, encoding="utf-8", errors="replace") as f:
            m = re.search(rf"\b{key}=(\S+)", f.readline())
        if m:
            return m[1]
    return None

def took(folder):
    try:
        env = (folder / "environment.txt").read_text(encoding="utf-8", errors="replace").splitlines()
        start = next(l[len("date: "):] for l in env if l.startswith("date: "))
        end = (folder / "DONE").read_text(encoding="utf-8", errors="replace").split()[0]
        t0, t1 = (datetime.datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ") for s in (start, end))
    except (OSError, StopIteration, IndexError, ValueError):
        return ""
    m = int((t1 - t0).total_seconds()) // 60
    return f" in {m // 60}h {m % 60:02d}m"

def settings_of(root):
    conf = {}
    p = root / "campaign.conf"
    if p.exists():
        for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                conf[k] = v.strip().strip('"')
    return conf

def task_folders(root):
    if (root / "tasks.txt").exists():
        out = []
        for line in (root / "tasks.txt").read_text(encoding="utf-8", errors="replace").splitlines():
            f = line.split()
            if len(f) >= 3:
                out.append((f[1], f[2], root / f[1]))
        return out
    return [(root.name, task_kind(root.name), root)]

def read_jsonl(path, problems):
    rows = []
    for n, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), 1):
        if line.startswith("{"):
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                problems.append(f"{path}: line {n} is not JSON")
    return rows

def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

def make_row(r, d, name, kind, path):
    row = {k: "" for k in RUN_FIELDS}
    row.update({k: v for k, v in d.items() if k in RUN_FIELDS})
    row.update(task=name, kind=kind, file=path.name)
    for k in RUN_FIELDS:
        if k in r and k not in ("task", "kind", "file", "workload", "cars", "suite", "family", "value",
                                "sweep"):
            row[k] = r[k]
    if kind == "rslcpp" and r.get("engine") == "rslcpp":
        row["tasks"] = r.get("tasks", "")
    row["model"] = r.get("model", d.get("model_file", ""))
    row["label"] = label_of(r)
    row["horizon_s"] = r.get("horizon_ns", 0) / 1e9
    if r.get("rss_peak_mb", -1) >= 0 and "rss_before_mb" in r:
        row["rss_delta_mb"] = r["rss_peak_mb"] - r["rss_before_mb"]
    if "callbacks" not in r and "timer_callbacks" in r:
        row["callbacks"] = r["timer_callbacks"] + r.get("sub_callbacks", 0)
    return row

def main(argv):
    ap = argparse.ArgumentParser(description=__doc__.split("\n")[0])
    ap.add_argument("root", type=Path, help="campaign folder or folder of one step")
    ap.add_argument("--out", type=Path, help="output folder (default <folder>/csv)")
    a = ap.parse_args(argv)
    root = a.root.resolve()
    if not root.is_dir():
        print(f"no folder {root}")
        return 2
    out = a.out or root / "csv"
    out.mkdir(parents=True, exist_ok=True)
    conf = settings_of(root)
    checks, problems = [], []
    runs, bounds, rslcpp_rows = [], [], []
    batch_rows, batch_runs = [], []
    synth = Synth(root)

    for name, kind, folder in task_folders(root):
        state = ("done" + took(folder) if (folder / "DONE").exists() else
                 "FAILED" if (folder / "FAILED").exists() else
                 "started, no DONE" if folder.exists() else "missing")
        checks.append(f"task {name:20s} {state}")
        if (folder / "FAILED").is_file():
            for line in (folder / "FAILED").read_text(encoding="utf-8", errors="replace").splitlines():
                checks.append(f"    {line}")
        if not folder.exists():
            continue
        if kind in ("synth", "fuzz", "frontier"):
            runs += synth.read_task(name, kind, folder, problems, read_jsonl, make_row)
            continue
        if kind == "batch":
            b, r = read_batch(name, folder, problems)
            batch_rows += b
            batch_runs += r
            continue
        if kind == "stereo":
            for sub in sorted(folder.glob("d*")):
                for path in sorted(sub.glob("*.jsonl")):
                    rows = read_jsonl(path, problems)
                    if not rows:
                        problems.append(f"{path}: no runs")
                        continue
                    if len({r.get("hash") for r in rows}) > 1:
                        problems.append(f"{path}: different hashes")
                    d = {"workload": "Stereo-4", "value": f"D={int(sub.name[1:]) / 1e6:.3g}ms"}
                    for r in rows:
                        row = make_row(r, d, name, kind, path)
                        row["file"] = f"{sub.name}/{path.name}"
                        runs.append(row)
            continue
        if kind == "empty":
            p = folder / "runs.jsonl"
            rows = read_jsonl(p, problems) if p.exists() else []
            if not rows:
                problems.append(f"{p}: no runs")
            graphs = defaultdict(list)
            for r in rows:
                graphs[(r.get("graph", "?"), r.get("pubsets", ""))].append(r)
            for (gid, ps), rs in graphs.items():
                if len({r.get("hash") for r in rs}) > 1:
                    problems.append(f"{p}: {gid} gives different hashes")
                d = {"workload": gid, "family": rs[0].get("family", ""), "value": ps}
                for r in rs:
                    row = make_row(r, d, name, kind, p)
                    row["file"] = f"{gid}.{ps}"
                    runs.append(row)
            continue
        if kind == "bounds":
            p = folder / "bounds.jsonl"
            if p.exists():
                for r in read_jsonl(p, problems):
                    row = {k: r.get(k, "") for k in ("workload", "cars", "step", "horizon", "jobs",
                                                     "work_s", "span_s", "bound", "window_ns", "dscale")}
                    for procs, sp in sorted(r.get("speedup", {}).items(), key=lambda kv: int(kv[0])):
                        for eng, v in sp.items():
                            row[f"{eng}_{procs}"] = v
                    bounds.append(row)
            continue
        conf_cars = conf.get({"sweeps": "SWEEP_CARS", "model": "MODEL_CARS"}.get(kind, ""), "8")
        cars = header_value(folder, "CARS") or conf_cars
        for path in sorted(folder.glob("*.jsonl")):
            d = describe(kind, path.name, cars)
            if d is None:
                continue
            rows = read_jsonl(path, problems)
            if not rows:
                problems.append(f"{path}: no runs")
                continue
            hashes = {r.get("hash") for r in rows if r.get("engine") != "rslcpp"}
            if len(hashes) > 1:
                problems.append(f"{path}: {len(hashes)} different hashes")
            cbs = {(r.get("timer_callbacks"), r.get("sub_callbacks")) for r in rows
                   if r.get("engine") != "rslcpp" and "timer_callbacks" in r}
            if len(cbs) > 1:
                problems.append(f"{path}: different callback counts {sorted(cbs)}")
            for r in rows:
                row = make_row(r, d, name, kind, path)
                runs.append(row)
                if kind == "rslcpp":
                    rslcpp_rows.append(row)

    groups = defaultdict(list)
    for r in runs:
        if r["kind"] == "rslcpp":
            continue
        groups[tuple(r[k] for k in CONFIG_KEYS)].append(r)
    meds = []
    for key, rs in groups.items():
        m = dict(zip(CONFIG_KEYS, key))
        times = [float(r["run_s"]) for r in rs]
        m.update(runs=len(rs), run_s=statistics.median(times), run_s_min=min(times),
                 run_s_max=max(times))
        for f in ("setup_s", "node_setup_s", "rss_delta_mb", "rss_peak_mb", "payload_bytes", "nulls",
                  "est_s"):
            vals = [fnum(r[f]) for r in rs if fnum(r[f]) is not None]
            m[f] = statistics.median(vals) if vals else ""
        for f in ("tasks", "jobs", "active_jobs", "timer_callbacks", "sub_callbacks", "callbacks",
                  "messages", "streams", "ring_slots", "window_ns", "windows", "horizon_s",
                  "d_scale", "engine", "cores"):
            m[f] = rs[0][f]
        meds.append(m)
    by_file = defaultdict(list)
    for m in meds:
        by_file[(m["task"], m["file"], m["model"])].append(m)
    for ms in by_file.values():
        seq = next((m["run_s"] for m in ms if m["label"] == "seq"), None)
        one = [m["run_s"] for m in ms if int(m["workers"]) == 1]
        best1 = min(one) if one else None
        ooo = {int(m["workers"]): m["run_s"] for m in ms if m["label"] == "ooo"}
        for m in ms:
            m["vs_seq"] = seq / m["run_s"] if seq and m["run_s"] else ""
            m["vs_1w"] = best1 / m["run_s"] if best1 and m["run_s"] else ""
            o = ooo.get(int(m["workers"]))
            m["to_ooo"] = m["run_s"] / o if o and m["label"] != "ooo" else ""
    med_fields = CONFIG_KEYS + ["engine", "runs", "run_s", "run_s_min", "run_s_max", "setup_s",
                                "node_setup_s",
                                "vs_seq", "vs_1w", "to_ooo", "rss_delta_mb", "rss_peak_mb",
                                "payload_bytes", "nulls", "est_s", "tasks", "jobs", "active_jobs",
                                "timer_callbacks", "sub_callbacks", "callbacks", "messages",
                                "streams", "ring_slots", "window_ns", "windows", "horizon_s",
                                "d_scale", "cores"]
    meds.sort(key=lambda m: (m["kind"], m["task"], m["file"], m["model"], m["label"],
                             int(m["workers"])))

    rsl = []
    per = defaultdict(lambda: defaultdict(list))
    for r in rslcpp_rows:
        per[r["workload"]][(r["label"], int(r["workers"]))].append(r)
    for wl, sims in sorted(per.items(), key=lambda kv: (kv[0] != "RS", kv[0])):
        base = sims.get(("rslcpp", 1))
        if not base:
            continue
        t_rsl = statistics.median(float(r["run_s"]) for r in base)
        cb_rsl = int(base[0]["callbacks"] or 0)
        for (lab, w), rs in sorted(sims.items(), key=lambda kv: (kv[0][0] != "rslcpp",
                                                                 kv[0][0] != "seq", kv[0][1])):
            t = statistics.median(float(r["run_s"]) for r in rs)
            tcb = int(rs[0]["timer_callbacks"] or 0)
            scb = int(rs[0]["sub_callbacks"] or 0)
            us = 1e6 * t / max(tcb + scb, 1)
            rsl.append({"workload": wl, "simulator": lab, "workers": w, "runs": len(rs),
                        "run_s": t, "timer_callbacks": tcb, "sub_callbacks": scb,
                        "us_per_callback": us,
                        "rslcpp_over_this": t_rsl / t if lab != "rslcpp" else "",
                        "per_callback": (1e6 * t_rsl / max(cb_rsl, 1)) / us if lab != "rslcpp" else ""})

    hashes = defaultdict(set)
    for r in runs:
        if r["kind"] == "model":
            hashes[("model", r["value"])].add(r["hash"])
    for (k, v), hs in hashes.items():
        if len(hs) > 1:
            problems.append(f"model task, horizon {v}: the two models give different hashes")

    def write(name, rows, fields):
        with open(out / name, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
            w.writeheader()
            w.writerows(rows)

    write("runs.csv", runs, RUN_FIELDS)
    write("medians.csv", meds, med_fields)
    bfields = sorted({k for b in bounds for k in b},
                     key=lambda k: (k not in ("workload", "cars", "step", "horizon", "jobs",
                                              "work_s", "span_s", "bound", "window_ns"), k))
    write("bounds.csv", bounds, bfields or ["workload"])
    write("rslcpp.csv", rsl, ["workload", "simulator", "workers", "runs", "run_s", "timer_callbacks",
                              "sub_callbacks", "us_per_callback", "rslcpp_over_this", "per_callback"])
    checks += synth.write(out, meds, write, problems)
    if batch_rows:
        write("batch.csv", batch_rows, BATCH_FIELDS)
        write("batch_runs.csv", batch_runs, BATCH_RUN_FIELDS)
    text = [f"campaign {root}", ""] + checks + [""]
    text += ["problems:"] + [f"  {p}" for p in problems] if problems else ["no problems found"]
    (out / "checks.txt").write_text("\n".join(text) + "\n", encoding="utf-8")
    print(f"{len(runs)} runs, {len(meds)} configurations, {len(bounds)} bounds, "
          f"{len(rsl)} RSLCPP rows -> {out}")
    print("\n".join(text))
    return 1 if problems else 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
