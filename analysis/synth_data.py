import csv
import json
import math
import re
import statistics
from collections import defaultdict
from pathlib import Path

def fnum(x):
    try:
        return float(x)
    except (TypeError, ValueError):
        return None

class Synth:
    def __init__(self, root=None):
        self.root = Path(root) if root else None
        self.manifest = {}
        self.files = {}
        self.ffiles = {}
        self.verified = {}
        self.fuzz = []
        self.suites = {}

    def read_task(self, name, kind, folder, problems, read_jsonl, make_row):
        gdir = folder / "graphs"
        meta = {}
        if (gdir / "meta.json").exists():
            try:
                meta = json.loads((gdir / "meta.json").read_text(encoding="utf-8"))
            except ValueError:
                problems.append(f"{gdir / 'meta.json'}: not JSON")
        suite = meta.get("suite") or (name.split("-")[1] if kind in ("synth", "frontier") else "fuzz")
        if meta:
            old = self.suites.get(suite)
            if old and old.get("suite_sha256") != meta.get("suite_sha256"):
                problems.append(f"{name}: suite {suite} differs from another shard (suite_sha256)")
            self.suites.setdefault(suite, meta)
        if (gdir / "manifest.csv").exists():
            with open(gdir / "manifest.csv", newline="", encoding="utf-8") as f:
                for row in csv.DictReader(f):
                    self.manifest.setdefault((suite, row["gid"]), row)
        if kind == "fuzz":
            p = folder / "fuzz.jsonl"
            if p.exists():
                for v in read_jsonl(p, problems):
                    v["suite"] = "fuzz"
                    v["task"] = name
                    self.fuzz.append(v)
                    if not v.get("ok"):
                        problems.append(f"fuzz graph {v.get('graph')}: {'; '.join(v.get('problems', []))}")
            return []
        rows = []
        for path in sorted(folder.glob("*.jsonl")):
            stem = path.name[:-len(".jsonl")]
            verify = stem.endswith(".verify")
            gid = stem[:-len(".verify")] if verify else stem
            m = self.manifest.get((suite, gid), {})
            runs = read_jsonl(path, problems)
            if not runs:
                problems.append(f"{path}: no runs")
                continue
            hashes = {r.get("hash") for r in runs}
            cbs = {(r.get("timer_callbacks"), r.get("sub_callbacks"), r.get("messages")) for r in runs}
            ok = len(hashes) == 1 and len(cbs) == 1
            if not ok:
                problems.append(f"{path}: {len(hashes)} hashes, {len(cbs)} callback counts")
            if verify:
                models = {r.get("model") for r in runs}
                self.verified[(suite, gid)] = ok and models >= {"streams", "calendar"}
            elif kind == "frontier":
                self.ffiles[(suite, gid)] = name
            else:
                self.files[(suite, gid)] = name
            d = {"workload": gid, "family": m.get("group", ""), "suite": suite,
                 "sweep": m.get("sweep", ""), "value": "verify" if verify else m.get("level", "")}
            for r in runs:
                rows.append(make_row(r, d, name, kind, path))
        return rows

    def write(self, out, meds, write, problems=None):
        per = defaultdict(list)
        for m in meds:
            if m["kind"] == "synth" and m.get("value") != "verify":
                per[(m.get("suite", ""), m["workload"])].append(m)
        long_rows, rows = [], []
        for (suite, gid), ms in sorted(per.items()):
            seq = next((fnum(m["run_s"]) for m in ms if m["label"] == "seq"), None)
            by = {(m["label"], int(m["workers"])): m for m in ms}
            for m in ms:
                long_rows.append({"suite": suite, "gid": gid, "group": m.get("family", ""),
                                  "label": m["label"], "workers": m["workers"], "runs": m["runs"],
                                  "run_s": m["run_s"], "run_s_min": m["run_s_min"],
                                  "run_s_max": m["run_s_max"], "vs_seq": m["vs_seq"],
                                  "to_ooo": m["to_ooo"]})
            par = sorted({w for (lab, w) in by if lab == "ooo" and w > 1})
            pmax = par[-1] if par else None
            row = dict(self.manifest.get((suite, gid), {"gid": gid}))
            row["suite"] = suite
            row["task"] = self.files.get((suite, gid), "")
            row["verified"] = {True: 1, False: 0}.get(self.verified.get((suite, gid)), "")
            first = ms[0]
            for k in ("tasks", "jobs", "active_jobs", "streams", "messages"):
                row[k] = first.get(k, "")
            row["t_seq"] = seq
            o1 = by.get(("ooo", 1))
            row["t_ooo_1"] = fnum(o1["run_s"]) if o1 else ""
            row["ooo_1_over_seq"] = fnum(o1["run_s"]) / seq if o1 and seq else ""
            row["p_max"] = pmax or ""
            if pmax:
                o = by.get(("ooo", pmax))
                b = by.get(("barrier", pmax))
                f = by.get(("ooo-full", pmax))
                to, tb = fnum(o["run_s"]) if o else None, fnum(b["run_s"]) if b else None
                row["t_ooo"] = to
                row["t_barrier"] = tb or ""
                row["t_ooo_full"] = fnum(f["run_s"]) if f else ""
                row["s_ooo"] = seq / to if seq and to else ""
                row["s_barrier"] = seq / tb if seq and tb else ""
                row["r"] = tb / to if tb and to else ""
                row["k_gain"] = to / fnum(f["run_s"]) if f and to else ""
                if b and fnum(b.get("windows")):
                    row["windows"] = b["windows"]
                    row["x"] = fnum(b["jobs"]) / fnum(b["windows"])
            est = fnum(row.get("est_seq_s"))
            row["seq_over_est"] = seq / est if seq and est else ""
            rows.append(row)
        if rows:
            fields = []
            for r in rows:
                for k in r:
                    if k not in fields:
                        fields.append(k)
            write("synth.csv", rows, fields)
            write("synth_runs.csv", long_rows, ["suite", "gid", "group", "label", "workers", "runs",
                                                "run_s", "run_s_min", "run_s_max", "vs_seq", "to_ooo"])
        if self.fuzz:
            frows = []
            for v in self.fuzz:
                m = self.manifest.get(("fuzz", v.get("graph")), {})
                r = {"gid": v.get("graph"), "task": v.get("task"), "ok": int(bool(v.get("ok"))),
                     "runs": v.get("runs"), "configs": len(v.get("configs", [])),
                     "problems": "; ".join(v.get("problems", [])), "hash": v.get("hash")}
                for k in ("tasks", "jobs", "active_jobs", "timer_callbacks", "sub_callbacks", "messages",
                          "streams"):
                    r[k] = v.get(k)
                for k in ("group", "family", "classes", "period_model", "d_model", "offset_model",
                          "cost_model", "phase", "d_scale", "nodes", "event_nodes", "timers",
                          "join_subs", "gate_subs", "relay_subs", "cache_subs", "outputs", "periods"):
                    r["m_" + k if k in r else k] = m.get(k, "")
                frows.append(r)
            fields = list(frows[0].keys())
            write("fuzz.csv", frows, fields)
        return self.summary(rows) + self.write_frontier(meds, write, problems)

    def summary(self, rows):
        lines = []
        for suite in sorted({r["suite"] for r in rows}):
            rs = [r for r in rows if r["suite"] == suite]
            ver = sum(1 for r in rs if r.get("verified") == 1)
            rr = [fnum(r.get("r")) for r in rs if fnum(r.get("r"))]
            gm = math.exp(statistics.mean(math.log(x) for x in rr)) if rr else float("nan")
            lines.append(f"synth {suite}: {len(rs)} graphs timed, {ver} verified on every engine and "
                         f"both models, barrier / ooo at the most workers: geometric mean {gm:.2f}, "
                         f"ooo faster on {sum(1 for x in rr if x > 1)} of {len(rr)}")
        if self.fuzz:
            ok = sum(1 for v in self.fuzz if v.get("ok"))
            lines.append(f"fuzz: {ok} of {len(self.fuzz)} graphs give one hash, the same callbacks and "
                         f"messages on all {max((v.get('runs') or 0) for v in self.fuzz)} runs")
        return lines

    def synth_reference(self):
        if not self.root:
            return {}, ""
        cands = [f for f in self.root.parent.glob("ros2-synth-*")
                 if f != self.root and (f / "csv" / "synth.csv").exists()
                 and (f / "csv" / "synth_runs.csv").exists()]
        if not cands:
            return {}, ""
        best = max(cands, key=lambda f: re.sub(r"^.*?(\d{8}-\d{6})$", r"\1", f.name))
        ref = defaultdict(dict)
        with open(best / "csv" / "synth.csv", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                d = ref[(r.get("suite", ""), r.get("gid", ""))]
                d["sha256"] = r.get("sha256", "")
                d["horizon_ns"] = r.get("horizon_ns", "")
                d["t_seq"] = fnum(r.get("t_seq"))
        with open(best / "csv" / "synth_runs.csv", newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                try:
                    w = int(r["workers"])
                except (KeyError, ValueError):
                    continue
                ref[(r.get("suite", ""), r.get("gid", ""))][(r.get("label", ""), w)] = r
        return ref, best.name

    def write_frontier(self, meds, write, problems):
        per = defaultdict(list)
        for m in meds:
            if m["kind"] == "frontier" and m.get("value") != "verify":
                per[(m.get("suite", ""), m["workload"])].append(m)
        if not per:
            return []
        ref, ref_name = self.synth_reference()
        here = self.root.name if self.root else ""
        rows, long_rows = [], []
        same = differ = absent = 0
        for (suite, gid), ms in sorted(per.items()):
            man = self.manifest.get((suite, gid), {})
            seq = next((fnum(m["run_s"]) for m in ms if m["label"] == "seq"), None)
            by = {}
            for m in ms:
                lab = "frontier-L" if m["label"].startswith("frontier-L") else m["label"]
                by[(lab, int(m["workers"]))] = m
            r0 = ref.get((suite, gid), {})
            alike = (bool(r0.get("sha256")) and r0.get("sha256") == man.get("sha256")
                     and r0.get("horizon_ns") == man.get("horizon_ns"))
            if r0:
                same += alike
                differ += not alike
            else:
                absent += 1
            for (lab, w), m in sorted(by.items()):
                long_rows.append({"suite": suite, "gid": gid, "group": man.get("group", ""),
                                  "campaign": here, "label": lab, "workers": w, "runs": m["runs"],
                                  "run_s": m["run_s"], "run_s_min": m["run_s_min"],
                                  "run_s_max": m["run_s_max"], "vs_seq": m["vs_seq"],
                                  "to_ooo": m["to_ooo"], "rss_delta_mb": m.get("rss_delta_mb", ""),
                                  "nulls": m.get("nulls", "") if lab.startswith("frontier") else "",
                                  "jobs": m.get("jobs", ""),
                                  "lead_ns": man.get("lead_ns", "") if lab == "frontier-L" else ""})
            ws = sorted({w for (lab, w) in by if lab == "ooo" and w > 1})
            for w in ws:
                b = r0.get(("barrier", w))
                if b:
                    long_rows.append({"suite": suite, "gid": gid, "group": man.get("group", ""),
                                      "campaign": ref_name, "label": "barrier", "workers": w,
                                      "runs": b.get("runs", ""), "run_s": b.get("run_s", ""),
                                      "run_s_min": b.get("run_s_min", ""),
                                      "run_s_max": b.get("run_s_max", ""), "vs_seq": b.get("vs_seq", ""),
                                      "to_ooo": b.get("to_ooo", ""), "rss_delta_mb": "", "nulls": "",
                                      "jobs": "", "lead_ns": ""})
            row = dict(man) if man else {"gid": gid}
            row["suite"] = suite
            row["task"] = self.ffiles.get((suite, gid), "")
            first = ms[0]
            for k in ("tasks", "jobs", "active_jobs", "streams", "messages"):
                row[k] = first.get(k, "")
            row["t_seq"] = seq
            row["p_max"] = ws[-1] if ws else ""
            row["lead_run"] = int(any(lab == "frontier-L" for (lab, _) in by))
            row["ref_campaign"] = ref_name
            row["same_graph"] = int(alike) if r0 else ""
            for suffix, w in (("_max", ws[-1] if ws else None), ("_8", 8 if 8 in ws else None)):
                if w is None:
                    continue
                t = {lab: fnum(by[(lab, w)]["run_s"]) if (lab, w) in by else None
                     for lab in ("ooo", "frontier", "frontier-L")}
                to, tf, tl = t["ooo"], t["frontier"], t["frontier-L"]
                tbest = min(x for x in (tf, tl) if x) if (tf or tl) else None
                row["t_ooo" + suffix] = to or ""
                row["t_frontier" + suffix] = tf or ""
                row["t_frontier_l" + suffix] = tl or ""
                row["f_over_o" + suffix] = tf / to if tf and to else ""
                row["fl_over_o" + suffix] = tl / to if tl and to else ""
                row["best_over_o" + suffix] = tbest / to if tbest and to else ""
                for name, x in (("ooo", to), ("frontier", tf), ("frontier_l", tl), ("best", tbest)):
                    row[f"s_{name}{suffix}"] = seq / x if seq and x else ""
                for name, lab in (("ooo", "ooo"), ("frontier", "frontier"), ("frontier_l", "frontier-L")):
                    m = by.get((lab, w))
                    row[f"mem_{name}{suffix}"] = m.get("rss_delta_mb", "") if m else ""
                    pay = fnum(m.get("payload_bytes")) if m else None
                    row[f"pay_mb_{name}{suffix}"] = pay / 1e6 if pay is not None else ""
                m = by.get(("frontier", w))
                jobs = fnum(m.get("jobs")) if m else None
                row["nulls_per_job" + suffix] = (fnum(m.get("nulls")) / jobs
                                                 if m and jobs and fnum(m.get("nulls")) is not None else "")
                rb, ro = r0.get(("barrier", w)), r0.get(("ooo", w))
                tb, tor = (fnum(rb["run_s"]) if rb else None), (fnum(ro["run_s"]) if ro else None)
                row["s_barrier_ref" + suffix] = r0["t_seq"] / tb if tb and r0.get("t_seq") else ""
                row["s_ooo_ref" + suffix] = r0["t_seq"] / tor if tor and r0.get("t_seq") else ""
                row["r_ref" + suffix] = tb / tor if tb and tor else ""
                row["ooo_drift" + suffix] = to / tor if to and tor else ""
            rows.append(row)
        fields = []
        for r in rows:
            for k in r:
                if k not in fields:
                    fields.append(k)
        write("frontier.csv", rows, fields)
        write("frontier_runs.csv", long_rows,
              ["suite", "gid", "group", "campaign", "label", "workers", "runs", "run_s", "run_s_min",
               "run_s_max", "vs_seq", "to_ooo", "rss_delta_mb", "nulls", "jobs", "lead_ns"])
        lines = []
        for suite in sorted({r["suite"] for r in rows}):
            rs = [r for r in rows if r["suite"] == suite]
            f = [fnum(r.get("f_over_o_max")) for r in rs if fnum(r.get("f_over_o_max"))]
            fl = [fnum(r.get("fl_over_o_max")) for r in rs if fnum(r.get("fl_over_o_max"))]
            bo = [fnum(r.get("best_over_o_max")) for r in rs if fnum(r.get("best_over_o_max"))]
            lines.append(f"frontier {suite}: {len(rs)} graphs timed, at the most workers frontier / ooo "
                         f"geometric mean {gmean(f):.2f}, with the lead {gmean(fl):.2f} "
                         f"({len(fl)} graphs), ooo faster than both on {sum(1 for x in bo if x > 1)} "
                         f"of {len(bo)}")
        if ref_name:
            lines.append(f"frontier graphs: {same} the same as in {ref_name} (graph and horizon), "
                         f"{differ} different, {absent} not there")
            if differ and problems is not None:
                problems.append(f"{differ} frontier graphs differ from those of {ref_name} "
                                f"(sha256 or horizon)")
        else:
            lines.append("frontier graphs: no synthetic campaign (results/ros2-synth-<time>/csv/) "
                         "to compare with")
        return lines

def gmean(xs):
    xs = [x for x in xs if x and x > 0]
    return math.exp(statistics.mean(math.log(x) for x in xs)) if xs else float("nan")
