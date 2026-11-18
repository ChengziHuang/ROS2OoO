#!/usr/bin/env python3
import csv, glob, json, math, os, statistics, sys
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.ticker import FixedLocator, NullLocator, FuncFormatter
from matplotlib.lines import Line2D
from matplotlib.patches import Patch

RES, OUT = sys.argv[1], sys.argv[2]
PNG = "--png" in sys.argv
os.makedirs(OUT, exist_ok=True)
P = lambda *a: os.path.join(RES, *a)

plt.rcParams.update({
    "font.family": "sans-serif", "font.sans-serif": ["DejaVu Sans"], "font.size": 6.5,
    "axes.labelsize": 6.5, "xtick.labelsize": 6, "ytick.labelsize": 6, "legend.fontsize": 6,
    "axes.linewidth": 0.5, "xtick.major.width": 0.5, "ytick.major.width": 0.5,
    "xtick.major.size": 2, "ytick.major.size": 2, "xtick.minor.size": 1.2, "ytick.minor.size": 1.2,
    "xtick.major.pad": 1.5, "ytick.major.pad": 1.5,
    "pdf.fonttype": 42, "ps.fonttype": 42, "legend.frameon": False,
    "axes.spines.top": False, "axes.spines.right": False,
})

STY = {
    "ooo":             dict(name="Ours",                color="#1f6fd1", ls="-",            marker="o", mfc="#1f6fd1"),
    "frontier-L200ms": dict(name="Async, lead 200 ms",  color="#9c7a00", ls=(0, (4, 1.5)),  marker="D", mfc="#9c7a00"),
    "frontier":        dict(name="Async",               color="#e0a800", ls=(0, (1, 1)),    marker="D", mfc="white"),
    "barrier":         dict(name="Sync-W",              color="#e8590c", ls=(0, (3, 1.5)),  marker="s", mfc="white"),
    "tag":             dict(name="Sync-I",              color="#2b9348", ls=(0, (3, 1, 1, 1)), marker="^", mfc="white"),
}
ORDER = ["ooo", "frontier-L200ms", "frontier", "barrier", "tag"]
GRAY = "#8f8f8f"
GRID = dict(color="#e8e8e8", lw=0.4)
LW, MS = 0.9, 2.8

def gmean(xs):
    return math.exp(sum(math.log(x) for x in xs) / len(xs))

def save(fig, name):
    fig.savefig(os.path.join(OUT, name + ".pdf"))
    if PNG:
        fig.savefig(os.path.join(OUT, name + ".png"), dpi=300)
    plt.close(fig)

def log2_axis(a, ticks, lo, hi):
    ax_ = a.axes
    (ax_.set_xscale if a is ax_.xaxis else ax_.set_yscale)("log", base=2)
    a.set_major_locator(FixedLocator(ticks))
    a.set_minor_locator(NullLocator())
    a.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))

def line(ax, xs, ys, key, **kw):
    s = STY[key]
    ax.plot(xs, ys, color=s["color"], ls=s["ls"], lw=LW, marker=s["marker"], ms=MS,
            mfc=s["mfc"], mec=s["color"], mew=0.7, zorder=kw.pop("zorder", 3), **kw)

W4, H4 = 1.74, 0.99
AX4 = [0.30 / W4, 0.25 / H4, 1 - 0.36 / W4, 1 - 0.30 / H4]
W5, H5 = 1.70, 1.08
AX5 = [0.29 / W5, 0.25 / H5, 1 - 0.35 / W5, 1 - 0.30 / H5]

main = list(csv.DictReader(open(P("ros2-20261024-181701/csv/medians.csv"))))
def mrow(task, label, workers, file=None):
    for r in main:
        if (r["task"] == task and r["label"] == label and r["workers"] == str(workers)
                and r["model"] == "streams" and r["sweep"] == "" and (file is None or r["file"] == file)):
            return r
    return None

bounds = {}
for f in ("ros2-20261024-181701/bounds/bounds.jsonl", "ros2-more-20261025-024140/bounds/bounds.jsonl"):
    for l in open(P(f)):
        d = json.loads(l)
        if d.get("dscale") not in (None, "1") and d["workload"].startswith("Fleet-8") and d.get("horizon") == "5s":
            continue
        if d.get("horizon") == "5s":
            continue
        bounds[(d["workload"], d.get("step"))] = d["bound"]
print("bounds W/S:", {k: round(v, 2) for k, v in sorted(bounds.items(), key=lambda kv: str(kv[0]))})

fig = plt.figure(figsize=(7.0, 0.17))
handles = [Line2D([], [], color=STY[k]["color"], ls=STY[k]["ls"], lw=LW, marker=STY[k]["marker"], ms=MS,
                  mfc=STY[k]["mfc"], mec=STY[k]["color"], mew=0.7, label=STY[k]["name"]) for k in ORDER]
handles.append(Line2D([], [], color=GRAY, ls=(0, (1, 1.2)), lw=0.8, label="Bound min(p, W/S)"))
fig.legend(handles=handles, loc="center", ncol=len(handles), handlelength=2.6, columnspacing=1.6,
           borderaxespad=0, frameon=True, edgecolor="#bdbdbd", fancybox=False, borderpad=0.35)
save(fig, "eval_legend")

ps = [1, 2, 4, 8, 16, 32]
ws8 = bounds[("Fleet-8", "37ms")]
fig = plt.figure(figsize=(W4, H4)); ax = fig.add_axes(AX4)
ax.plot(ps, [min(p, ws8) for p in ps], color=GRAY, ls=(0, (1, 1.2)), lw=0.8, zorder=1)
print("(a) Fleet-8, 32-core runs, speedup over seq:")
for k in ORDER[::-1]:
    ys = [float(mrow("fleet-c8-s37ms", k, p)["vs_seq"]) for p in ps]
    line(ax, ps, ys, k)
    print("   ", STY[k]["name"].ljust(20), " ".join(f"{y:6.2f}" for y in ys))
log2_axis(ax.xaxis, ps, 1, 32); log2_axis(ax.yaxis, [1, 2, 4, 8, 16, 32], 1, 32)
ax.set_xlim(0.85, 38); ax.set_ylim(0.85, 38)
ax.set_xlabel("Workers", labelpad=1); ax.set_ylabel("Speedup", labelpad=1)
ax.grid(True, **GRID)
save(fig, "eval_workers")

cars = [1, 2, 4, 8, 16]
fig = plt.figure(figsize=(W4, H4)); ax = fig.add_axes(AX4)
ax.plot(cars, [min(32, bounds[(f"Fleet-{c}", "37ms")]) for c in cars], color=GRAY, ls=(0, (1, 1.2)), lw=0.8, zorder=1)
print("(b) 32 workers against vehicles, bound", [round(min(32, bounds[(f'Fleet-{c}', '37ms')]), 2) for c in cars])
for k in ORDER[::-1]:
    ys = [float(mrow(f"fleet-c{c}-s37ms", k, 32)["vs_seq"]) for c in cars]
    line(ax, cars, ys, k)
    print("   ", STY[k]["name"].ljust(20), " ".join(f"{y:6.2f}" for y in ys))
log2_axis(ax.xaxis, cars, 1, 16); log2_axis(ax.yaxis, [1, 2, 4, 8, 16, 32], 1, 32)
ax.set_xlim(0.85, 19); ax.set_ylim(1.6, 38)
ax.set_xlabel("Vehicles", labelpad=1); ax.set_ylabel("Speedup", labelpad=1)
ax.grid(True, **GRID)
save(fig, "eval_cars")

node = [r for r in csv.DictReader(open(P("ros2-node-20261025-175142/csv/medians.csv")))
        if r["task"] == "node-fleet" and r["value"] == "compact"]
def nseries(c, k):
    pts = sorted((int(r["workers"]), float(r["vs_seq"])) for r in node if r["cars"] == str(c) and r["label"] == k)
    return [p for p, _ in pts], [s for _, s in pts]
fig = plt.figure(figsize=(W4, H4)); ax = fig.add_axes(AX4)
pp = [4, 8, 16, 32, 64, 128, 256]
FM = {16: "^", 32: "s", 64: "o"}
print("(c) whole machine, compact cores:")
for c in (16, 32, 64):
    wsc = bounds[(f"Fleet-{c}", "37ms")]
    ax.plot(pp, [min(p, wsc) for p in pp], color=GRAY, ls=(0, (1, 1.2)), lw=0.7, zorder=1)
for c in (32, 64):
    x, y = nseries(c, "barrier")
    ax.plot(x, y, color=STY["barrier"]["color"], ls=STY["barrier"]["ls"], lw=LW, marker=FM[c], ms=MS,
            mfc="white", mec=STY["barrier"]["color"], mew=0.7, zorder=3)
    print(f"    Sync-W Fleet-{c}:", " ".join(f"{p}:{s:.1f}" for p, s in zip(x, y)))
x, y = nseries(64, "frontier-L200ms")
ax.plot(x, y, color=STY["frontier-L200ms"]["color"], ls="none", marker="D", ms=MS + 0.8, mfc="none",
        mec=STY["frontier-L200ms"]["color"], mew=0.7, zorder=4)
print("    Async-200ms Fleet-64:", " ".join(f"{p}:{s:.1f}" for p, s in zip(x, y)))
for c in (16, 32, 64):
    x, y = nseries(c, "ooo")
    ax.plot(x, y, color=STY["ooo"]["color"], ls="-", lw=LW, marker=FM[c], ms=MS, mfc=STY["ooo"]["color"],
            mec=STY["ooo"]["color"], mew=0.6, zorder=3)
    print(f"    Ours Fleet-{c}:", " ".join(f"{p}:{s:.1f}" for p, s in zip(x, y)))
log2_axis(ax.xaxis, [4, 16, 64, 256], 4, 256); log2_axis(ax.yaxis, [4, 16, 64, 256], 4, 256)
ax.xaxis.set_minor_locator(FixedLocator([8, 32, 128])); ax.yaxis.set_minor_locator(FixedLocator([8, 32, 128]))
ax.xaxis.set_minor_formatter(FuncFormatter(lambda v, _: "")); ax.yaxis.set_minor_formatter(FuncFormatter(lambda v, _: ""))
ax.set_xlim(3.4, 300); ax.set_ylim(3.4, 300)
ax.set_xlabel("Workers", labelpad=1); ax.set_ylabel("Speedup", labelpad=1)
ax.grid(True, **GRID)
leg = [Line2D([], [], color="#555555", ls="none", marker=FM[c], ms=MS, mfc="#555555", mec="#555555",
              label=f"Fleet-{c}") for c in (16, 32, 64)]
ax.legend(handles=leg, loc="lower right", handletextpad=0.1, borderaxespad=0.1, labelspacing=0.15, fontsize=5.6)
save(fig, "eval_node")

WL = [("RS", "reference", "autoware.jsonl"), ("Fleet-8", "fleet-c8-s37ms", None),
      ("Fleet-16", "fleet-c16-s37ms", None), ("Stereo", "stereo", "d33333333/r2b-four_streams.jsonl")]
BARS = ["ooo", "frontier-L200ms", "frontier", "barrier"]
STEREO_LEAD = "ros2-stereo-lead-5440779/d33333333/r2b-four_streams.jsonl"
def lead_bytes(path, lead_ns=200000000, workers=32):
    xs = []
    if os.path.exists(P(path)):
        for l in open(P(path)):
            if l.startswith("{"):
                r = json.loads(l)
                if r["engine"] == "frontier" and r["lead_ns"] == lead_ns and r["workers"] == workers:
                    xs.append(r["payload_bytes"])
    return statistics.median(xs) / 1e6 if xs else None
fig = plt.figure(figsize=(W4, H4)); ax = fig.add_axes(AX4)
bw = 0.19
print("(d) message bytes (MB) at 32 workers:")
for i, (nm, task, file) in enumerate(WL):
    vals = {}
    for k in BARS:
        r = mrow(task, k, 32, file)
        if r is not None:
            vals[k] = float(r["payload_bytes"]) / 1e6
    if nm == "Stereo" and "frontier-L200ms" not in vals:
        v = lead_bytes(STEREO_LEAD)
        if v is not None:
            vals["frontier-L200ms"] = v
    print("   ", nm.ljust(9), {STY[k]["name"]: round(v, 2) for k, v in vals.items()})
    for j, k in enumerate(BARS):
        if k not in vals:
            continue
        s = STY[k]
        xpos = i + (j - 1.5) * bw
        face = s["color"] if s["mfc"] != "white" else "white"
        ax.bar(xpos, vals[k], width=bw * 0.92, color=face, edgecolor=s["color"], linewidth=0.7,
               hatch="//////" if k == "barrier" else None, zorder=3)
    ratio = vals["frontier"] / vals["ooo"]
    ax.annotate(f"{ratio:.0f}\u00d7", (i + (BARS.index("frontier") - 1.5) * bw, vals["frontier"]), xytext=(0, 1.5), textcoords="offset points",
                ha="center", va="bottom", fontsize=5.4, color="#6b5300")
ax.set_yscale("log")
ax.set_ylim(0.08, 30000)
ax.yaxis.set_major_locator(FixedLocator([0.1, 1, 10, 100, 1000, 10000]))
ax.yaxis.set_minor_locator(NullLocator())
ax.yaxis.set_major_formatter(FuncFormatter(lambda v, _: {0.1: "0.1", 1: "1", 10: "10", 100: "100", 1000: "1k", 10000: "10k"}[v]))
ax.set_xticks(range(len(WL))); ax.set_xticklabels([w[0] for w in WL], fontsize=5.6)
ax.tick_params(axis="x", length=0)
ax.set_xlim(-0.55, len(WL) - 0.45)
ax.set_ylabel("Message MB", labelpad=0.5)
ax.grid(True, axis="y", **GRID)
save(fig, "eval_bytes")
plt.rcParams["hatch.linewidth"] = 0.5

syn = [r for r in csv.DictReader(open(P("ros2-synth-20261024-172207/csv/synth.csv"))) if r["r"] and r["x"]]
fig = plt.figure(figsize=(W5, H5)); ax = fig.add_axes(AX5)
xs = [float(r["x"]) for r in syn]
xx = [2 ** (i / 4) for i in range(0, 4 * 12 + 1)]
ax.plot(xx, [min(x, 32) for x in xx], color=GRAY, ls=(0, (3, 2)), lw=0.8, zorder=1)
ax.scatter(xs, [float(r["s_barrier"]) for r in syn], s=4, marker="s", facecolors="none",
           edgecolors=STY["barrier"]["color"], linewidths=0.35, alpha=0.75, zorder=2, rasterized=True)
ax.scatter(xs, [float(r["s_ooo"]) for r in syn], s=3.2, marker="o", color=STY["ooo"]["color"],
           linewidths=0, alpha=0.6, zorder=3, rasterized=True)
ax.text(300, 33.5, "min(x, 32)", fontsize=5.6, color=GRAY, ha="center", va="bottom")
for key, col, ls in (("s_barrier", STY["barrier"]["color"], STY["barrier"]["ls"]), ("s_ooo", STY["ooo"]["color"], "-")):
    bins = defaultdict(list)
    for r in syn:
        bins[int(math.floor(math.log2(float(r["x"])) * 2))].append(float(r[key]))
    ks = sorted(k for k in bins if len(bins[k]) >= 8)
    ax.plot([2 ** ((k + 0.5) / 2) for k in ks], [statistics.median(bins[k]) for k in ks], color="white", lw=2.2, zorder=4)
    ax.plot([2 ** ((k + 0.5) / 2) for k in ks], [statistics.median(bins[k]) for k in ks], color=col, lw=1.1, ls=ls, zorder=5)
log2_axis(ax.xaxis, [1, 8, 64, 512, 4096], 1, 4096)
ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: "4k" if v == 4096 else f"{v:g}"))
log2_axis(ax.yaxis, [0.5, 1, 2, 4, 8, 16, 32], 0.5, 32)
ax.set_xlim(0.9, 5000); ax.set_ylim(0.45, 45)
ax.set_xlabel("Jobs per window x", labelpad=1); ax.set_ylabel("Speedup", labelpad=1)
ax.grid(True, **GRID)
ax.legend(handles=[Line2D([], [], ls="none", marker="o", ms=2.6, color=STY["ooo"]["color"], label="Ours"),
                   Line2D([], [], ls="none", marker="s", ms=2.6, mfc="none", mec=STY["barrier"]["color"], mew=0.6, label="Sync-W")],
          loc="lower right", handletextpad=0.1, borderaxespad=0.1, labelspacing=0.15)
R = [float(r["r"]) for r in syn]
print(f"Fig. 5(a): {len(syn)} graphs, ours faster than Sync-W on {sum(x > 1 for x in R)}, gmean Sync-W/ours time {gmean(R):.3f}, "
      f"ours slower than sequential on {sum(float(r['s_ooo']) < 1 for r in syn)}")
save(fig, "eval_window")

def med(v):
    return statistics.median(v)
curves = {}
per = defaultdict(list)
for f in sorted(glob.glob(P("ros2-more-20261025-024140/leads-synth/*.jsonl"))):
    by = defaultdict(list)
    for l in open(f):
        r = json.loads(l)
        by[(r["engine"], r["lead_ns"], r["workers"])].append(r)
    get = {k: (med([x["run_s"] for x in v]), med([x["payload_bytes"] for x in v])) for k, v in by.items()}
    o = get.get(("ooo", 0, 32))
    leads = sorted(k[1] for k in get if k[0] == "frontier" and k[1] > 0 and k[2] == 32)
    if not o or not leads:
        continue
    tmax = leads[0] / 0.25
    for (e, lead, w), (t, b) in get.items():
        if e != "frontier" or w != 32:
            continue
        key = math.inf if lead < 0 else round(lead / tmax, 2)
        per[key].append((t / o[0], b / o[1]))
keys = sorted(per)
curves["Synthetic"] = [(k, gmean([b for _, b in per[k]]), gmean([t for t, _ in per[k]]), len(per[k])) for k in keys]
for c, step, nm in ((8, "37ms", "Fleet-8"), (16, "37ms", "Fleet-16")):
    by = defaultdict(list)
    for l in open(P(f"ros2-more-20261025-024140/leads-fleet/fleet-c{c}-s{step}.jsonl")):
        r = json.loads(l)
        by[(r["engine"], r["lead_ns"], r["workers"])].append(r)
    get = {k: (med([x["run_s"] for x in v]), med([x["payload_bytes"] for x in v])) for k, v in by.items()}
    o = get[("ooo", 0, 32)]
    pts = []
    for (e, lead, w), (t, b) in get.items():
        if e == "frontier" and w == 32:
            pts.append((math.inf if lead < 0 else lead / 1e6, b / o[1], t / o[0], 1))
    curves[nm] = sorted(pts)
fig = plt.figure(figsize=(W5, H5)); ax = fig.add_axes(AX5)
CS = {"Fleet-8": ("#9c7a00", "D"), "Fleet-16": ("#c49a00", "v"), "Synthetic": ("#5c4a00", "s")}
for nm, pts in curves.items():
    col, mk = CS[nm]
    fin = [p for p in pts if p[0] != math.inf]
    inf = [p for p in pts if p[0] == math.inf]
    ax.plot([p[1] for p in fin], [p[2] for p in fin], color=col, lw=0.8, marker=mk, ms=2.4, mfc=col, mec=col, zorder=3)
    if inf:
        ax.plot([fin[-1][1], inf[0][1]], [fin[-1][2], inf[0][2]], color=col, lw=0.6, ls=(0, (1, 1)), zorder=2)
        ax.plot([inf[0][1]], [inf[0][2]], color=col, marker=mk, ms=3.2, mfc="white", mec=col, mew=0.8, zorder=4)
    print(f"Fig. 5(b) {nm}: " + ", ".join(f"{'none' if p[0] == math.inf else p[0]}: ({p[1]:.1f}x bytes, {p[2]:.3f} time)" for p in pts))
ax.plot([1], [1], marker="*", ms=6, color=STY["ooo"]["color"], ls="none", zorder=5)
ax.annotate("Ours", (1, 1), xytext=(-4, 6), textcoords="offset points", fontsize=6, color=STY["ooo"]["color"], ha="center", va="bottom")
lab = {"Fleet-8": (0, 4), "Fleet-16": (0, 4), "Synthetic": (0, -10.5)}
for nm, pts in curves.items():
    inf = [p for p in pts if p[0] == math.inf][0]
    ax.annotate(nm, (inf[1], inf[2]), xytext=lab[nm], textcoords="offset points",
                ha="center", va="bottom", fontsize=5.6, color=CS[nm][0])
ax.set_xscale("log")
ax.xaxis.set_major_locator(FixedLocator([1, 10, 100]))
ax.xaxis.set_minor_locator(NullLocator())
ax.xaxis.set_major_formatter(FuncFormatter(lambda v, _: f"{v:g}"))
ax.set_xlim(0.5, 230)
ax.set_ylim(0.79, 1.48)
ax.set_xlabel("Message bytes, ours = 1", labelpad=1); ax.set_ylabel("Time, ours = 1", labelpad=1)
ax.grid(True, axis="y", **GRID)
ax.legend(handles=[Line2D([], [], color="#777777", marker="D", ms=2.4, lw=0.8, label="with a lead"),
                   Line2D([], [], color="#777777", marker="D", ms=3.0, mfc="white", ls="none", label="no lead")],
          loc="upper left", handletextpad=0.3, borderaxespad=0.1, labelspacing=0.15, fontsize=5.6)
save(fig, "eval_leads")
print("written to", OUT)
