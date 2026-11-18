#!/usr/bin/env python3
import sys
from collections import defaultdict

args = sys.argv[1:]
split = set()
procs_list = []
if "--procs" in args:
    i = args.index("--procs")
    procs_list = [int(x) for x in args[i + 1].split(",")]
    del args[i:i + 2]
while "--split" in args:
    i = args.index("--split")
    split.add(args[i + 1])
    del args[i:i + 2]
path, window = args[0], int(args[1])
model = args[2] if len(args) > 2 else "unit"
skip = 0.01
jobs = []
for line in open(path):
    f = line.split()
    i, k, r, a, pt, pj, w = map(int, f[:7])
    jobs.append((r, i, k, a, pt, pj, w, f[7]))
jobs.sort()

def cost_of(a, w, name):
    if model == "unit":
        return 1.0 if a else skip
    sensors = ("PointCloudMap", "Visualizer", "Lanelet2Map", "EuclideanClusterSettings")
    return 1.0 if (w and not name.endswith("Driver") and name not in sensors) else skip

prev_of = {}
last_job = {}
finish = {}
last = {}
work = 0.0
per_win = defaultdict(lambda: defaultdict(float))
for r, i, k, a, pt, pj, w, name in jobs:
    c = cost_of(a, w, name)
    work += c
    task = (i, pt) if name in split else (i, None)
    if task in last_job:
        prev_of[(i, k)] = last_job[task]
    last_job[task] = (i, k)
    start = last.get(task, 0.0)
    if pt >= 0 and pj > 0:
        start = max(start, finish[(pt, pj)])
    finish[(i, k)] = start + c
    last[task] = start + c
    per_win[r // window][task] += c
span = max(finish.values())
bspan = sum(max(d.values()) for d in per_win.values())
print(f"{model:6} split={sorted(split) or '-'}  work {work:.0f}  ooo bound {work / span:.1f}x  "
      f"barrier bound {work / bspan:.1f}x")

def simulate(procs):
    import heapq
    costs = {}
    deps = {}
    for r, i, k, a, pt, pj, w, name in jobs:
        task = (i, pt) if name in split else (i, None)
        costs[(i, k)] = cost_of(a, w, name)
        deps[(i, k)] = [d for d in (prev_of.get((i, k)), (pt, pj) if pt >= 0 and pj > 0 else None) if d]
    children = defaultdict(list)
    missing = {}
    for j, ds in deps.items():
        missing[j] = len(ds)
        for d in ds:
            children[d].append(j)
    release = {(i, k): r for r, i, k, *_ in jobs}
    ready = [(release[j], j) for j, m in missing.items() if m == 0]
    heapq.heapify(ready)
    running = []
    free = procs
    t = 0.0
    done = 0
    while done < len(costs):
        while free and ready:
            _, j = heapq.heappop(ready)
            heapq.heappush(running, (t + costs[j], j))
            free -= 1
        t, j = heapq.heappop(running)
        free += 1
        done += 1
        for c in children[j]:
            missing[c] -= 1
            if missing[c] == 0:
                heapq.heappush(ready, (release[c], c))
    ooo = t
    total = 0.0
    for win, per_task in per_win.items():
        loads = [0.0] * procs
        for c in sorted(per_task.values(), reverse=True):
            m = min(range(procs), key=loads.__getitem__)
            loads[m] += c
        total += max(loads)
    return ooo, total

if procs_list:
    for p in procs_list:
        o, b = simulate(p)
        print(f"  {p:3d} cores  ooo {work / o:5.1f}x  barrier {work / b:5.1f}x  ratio {b / o:.2f}")
