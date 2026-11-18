#!/usr/bin/env python3
"""Predicted speedups of the engines from a job DAG and measured job costs.

usage: schedule_bound.py dag.txt costs.txt window_ns --procs 8,16,32 [--json FILE] [--label "key=value ..."]
"""
import heapq
import json
import sys
from collections import defaultdict

def load_costs(path):
    out = {}
    for line in open(path):
        if line.startswith("#") or not line.strip():
            continue
        f = line.split()
        out[f[0]] = [int(x) for x in f[2:2 + int(f[1])]]
    return out

def main(argv):
    if "--procs" not in argv or len(argv) < 5:
        print(__doc__)
        return 2
    i = argv.index("--procs")
    procs = [int(x) for x in argv[i + 1].split(",")]
    del argv[i:i + 2]
    json_path = None
    label = {}
    if "--json" in argv:
        i = argv.index("--json")
        json_path = argv[i + 1]
        del argv[i:i + 2]
    if "--label" in argv:
        i = argv.index("--label")
        for kv in argv[i + 1].split():
            k, _, v = kv.partition("=")
            label[k] = v
        del argv[i:i + 2]
    dag_path, cost_path, window = argv[1], argv[2], int(argv[3])
    costs = load_costs(cost_path)
    jobs = []
    cost = {}
    for line in open(dag_path):
        f = line.split()
        t, k, r, a, pt, pj, w = map(int, f[:7])
        fqn = f[8] if len(f) > 8 else f[7]
        if fqn not in costs or len(costs[fqn]) < k:
            print(f"no cost for job {k} of {fqn}, record the costs with the same options")
            return 1
        cost[(t, k)] = costs[fqn][k - 1] * 1e-9
        jobs.append((r, t, k, pt, pj))
    jobs.sort()
    release = {(t, k): r for r, t, k, _, _ in jobs}
    deps = {}
    for r, t, k, pt, pj in jobs:
        d = []
        if k > 1:
            d.append((t, k - 1))
        if pt >= 0 and pj > 0:
            d.append((pt, pj))
        deps[(t, k)] = d
    succ = defaultdict(list)
    for j, ds in deps.items():
        for d in ds:
            succ[d].append(j)
    work = sum(cost.values())
    level = {}
    for r, t, k, _, _ in reversed(jobs):
        j = (t, k)
        level[j] = cost[j] + max((level[s] for s in succ[j]), default=0.0)
    span = max(level.values())

    def list_schedule(p, prio):
        missing = {j: len(ds) for j, ds in deps.items()}
        ready_at = defaultdict(float)
        pending = [(0.0, prio(j), j) for j, m in missing.items() if m == 0]
        heapq.heapify(pending)
        ready = []
        free = [(0.0, w) for w in range(p)]
        heapq.heapify(free)
        end = 0.0
        for _ in range(len(cost)):
            tw, w = heapq.heappop(free)
            while pending and pending[0][0] <= tw:
                _, pr, j = heapq.heappop(pending)
                heapq.heappush(ready, (pr, j))
            if not ready:
                tw = pending[0][0]
                while pending and pending[0][0] <= tw:
                    _, pr, j = heapq.heappop(pending)
                    heapq.heappush(ready, (pr, j))
            pr, j = heapq.heappop(ready)
            start = max(tw, ready_at[j])
            fin = start + cost[j]
            end = max(end, fin)
            heapq.heappush(free, (fin, w))
            for s in succ[j]:
                ready_at[s] = max(ready_at[s], fin)
                missing[s] -= 1
                if missing[s] == 0:
                    heapq.heappush(pending, (ready_at[s], prio(s), s))
        return end

    def windows(p, width):
        per = defaultdict(lambda: defaultdict(float))
        for r, t, k, _, _ in jobs:
            per[r // width][t] += cost[(t, k)]
        total = 0.0
        for chains in per.values():
            loads = [0.0] * p
            heapq.heapify(loads)
            for c in sorted(chains.values(), reverse=True):
                heapq.heapreplace(loads, loads[0] + c)
            total += max(loads)
        return total, len(per)

    print(f"jobs {len(jobs)}  work {work:.3f} s  span {span:.4f} s  bound {work / span:.1f}x")
    print(f"{'cores':>5} {'ooo':>7} {'static':>7} {'barrier':>8} {'tag':>7}   "
          f"{'static/ooo':>10} {'barrier/ooo':>11} {'tag/ooo':>8}")
    rows = {}
    nb = ng = 0
    for p in procs:
        o = list_schedule(p, lambda j: (release[j], j))
        s = list_schedule(p, lambda j: (-level[j], j))
        b, nb = windows(p, window)
        g, ng = windows(p, 1)
        rows[p] = {"ooo": work / o, "static": work / s, "barrier": work / b, "tag": work / g}
        print(f"{p:5d} {work / o:6.1f}x {work / s:6.1f}x {work / b:7.1f}x {work / g:6.1f}x   "
              f"{s / o:10.2f} {b / o:11.2f} {g / o:8.2f}")
    print(f"windows: barrier {nb}, tag {ng}")
    if json_path:
        out = dict(label)
        out.update({"jobs": len(jobs), "work_s": work, "span_s": span, "bound": work / span,
                    "window_ns": window, "windows_barrier": nb, "windows_tag": ng,
                    "speedup": {str(p): v for p, v in rows.items()}})
        with open(json_path, "a") as f:
            f.write(json.dumps(out) + "\n")
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv))
