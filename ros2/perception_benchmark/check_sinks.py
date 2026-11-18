#!/usr/bin/env python3
"""Check that every PointCloudSink consumes every selected frame."""
import argparse
import json
import re
from pathlib import Path

def validate(config_path, log_path, expected_runs):
    if expected_runs <= 0:
        raise ValueError("expected_runs must be positive")
    expected = json.loads(Path(config_path).read_text(encoding="utf-8"))["expected_pointcloud_frames"]
    found = {name: [] for name in expected}
    pattern = re.compile(r"LET_PERCEPTION_SINK node=(\S+) frames=(\d+) finite_xyz=(\d+)")
    for match in pattern.finditer(Path(log_path).read_text(encoding="utf-8", errors="replace")):
        name, frames, finite = match.group(1), int(match.group(2)), int(match.group(3))
        if name not in expected:
            raise ValueError(f"unexpected pointcloud sink {name}")
        if frames <= 0 or frames != expected[name] or finite <= 0:
            raise ValueError(f"sink {name}: frames={frames}, expected={expected[name]}, "
                             f"finite_xyz={finite}; reject incomplete/empty benchmark")
        found[name].append({"frames": frames, "finite_xyz": finite})
    if any(len(records) != expected_runs for records in found.values()):
        counts = {name: len(records) for name, records in found.items()}
        raise ValueError(f"missing or inconsistent per-run sink observations: {counts}; "
                         f"expected {expected_runs} of each sink")
    return found

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("log", type=Path)
    parser.add_argument("--expected-runs", required=True, type=int,
                        help="reported engine/worker/repeat runs, plus static calibration if any")
    args = parser.parse_args()
    try:
        found = validate(args.config, args.log, args.expected_runs)
    except (OSError, ValueError, KeyError) as error:
        raise SystemExit(str(error))
    counts = {name: len(records) for name, records in found.items()}
    print(f"SINK CHECK OK: {counts}; every instance consumed all selected real frames")

if __name__ == "__main__":
    main()
