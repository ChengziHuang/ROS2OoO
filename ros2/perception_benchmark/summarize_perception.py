#!/usr/bin/env python3
"""Summarize the results of the perception benchmark."""
import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import io
import json
import math
from pathlib import Path
import re
import statistics

import check_sinks

TIMES = ("node_setup_s", "setup_s", "run_s", "total_s")
MEMORY = ("rss_before_mb", "rss_peak_mb")
STRUCTURE = ("tasks", "channels", "streams", "jobs", "active_jobs", "messages",
             "ring_slots", "payload_bytes", "nulls")
FIELDS = ("workload", "engine", "model", "workers", "cost_model", "runs", "hash",
          *TIMES, "speedup_run", "speedup_total", "frames_per_run", "frames_per_run_s",
          "rss_before_mb", "rss_peak_mb", "rss_added_mb", *STRUCTURE)

def numeric(row, name, context, positive=False):
    value = row.get(name)
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ValueError(f"{context}: {name} must be a finite number")
    if value < 0 or (positive and value <= 0):
        raise ValueError(f"{context}: {name} must be {'positive' if positive else 'nonnegative'}")
    return value

def number_list(row, name):
    return [value[name] for value in row if value.get(name) is not None]

def expected_configurations(manifest):
    engines = manifest["engines"]
    workers = manifest["workers"]
    repeat = manifest["repeat"]
    if not engines or not workers or not isinstance(repeat, int) or repeat <= 0:
        raise ValueError("run_manifest has an empty engine/worker list or invalid repeat")
    if any(not isinstance(value, int) or value <= 0 for value in workers):
        raise ValueError("run_manifest worker counts must be positive integers")
    if any(value not in {"seq", "barrier", "tag", "frontier", "static", "ooo"} for value in engines):
        raise ValueError("run_manifest has an unrecognized engine")
    expected = Counter()
    for engine in engines:
        for worker in [1] if engine == "seq" else workers:
            expected[(engine, worker)] += repeat
    return expected

def analyze_case(directory):
    directory = directory.resolve(strict=True)
    failed = directory / "FAILED"
    if failed.is_file() and failed.stat().st_size:
        raise ValueError(f"{failed}: recorded failed run; use a fresh successful result directory")
    manifest_path = directory / "run_manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    config_path = directory / manifest["config"]
    result_path = directory / manifest["results"]
    log_path = directory / manifest["stderr"]
    config = json.loads(config_path.read_text(encoding="utf-8"))
    for name, item in config.get("datasets", {}).items():
        for source_key in ("manifest", "metadata"):
            relative = item.get(source_key + "_snapshot")
            if relative is not None:
                snapshot = config_path.parent / relative
                digest = hashlib.sha256(snapshot.read_bytes()).hexdigest()
                if digest != item.get(source_key + "_sha256"):
                    raise ValueError(f"{snapshot}: {name} provenance digest differs from run_config")
    expected = expected_configurations(manifest)
    rows = []
    for number, line in enumerate(result_path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        context = f"{result_path}:{number}"
        try:
            row = json.loads(line)
        except json.JSONDecodeError as error:
            raise ValueError(f"{context}: non-JSON result line: {error}") from error
        if not isinstance(row, dict):
            raise ValueError(f"{context}: result must be an object")
        if not isinstance(row.get("hash"), str) or not re.fullmatch("[0-9a-fA-F]{16}", row["hash"]):
            raise ValueError(f"{context}: missing or malformed 64-bit output hash")
        if row.get("model") != manifest["model"]:
            raise ValueError(f"{context}: model differs from the recorded invocation")
        if row.get("horizon_ns") != config["horizon_ns"]:
            raise ValueError(f"{context}: horizon differs from the recorded graph")
        for field in TIMES:
            numeric(row, field, context, positive=field in ("run_s", "total_s"))
        timing_sum = sum(row[field] for field in ("node_setup_s", "setup_s", "run_s"))
        if not math.isclose(row["total_s"], timing_sum, rel_tol=3e-5, abs_tol=3e-6):
            raise ValueError(f"{context}: total_s does not equal node_setup_s+setup_s+run_s")
        for field in MEMORY:
            if field in row:
                numeric(row, field, context)
        if ("rss_before_mb" in row) != ("rss_peak_mb" in row):
            raise ValueError(f"{context}: partial RSS measurement")
        for field in STRUCTURE:
            if field in row:
                numeric(row, field, context)
        numeric(row, "messages", context, positive=True)
        numeric(row, "active_jobs", context, positive=True)
        if not isinstance(row.get("workers"), int) or isinstance(row["workers"], bool):
            raise ValueError(f"{context}: workers must be an integer")
        rows.append(row)
    actual = Counter((row.get("engine"), row["workers"]) for row in rows)
    if actual != expected:
        raise ValueError(f"{result_path}: incomplete/unexpected runs; got {dict(actual)}, "
                         f"expected {dict(expected)}")
    hashes = {row["hash"].lower() for row in rows}
    if len(hashes) != 1:
        raise ValueError(f"{result_path}: engines/worker counts/repeats have different hashes")
    log = log_path.read_text(encoding="utf-8", errors="replace")
    if "CHECK FAILED" in log or "CHECK OK:" not in log:
        raise ValueError(f"{log_path}: no successful driver --check result")
    calibration_runs = int("static" in manifest["engines"])
    expected_sink_runs = sum(expected.values()) + calibration_runs
    if manifest.get("expected_sink_runs", expected_sink_runs) != expected_sink_runs:
        raise ValueError("run_manifest has an inconsistent expected_sink_runs")
    observations = check_sinks.validate(config_path, log_path, expected_sink_runs)
    for name, records in observations.items():
        if len({record["finite_xyz"] for record in records}) != 1:
            raise ValueError(f"{log_path}: sink {name} has differing finite geometry counts")
    calibration_times = re.findall(
        r"calibration: a seq run measured the job costs in ([0-9.eE+-]+) s", log)
    if len(calibration_times) != calibration_runs:
        raise ValueError(f"{log_path}: missing/unexpected static calibration timing")
    groups = defaultdict(list)
    for row in rows:
        groups[(row["engine"], row["model"], row["workers"], row.get("cost_model", ""))].append(row)
    seq_run = [row["run_s"] for row in rows if row["engine"] == "seq"]
    seq_total = [row["total_s"] for row in rows if row["engine"] == "seq"]
    frames = sum(config["expected_pointcloud_frames"].values())
    aggregates = []
    for (engine, model, workers, cost_model), values in sorted(groups.items()):
        report = {
            "workload": manifest["workload"], "engine": engine, "model": model,
            "workers": workers, "cost_model": cost_model, "runs": len(values),
            "hash": next(iter(hashes)), "frames_per_run": frames,
        }
        for field in TIMES:
            report[field] = statistics.median(row[field] for row in values)
        report["speedup_run"] = statistics.median(seq_run) / report["run_s"] if seq_run else None
        report["speedup_total"] = statistics.median(seq_total) / report["total_s"] if seq_total else None
        report["frames_per_run_s"] = frames / report["run_s"]
        for field in MEMORY + STRUCTURE:
            samples = number_list(values, field)
            report[field] = statistics.median(samples) if samples else None
        peaks = number_list(values, "rss_peak_mb")
        report["rss_peak_mb"] = max(peaks) if peaks else None
        added = [max(0.0, row["rss_peak_mb"] - row["rss_before_mb"])
                 for row in values if "rss_peak_mb" in row]
        report["rss_added_mb"] = statistics.median(added) if added else None
        aggregates.append(report)
    finite_counts = {name: records[0]["finite_xyz"] for name, records in observations.items()}
    audit = {
        "workload": manifest["workload"], "result_directory": str(directory),
        "hash": next(iter(hashes)), "reported_runs": len(rows),
        "sink_instances_per_rig": expected_sink_runs,
        "pointcloud_frames_per_rig": config["expected_pointcloud_frames"],
        "finite_xyz_per_rig": finite_counts,
        "calibration_run_s": float(calibration_times[0]) if calibration_times else None,
        "validation": "all expected runs, one hash, all sink instances complete and finite",
    }
    return aggregates, audit

def display(value, digits=4):
    return "—" if value is None else f"{value:.{digits}g}"

def markdown(rows, audits):
    output = ["# Real perception benchmark", "",
              "Every workload passed the expected run-count, output-hash and per-instance sink checks.",
              "Times are medians in seconds. RSS peak is the maximum observed process peak in MiB.", "",
              "| Workload | Engine | Workers | Runs | Node setup | Sim setup | Run | Total | Run speedup | Peak RSS | Added RSS |",
              "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|"]
    for row in rows:
        engine = row["engine"] + ("-" + row["cost_model"] if row["cost_model"] else "")
        output.append("| " + " | ".join([
            row["workload"].replace("|", "\\|"), engine, str(row["workers"]), str(row["runs"]),
            *(display(row[field]) for field in TIMES), display(row["speedup_run"]),
            display(row["rss_peak_mb"]), display(row["rss_added_mb"]),
        ]) + " |")
    output.extend(["", "`total_s = node_setup_s + setup_s + run_s`; it excludes process startup/destruction,",
                   "component-library loading, offline input preparation and any separate static profiling run.",
                   "Node setup includes",
                   "record preload and component initialization. RSS before already includes preloaded",
                   "inputs; added RSS is the nonnegative peak-minus-before difference, not total engine memory.",
                   "A missing RSS measurement is reported as unavailable, never zero.", ""])
    for audit in audits:
        output.append(f"- **{audit['workload']}**: hash `{audit['hash']}`, "
                      f"{audit['reported_runs']} reported runs, "
                      f"{audit['sink_instances_per_rig']} complete sink instances per rig; "
                      f"frames `{audit['pointcloud_frames_per_rig']}`; "
                      f"finite XYZ `{audit['finite_xyz_per_rig']}`.")
        if audit["calibration_run_s"] is not None:
            output.append(f"  Separate static calibration callback execution: "
                          f"{display(audit['calibration_run_s'])} s; its node/setup costs are not in this log field.")
    return "\n".join(output) + "\n"

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directories", nargs="+", type=Path)
    parser.add_argument("--output-dir", type=Path,
                        help="default: RESULT_DIR/analysis for a single workload")
    args = parser.parse_args()
    if not args.output_dir and len(args.directories) != 1:
        parser.error("--output-dir is required when combining multiple workloads")
    try:
        rows, audits = [], []
        for directory in args.directories:
            values, audit = analyze_case(directory)
            rows.extend(values)
            audits.append(audit)
        names = [audit["workload"] for audit in audits]
        if len(names) != len(set(names)):
            raise ValueError("combined workloads need distinct names in run_manifest.json")
        destination = (args.output_dir or args.directories[0] / "analysis").resolve()
        if destination in {directory.resolve() for directory in args.directories}:
            raise ValueError("analysis output must be a separate directory, not a raw result directory")
        csv_text = io.StringIO(newline="")
        writer = csv.DictWriter(csv_text, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)
        report = {"schema_version": 1, "aggregation": "median timings; maximum RSS peak",
                  "workloads": audits, "configurations": rows}
        destination.mkdir(parents=True, exist_ok=True)
        (destination / "perception_summary.csv").write_text(csv_text.getvalue(), encoding="utf-8")
        (destination / "perception_summary.md").write_text(markdown(rows, audits), encoding="utf-8")
        (destination / "perception_summary.json").write_text(
            json.dumps(report, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    except (OSError, ValueError, KeyError, TypeError) as error:
        parser.exit(1, f"perception summary rejected: {error}\n")
    print(f"PERCEPTION SUMMARY OK: {len(audits)} workload(s), {len(rows)} configurations; {destination}")

if __name__ == "__main__":
    main()
