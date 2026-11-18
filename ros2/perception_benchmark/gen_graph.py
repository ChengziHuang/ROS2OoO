#!/usr/bin/env python3
"""Build a graph of stereo components fed by prepared records."""
import argparse
import hashlib
import json
import re
from pathlib import Path

RIGS = ("front", "left", "right", "back")
PKG = "let_ros_perception"

def positive(text):
    value = int(text)
    if value <= 0:
        raise argparse.ArgumentTypeError("must be positive")
    return value

def nonnegative(text):
    value = int(text)
    if value < 0:
        raise argparse.ArgumentTypeError("must be nonnegative")
    return value

def manifest_info(path, maximum, period_override):
    path = path.expanduser().resolve(strict=True)
    rows = []
    with path.open(encoding="utf-8-sig", newline="") as stream:
        for number, line in enumerate(stream, 1):
            line = line.rstrip("\r\n")
            if not line.strip() or line.lstrip().startswith("#"):
                continue
            fields = line.split("\t")
            if len(fields) != 4 or any(not value for value in fields):
                raise ValueError(f"{path}:{number}: expected exactly four nonempty TSV paths")
            if maximum and len(rows) >= maximum:
                break
            paths = []
            for value in fields:
                record = Path(value)
                if not record.is_absolute():
                    record = path.parent / record
                record = record.resolve(strict=True)
                if not record.is_file() or record.stat().st_size == 0:
                    raise ValueError(f"{path}:{number}: missing or empty record: {record}")
                paths.append(record)
            rows.append(paths)
    if not rows:
        raise ValueError(f"{path}: no selected input frames")
    metadata_path = path.parent / "metadata.json"
    metadata = {}
    metadata_hash = None
    if metadata_path.is_file():
        metadata_bytes = metadata_path.read_bytes()
        metadata_hash = hashlib.sha256(metadata_bytes).hexdigest()
        metadata = json.loads(metadata_bytes.decode("utf-8"))
    period = period_override
    if period is None:
        period = metadata.get("replay", {}).get("period_ns", 33333333)
    if isinstance(period, bool) or not isinstance(period, int) or period <= 0:
        raise ValueError(f"{metadata_path}: replay.period_ns must be a positive integer")
    return {
        "manifest": str(path),
        "frames": len(rows),
        "period_ns": period,
        "selected_cdr_bytes": sum(item.stat().st_size for row in rows for item in row),
        "metadata": str(metadata_path) if metadata_path.is_file() else None,
        "metadata_sha256": metadata_hash,
        "manifest_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
    }

def choose_inputs(args):
    selected = {}
    for item in args.manifest:
        if "=" in item:
            name, value = item.split("=", 1)
        elif args.mode == "single":
            name, value = "stereo", item
        else:
            raise ValueError("four-rig --manifest arguments must be NAME=PATH")
        if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]*", name):
            raise ValueError(f"invalid rig/ROS namespace: {name!r}")
        if name in selected:
            raise ValueError(f"duplicate rig: {name}")
        selected[name] = Path(value)
    if args.dataset_root:
        if selected:
            raise ValueError("use --dataset-root or --manifest, not both")
        if args.mode == "single":
            selected["stereo"] = args.dataset_root / "frames.tsv"
        else:
            selected = {name: args.dataset_root / name / "frames.tsv" for name in RIGS}
    if args.mode == "single" and len(selected) != 1:
        raise ValueError("single mode requires exactly one real manifest")
    if args.mode == "r2b-four" and set(selected) != set(RIGS):
        raise ValueError("r2b-four requires front, left, right and back real manifests; "
                         "missing rigs are never replicated")
    resolved = [str(path.expanduser().resolve()) for path in selected.values()]
    if len(set(resolved)) != len(resolved):
        raise ValueError("each rig must use a distinct real manifest")
    return selected

class Graph:
    def __init__(self, output, deadline):
        self.output = output
        self.deadline = deadline
        self.nodes = []
        self.publish_sets = []
        self.deadlines = []

    def node(self, package, plugin, namespace, name, params, remaps=()):
        full_name = f"{namespace}/{name}"
        filename = full_name.strip("/").replace("/", "_") + ".yaml"
        parameter_file = self.output / "params" / filename
        all_params = {"use_sim_time": True, **params}
        text = "/**:\n  ros__parameters:\n"
        for key, value in all_params.items():
            text += f"    {key}: {json.dumps(value, ensure_ascii=True)}\n"
        parameter_file.write_text(text, encoding="utf-8")
        tokens = [package, plugin, "-r", f"__ns:={namespace}", "-r", f"__node:={name}",
                  "--params-file", str(parameter_file)]
        for original, mapped in remaps:
            tokens.extend(("-r", f"{original}:={mapped}"))
        self.nodes.append(" ".join(tokens))
        self.deadlines.append(f"{full_name}: {self.deadline}ns")
        return full_name

    def allow(self, full_name, callback, topics):
        self.publish_sets.append(f"{full_name} {callback} " + " ".join(topics or ["-"]))

    def sink(self, namespace, name, kind, topic, params=None):
        full_name = self.node(PKG, f"let_ros_perception::{kind}Sink", namespace, name,
                              params or {}, (("input", topic),))
        summary = full_name + "/summary"
        self.allow(full_name, topic, [summary])
        return summary

    def stereo_pair(self, name, data, args):
        namespace = "/" + name
        raw = [namespace + suffix for suffix in
               ("/left/image_raw", "/left/camera_info", "/right/image_raw", "/right/camera_info")]
        source = self.node(PKG, "let_ros_perception::StereoReplaySource", namespace, "replay", {
            "manifest": data["manifest"], "period_ns": data["period_ns"],
            "max_frames": args.max_frames,
        })
        self.allow(source, "timer", raw)
        rectified = []
        for side, image, info in (("left", raw[0], raw[1]), ("right", raw[2], raw[3])):
            rect = namespace + f"/{side}/image_rect"
            rectify = self.node("image_proc", "image_proc::RectifyNode",
                                namespace + "/" + side, "rectify", {
                                    "image_transport": "raw", "queue_size": args.queue_size,
                                }, (("image", image), ("image_rect", rect)))
            self.allow(rectify, image, [rect])
            self.allow(rectify, info, [rect])
            self.allow(rectify, "timer", [])
            rectified.append(rect)
        disparity_topic = namespace + "/disparity"
        disparity = self.node("stereo_image_proc", "stereo_image_proc::DisparityNode",
                              namespace, "disparity", {
                                  "image_transport": "raw", "queue_size": args.queue_size,
                                  "approximate_sync": False,
                                  "stereo_algorithm": 0 if args.stereo_algorithm == "bm" else 1,
                                  "disparity_range": args.disparity_range,
                                  "correlation_window_size": args.correlation_window_size,
                              })
        for topic in (rectified[0], raw[1], rectified[1], raw[3]):
            self.allow(disparity, topic, [disparity_topic])
        points_topic = namespace + "/points2"
        pointcloud = self.node("stereo_image_proc", "stereo_image_proc::PointCloudNode",
                               namespace, "pointcloud", {
                                   "image_transport": "raw", "queue_size": args.queue_size,
                                   "approximate_sync": False,
                                   "use_color": False, "avoid_point_cloud_padding": True,
                               }, (("left/image_rect_color", rectified[0]),))
        for topic in (rectified[0], raw[1], raw[3], disparity_topic):
            self.allow(pointcloud, topic, [points_topic])
        required = [self.sink(namespace, "pointcloud_sink", "PointCloud", points_topic,
                              {"min_finite_points": args.min_finite_points})]
        if args.observe_intermediates:
            for side, rect, info in (("left", rectified[0], raw[1]), ("right", rectified[1], raw[3])):
                required.append(self.sink(namespace, side + "_image_sink", "Image", rect))
                required.append(self.sink(namespace, side + "_info_sink", "CameraInfo", info))
            required.append(self.sink(namespace, "disparity_sink", "Disparity", disparity_topic))
        return required

    def write(self):
        for filename, lines in (("nodes.txt", self.nodes),
                                ("publish_sets.txt", self.publish_sets),
                                ("deadlines.txt", self.deadlines)):
            (self.output / filename).write_text("\n".join(lines) + "\n", encoding="utf-8")

def generate(args):
    inputs = choose_inputs(args)
    data = {name: manifest_info(path, args.max_frames, args.period_ns)
            for name, path in inputs.items()}
    output = args.output.expanduser().resolve()
    if any(char.isspace() or char == "#" for char in str(output)):
        raise ValueError("output graph path must contain neither whitespace nor '#': "
                         "let_ros_run uses a whitespace-delimited node list")
    if output.exists() and (not output.is_dir() or any(output.iterdir())):
        raise ValueError(f"graph output must be new or empty; refusing to overwrite {output}")
    (output / "params").mkdir(parents=True, exist_ok=True)
    provenance = output / "provenance"
    provenance.mkdir()
    for name, item in data.items():
        for source_key, hash_key, suffix in (
                ("manifest", "manifest_sha256", "frames.tsv"),
                ("metadata", "metadata_sha256", "metadata.json")):
            snapshot_key = source_key + "_snapshot"
            item[snapshot_key] = None
            if item[source_key] is not None:
                content = Path(item[source_key]).read_bytes()
                if hashlib.sha256(content).hexdigest() != item[hash_key]:
                    raise ValueError(f"input {source_key} changed while generating graph: {item[source_key]}")
                snapshot = provenance / f"{name}_{suffix}"
                snapshot.write_bytes(content)
                item[snapshot_key] = snapshot.relative_to(output).as_posix()
    graph = Graph(output, args.deadline_ns)
    required = []
    for name, item in data.items():
        required.extend(graph.stereo_pair(name, item, args))
    graph.write()
    horizon = max(item["period_ns"] * item["frames"] for item in data.values()) + \
        5 * args.deadline_ns + 1
    if horizon >= 2**63:
        raise ValueError("computed horizon exceeds signed 64-bit nanoseconds")
    driver_args = ["--nodes", str(output / "nodes.txt"),
                   "--publish-sets", str(output / "publish_sets.txt"),
                   "--deadlines", str(output / "deadlines.txt"),
                   "--horizon", str(horizon) + "ns", "--timer-phase", "period",
                   "--initialize-components", "--opencv-threads", "1"]
    for topic in required:
        driver_args.extend(("--require-output-topic", topic))
    config = {
        "schema_version": 1,
        "mode": args.mode,
        "input_contract": "real serialized inputs, periodic replay, original headers preserved",
        "replicated_rigs": False,
        "datasets": data,
        "node_count": len(graph.nodes),
        "deadline_ns": args.deadline_ns,
        "horizon_ns": horizon,
        "required_output_topics": required,
        "expected_pointcloud_frames": {
            f"/{name}/pointcloud_sink": item["frames"] for name, item in data.items()},
        "driver_args": driver_args,
        "observe_intermediates": args.observe_intermediates,
        "stereo_algorithm": args.stereo_algorithm,
        "opencv_threads": 1,
        "upstream_plugins": ["image_proc::RectifyNode", "stereo_image_proc::DisparityNode",
                             "stereo_image_proc::PointCloudNode"],
    }
    (output / "run_config.json").write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
    return config

def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--dataset-root", "--dataset", type=Path,
                        help="single: DIR/frames.tsv; r2b-four: DIR/{front,left,right,back}/frames.tsv")
    result.add_argument("--manifest", action="append", default=[], metavar="[NAME=]FILE",
                        help="repeat with all four named real manifests for r2b-four")
    result.add_argument("--mode", choices=("single", "r2b-four"), default="single")
    result.add_argument("--output", "-o", required=True, type=Path)
    result.add_argument("--period-ns", type=positive,
                        help="override metadata.json replay.period_ns (fallback: 33333333)")
    result.add_argument("--max-frames", type=nonnegative, default=0)
    result.add_argument("--deadline-ns", type=positive, default=1000000)
    result.add_argument("--queue-size", type=positive, default=10)
    result.add_argument("--min-finite-points", type=nonnegative, default=1)
    result.add_argument("--stereo-algorithm", choices=("bm", "sgbm"), default="bm")
    result.add_argument("--disparity-range", type=positive, default=64)
    result.add_argument("--correlation-window-size", type=positive, default=15)
    result.add_argument("--observe-intermediates", action="store_true",
                        help="add five extra diagnostic sinks per rig; report their cost separately")
    return result

def main():
    options = parser()
    args = options.parse_args()
    if args.disparity_range < 32 or args.disparity_range > 4096 or args.disparity_range % 16:
        options.error("--disparity-range must be a multiple of 16 in [32,4096]")
    if not 5 <= args.correlation_window_size <= 255 or args.correlation_window_size % 2 == 0:
        options.error("--correlation-window-size must be odd and in [5,255]")
    try:
        result = generate(args)
    except (OSError, ValueError, TypeError, KeyError) as error:
        options.error(str(error))
    print(f"generated {result['node_count']} upstream/replay/sink nodes; "
          f"horizon={result['horizon_ns']}ns; "
          f"expected pointcloud frames={result['expected_pointcloud_frames']}")

if __name__ == "__main__":
    main()
