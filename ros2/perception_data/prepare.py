#!/usr/bin/env python3
"""Export the stereo messages of a rosbag2 recording as the input of the perception source."""
from __future__ import annotations

import argparse
import copy
import json
import math
import os
from pathlib import Path
import sys
import tempfile
import time
from datetime import datetime, timezone
from fractions import Fraction

from core import (DataError, FIELDS, RIGS, Record, match_infos, pair_images,
                  period_provenance, r2b_topics, sha256_file, source_fingerprint,
                  validate_dimensions, write_manifest)

def stamp_ns(message):
    s = message.header.stamp
    if not 0 <= s.nanosec < 1_000_000_000:
        raise DataError("invalid nanosecond field in message header")
    return int(s.sec) * 1_000_000_000 + int(s.nanosec)

def set_stamp(message, ns):
    message.header.stamp.sec, message.header.stamp.nanosec = divmod(ns, 1_000_000_000)

def calibration_signature(m):
    values = list(m.d) + list(m.k) + list(m.r) + list(m.p)
    if not all(math.isfinite(float(x)) for x in values):
        raise DataError("CameraInfo contains non-finite calibration values")
    if len(m.k) != 9 or len(m.r) != 9 or len(m.p) != 12 or m.k[0] <= 0 or m.k[4] <= 0:
        raise DataError("CameraInfo is uncalibrated or has invalid K/R/P")
    return json.dumps({"d": list(m.d), "k": list(m.k), "r": list(m.r), "p": list(m.p),
                       "distortion_model": m.distortion_model,
                       "binning_x": m.binning_x, "binning_y": m.binning_y,
                       "roi": {k: getattr(m.roi, k) for k in
                               ("x_offset", "y_offset", "width", "height", "do_rectify")}},
                      sort_keys=True, allow_nan=False)

class BagReader:
    def __init__(self, bag, storage_id=""):
        try:
            import rosbag2_py
            from rclpy.serialization import deserialize_message, serialize_message
            from sensor_msgs.msg import CameraInfo, CompressedImage, Image
        except ImportError as e:
            raise DataError("run in a sourced ROS 2 Linux environment with rosbag2_py, "
                            "rclpy and sensor_msgs; no synthetic fallback exists") from e
        self.rosbag = rosbag2_py
        self.deserialize = deserialize_message
        self.serialize = serialize_message
        self.types = {"sensor_msgs/msg/Image": Image,
                      "sensor_msgs/msg/CompressedImage": CompressedImage,
                      "sensor_msgs/msg/CameraInfo": CameraInfo}
        self.bag = Path(bag).resolve()
        if not self.bag.exists():
            raise DataError(f"bag does not exist: {self.bag}")
        self.yaml_metadata = {}
        metadata_path = self.bag / "metadata.yaml" if self.bag.is_dir() else self.bag.parent / "metadata.yaml"
        if metadata_path.is_file():
            import yaml
            self.yaml_metadata = yaml.safe_load(metadata_path.read_text(encoding="utf-8")) or {}
        info = self.yaml_metadata.get("rosbag2_bagfile_information", {})
        self.storage_id = storage_id or info.get("storage_identifier", "")
        if not self.storage_id and self.bag.is_file():
            self.storage_id = {".mcap": "mcap", ".db3": "sqlite3"}.get(self.bag.suffix, "")
        compression = info.get("compression_format", "")
        self.reader_type = (rosbag2_py.SequentialCompressionReader if compression
                            else rosbag2_py.SequentialReader)
        self.reader = self.open()
        self.topics = {t.name: t.type for t in self.reader.get_all_topics_and_types()}
        md = self.reader.get_metadata()
        self.storage_files = list(md.relative_file_paths)

    def open(self):
        reader = self.reader_type()
        reader.open(self.rosbag.StorageOptions(uri=str(self.bag), storage_id=self.storage_id),
                    self.rosbag.ConverterOptions(input_serialization_format="cdr",
                                                output_serialization_format="cdr"))
        return reader

    def records(self, topics):
        reader = self.open()
        reader.set_filter(self.rosbag.StorageFilter(topics=sorted(topics)))
        uid = 0
        while reader.has_next():
            topic, raw, bag_ns = reader.read_next()
            yield uid, topic, raw, int(bag_ns)
            uid += 1

    def message(self, topic, raw):
        return self.deserialize(raw, self.types[self.topics[topic]])

def configurations(args):
    custom = [getattr(args, field + "_topic") for field in FIELDS]
    if args.all_rigs and (args.rig or any(custom)):
        raise DataError("--all-rigs cannot be combined with --rig or explicit topics")
    if any(custom):
        if args.rig or not all(custom):
            raise DataError("supply all four explicit topics, without --rig")
        if len(set(custom)) != 4:
            raise DataError("the four stereo topics must be distinct")
        return {"custom": dict(zip(FIELDS, custom))}
    if args.all_rigs:
        return {rig: r2b_topics(rig) for rig in RIGS}
    return {args.rig or "front": r2b_topics(args.rig or "front")}

def scan_and_plan(bag, configs, args):
    all_topics = {t for cfg in configs.values() for t in cfg.values()}
    for rig, cfg in configs.items():
        for field, topic in cfg.items():
            allowed = ({"sensor_msgs/msg/CameraInfo"} if field.endswith("info") else
                       {"sensor_msgs/msg/Image", "sensor_msgs/msg/CompressedImage"})
            if topic not in bag.topics:
                raise DataError(f"{rig}: missing topic {topic}; --all-rigs never copies another camera")
            if bag.topics[topic] not in allowed:
                raise DataError(f"{topic}: expected {sorted(allowed)}, got {bag.topics[topic]}")
    indexed = {topic: [] for topic in all_topics}
    for uid, topic, raw, recorded_ns in bag.records(all_topics):
        m = bag.message(topic, raw)
        is_info = bag.topics[topic] == "sensor_msgs/msg/CameraInfo"
        r = Record(uid, stamp_ns(m), recorded_ns, m.header.frame_id,
                   int(getattr(m, "width", 0)), int(getattr(m, "height", 0)),
                   calibration_signature(m) if is_info else "")
        indexed[topic].append(r)
    plans = {}
    for rig, cfg in configs.items():
        pairs, counts = pair_images(indexed[cfg["left_image"]], indexed[cfg["right_image"]],
                                    args.tolerance_ns)
        print(f"{rig}: " + json.dumps(counts, sort_keys=True), flush=True)
        if counts["unmatched_left"] + counts["unmatched_right"] > args.max_unmatched:
            raise DataError(f"{rig}: unmatched images exceed --max-unmatched={args.max_unmatched}; "
                            "inspect reported counts, then explicitly allow a bounded omission")
        selected = pairs[args.start_frame:args.start_frame + args.max_frames if args.max_frames else None]
        if not selected:
            raise DataError(f"{rig}: no stereo frames selected")
        li = match_infos([p[0] for p in selected], indexed[cfg["left_info"]],
                         args.reuse_camera_info, "left")
        ri = match_infos([p[1] for p in selected], indexed[cfg["right_info"]],
                         args.reuse_camera_info, "right")
        rows = [{"left_image": pair[0], "left_info": l,
                 "right_image": pair[1], "right_info": r}
                for pair, l, r in zip(selected, li, ri)]
        counts.update(frames_exported=len(rows),
                      pairs_omitted_by_explicit_slice=len(pairs) - len(rows),
                      camera_infos_read={side: len(indexed[cfg[side + "_info"]])
                                         for side in ("left", "right")},
                      camera_infos_not_referenced_by_export={
                          side: len(indexed[cfg[side + "_info"]]) - len({row[side + "_info"].uid
                                                                        for row in rows})
                          for side in ("left", "right")})
        plans[rig] = {"rows": rows, "counts": counts,
                      "replay": period_provenance([p[0].stamp_ns for p in selected], args.period_ns)}
    return plans, all_topics

def decode_image(message, type_name):
    if type_name == "sensor_msgs/msg/Image":
        if message.step <= 0 or len(message.data) != message.step * message.height:
            raise DataError("raw Image byte count does not match height * step")
        return message, {"decoder": "none (original sensor_msgs/Image)",
                         "encoding": message.encoding}
    if "compressedDepth" in message.format:
        raise DataError("compressedDepth is not an ordinary stereo image; unsupported")
    if message.format.strip().lower() in ("h264", "h.264", "avc"):
        raise DataError("H264 must be decoded sequentially from the beginning of each topic")
    try:
        import cv2
        import numpy as np
        from cv_bridge import CvBridge
    except ImportError as e:
        raise DataError("CompressedImage input requires cv_bridge, OpenCV and NumPy") from e
    decoded = cv2.imdecode(np.frombuffer(bytes(message.data), dtype=np.uint8), cv2.IMREAD_UNCHANGED)
    if decoded is None:
        raise DataError("OpenCV could not decode CompressedImage")
    bits = {np.dtype("uint8"): "8", np.dtype("uint16"): "16"}.get(decoded.dtype)
    if bits is None:
        raise DataError(f"unsupported decoded pixel dtype: {decoded.dtype}")
    channels = 1 if decoded.ndim == 2 else decoded.shape[2] if decoded.ndim == 3 else 0
    layout = {1: "mono", 3: "bgr", 4: "bgra"}.get(channels)
    if layout is None:
        raise DataError(f"unsupported decoded shape: {decoded.shape}")
    encoding = layout + bits
    out = CvBridge().cv2_to_imgmsg(decoded, encoding=encoding)
    out.header = copy.deepcopy(message.header)
    return out, {"decoder": "cv2.imdecode(IMREAD_UNCHANGED) + cv_bridge.cv2_to_imgmsg",
                 "opencv_version": cv2.__version__, "format": message.format,
                 "encoding": encoding, "lossless_pixel_recovery_claimed": "png" in message.format.lower(),
                 "note": "JPEG is decoded from the original lossy recording; no resize; native decoder color conversion only"}

class H264Decoder:
    def __init__(self, codec=None, packet_factory=None):
        self.av_version = "test-injected-codec"
        self.ffmpeg_libraries = {}
        if codec is None:
            try:
                import av
            except ImportError as e:
                raise DataError("H264 CompressedImage requires PyAV: python -m pip install av") from e
            codec = av.CodecContext.create("h264", "r")
            codec.thread_count = 1
            packet_factory = av.Packet
            self.av_version = av.__version__
            self.ffmpeg_libraries = av.library_versions
        self.codec = codec
        self.packet_factory = packet_factory
        self.pending = {}
        self.packets = self.frames = 0
        self.time_base = Fraction(1, 1_000_000_000)

    def mapped(self, frames):
        outputs = []
        for frame in frames:
            uid = frame.pts
            if uid is None or uid not in self.pending:
                raise DataError(f"H264 decoder returned missing/unknown/duplicate PTS {uid}; "
                                "cannot preserve the original ROS-header mapping")
            if getattr(frame, "is_corrupt", False):
                raise DataError(f"H264 decoder flagged corrupt frame at source record {uid}")
            header = self.pending.pop(uid)
            self.frames += 1
            outputs.append((uid, frame, header))
        return outputs

    def feed(self, uid, message):
        if uid in self.pending:
            raise DataError("duplicate H264 source record")
        self.pending[uid] = copy.deepcopy(message.header)
        packet = self.packet_factory(bytes(message.data))
        packet.pts = uid
        packet.time_base = self.time_base
        self.packets += 1
        try:
            frames = self.codec.decode(packet)
        except Exception as e:
            raise DataError(f"H264 decoder failed at source record {uid}: {e}") from e
        return self.mapped(frames)

    def finish(self):
        try:
            outputs = self.mapped(self.codec.decode(None))
        except DataError:
            raise
        except Exception as e:
            raise DataError(f"H264 decoder flush failed: {e}") from e
        if self.pending or self.packets != self.frames:
            raise DataError(f"H264 packet/frame mapping is not one-to-one: "
                            f"{self.packets} packets, {self.frames} frames, "
                            f"{len(self.pending)} unmapped headers")
        return outputs

    def provenance(self):
        return {"decoder": "PyAV/FFmpeg h264, per-topic sequential decode from first bag packet",
                "pyav_version": self.av_version, "decoder_threads": 1,
                "ffmpeg_libraries": self.ffmpeg_libraries,
                "packets_decoded_including_preroll": self.packets,
                "frames_mapped_including_preroll": self.frames,
                "mapping": "unique bag record ID set as packet PTS; decoded frame PTS selects original ROS header",
                "one_packet_one_mapped_frame_verified": True,
                "encoding": "bgr8", "pixel_conversion": "decoded video frame to bgr24; no resize",
                "lossless_pixel_recovery_claimed": False}

def video_image(frame, header):
    try:
        from cv_bridge import CvBridge
    except ImportError as e:
        raise DataError("H264 export requires cv_bridge") from e
    image = CvBridge().cv2_to_imgmsg(frame.to_ndarray(format="bgr24"), encoding="bgr8")
    image.header = header
    return image

def export(bag, configs, plans, topics, stage, args, source, started):
    wanted = {r.uid for plan in plans.values() for row in plan["rows"] for r in row.values()}
    with tempfile.TemporaryDirectory(prefix=".selected-cdr-", dir=stage.parent) as temporary:
        spool = Path(temporary)
        found, decoded, decoders = set(), {}, {}
        def save_video(topic, outputs):
            for uid, frame, header in outputs:
                if uid in wanted:
                    (spool / str(uid)).write_bytes(bag.serialize(video_image(frame, header)))
                    found.add(uid)
                    decoded[uid] = topic

        for uid, topic, raw, _ in bag.records(topics):
            if bag.topics[topic] == "sensor_msgs/msg/CompressedImage":
                message = bag.message(topic, raw)
                h264 = message.format.strip().lower() in ("h264", "h.264", "avc")
                if h264:
                    decoder = decoders.setdefault(topic, None)
                    if decoder is None:
                        decoder = decoders[topic] = H264Decoder()
                    save_video(topic, decoder.feed(uid, message))
                elif topic in decoders:
                    raise DataError(f"{topic}: mixed H264 and non-video packets are unsupported")
                elif uid in wanted:
                    (spool / str(uid)).write_bytes(raw)
                    found.add(uid)
            elif uid in wanted:
                (spool / str(uid)).write_bytes(raw)
                found.add(uid)
            if found == wanted:
                break
        for topic, decoder in decoders.items():
            save_video(topic, decoder.finish())
        if found != wanted:
            raise DataError("bag changed between passes or a required selected message disappeared")
        decode_info = {uid: {**decoders[topic].provenance(), "source_topic": topic}
                       for uid, topic in decoded.items()}
        return export_spooled(bag, configs, plans, spool, stage, args, source, started, decode_info)

def export_spooled(bag, configs, plans, spool, stage, args, source, started, decode_info):
    result = {}
    for rig, plan in plans.items():
        folder = stage / rig if args.all_rigs else stage
        (folder / "frames").mkdir(parents=True)
        manifest_rows, provenance, decode_modes, files = [], [], {}, []
        for i, row in enumerate(plan["rows"]):
            messages = {field: bag.deserialize((spool / str(r.uid)).read_bytes(),
                        bag.types["sensor_msgs/msg/Image" if r.uid in decode_info
                                  else bag.topics[configs[rig][field]]]) for field, r in row.items()}
            images, descriptions = {}, {}
            for side in ("left", "right"):
                field = side + "_image"
                images[side], description = decode_image(messages[field],
                    "sensor_msgs/msg/Image" if row[field].uid in decode_info
                    else bag.topics[configs[rig][field]])
                descriptions[side] = decode_info.get(row[field].uid, description)
            lm, rm = images["left"], images["right"]
            ldec, rdec = descriptions["left"], descriptions["right"]
            lm, rm = copy.deepcopy(lm), copy.deepcopy(rm)
            lin, rin = (copy.deepcopy(messages[s + "_info"]) for s in ("left", "right"))
            validate_dimensions(lm.width, lm.height, rm.width, rm.height,
                                row["left_info"], row["right_info"])
            original = {field: r.stamp_ns for field, r in row.items()}
            if args.restamp_paired_images:
                set_stamp(rm, stamp_ns(lm))
            if args.reuse_camera_info or args.restamp_paired_images:
                set_stamp(lin, stamp_ns(lm))
                set_stamp(rin, stamp_ns(rm))
            outputs = (lm, lin, rm, rin)
            paths = []
            for field, m in zip(FIELDS, outputs):
                rel = f"frames/{i:06d}_{field}.cdr"
                path = folder / rel
                payload = bag.serialize(m)
                path.write_bytes(payload)
                files.append({"path": rel, "bytes": len(payload), "sha256": sha256_file(path)})
                paths.append(rel)
            manifest_rows.append(paths)
            provenance.append({"frame": i,
                               "source_record_ids": {k: r.uid for k, r in row.items()},
                               "original_header_stamp_ns": original,
                               "output_header_stamp_ns": {k: stamp_ns(m) for k, m in zip(FIELDS, outputs)},
                               "image_frame_ids": {"left": lm.header.frame_id, "right": rm.header.frame_id},
                               "bag_record_stamp_ns": {k: r.bag_ns for k, r in row.items()}})
            decode_modes[json.dumps(ldec, sort_keys=True)] = ldec
            decode_modes[json.dumps(rdec, sort_keys=True)] = rdec
        write_manifest(folder / "frames.tsv", manifest_rows)
        meta = {"schema_version": 1, "pair": rig, "frames": len(manifest_rows),
                "created_utc": datetime.now(timezone.utc).isoformat(),
                "source": source, "topics": {k: {"name": v, "type": bag.topics[v]}
                                              for k, v in configs[rig].items()},
                "parameters": vars(args), "counts": plan["counts"], "replay": plan["replay"],
                "time_range_ns": {"first_left_image_header": plan["rows"][0]["left_image"].stamp_ns,
                                  "last_left_image_header": plan["rows"][-1]["left_image"].stamp_ns},
                "decoding": list(decode_modes.values()),
                "transformations": {"resize": False, "synthetic_pixels": False,
                                    "reuse_static_camera_info": args.reuse_camera_info,
                                    "camera_info_restamp": args.reuse_camera_info or args.restamp_paired_images,
                                    "right_image_restamp_to_left": args.restamp_paired_images,
                                    "tolerance_ns": args.tolerance_ns},
                "frame_provenance": provenance, "output_files": files,
                "manifest_sha256": sha256_file(folder / "frames.tsv"),
                "tool_sha256": {p.name: sha256_file(p) for p in
                                (Path(__file__), Path(__file__).with_name("core.py"))},
                "preparation_s": time.perf_counter() - started,
                "preparation_in_timed_simulation": False}
        (folder / "metadata.json").write_text(json.dumps(meta, indent=2, sort_keys=True) + "\n", encoding="utf-8")
        result[rig] = {"manifest": f"{rig}/frames.tsv" if args.all_rigs else "frames.tsv",
                       "frames": meta["frames"], "period_ns": meta["replay"]["period_ns"]}
    if args.all_rigs:
        (stage / "metadata.json").write_text(json.dumps({"schema_version": 1,
            "source": source, "rigs": result, "parameters": vars(args),
            "preparation_s": time.perf_counter() - started}, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return result

def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("bag", help="real rosbag2 directory, .mcap or .db3")
    p.add_argument("--out", help="new output directory (existing directories are never overwritten)")
    p.add_argument("--storage-id", default="", help="normally inferred from metadata.yaml")
    p.add_argument("--list-topics", action="store_true", help="inspect bag topic names/types without export")
    p.add_argument("--rig", choices=RIGS)
    p.add_argument("--all-rigs", action="store_true", help="require and export four distinct R2B2024 rigs")
    for field in FIELDS:
        p.add_argument("--" + field.replace("_", "-") + "-topic")
    p.add_argument("--max-frames", type=int, default=100, help="per rig; 0 exports all, default 100")
    p.add_argument("--start-frame", type=int, default=0, help="index in matched stereo pairs")
    p.add_argument("--max-unmatched", type=int, default=0,
                   help="maximum explicitly allowed total unmatched left/right images in the entire bag")
    p.add_argument("--reuse-camera-info", action="store_true",
                   help="reuse only globally invariant calibration, explicitly restamping info to each image")
    p.add_argument("--tolerance-ns", type=int, default=0, help="default exact left/right image stamps")
    p.add_argument("--restamp-paired-images", action="store_true",
                   help="explicitly stamp right images and both infos with left image time for exact-sync nodes")
    p.add_argument("--period-ns", type=int, help="source replay period; default median selected left-image interval")
    return p

def main(argv=None):
    args = parser().parse_args(argv)
    started = time.perf_counter()
    if min(args.max_frames, args.start_frame, args.max_unmatched, args.tolerance_ns) < 0:
        raise DataError("frame limits, unmatched limit and tolerance must be nonnegative")
    if args.period_ns is not None and args.period_ns <= 0:
        raise DataError("--period-ns must be > 0")
    if args.tolerance_ns and not args.restamp_paired_images:
        raise DataError("nonzero --tolerance-ns requires explicit --restamp-paired-images; "
                        "otherwise exact-sync stereo nodes would receive differing stamps")
    bag = BagReader(args.bag, args.storage_id)
    if args.list_topics:
        print(json.dumps(bag.topics, indent=2, sort_keys=True))
        return 0
    if not args.out:
        raise DataError("--out is required unless --list-topics")
    out = Path(args.out).resolve()
    if out.exists():
        raise DataError(f"output already exists: {out}")
    configs = configurations(args)
    plans, topics = scan_and_plan(bag, configs, args)
    source = source_fingerprint(bag.bag, bag.storage_files)
    snapshot = {f["path"]: ((bag.bag if bag.bag.is_dir() else bag.bag.parent) / f["path"]).stat().st_mtime_ns
                for f in source["files"]}
    out.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix=".perception-prepare-", dir=out.parent) as temporary:
        stage = Path(temporary) / "export"
        stage.mkdir()
        result = export(bag, configs, plans, topics, stage, args, source, started)
        root = bag.bag if bag.bag.is_dir() else bag.bag.parent
        if any((root / f["path"]).stat().st_mtime_ns != snapshot[f["path"]]
               or (root / f["path"]).stat().st_size != f["bytes"] for f in source["files"]):
            raise DataError("source bag changed during preparation")
        os.rename(stage, out)
    print(json.dumps({"output": str(out), "rigs": result,
                      "preparation_s": time.perf_counter() - started}, indent=2, sort_keys=True))
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (DataError, OSError, RuntimeError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
