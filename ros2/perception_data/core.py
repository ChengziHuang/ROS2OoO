from __future__ import annotations

import hashlib
import json
import statistics
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Iterable

RIGS = ("front", "left", "right", "back")
FIELDS = ("left_image", "left_info", "right_image", "right_info")

class DataError(ValueError):
    pass

@dataclass(frozen=True)
class Record:
    uid: int
    stamp_ns: int
    bag_ns: int
    frame_id: str
    width: int = 0
    height: int = 0
    calibration: str = ""

def r2b_topics(rig: str) -> dict[str, str]:
    if rig not in RIGS:
        raise DataError(f"unknown R2B2024 rig: {rig}")
    p = f"/{rig}_stereo_camera"
    return {f"{s}_{kind}": f"{p}/{s}/" +
            ("image_compressed" if kind == "image" else "camera_info")
            for s in ("left", "right") for kind in ("image", "info")}

def unique_stamps(records: Iterable[Record], what: str) -> dict[int, Record]:
    out = {}
    for r in records:
        if r.stamp_ns < 0:
            raise DataError(f"{what}: negative header stamp {r.stamp_ns}")
        if r.stamp_ns in out:
            raise DataError(f"{what}: duplicate header stamp {r.stamp_ns}; ambiguous pairing")
        out[r.stamp_ns] = r
    return out

def pair_images(left: list[Record], right: list[Record], tolerance_ns: int = 0):
    if tolerance_ns < 0:
        raise DataError("tolerance must be >= 0")
    ls = sorted(unique_stamps(left, "left images").values(), key=lambda r: r.stamp_ns)
    rs = sorted(unique_stamps(right, "right images").values(), key=lambda r: r.stamp_ns)
    pairs, unmatched_l, unmatched_r = [], [], []
    i = j = 0
    while i < len(ls) and j < len(rs):
        l, r = ls[i], rs[j]
        delta = r.stamp_ns - l.stamp_ns
        if delta < -tolerance_ns:
            unmatched_r.append(r.stamp_ns)
            j += 1
        elif delta > tolerance_ns:
            unmatched_l.append(l.stamp_ns)
            i += 1
        else:
            if ((i + 1 < len(ls) and abs(ls[i + 1].stamp_ns - r.stamp_ns) <= tolerance_ns)
                    or (j + 1 < len(rs) and abs(rs[j + 1].stamp_ns - l.stamp_ns) <= tolerance_ns)):
                raise DataError(f"ambiguous stereo match around {l.stamp_ns}, {r.stamp_ns}; "
                                "reduce --tolerance-ns")
            pairs.append((l, r))
            i += 1
            j += 1
    unmatched_l.extend(r.stamp_ns for r in ls[i:])
    unmatched_r.extend(r.stamp_ns for r in rs[j:])
    return pairs, {"left_images_read": len(ls), "right_images_read": len(rs),
                   "pairs_available": len(pairs),
                   "unmatched_left": len(unmatched_l), "unmatched_right": len(unmatched_r),
                   "unmatched_left_stamp_examples": unmatched_l[:20],
                   "unmatched_right_stamp_examples": unmatched_r[:20]}

def match_infos(images: list[Record], infos: list[Record], reuse_static: bool, side: str):
    if not infos:
        raise DataError(f"{side}: no CameraInfo messages")
    if reuse_static:
        signatures = {(r.frame_id, r.width, r.height, r.calibration) for r in infos}
        if len(signatures) != 1:
            raise DataError(f"{side}: --reuse-camera-info requires identical calibration and "
                            "frame_id in every CameraInfo")
        indexed = None
        representative = min(infos, key=lambda r: r.bag_ns)
    else:
        indexed = unique_stamps(infos, f"{side} CameraInfo")
    matched = []
    missing = []
    for image in images:
        info = representative if reuse_static else indexed.get(image.stamp_ns)
        if info is None:
            missing.append(image.stamp_ns)
            continue
        if not image.frame_id or image.frame_id != info.frame_id:
            raise DataError(f"{side}: image and CameraInfo frame_id differ: "
                            f"{image.frame_id!r} vs {info.frame_id!r}")
        if info.width <= 0 or info.height <= 0:
            raise DataError(f"{side}: invalid CameraInfo dimensions")
        if image.width and (image.width, image.height) != (info.width, info.height):
            raise DataError(f"{side}: image dimensions do not match CameraInfo")
        matched.append(info)
    if missing:
        raise DataError(f"{side}: {len(missing)} images lack exact-stamp CameraInfo "
                        f"(examples {missing[:5]}). Reuse is only available with "
                        "explicit --reuse-camera-info and invariant calibration.")
    return matched

def validate_dimensions(lw: int, lh: int, rw: int, rh: int,
                        li: Record, ri: Record):
    if min(lw, lh, rw, rh) <= 0:
        raise DataError("decoded image has invalid dimensions")
    if (lw, lh) != (rw, rh):
        raise DataError("stereo images have different dimensions")
    if (lw, lh) != (li.width, li.height) or (rw, rh) != (ri.width, ri.height):
        raise DataError("decoded image dimensions do not match CameraInfo; no implicit resize")

def period_provenance(stamps: list[int], period_ns: int | None) -> dict:
    intervals = [b - a for a, b in zip(stamps, stamps[1:])]
    if any(d <= 0 for d in intervals):
        raise DataError("selected frame stamps must be strictly increasing")
    if period_ns is None:
        if not intervals:
            raise DataError("one frame needs an explicit --period-ns")
        period_ns = int(statistics.median(intervals))
        selection = "median selected left-image header-stamp interval (rounded down)"
    else:
        selection = "explicit --period-ns"
    if period_ns <= 0:
        raise DataError("period_ns must be > 0")
    errors = [s - stamps[0] - i * period_ns for i, s in enumerate(stamps)]
    return {"period_ns": period_ns, "period_selection": selection,
            "schedule": "one stereo frame per source timer activation; no source looping",
            "header_stamps": "original epoch and frame_id retained unless explicit opt-in restamp",
            "original_interval_ns": {"min": min(intervals) if intervals else None,
                                     "median": statistics.median(intervals) if intervals else None,
                                     "max": max(intervals) if intervals else None},
            "max_abs_periodic_schedule_error_ns": max(map(abs, errors), default=0),
            "original_header_stamps_are_release_schedule": False,
            "description": "Real sensor pixels and calibration, replayed periodically under LET. "
                           "This changes arrival timing; it is not a native-time rosbag replay."}

def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for data in iter(lambda: f.read(1024 * 1024), b""):
            h.update(data)
    return h.hexdigest()

def source_fingerprint(bag: Path, relative_files: list[str]) -> dict:
    bag = bag.resolve()
    root = bag if bag.is_dir() else bag.parent
    paths = [bag] if bag.is_file() else [root / p for p in relative_files]
    meta = root / "metadata.yaml"
    if meta.is_file() and meta not in paths:
        paths.append(meta)
    if not paths:
        raise DataError("bag metadata lists no storage files")
    rows = []
    for p in sorted(set(paths)):
        p = p.resolve()
        if not p.is_relative_to(root) or not p.is_file():
            raise DataError(f"invalid/missing bag storage file: {p}")
        rows.append({"path": p.relative_to(root).as_posix(), "bytes": p.stat().st_size,
                     "sha256": sha256_file(p)})
    digest = hashlib.sha256(json.dumps(rows, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    return {"uri": str(bag), "files": rows, "sha256": digest,
            "sha256_definition": "SHA256 of canonical JSON file list (path, bytes, sha256)",
            "metadata_yaml": meta.read_text(encoding="utf-8") if meta.is_file() else None}

def safe_relative_path(name: str) -> str:
    p = PurePosixPath(name)
    if (not name or "\\" in name or "\t" in name or "\n" in name or "\r" in name
            or p.is_absolute() or ".." in p.parts or ":" in name or name.startswith("#")):
        raise DataError(f"unsafe relative path: {name!r}")
    return p.as_posix()

def write_manifest(path: Path, rows: Iterable[Iterable[str]]):
    lines = ["# left Image CDR\tleft CameraInfo CDR\tright Image CDR\tright CameraInfo CDR"]
    count = 0
    for row in rows:
        row = list(row)
        if len(row) != 4:
            raise DataError("frames.tsv requires exactly four paths per row")
        checked = [safe_relative_path(s) for s in row]
        for s in checked:
            if not (path.parent / s).is_file():
                raise DataError(f"manifest target is missing: {s}")
        lines.append("\t".join(checked))
        count += 1
    if not count:
        raise DataError("refusing to write an empty frames.tsv")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
