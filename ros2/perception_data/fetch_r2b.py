#!/usr/bin/env python3
"""List or download selected R2B2024 sequences from the public NGC API."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import sys
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from core import DataError, safe_relative_path

API = "https://api.ngc.nvidia.com/v2/resources/nvidia/isaac/r2bdataset2024/versions/1/files"
LICENSE_URL = "https://creativecommons.org/licenses/by/4.0/"

def request(url):
    return urlopen(Request(url, headers={"User-Agent": "let-ros-perception-data/1"}), timeout=60)

def parse_index(payload):
    if payload.get("requestStatus", {}).get("statusCode", "SUCCESS") != "SUCCESS":
        raise DataError("NGC file listing did not report SUCCESS")
    files = payload.get("recipeFiles")
    if not isinstance(files, list):
        raise DataError("NGC file listing has no recipeFiles array; API schema may have changed")
    out = []
    for f in files:
        path = safe_relative_path(f["path"])
        size = int(f["sizeInBytes"])
        if size < 0 or len(Path(path).parts) < 2:
            raise DataError(f"invalid NGC file entry: {path}")
        out.append({"path": path, "bytes": size, "url": API + "/" + quote(path, safe="/")})
    return out

def list_files():
    files, page = [], 0
    while True:
        url = API if page == 0 else API + f"?page={page}&pageSize=100"
        with request(url) as response:
            payload = json.load(response)
        batch = parse_index(payload)
        prior = {f["path"] for f in files}
        if any(f["path"] in prior for f in batch):
            raise DataError("NGC pagination repeated files; refusing an incomplete or duplicated listing")
        files.extend(batch)
        paging = payload.get("paginationInfo", {})
        if page + 1 >= int(paging.get("totalPages", 1)):
            expected = paging.get("totalResults")
            if expected is not None and len(files) != int(expected):
                raise DataError("NGC listing count does not match totalResults")
            return sorted(files, key=lambda f: f["path"])
        page += 1
        if page > 100:
            raise DataError("too many NGC file listing pages")

def select_files(files, sequence, metadata_only=False):
    if not sequence or "/" in sequence or "\\" in sequence or sequence in (".", ".."):
        raise DataError("one explicit --sequence is required to download; use --list to inspect")
    selected = [f for f in files if f["path"].startswith(sequence + "/")
                and (not metadata_only or f["path"].endswith("/metadata.yaml"))]
    if not selected:
        raise DataError(f"NGC has no selected files for sequence {sequence!r}")
    if not any(f["path"].endswith("/metadata.yaml") for f in selected):
        raise DataError("selected sequence has no rosbag2 metadata.yaml")
    return selected

def open_download(url):
    response = request(url)
    location = response.headers.get("Location")
    if location:
        response.close()
        parsed = urlparse(location)
        if parsed.scheme != "https" or not (parsed.hostname or "").endswith(".ngc.nvidia.com"):
            raise DataError("NGC supplied an unexpected or non-HTTPS download location")
        return request(location)
    if "application/json" in response.headers.get("Content-Type", ""):
        response.close()
        raise DataError("NGC file endpoint returned JSON without a download Location")
    return response

def download_one(entry, out):
    relative = safe_relative_path(entry["path"])
    target = (out / relative).resolve()
    if not target.is_relative_to(out.resolve()):
        raise DataError("download target escaped output directory")
    part = target.with_name(target.name + ".part")
    if target.exists() or part.exists():
        raise DataError(f"refusing to overwrite existing file: {target} (or its .part)")
    target.parent.mkdir(parents=True, exist_ok=True)
    count, digest = 0, hashlib.sha256()
    with open_download(entry["url"]) as response, part.open("xb") as f:
        for chunk in iter(lambda: response.read(1024 * 1024), b""):
            count += len(chunk)
            if count > entry["bytes"]:
                raise DataError(f"download larger than official size: {relative}; .part retained")
            f.write(chunk)
            digest.update(chunk)
    if count != entry["bytes"]:
        raise DataError(f"short download: {relative}: {count} != {entry['bytes']}; .part retained")
    part.rename(target)
    return {**entry, "sha256": digest.hexdigest(), "downloaded_bytes": count}

def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--list", action="store_true", help="only list official file names and sizes")
    p.add_argument("--sequence", help="one exact name, e.g. r2b_galileo or r2b_whitetunnel")
    p.add_argument("--out", type=Path, default=Path("data/r2b2024"))
    p.add_argument("--metadata-only", action="store_true", help="download only this sequence's small YAML")
    p.add_argument("--dry-run", action="store_true", help="list selected URLs and bytes, write nothing")
    p.add_argument("--max-total-bytes", type=int, default=600 * 1024 * 1024,
                   help="explicit download budget, default 600 MiB (no entire multi-GB collection)")
    a = p.parse_args(argv)
    if a.max_total_bytes <= 0:
        raise DataError("--max-total-bytes must be positive")
    if not a.list and not a.sequence:
        raise DataError("choose --list or one explicit --sequence")
    files = list_files()
    selected = select_files(files, a.sequence, a.metadata_only) if a.sequence else files
    print(json.dumps({"api": API, "license": LICENSE_URL, "files": selected,
                      "total_bytes": sum(f["bytes"] for f in selected)}, indent=2))
    if a.list or a.dry_run:
        return 0
    total = sum(f["bytes"] for f in selected)
    if total > a.max_total_bytes:
        raise DataError(f"selected sequence needs {total} bytes, exceeding "
                        f"--max-total-bytes={a.max_total_bytes}; raise the budget explicitly")
    out = a.out.resolve()
    manifest = out / a.sequence / "download_manifest.json"
    if manifest.exists():
        raise DataError(f"download manifest already exists: {manifest}")
    results = []
    for entry in selected:
        print(f"downloading {entry['path']} ({entry['bytes']} bytes)", flush=True)
        results.append(download_one(entry, out))
    manifest.write_text(json.dumps({"schema_version": 1, "api": API,
        "sequence": a.sequence, "license": LICENSE_URL,
        "created_utc": datetime.now(timezone.utc).isoformat(), "files": results,
        "checksum_note": "Local SHA256 plus official API size. NGC supplied no per-file SHA256; "
                         "this is not an independently verified publisher checksum."},
        indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return 0

if __name__ == "__main__":
    try:
        sys.exit(main())
    except (DataError, OSError, ValueError) as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
