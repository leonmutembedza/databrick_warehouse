# Databricks notebook source
# Bounded PTB-XL landing task. Full release download is opt-in via max_records=0.

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path
import re
import urllib.request


dbutils.widgets.text("target_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/ptb-xl/1.0.3", "PTB-XL landing folder")
dbutils.widgets.text("version", "1.0.3", "Available PTB-XL release (1.0.0 is no longer served)")
dbutils.widgets.text("max_records", "5", "Records to land; 0 means the full release")
target = Path(dbutils.widgets.get("target_path").rstrip("/"))
version = dbutils.widgets.get("version").strip()
max_records = int(dbutils.widgets.get("max_records"))
if not str(target).startswith("/Volumes/"):
    raise ValueError("target_path must be inside a Unity Catalog Volume")
if version != "1.0.3":
    raise ValueError("PTB-XL v1.0.0 files are no longer available on PhysioNet; use v1.0.3")
if max_records < 0:
    raise ValueError("max_records must be zero or positive")

base_url = f"https://physionet.org/files/ptb-xl/{version}/"
target.mkdir(parents=True, exist_ok=True)


def download(relative: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9_./-]+", relative) or ".." in Path(relative).parts:
        raise ValueError(f"Unsafe PTB-XL path: {relative}")
    destination = target / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists() and destination.stat().st_size:
        return
    request = urllib.request.Request(base_url + relative, headers={"User-Agent": "thesis-batch-pipeline/0.1"})
    with urllib.request.urlopen(request, timeout=60) as response, destination.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)


download("ptbxl_database.csv")
download("scp_statements.csv")
metadata = target / "ptbxl_database.csv"
with metadata.open("r", encoding="utf-8", newline="") as handle:
    records = list(csv.DictReader(handle))
if max_records:
    records = records[:max_records]
if not records:
    raise RuntimeError("PTB-XL metadata contains no records")
for row in records:
    base = row.get("filename_lr")
    if not base:
        raise ValueError("PTB-XL row has no filename_lr; check the source metadata format")
    download(base + ".hea")
    download(base + ".dat")

manifest = []
for path in sorted(target.rglob("*")):
    if path.is_file():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest.append({"path": str(path), "bytes": path.stat().st_size, "sha256": digest})
print(json.dumps({"dataset": "ptb-xl", "version": version, "records_landed": len(records), "files": len(manifest), "file_manifest": manifest[:20]}, indent=2))
