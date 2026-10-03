# Databricks notebook source
# Download a small, public BIDMC sample into the Unity Catalog raw Volume.
# Configure this notebook as a task before `ingest_files` in the batch Job.

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path
from urllib.request import Request, urlopen


dbutils.widgets.text(
    "target_path",
    "/Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0",
    "Unity Catalog Volume landing directory",
)

target_path = dbutils.widgets.get("target_path").rstrip("/")
if not target_path.startswith("/Volumes/"):
    raise ValueError("target_path must be a Unity Catalog Volume path beginning with /Volumes/.")

source_base_url = "https://physionet.org/files/bidmc/1.0.0/bidmc_csv"
dataset_license = "Open Data Commons Attribution License v1.0"
source_files = [
    "bidmc_01_Signals.csv",
    "bidmc_01_Numerics.csv",
]

dbutils.fs.mkdirs(target_path)
results = []

for filename in source_files:
    source_url = f"{source_base_url}/{filename}"
    request = Request(source_url, headers={"User-Agent": "Thesis-BIDMC-Batch-Pipeline/0.1"})
    with urlopen(request, timeout=60) as response:
        if response.status != 200:
            raise RuntimeError(f"Download failed for {source_url}: HTTP {response.status}")
        payload = response.read()

    # Reject HTML error pages or unexpected content before writing into the raw zone.
    try:
        first_line = payload.decode("utf-8-sig").splitlines()[0]
    except (UnicodeDecodeError, IndexError) as exc:
        raise RuntimeError(f"Downloaded content is not a non-empty UTF-8 CSV: {source_url}") from exc
    if "," not in first_line or "<html" in first_line.lower():
        raise RuntimeError(f"Downloaded content does not look like a CSV: {source_url}")

    checksum = hashlib.sha256(payload).hexdigest()
    destination = f"{target_path}/{filename}"

    if os.path.exists(destination):
        with Path(destination).open("rb") as existing_file:
            existing_checksum = hashlib.sha256(existing_file.read()).hexdigest()
        if existing_checksum != checksum:
            raise RuntimeError(
                f"Refusing to replace existing file with different content: {destination}. "
                "Resolve the source-file conflict before rerunning."
            )
        status = "already_present"
    else:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(prefix="bidmc_", suffix=".csv", delete=False) as temp_file:
                temp_file.write(payload)
                temp_path = temp_file.name
            dbutils.fs.cp(f"file:{temp_path}", destination)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)
        status = "downloaded"

    results.append(
        {
            "file_name": filename,
            "source_url": source_url,
            "destination": destination,
            "sha256": checksum,
            "bytes": len(payload),
            "status": status,
        }
    )

summary = {
    "dataset": "BIDMC PPG and Respiration Dataset",
    "version": "1.0.0",
    "license": dataset_license,
    "license_url": "https://physionet.org/content/bidmc/1.0.0/",
    "files": results,
}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
