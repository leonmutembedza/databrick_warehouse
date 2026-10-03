# Databricks notebook source
# Land the official ICBHI archive (or a direct official-file URL) into the UC Volume.
# Access/download URL must be supplied by the user; no credentials are embedded here.

from __future__ import annotations

import hashlib
import json
from pathlib import Path, PurePosixPath
import urllib.request
import zipfile


dbutils.widgets.text("source_archive_url", "", "Direct official ICBHI archive URL")
dbutils.widgets.text("target_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/icbhi-2017/official_release", "ICBHI landing folder")
dbutils.widgets.text("max_recordings", "5", "WAV/TXT pairs to land; 0 means all")
source_url = dbutils.widgets.get("source_archive_url").strip()
target = Path(dbutils.widgets.get("target_path").rstrip("/"))
max_recordings = int(dbutils.widgets.get("max_recordings"))
if not source_url.startswith("https://"):
    raise ValueError("Paste the HTTPS download URL from the official ICBHI site into source_archive_url")
if not str(target).startswith("/Volumes/"):
    raise ValueError("target_path must be inside a Unity Catalog Volume")
if max_recordings < 0:
    raise ValueError("max_recordings must be zero or positive")
target.mkdir(parents=True, exist_ok=True)
archive = target / "source" / "icbhi_2017_source.zip"
archive.parent.mkdir(parents=True, exist_ok=True)
if not archive.exists():
    request = urllib.request.Request(source_url, headers={"User-Agent": "thesis-batch-pipeline/0.1"})
    with urllib.request.urlopen(request, timeout=120) as response, archive.open("wb") as output:
        while chunk := response.read(1024 * 1024):
            output.write(chunk)
if not zipfile.is_zipfile(archive):
    raise ValueError("Downloaded ICBHI source is not a ZIP archive; check the official download URL")

with zipfile.ZipFile(archive) as zipped:
    wav_members = sorted(name for name in zipped.namelist() if name.lower().endswith(".wav"))
    if max_recordings:
        wav_members = wav_members[:max_recordings]
    selected_stems = {str(PurePosixPath(name).with_suffix("")) for name in wav_members}
    selected = set(wav_members)
    selected.update(
        name for name in zipped.namelist()
        if name.lower().endswith(".txt") and str(PurePosixPath(name).with_suffix("")) in selected_stems
    )
    for name in sorted(selected):
        relative = PurePosixPath(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError(f"Unsafe member in ICBHI archive: {name}")
        destination = target / "files" / Path(*relative.parts)
        destination.parent.mkdir(parents=True, exist_ok=True)
        with zipped.open(name) as source, destination.open("wb") as output:
            while chunk := source.read(1024 * 1024):
                output.write(chunk)

manifest = []
for path in sorted((target / "files").rglob("*")):
    if path.is_file():
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        manifest.append({"path": str(path), "bytes": path.stat().st_size, "sha256": digest})
print(json.dumps({"dataset": "icbhi_2017", "recordings_landed": len(wav_members), "files": len(manifest), "file_manifest": manifest[:20]}, indent=2))
