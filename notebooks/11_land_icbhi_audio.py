# Databricks notebook source
# Land a bounded selection from the Kaggle-hosted ICBHI mirror into the UC Volume.
# The mirror is community-maintained; its Kaggle license is listed as Unknown.

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import uuid
import zipfile


dbutils.widgets.text("target_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/icbhi-2017/kaggle_vbookshelf", "ICBHI landing folder")
dbutils.widgets.text("kaggle_dataset", "vbookshelf/respiratory-sound-database", "Kaggle dataset ID")
dbutils.widgets.text("secret_scope", "thesis", "Databricks secret scope containing Kaggle credentials")
dbutils.widgets.text("username_secret_key", "kaggle_username", "Kaggle username secret key")
dbutils.widgets.text("api_key_secret_key", "kaggle_key", "Kaggle API key secret key")
dbutils.widgets.text("max_recordings", "5", "WAV/TXT pairs to land; 0 means all")
dbutils.widgets.text("run_id", "", "Landing run ID")

target = Path(dbutils.widgets.get("target_path").rstrip("/"))
kaggle_dataset = dbutils.widgets.get("kaggle_dataset").strip()
secret_scope = dbutils.widgets.get("secret_scope").strip()
username_key = dbutils.widgets.get("username_secret_key").strip()
api_key_key = dbutils.widgets.get("api_key_secret_key").strip()
max_recordings = int(dbutils.widgets.get("max_recordings"))
run_id = dbutils.widgets.get("run_id").strip() or f"icbhi-kaggle-land-{uuid.uuid4().hex}"

if not target.is_absolute() or ".." in target.parts or not str(target).startswith("/Volumes/") or len(target.parts) < 5:
    raise ValueError("target_path must be inside a Unity Catalog Volume")
if not re.fullmatch(r"[A-Za-z0-9_-]+/[A-Za-z0-9_-]+", kaggle_dataset):
    raise ValueError("kaggle_dataset must be a Kaggle owner/dataset identifier")
if max_recordings < 0:
    raise ValueError("max_recordings must be zero or positive")
if not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", run_id):
    raise ValueError("run_id may contain only letters, numbers, underscores, and hyphens")
try:
    kaggle_username = dbutils.secrets.get(secret_scope, username_key)
    kaggle_key = dbutils.secrets.get(secret_scope, api_key_key)
except Exception as exc:
    raise RuntimeError("Kaggle credentials are unavailable; configure the thesis scope and its username/key secrets") from exc

kaggle_cli = shutil.which("kaggle")
if not kaggle_cli:
    raise RuntimeError("Kaggle CLI is missing; add the PyPI package 'kaggle' to this landing task's environment")

target.mkdir(parents=True, exist_ok=True)
files_root = target / "files"
files_root.mkdir(parents=True, exist_ok=True)
staging_root = target.parent / f".{target.name}_kaggle_staging"
staging_root.mkdir(parents=True, exist_ok=True)
staging = staging_root / run_id
download_dir = staging / "download"
payload_dir = staging / "payload"
download_dir.mkdir(parents=True, exist_ok=False)
payload_dir.mkdir(parents=True, exist_ok=False)


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def safe_extract(archive_path: Path, destination_root: Path) -> None:
    destination_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(archive_path) as archive:
        for member in archive.infolist():
            normalized_name = member.filename.replace("\\", "/")
            relative = PurePosixPath(normalized_name)
            if relative.is_absolute() or ".." in relative.parts:
                raise ValueError(f"Unsafe member in Kaggle archive: {member.filename}")
            if not relative.parts:
                continue
            unix_mode = (member.external_attr >> 16) & 0o170000
            if unix_mode == stat.S_IFLNK:
                raise ValueError(f"Symlink member is not allowed in Kaggle archive: {member.filename}")
            destination = destination_root.joinpath(*relative.parts)
            if member.is_dir():
                destination.mkdir(parents=True, exist_ok=True)
                continue
            destination.parent.mkdir(parents=True, exist_ok=True)
            with archive.open(member) as source, destination.open("wb") as output:
                shutil.copyfileobj(source, output, length=1024 * 1024)


def unpack_nested_archives(root: Path) -> None:
    unpacked: set[str] = set()
    while True:
        candidates = [path for path in root.rglob("*.zip") if str(path.resolve()) not in unpacked]
        if not candidates:
            return
        for archive_path in sorted(candidates):
            unpacked.add(str(archive_path.resolve()))
            extract_root = archive_path.parent / f"{archive_path.stem}_contents"
            safe_extract(archive_path, extract_root)
            archive_path.unlink()


def copy_landed_file(source: Path, destination: Path) -> str:
    source_digest = sha256(source)
    if destination.exists():
        if sha256(destination) != source_digest:
            raise RuntimeError(f"Existing landed file has different content; review before replacing: {destination}")
        return source_digest
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_name(f"{destination.name}.{uuid.uuid4().hex}.tmp")
    try:
        shutil.copyfile(source, temporary)
        if sha256(temporary) != source_digest:
            raise IOError(f"Checksum mismatch while copying {source} to {destination}")
        temporary.replace(destination)
    finally:
        if temporary.exists():
            temporary.unlink()
    return source_digest


archive_sha256 = None
landed_files = []
try:
    os.environ["KAGGLE_USERNAME"] = kaggle_username
    os.environ["KAGGLE_KEY"] = kaggle_key
    subprocess.run(
        [kaggle_cli, "datasets", "download", kaggle_dataset, "--path", str(download_dir), "--force", "--quiet"],
        check=True,
    )
    downloaded_archives = sorted(download_dir.glob("*.zip"))
    if len(downloaded_archives) != 1:
        raise RuntimeError(f"Expected one Kaggle dataset ZIP, found {len(downloaded_archives)} in {download_dir}")
    downloaded_archive = downloaded_archives[0]
    archive_sha256 = sha256(downloaded_archive)
    safe_extract(downloaded_archive, payload_dir)
    downloaded_archive.unlink()
    unpack_nested_archives(payload_dir)

    wav_paths = sorted(path for path in payload_dir.rglob("*") if path.is_file() and path.suffix.lower() == ".wav")
    if not wav_paths:
        raise RuntimeError("No WAV files found in the Kaggle dataset archive; inspect its contents before retrying")
    annotations = {
        path.relative_to(payload_dir).with_suffix("").as_posix().casefold(): path
        for path in payload_dir.rglob("*")
        if path.is_file() and path.suffix.lower() == ".txt"
    }
    if max_recordings:
        wav_paths = wav_paths[:max_recordings]

    for wav_path in wav_paths:
        relative_wav = wav_path.relative_to(payload_dir)
        annotation_key = relative_wav.with_suffix("").as_posix().casefold()
        annotation_path = annotations.get(annotation_key)
        if annotation_path is None:
            raise FileNotFoundError(f"Missing matching TXT annotation for {relative_wav}")
        relative_annotation = annotation_path.relative_to(payload_dir)
        for source_path, relative_path in ((wav_path, relative_wav), (annotation_path, relative_annotation)):
            destination = files_root / Path(*relative_path.parts)
            checksum = copy_landed_file(source_path, destination)
            landed_files.append({"path": str(destination), "bytes": destination.stat().st_size, "sha256": checksum})

    metadata = {
        "dataset": "icbhi-2017",
        "dataset_version": "kaggle-vbookshelf-snapshot",
        "kaggle_dataset_id": kaggle_dataset,
        "source_url": f"https://www.kaggle.com/datasets/{kaggle_dataset}",
        "download_archive_sha256": archive_sha256,
        "landing_run_id": run_id,
        "downloaded_at": datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
        "landed_files": [
            {
                "relative_path": Path(item["path"]).relative_to(files_root).as_posix(),
                "sha256": item["sha256"],
                "bytes": item["bytes"],
            }
            for item in landed_files
        ],
        "license_status": "Kaggle mirror license listed as Unknown; verify terms before research use or publication.",
    }
    (target / "source_metadata.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
    summary = {
        **metadata,
        "landing_run_id": run_id,
        "downloaded_at": metadata["downloaded_at"],
        "recordings_landed": len(wav_paths),
        "files_landed": len(landed_files),
        "file_manifest": landed_files[:20],
    }
    print(json.dumps(summary, indent=2))
finally:
    shutil.rmtree(staging, ignore_errors=True)
    try:
        staging_root.rmdir()
    except OSError:
        pass
