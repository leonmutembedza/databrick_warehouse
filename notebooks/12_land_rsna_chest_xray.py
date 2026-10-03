# Databricks notebook source
# Pull RSNA labels through Kaggle; image archive download is explicit and full-size.
# Store Kaggle credentials in Databricks Secrets, never in notebook parameters/code.

from __future__ import annotations

import json
import os
import shutil
import subprocess
from pathlib import Path
import zipfile


dbutils.widgets.text("target_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/rsna-pneumonia/stage_2", "RSNA landing folder")
dbutils.widgets.text("secret_scope", "thesis", "Databricks secret scope containing Kaggle credentials")
dbutils.widgets.text("username_secret_key", "kaggle_username", "Kaggle username secret key")
dbutils.widgets.text("api_key_secret_key", "kaggle_key", "Kaggle API key secret key")
dbutils.widgets.dropdown("download_image_archives", "false", ["false", "true"], "Download the full multi-GB image archives")
target = Path(dbutils.widgets.get("target_path").rstrip("/"))
scope = dbutils.widgets.get("secret_scope").strip()
download_images = dbutils.widgets.get("download_image_archives").lower() == "true"
if not str(target).startswith("/Volumes/"):
    raise ValueError("target_path must be inside a Unity Catalog Volume")
if not scope:
    raise ValueError("Configure a Databricks secret scope before running this task")
try:
    username = dbutils.secrets.get(scope, dbutils.widgets.get("username_secret_key").strip())
    api_key = dbutils.secrets.get(scope, dbutils.widgets.get("api_key_secret_key").strip())
except Exception as exc:
    raise RuntimeError("Kaggle username/API key secrets are unavailable. Create them in the configured Databricks secret scope.") from exc
kaggle_cli = shutil.which("kaggle")
if not kaggle_cli:
    raise RuntimeError("Kaggle CLI is not installed for this task; attach the kaggle PyPI library in the Job task settings")
target.mkdir(parents=True, exist_ok=True)
os.environ["KAGGLE_USERNAME"] = username
os.environ["KAGGLE_KEY"] = api_key
files = ["stage_2_train_labels.csv", "stage_2_detailed_class_info.csv", "stage_2_sample_submission.csv"]
if download_images:
    files += ["stage_2_train_images.zip", "stage_2_test_images.zip"]
for name in files:
    subprocess.run([
        kaggle_cli, "competitions", "download",
        "-c", "rsna-pneumonia-detection-challenge", "-f", name, "-p", str(target),
    ], check=True, stdout=subprocess.DEVNULL)
    zip_path = target / (name + ".zip")
    if zip_path.is_file():
        with zipfile.ZipFile(zip_path) as zipped:
            for member in zipped.infolist():
                member_path = Path(member.filename)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ValueError(f"Unsafe member in Kaggle archive: {member.filename}")
                if name.endswith(".csv") or name.endswith(".csv.zip"):
                    if not member.filename.lower().endswith(".csv"):
                        continue
                    destination = target / Path(*member_path.parts)
                else:
                    destination = target / Path(*member_path.parts)
                destination.parent.mkdir(parents=True, exist_ok=True)
                with zipped.open(member) as source, destination.open("wb") as output:
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
if download_images:
    for split in ("train", "test"):
        image_archive = target / f"stage_2_{split}_images.zip"
        if not image_archive.is_file():
            raise FileNotFoundError(f"Kaggle did not produce expected image archive {image_archive}")
        image_dir = target / f"stage_2_{split}_images"
        image_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(image_archive) as zipped:
            for member in zipped.infolist():
                member_path = Path(member.filename)
                if member_path.is_absolute() or ".." in member_path.parts:
                    raise ValueError(f"Unsafe member in DICOM archive: {member.filename}")
                destination = image_dir / member_path.name
                destination.parent.mkdir(parents=True, exist_ok=True)
                with zipped.open(member) as source, destination.open("wb") as output:
                    while chunk := source.read(1024 * 1024):
                        output.write(chunk)
print(json.dumps({"dataset": "rsna_pneumonia", "stage": "stage_2", "files_landed": files, "downloaded_images": download_images, "image_note": "Image archives total about 4 GB; downloading is opt-in."}, indent=2))
