# Databricks notebook source
# Generic file-level Bronze manifest for one dataset landing directory.

from __future__ import annotations

# Serverless Python-file tasks need Spark initialized as the first notebook command.
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().profile("DEFAULT").getOrCreate()

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
from pathlib import PurePosixPath
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType


dbutils.widgets.text("raw_path", "", "Dataset landing folder in the raw_files Volume")
dbutils.widgets.text("dataset", "", "Dataset key, e.g. ptb-xl, icbhi-2017, rsna-pneumonia")
dbutils.widgets.text("dataset_version", "", "Dataset release")
dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("license_reference", "", "Official license/terms URL")
dbutils.widgets.text("source_dataset_id", "", "Source platform dataset ID for ICBHI, e.g. Kaggle owner/dataset")
dbutils.widgets.text("run_id", "", "Run ID; blank creates a unique ID")
root_text = dbutils.widgets.get("raw_path").rstrip("/")
dataset = dbutils.widgets.get("dataset").strip()
version = dbutils.widgets.get("dataset_version").strip()
manifest_table = dbutils.widgets.get("manifest_table").strip()
license_reference = dbutils.widgets.get("license_reference").strip() or None
expected_source_dataset_id = dbutils.widgets.get("source_dataset_id").strip() or None
run_id = dbutils.widgets.get("run_id").strip() or f"register-{uuid.uuid4().hex}"
if not root_text.startswith("/Volumes/"):
    raise ValueError("raw_path must be a Unity Catalog Volume path")
if not re.fullmatch(r"[a-z0-9][a-z0-9_-]*", dataset) or not version:
    raise ValueError("Provide a lowercase dataset key and non-empty version")
if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", manifest_table):
    raise ValueError("manifest_table must be a three-part catalog.schema.table identifier")
root = Path(root_text)
if not root.is_dir():
    raise FileNotFoundError(f"Dataset landing folder does not exist: {root_text}")

landing_metadata = {}
if dataset == "icbhi-2017":
    if not expected_source_dataset_id:
        raise ValueError("Set source_dataset_id to the Kaggle owner/dataset ID for ICBHI registration")
    metadata_path = root / "source_metadata.json"
    if not metadata_path.is_file():
        raise FileNotFoundError(f"ICBHI Kaggle landing metadata is missing: {metadata_path}")
    landing_metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    required_metadata = {"dataset", "dataset_version", "kaggle_dataset_id", "source_url", "download_archive_sha256", "landing_run_id", "downloaded_at", "landed_files", "license_status"}
    missing_metadata = sorted(required_metadata - landing_metadata.keys())
    if missing_metadata:
        raise ValueError(f"ICBHI source metadata is missing required Kaggle provenance fields: {missing_metadata}")
    if landing_metadata["dataset"] != dataset or landing_metadata["dataset_version"] != version:
        raise ValueError(
            "ICBHI landing metadata does not match the registration dataset/version: "
            f"metadata={landing_metadata['dataset']}/{landing_metadata['dataset_version']}, "
            f"requested={dataset}/{version}"
        )
    if expected_source_dataset_id and landing_metadata["kaggle_dataset_id"] != expected_source_dataset_id:
        raise ValueError(
            "ICBHI Kaggle dataset ID mismatch: "
            f"metadata={landing_metadata['kaggle_dataset_id']!r}, requested={expected_source_dataset_id!r}"
        )
    expected_source_url = f"https://www.kaggle.com/datasets/{expected_source_dataset_id}"
    if landing_metadata["source_url"] != expected_source_url:
        raise ValueError(f"ICBHI source URL does not match the configured Kaggle dataset ID: {landing_metadata['source_url']!r}")
    if not re.fullmatch(r"[0-9a-f]{64}", str(landing_metadata["download_archive_sha256"])):
        raise ValueError("ICBHI Kaggle archive checksum in source metadata is invalid")


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def modality_for(path: Path) -> str:
    if dataset == "bidmc":
        return "physiological_signals"
    if dataset == "ptb-xl":
        return "ecg"
    if dataset == "icbhi-2017":
        return "respiratory_audio"
    if dataset == "rsna-pneumonia":
        return "chest_xray"
    raise ValueError(f"No modality mapping configured for dataset {dataset!r}")


if dataset == "icbhi-2017":
    declared_files = landing_metadata["landed_files"]
    if not isinstance(declared_files, list) or not declared_files:
        raise ValueError("ICBHI source metadata must list at least one landed WAV/TXT file")
    files = []
    for entry in declared_files:
        if not isinstance(entry, dict) or not {"relative_path", "sha256", "bytes"}.issubset(entry):
            raise ValueError("Invalid file entry in ICBHI source metadata")
        relative = PurePosixPath(str(entry["relative_path"]))
        if relative.is_absolute() or not relative.parts or ".." in relative.parts:
            raise ValueError(f"Unsafe ICBHI relative path in landing metadata: {entry['relative_path']!r}")
        path = root / "files" / Path(*relative.parts)
        if not path.is_file() or path.name.startswith("."):
            raise FileNotFoundError(f"ICBHI landed file listed in metadata is missing: {path}")
        if path.stat().st_size != int(entry["bytes"]) or sha256(path) != entry["sha256"]:
            raise RuntimeError(f"ICBHI landed file does not match its landing metadata checksum: {path}")
        files.append(path)
else:
    files = sorted(path for path in root.rglob("*") if path.is_file() and not path.name.startswith("."))
if not files:
    raise RuntimeError(f"No files found beneath {root_text}")
ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
rows = []
for path in files:
    relative = path.relative_to(root).as_posix()
    checksum = sha256(path)
    record_id = hashlib.sha256(f"{dataset}\x1f{version}\x1f{relative}".encode()).hexdigest()[:32]
    file_source_metadata = {"file_name": path.name, "relative_path": relative, "file_size_bytes": str(path.stat().st_size)}
    if dataset == "icbhi-2017":
        file_source_metadata.update({
            "source_platform": "kaggle",
            "kaggle_dataset_id": landing_metadata["kaggle_dataset_id"],
            "source_url": landing_metadata["source_url"],
            "download_archive_sha256": landing_metadata["download_archive_sha256"],
            "landing_run_id": landing_metadata["landing_run_id"],
            "downloaded_at": landing_metadata["downloaded_at"],
            "license_status": landing_metadata["license_status"],
        })
    rows.append({
        "record_id": record_id,
        "subject_id_hash": None,
        "modality": modality_for(path),
        "source_dataset": dataset,
        "source_version": version,
        "source_record_key": relative,
        "event_timestamp": None,
        "time_precision": None,
        "raw_uri": str(path),
        "file_checksum": checksum,
        "file_format": path.suffix.lower().lstrip(".") or "unknown",
        "source_metadata": file_source_metadata,
        "ingestion_run_id": run_id,
        "ingested_at": ingested_at,
        "schema_version": "1.0.0",
        "pipeline_version": "0.2.0",
        "license_reference": license_reference,
        "deidentification_status": "source_deidentified",
    })
schema = StructType([
    StructField("record_id", StringType(), False), StructField("subject_id_hash", StringType(), True),
    StructField("modality", StringType(), False), StructField("source_dataset", StringType(), False),
    StructField("source_version", StringType(), False), StructField("source_record_key", StringType(), False),
    StructField("event_timestamp", StringType(), True), StructField("time_precision", StringType(), True),
    StructField("raw_uri", StringType(), False), StructField("file_checksum", StringType(), False),
    StructField("file_format", StringType(), False), StructField("source_metadata", MapType(StringType(), StringType()), False),
    StructField("ingestion_run_id", StringType(), False), StructField("ingested_at", StringType(), False),
    StructField("schema_version", StringType(), False), StructField("pipeline_version", StringType(), False),
    StructField("license_reference", StringType(), True), StructField("deidentification_status", StringType(), False),
])
incoming = spark.createDataFrame(rows, schema=schema)
if not spark.catalog.tableExists(manifest_table):
    incoming.limit(0).write.format("delta").saveAsTable(manifest_table)
target = spark.table(manifest_table)
identity = ["source_dataset", "source_version", "source_record_key"]
conflicts = incoming.alias("n").join(target.alias("o"), identity, "inner").where(
    F.lower(F.col("n.file_checksum")) != F.lower(F.col("o.file_checksum"))
)
if conflicts.limit(1).count():
    examples = [row.asDict() for row in conflicts.select("n.source_record_key", "o.file_checksum", "n.file_checksum").limit(10).collect()]
    raise RuntimeError("Changed payload for an existing dataset key; review before replacing: " + json.dumps(examples))
replays = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum"], "inner").count()
new_rows = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum"], "left_anti")
new_count = new_rows.count()
new_rows.createOrReplaceTempView("landed_file_manifest_incoming")
if new_count:
    spark.sql(f"""
      MERGE INTO {manifest_table} target USING landed_file_manifest_incoming source
      ON target.source_dataset=source.source_dataset AND target.source_version=source.source_version
       AND target.source_record_key=source.source_record_key AND target.file_checksum=source.file_checksum
      WHEN NOT MATCHED THEN INSERT *
    """)
summary = {"dataset": dataset, "version": version, "files_seen": len(rows), "inserted": new_count, "exact_replays": replays, "manifest_table": manifest_table}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
