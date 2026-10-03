# Databricks notebook source
# Generic file-level Bronze manifest for one dataset landing directory.

from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.types import MapType, StringType, StructField, StructType


dbutils.widgets.text("raw_path", "", "Dataset landing folder in the raw_files Volume")
dbutils.widgets.text("dataset", "", "Dataset key, e.g. ptb-xl, icbhi-2017, rsna-pneumonia")
dbutils.widgets.text("dataset_version", "", "Dataset release")
dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("license_reference", "", "Official license/terms URL")
dbutils.widgets.text("run_id", "", "Run ID; blank creates a unique ID")
root_text = dbutils.widgets.get("raw_path").rstrip("/")
dataset = dbutils.widgets.get("dataset").strip()
version = dbutils.widgets.get("dataset_version").strip()
manifest_table = dbutils.widgets.get("manifest_table").strip()
license_reference = dbutils.widgets.get("license_reference").strip() or None
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


files = sorted(path for path in root.rglob("*") if path.is_file() and not path.name.startswith("."))
if not files:
    raise RuntimeError(f"No files found beneath {root_text}")
ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
rows = []
for path in files:
    relative = path.relative_to(root).as_posix()
    checksum = sha256(path)
    record_id = hashlib.sha256(f"{dataset}\x1f{version}\x1f{relative}".encode()).hexdigest()[:32]
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
        "source_metadata": {"file_name": path.name, "relative_path": relative, "file_size_bytes": str(path.stat().st_size)},
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
