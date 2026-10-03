# Databricks notebook source
# Decode a bounded set of PTB-XL WFDB records into one Bronze row per ECG.
# Attach the wfdb PyPI library to this task in the Job; do not install globally.

from __future__ import annotations

# Initialize Spark before any widget access, file processing, or other notebook work.
# Serverless Python-file tasks require an explicit Databricks Connect session.
from databricks.connect import DatabricksSession

spark = DatabricksSession.builder.serverless().profile("DEFAULT").getOrCreate()

import csv
import hashlib
import json
from pathlib import Path
import uuid

import wfdb
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, LongType, StringType, StructField, StructType, TimestampType


dbutils.widgets.text("raw_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/ptb-xl/1.0.3", "PTB-XL landing path")
dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("max_records", "5", "Records per run; 0 means all")
dbutils.widgets.text("run_id", "", "Run ID")
root = Path(dbutils.widgets.get("raw_path").rstrip("/"))
manifest_table = dbutils.widgets.get("manifest_table").strip()
max_records = int(dbutils.widgets.get("max_records"))
run_id = dbutils.widgets.get("run_id").strip() or f"ptbxl-bronze-{uuid.uuid4().hex}"
if max_records < 0 or not str(root).startswith("/Volumes/"):
    raise ValueError("Use a Volume path and max_records >= 0")
metadata_path = root / "ptbxl_database.csv"
if not metadata_path.is_file():
    raise FileNotFoundError(f"Missing PTB-XL metadata: {metadata_path}")
with metadata_path.open("r", encoding="utf-8", newline="") as handle:
    records = list(csv.DictReader(handle))
if max_records:
    records = records[:max_records]

rows = []
for meta in records:
    relative_base = meta.get("filename_lr")
    if not relative_base:
        raise ValueError("PTB-XL metadata is missing filename_lr")
    record_path = root / relative_base
    if not Path(str(record_path) + ".dat").is_file() or not Path(str(record_path) + ".hea").is_file():
        raise FileNotFoundError(f"WFDB pair missing for {relative_base}; run the PTB-XL landing task first")
    signal, fields = wfdb.rdsamp(str(record_path))
    digest = hashlib.sha256()
    for suffix in (".hea", ".dat"):
        with Path(str(record_path) + suffix).open("rb") as source:
            for chunk in iter(lambda: source.read(1024 * 1024), b""):
                digest.update(chunk)
    rows.append({
        "source_dataset": "ptb-xl", "source_version": "1.0.3", "source_record_key": relative_base,
        "file_checksum": digest.hexdigest(), "raw_uri": str(record_path),
        "ecg_id": str(meta.get("ecg_id", "")), "patient_id": str(meta.get("patient_id", "")),
        "lead_names": fields["sig_name"], "sampling_frequency_hz": float(fields["fs"]),
        "sample_count": int(signal.shape[0]), "samples_by_lead": signal.T.tolist(),
        "extraction_run_id": run_id,
    })
if not rows:
    raise RuntimeError("No WFDB ECG records were extracted")
schema = StructType([
    StructField("source_dataset", StringType(), False), StructField("source_version", StringType(), False),
    StructField("source_record_key", StringType(), False), StructField("file_checksum", StringType(), False),
    StructField("raw_uri", StringType(), False), StructField("ecg_id", StringType(), True),
    StructField("patient_id", StringType(), True), StructField("lead_names", ArrayType(StringType()), False),
    StructField("sampling_frequency_hz", DoubleType(), False), StructField("sample_count", LongType(), False),
    StructField("samples_by_lead", ArrayType(ArrayType(DoubleType())), False),
    StructField("extraction_run_id", StringType(), False),
])
incoming = spark.createDataFrame(rows, schema)
target_table = "thesis_biomed.bronze.ptbxl_ecg_records"
if not spark.catalog.tableExists(target_table):
    incoming.limit(0).write.format("delta").saveAsTable(target_table)
target = spark.table(target_table)
identity = ["source_dataset", "source_version", "source_record_key"]
conflicts = incoming.alias("n").join(target.alias("o"), identity, "inner").where(F.col("n.file_checksum") != F.col("o.file_checksum"))
if conflicts.limit(1).count():
    raise RuntimeError("PTB-XL record checksum changed for an existing source key; review before updating Bronze")
replays = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum"], "inner").count()
new_rows = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum"], "left_anti")
inserted = new_rows.count()
new_rows.createOrReplaceTempView("ptbxl_ecg_incoming")
spark.sql(f"""MERGE INTO {target_table} t USING ptbxl_ecg_incoming s
 ON t.source_dataset=s.source_dataset AND t.source_version=s.source_version AND t.source_record_key=s.source_record_key AND t.file_checksum=s.file_checksum
 WHEN NOT MATCHED THEN INSERT *""")
summary = {"table": target_table, "records_read": len(rows), "inserted": inserted, "exact_replays": replays, "run_id": run_id}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
