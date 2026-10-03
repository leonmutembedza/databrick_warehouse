# Databricks notebook source
# Extract DICOM header metadata and bounding-box labels to Bronze. Pixel data remains in the Volume.
# Attach pydicom as a PyPI task library; do not copy image pixels into a wide Delta row.

from __future__ import annotations

import csv
import json
from pathlib import Path
import uuid

import pydicom
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType


dbutils.widgets.text("raw_path", "/Volumes/thesis_biomed/bronze/raw_files/landing/rsna-pneumonia/stage_2", "RSNA landing folder")
dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("max_images", "5", "Images per run; 0 means all")
dbutils.widgets.text("run_id", "", "Run ID")
root = Path(dbutils.widgets.get("raw_path").rstrip("/"))
manifest_table = dbutils.widgets.get("manifest_table").strip()
max_images = int(dbutils.widgets.get("max_images"))
run_id = dbutils.widgets.get("run_id").strip() or f"rsna-bronze-{uuid.uuid4().hex}"
if not str(root).startswith("/Volumes/") or max_images < 0:
    raise ValueError("Use a Volume path and max_images >= 0")
labels_file = root / "stage_2_train_labels.csv"
if not labels_file.is_file():
    raise FileNotFoundError(f"Missing RSNA labels CSV: {labels_file}")
labels: dict[str, list[dict]] = {}
with labels_file.open("r", encoding="utf-8", newline="") as handle:
    for row in csv.DictReader(handle):
        boxes = labels.setdefault(row["patientId"], [])
        if row.get("Target") == "1":
            boxes.append({"x": float(row["x"]), "y": float(row["y"]), "width": float(row["width"]), "height": float(row["height"])})
images = (
    spark.table(manifest_table)
    .where((F.col("source_dataset") == "rsna-pneumonia") & F.lower(F.col("file_format")).isin("dcm", "dicom"))
    .select("source_record_key", "raw_uri", "file_checksum")
    .orderBy("source_record_key")
)
if max_images:
    images = images.limit(max_images)
image_files = images.collect()
if not image_files:
    raise RuntimeError("No RSNA DICOM images are registered; land/extract the image archive and register its files first")
rows = []
for item in image_files:
    path = Path(item.raw_uri)
    patient_id = path.stem
    dicom = pydicom.dcmread(str(path), stop_before_pixels=True)
    patient_boxes = labels.get(patient_id, [])
    rows.append({
        "source_dataset": "rsna-pneumonia", "source_version": "stage_2",
        "source_record_key": item.source_record_key, "file_checksum": item.file_checksum,
        "raw_uri": str(path), "patient_id": patient_id,
        "rows": int(getattr(dicom, "Rows", 0)), "columns": int(getattr(dicom, "Columns", 0)),
        "samples_per_pixel": int(getattr(dicom, "SamplesPerPixel", 1)),
        "photometric_interpretation": str(getattr(dicom, "PhotometricInterpretation", "UNKNOWN")),
        "target": int(bool(patient_boxes)), "bounding_boxes_json": json.dumps(patient_boxes),
        "extraction_run_id": run_id,
    })
schema = StructType([
    StructField("source_dataset", StringType(), False), StructField("source_version", StringType(), False),
    StructField("source_record_key", StringType(), False), StructField("file_checksum", StringType(), False),
    StructField("raw_uri", StringType(), False), StructField("patient_id", StringType(), False),
    StructField("rows", IntegerType(), False), StructField("columns", IntegerType(), False),
    StructField("samples_per_pixel", IntegerType(), False), StructField("photometric_interpretation", StringType(), False),
    StructField("target", IntegerType(), False), StructField("bounding_boxes_json", StringType(), False),
    StructField("extraction_run_id", StringType(), False),
])
incoming = spark.createDataFrame(rows, schema)
table = "thesis_biomed.bronze.rsna_chest_xray_records"
if not spark.catalog.tableExists(table):
    incoming.limit(0).write.format("delta").saveAsTable(table)
existing = spark.table(table)
identity = ["source_dataset", "source_version", "source_record_key"]
conflicts = incoming.alias("n").join(existing.alias("o"), identity, "inner").where(F.col("n.file_checksum") != F.col("o.file_checksum"))
if conflicts.limit(1).count():
    raise RuntimeError("RSNA image checksum changed for an existing source key; review before updating Bronze")
replays = incoming.alias("n").join(existing.alias("o"), identity + ["file_checksum"], "inner").count()
new = incoming.alias("n").join(existing.alias("o"), identity + ["file_checksum"], "left_anti")
inserted = new.count()
new.createOrReplaceTempView("rsna_images_incoming")
spark.sql(f"""MERGE INTO {table} t USING rsna_images_incoming s
 ON t.source_dataset=s.source_dataset AND t.source_version=s.source_version AND t.source_record_key=s.source_record_key AND t.file_checksum=s.file_checksum
 WHEN NOT MATCHED THEN INSERT *""")
summary = {"table": table, "images": len(rows), "inserted": inserted, "exact_replays": replays, "run_id": run_id}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
