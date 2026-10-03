# Databricks notebook source
# Extract WAV headers and respiratory-cycle annotations into Bronze Delta tables.

from __future__ import annotations

import hashlib
import json
from pathlib import Path
import uuid
import wave

from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, DoubleType, IntegerType, LongType, StringType, StructField, StructType


dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
dbutils.widgets.text("max_recordings", "5", "Recordings per run; 0 means all")
dbutils.widgets.text("run_id", "", "Run ID")
manifest_table = dbutils.widgets.get("manifest_table").strip()
max_recordings = int(dbutils.widgets.get("max_recordings"))
run_id = dbutils.widgets.get("run_id").strip() or f"icbhi-bronze-{uuid.uuid4().hex}"
if max_recordings < 0:
    raise ValueError("max_recordings must be zero or positive")
manifest = spark.table(manifest_table).where(
    (F.col("source_dataset") == "icbhi-2017")
    & (F.col("source_version") == "official_release")
    & (F.lower(F.col("file_format")) == "wav")
).select("source_record_key", "raw_uri", "file_checksum").orderBy("source_record_key")
if max_recordings:
    manifest = manifest.limit(max_recordings)
audio_files = manifest.collect()
if not audio_files:
    raise RuntimeError("No ICBHI WAV files in the manifest; land and register audio files first")

record_rows, annotation_rows = [], []
for row in audio_files:
    audio_path = Path(row.raw_uri)
    annotation_path = audio_path.with_suffix(".txt")
    if not audio_path.is_file() or not annotation_path.is_file():
        raise FileNotFoundError(f"Expected matching WAV and TXT annotation for {row.source_record_key}")
    with wave.open(str(audio_path), "rb") as source:
        channels, rate, frames, width = source.getnchannels(), source.getframerate(), source.getnframes(), source.getsampwidth()
        compression = source.getcomptype()
    record_rows.append({
        "source_dataset": "icbhi-2017", "source_version": "official_release",
        "source_record_key": row.source_record_key, "file_checksum": row.file_checksum,
        "raw_uri": str(audio_path), "annotation_uri": str(annotation_path),
        "participant_id": audio_path.stem.split("_")[0], "channels": channels,
        "sample_rate_hz": rate, "frame_count": frames, "sample_width_bytes": width,
        "duration_seconds": frames / rate if rate else None, "compression": compression,
        "extraction_run_id": run_id,
    })
    annotation_digest = hashlib.sha256(annotation_path.read_bytes()).hexdigest()
    with annotation_path.open("r", encoding="utf-8") as source:
        for index, line in enumerate(source):
            parts = line.strip().split()
            if not parts:
                continue
            if len(parts) != 4:
                raise ValueError(f"Invalid ICBHI annotation row in {annotation_path}: {line!r}")
            start, end = float(parts[0]), float(parts[1])
            crackles, wheezes = int(parts[2]), int(parts[3])
            if start < 0 or end <= start or crackles not in (0, 1) or wheezes not in (0, 1):
                raise ValueError(f"Out-of-contract ICBHI annotation in {annotation_path}: {line!r}")
            annotation_rows.append({
                "source_record_key": row.source_record_key, "annotation_index": index,
                "cycle_start_seconds": start, "cycle_end_seconds": end,
                "crackles_present": bool(crackles), "wheezes_present": bool(wheezes),
                "annotation_uri": str(annotation_path), "annotation_checksum": annotation_digest,
                "extraction_run_id": run_id,
            })

record_schema = StructType([
    StructField("source_dataset", StringType(), False), StructField("source_version", StringType(), False),
    StructField("source_record_key", StringType(), False), StructField("file_checksum", StringType(), False),
    StructField("raw_uri", StringType(), False), StructField("annotation_uri", StringType(), False),
    StructField("participant_id", StringType(), False), StructField("channels", IntegerType(), False),
    StructField("sample_rate_hz", IntegerType(), False), StructField("frame_count", LongType(), False),
    StructField("sample_width_bytes", IntegerType(), False), StructField("duration_seconds", DoubleType(), True),
    StructField("compression", StringType(), False), StructField("extraction_run_id", StringType(), False),
])
annotation_schema = StructType([
    StructField("source_record_key", StringType(), False), StructField("annotation_index", IntegerType(), False),
    StructField("cycle_start_seconds", DoubleType(), False), StructField("cycle_end_seconds", DoubleType(), False),
    StructField("crackles_present", BooleanType(), False), StructField("wheezes_present", BooleanType(), False),
    StructField("annotation_uri", StringType(), False), StructField("annotation_checksum", StringType(), False),
    StructField("extraction_run_id", StringType(), False),
])


def merge_delta(frame, table: str, keys: list[str]) -> tuple[int, int]:
    if not spark.catalog.tableExists(table):
        frame.limit(0).write.format("delta").saveAsTable(table)
    existing = spark.table(table)
    replay_count = frame.alias("n").join(existing.alias("o"), keys, "inner").count()
    new = frame.alias("n").join(existing.alias("o"), keys, "left_anti")
    inserted = new.count()
    new.createOrReplaceTempView("modality_delta_incoming")
    condition = " AND ".join(f"t.{key}=s.{key}" for key in keys)
    spark.sql(f"MERGE INTO {table} t USING modality_delta_incoming s ON {condition} WHEN NOT MATCHED THEN INSERT *")
    return inserted, replay_count


record_table = "thesis_biomed.bronze.icbhi_audio_records"
annotation_table = "thesis_biomed.bronze.icbhi_cycle_annotations"
record_inserted, record_replays = merge_delta(spark.createDataFrame(record_rows, record_schema), record_table, ["source_record_key", "file_checksum"])
annotation_inserted, annotation_replays = merge_delta(spark.createDataFrame(annotation_rows, annotation_schema), annotation_table, ["source_record_key", "annotation_index", "annotation_checksum"])
summary = {"records_table": record_table, "annotations_table": annotation_table, "recordings": len(record_rows), "record_rows_inserted": record_inserted, "record_replays": record_replays, "annotation_rows": len(annotation_rows), "annotation_rows_inserted": annotation_inserted, "annotation_replays": annotation_replays, "run_id": run_id}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
