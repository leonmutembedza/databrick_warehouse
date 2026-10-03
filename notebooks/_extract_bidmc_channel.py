# Databricks notebook source
# Shared implementation for the ECG, PPG, and respiration BIDMC task notebooks.
# The source CSV is immutable in the Volume; each task emits one modality table.

from __future__ import annotations

import json
import re
import uuid

from pyspark.sql import functions as F
from pyspark.sql.window import Window


def run_bidmc_channel(modality: str, source_columns: list[str]) -> None:
    dbutils.widgets.text("manifest_table", "thesis_biomed.bronze.file_manifest", "Bronze file manifest")
    dbutils.widgets.text("run_id", "", "Task run ID; blank creates a unique ID")
    manifest_table = dbutils.widgets.get("manifest_table").strip()
    run_id = dbutils.widgets.get("run_id").strip() or f"extract-{modality}-{uuid.uuid4().hex}"
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", manifest_table):
        raise ValueError("manifest_table must be a three-part catalog.schema.table identifier")
    catalog = manifest_table.split(".")[0]

    files = (
        spark.table(manifest_table)
        .where(
            (F.col("source_dataset") == "bidmc")
            & (F.col("source_version") == "1.0.0")
            & F.col("source_record_key").endswith("_Signals.csv")
        )
        .select("record_id", "source_record_key", "raw_uri", "file_checksum")
        .collect()
    )
    if not files:
        raise RuntimeError(f"No BIDMC signal CSV is registered in {manifest_table}; run file inventory first")

    target_table = f"{catalog}.bronze.bidmc_{modality}"
    totals = {"modality": modality, "files": 0, "source_rows": 0, "inserted_rows": 0, "replay_rows": 0}
    for entry in files:
        raw = spark.read.format("csv").option("header", "true").option("inferSchema", "false").option("mode", "FAILFAST").load(entry.raw_uri)
        normalized = [re.sub(r"[^A-Za-z0-9]+", "_", name.strip()).strip("_").lower() for name in raw.columns]
        if len(normalized) != len(set(normalized)):
            raise ValueError(f"Column names collide after normalization in {entry.source_record_key}: {raw.columns}")
        frame = raw.toDF(*normalized)
        if "time_s" not in normalized:
            raise ValueError(f"Missing time_s column in {entry.source_record_key}")
        available = [name for name in source_columns if name in normalized]
        if not available:
            raise ValueError(f"No source column for {modality} found in {entry.source_record_key}; expected one of {source_columns}")

        source_count = frame.count()
        time_value = F.col("time_s").cast("double")
        cells = []
        for col_name in available:
            cells.append(
                frame.select(
                    F.lit("bidmc").alias("source_dataset"),
                    F.lit("1.0.0").alias("source_version"),
                    F.lit(entry.source_record_key).alias("source_record_key"),
                    F.lit(entry.record_id).alias("record_id"),
                    F.lit(entry.raw_uri).alias("raw_uri"),
                    F.lit(entry.file_checksum).alias("file_checksum"),
                    time_value.alias("source_time_seconds"),
                    F.lit(col_name).alias("source_channel"),
                    F.col(col_name).cast("string").alias("source_value"),
                    F.col(col_name).cast("double").alias("numeric_value"),
                    F.sha2(F.to_json(F.struct(*[F.col(c) for c in normalized]), {"ignoreNullFields": "false"}), 256).alias("source_row_hash"),
                    F.lit(run_id).alias("extraction_run_id"),
                    F.current_timestamp().alias("extracted_at"),
                )
            )
        incoming = cells[0]
        for cell in cells[1:]:
            incoming = incoming.unionByName(cell)
        incoming = incoming.withColumn(
            "source_row_number",
            F.row_number().over(Window.partitionBy("source_record_key", "source_channel").orderBy(
                F.col("source_time_seconds").asc_nulls_first(), F.col("source_row_hash").asc()
            )).cast("long"),
        )
        if incoming.where(F.col("source_time_seconds").isNull() | F.isnan("source_time_seconds")).limit(1).count():
            raise ValueError(f"Unparseable or missing timestamps found in {entry.source_record_key}")
        if not spark.catalog.tableExists(target_table):
            incoming.limit(0).write.format("delta").saveAsTable(target_table)
        identity = ["source_dataset", "source_version", "source_record_key", "source_channel", "source_row_number"]
        target = spark.table(target_table)
        conflicts = incoming.alias("n").join(target.alias("o"), identity, "inner").where(
            (F.col("n.file_checksum") != F.col("o.file_checksum"))
            | (F.col("n.source_row_hash") != F.col("o.source_row_hash"))
        )
        if conflicts.limit(1).count():
            raise RuntimeError(f"Changed source payload conflicts with existing rows in {target_table}")
        replay_count = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum", "source_row_hash"], "inner").count()
        insert_count = incoming.alias("n").join(target.alias("o"), identity + ["file_checksum", "source_row_hash"], "left_anti").count()
        incoming.createOrReplaceTempView("modality_incoming")
        spark.sql(f"""
            MERGE INTO {target_table} target
            USING modality_incoming source
            ON target.source_dataset = source.source_dataset
             AND target.source_version = source.source_version
             AND target.source_record_key = source.source_record_key
             AND target.source_channel = source.source_channel
             AND target.source_row_number = source.source_row_number
             AND target.file_checksum = source.file_checksum
             AND target.source_row_hash = source.source_row_hash
            WHEN NOT MATCHED THEN INSERT *
        """)
        totals["files"] += 1
        totals["source_rows"] += source_count * len(available)
        totals["inserted_rows"] += insert_count
        totals["replay_rows"] += replay_count

    print(json.dumps({**totals, "table": target_table}, indent=2))
    dbutils.notebook.exit(json.dumps({**totals, "table": target_table}))
