# Databricks notebook source
# Final gated smoke check. Depend on every enabled modality Bronze extraction task.

from __future__ import annotations

import json


dbutils.widgets.text(
    "required_tables",
    "thesis_biomed.bronze.bidmc_ecg,thesis_biomed.bronze.bidmc_ppg,thesis_biomed.bronze.bidmc_respiration",
    "Comma-separated three-part tables required for this run",
)
tables = [value.strip() for value in dbutils.widgets.get("required_tables").split(",") if value.strip()]
if not tables:
    raise ValueError("required_tables must contain at least one fully qualified table")
results = []
for table in tables:
    parts = table.split(".")
    if len(parts) != 3:
        raise ValueError(f"Expected a three-part table name, got: {table}")
    if not spark.catalog.tableExists(table):
        raise RuntimeError(f"Expected Bronze table does not exist: {table}")
    count = spark.table(table).count()
    results.append({"table": table, "rows": count})
    if count == 0:
        raise RuntimeError(f"Bronze output table is empty: {table}")
summary = {"status": "SUCCEEDED", "bronze_tables": results}
print(json.dumps(summary, indent=2))
dbutils.notebook.exit(json.dumps(summary))
