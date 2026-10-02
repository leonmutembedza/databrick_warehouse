# Multimodal Biomedical Data Quality Pipeline

Batch lakehouse implementation for the thesis project. The first implementation milestone is a common data contract and replay-safe batch manifest. Databricks notebooks and Workflows will call the Python package as the project grows.

## Current baseline

- Batch processing of bounded dataset drops; no Kafka or streaming.
- Databricks Free Edition as the target execution environment.
- Raw files land in a Unity Catalog Volume, with Delta tables for Bronze, Silver, Quarantine, Gold, and operational metadata.
- First end-to-end dataset: BIDMC PPG and Respiration CSV subset.
- Other candidates: PTB-XL (ECG), ICBHI (respiratory audio), and RSNA Pneumonia (chest X-ray).

See [the project execution document](Thesis%20Data%20Quality%20Pipeline%20Project%20Document.md) for the research framing, architecture, dataset notes, quality approach, evaluation, and decisions still to resolve.

## Project layout

```text
config/       dataset registry and draft quality rule registry
schemas/      versioned table contracts
src/          shared, testable Python logic
notebooks/    Databricks task notebooks
resources/    Databricks Workflow setup guide
```

## Landing and table locations

The thesis workspace uses the Unity Catalog Volume below for immutable BIDMC source files:

```text
/Volumes/thesis_biomed/bronze/raw_files/landing/<dataset>/<dataset_version>/...
```

Bronze manifests should store the corresponding Volume URI in `raw_uri`; decoded files should remain in the raw landing zone. Write curated metadata and features to Delta tables.

## First milestone

1. Create the catalog and schemas in Databricks. **Catalog, schemas, and `thesis_biomed.bronze.raw_files` Volume are created** in the workspace.
2. Download the selected BIDMC CSV files and documentation; record checksums and dataset terms.
3. Upload the extracted BIDMC CSV files to `/Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0/`.
4. The bootstrap SQL has already been run. Upload [the first ingestion notebook](notebooks/01_ingest_bronze.py) to the workspace and create the one-task Workflow described in [the setup guide](resources/workflow_setup.md).
5. Run a small bounded file subset, inspect `thesis_biomed.bronze.file_manifest`, and repeat the same run. Exact replays should add zero records; a changed checksum for the same source key should fail visibly for review.

Do not run the full datasets until the small-batch replay behavior is confirmed. Never place real patient-identifiable data in the Free Edition workspace.
