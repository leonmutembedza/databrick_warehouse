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

Create a Unity Catalog Volume for immutable source files. Configure the actual catalog, schema, and volume names in the Databricks workspace; this repository intentionally contains placeholders rather than account-specific identifiers.

```text
/Volumes/<catalog>/<schema>/<volume>/landing/<dataset>/<dataset_version>/...
```

Bronze manifests should store the corresponding Volume URI in `raw_uri`; decoded files should remain in the raw landing zone. Write curated metadata and features to Delta tables.

## First milestone

1. Create the catalog/schema and raw Volume in Databricks.
2. Download the selected BIDMC CSV files and documentation; record checksums and dataset terms.
3. Upload the extracted BIDMC CSV files to the Volume landing path above.
4. Run [the bootstrap SQL](resources/sql/bootstrap.sql) after replacing `YOUR_CATALOG` with the catalog available in the workspace.
5. Upload [the first ingestion notebook](notebooks/01_ingest_bronze.py) to the workspace and create the one-task Workflow described in [the setup guide](resources/workflow_setup.md).
6. Run a small bounded file subset, inspect `bronze.file_manifest`, and repeat the same run. Exact replays should add zero records; a changed checksum for the same source key should fail visibly for review.

Do not run the full datasets until the small-batch replay behavior is confirmed. Never place real patient-identifiable data in the Free Edition workspace.
