# Prompt for Databricks Genie

Paste this into Genie in the `databrick_warehouse` Git folder context. Review the generated job settings before saving or running.

```text
Set up a Databricks Workflow Job for my batch-only thesis repository. Use the existing Git folder files; do not rewrite or duplicate the notebook code.

Workspace/catalog context:
- Catalog: thesis_biomed
- Existing schemas: bronze, silver, gold, quarantine, ops
- Existing Volume: thesis_biomed.bronze.raw_files
- Current BIDMC sample landing path: /Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0
- Project branch: main
- Do not use Kafka, streaming, Auto Loader, or continuous triggers.

Create these runnable tasks and exact dependencies:
1) download_bidmc_sample -> notebooks/00_download_bidmc_sample.py
   Set target_path to /Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0
2) ingest_files -> notebooks/01_ingest_bronze.py
   Depends on download_bidmc_sample. Set raw_path to the same BIDMC landing path, manifest_table to thesis_biomed.bronze.file_manifest, and file_limit to 5 for this test.
3) extract_bidmc_ecg -> notebooks/02_extract_bidmc_ecg.py
   Depends on ingest_files. Set manifest_table to thesis_biomed.bronze.file_manifest.
4) extract_bidmc_ppg -> notebooks/02_extract_bidmc_ppg.py
   Depends on ingest_files. Set manifest_table to thesis_biomed.bronze.file_manifest.
5) extract_bidmc_respiration -> notebooks/02_extract_bidmc_respiration.py
   Depends on ingest_files. Set manifest_table to thesis_biomed.bronze.file_manifest.

The three extraction tasks must be sibling tasks in the DAG so Databricks can run them in parallel after ingest_files. They write separate Bronze tables: thesis_biomed.bronze.bidmc_ecg, thesis_biomed.bronze.bidmc_ppg, thesis_biomed.bronze.bidmc_respiration. Use serverless job compute available to this Free Edition workspace, maximum concurrent runs 1, and no schedule. Use the lowest suitable concurrency/resource configuration. Do not create fake-success validation or Silver tasks; those scripts are not implemented yet.

Do not enable PTB-XL, ICBHI, or RSNA extraction tasks yet. Their landing folders are being prepared, but their files are not present and their format-specific extractors are not implemented. Do not download or modify those datasets.

Before the test run, verify all five notebook paths resolve and all parameters above are set. Then run the Job once using the existing two-file BIDMC sample only. After completion, report task statuses, the three Bronze table row counts, and whether the run is a replay-safe success. Do not publish, schedule, or otherwise alter the Job after the test without asking.
```
