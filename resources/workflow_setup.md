# First Databricks Workflow setup

Use a manually triggered Job for the first BIDMC batch. Add a schedule only after a replay has been checked.

## Before creating the Job

1. The `thesis_biomed` catalog and its schemas have already been created, along with the `bronze.raw_files` Volume.
2. Upload the extracted BIDMC release to:

   ```text
   /Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0/
   ```

3. Add `notebooks/01_ingest_bronze.py` to the Databricks workspace or open this project as a Git folder.
4. Confirm that the account can read the Volume and create managed Delta tables in the target schema.

## Job configuration

Create a new Workflow Job with two notebook tasks in sequence. The download task should complete before `ingest_files` starts:

| Setting | Value |
|---|---|
| Download task key | `download_bidmc_sample` |
| Download notebook | `notebooks/00_download_bidmc_sample.py` |
| Ingest task key | `ingest_files` |
| Ingest notebook | `notebooks/01_ingest_bronze.py` |
| Dependency | `ingest_files` depends on `download_bidmc_sample` |
| Compute | Serverless job compute available in the Free Edition account |
| Maximum concurrent runs | 1 |
| Schedule | Off during initial development |
| Retries | 0 for the first run; enable one retry after replay safety is confirmed |
| `raw_path` parameter | `/Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0` |
| `manifest_table` parameter | `thesis_biomed.bronze.file_manifest` |
| `run_id` parameter | Job run ID, or leave blank for a manual run |
| `file_limit` parameter | `5` for the initial subset; change to `0` to process all matching files. Notebook default is `0`. |

The download notebook fetches two public sample CSVs for BIDMC record 01 (signals and numerics) from PhysioNet into the Volume. It verifies that the response looks like CSV, computes SHA-256, and does not overwrite an existing file if its content differs. The task needs outbound internet access and Volume write permissions. The dataset is distributed under the Open Data Commons Attribution License v1.0; retain attribution and the [dataset citation and terms](https://physionet.org/content/bidmc/1.0.0/).

The notebook inventories `.csv` files inside the Volume landing folder and uses Spark SQL `MERGE` to insert unseen file identities. It treats an exact source-key/checksum match as a replay and stops if a source key appears more than once with different checksums. It records metadata only; raw payloads remain in the Volume.

## First-run checks

1. Run with `file_limit=5` and confirm the notebook reports five or fewer files and the expected target table.
2. Inspect the manifest rows and raw URIs.
3. Re-run against the same files. Expected result: zero inserted rows and all files counted as exact replays.
4. Confirm that the notebook fails visibly if an existing relative path has a different checksum.
5. After the initial subset succeeds, increase the file limit or set it to `0` for the full BIDMC drop.

## Official documentation

- [Create and manage scheduled notebook jobs](https://docs.databricks.com/aws/en/jobs/notebook-jobs)
- [Run serverless jobs](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs)
- [Read files from a Unity Catalog Volume](https://docs.databricks.com/aws/en/query)
