# First Databricks Workflow setup

Use a manually triggered Job for the first BIDMC batch. Add a schedule only after a replay has been checked.

## Before creating the Job

1. The `thesis_biomed` catalog and its schemas have already been created, along with the `bronze.raw_files` Volume.
2. Upload the extracted BIDMC release to:

   ```text
   /Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0/
   ```

3. Open this project as a Databricks Git folder or add the task notebooks to the workspace.
4. Confirm that the account can read the Volume and create managed Delta tables in the target schema.

## Job configuration

Create a new Workflow Job with three notebook tasks in sequence:

| Setting | Value |
|---|---|
| Download task key | `download_bidmc_sample` |
| Download notebook | `notebooks/00_download_bidmc_sample.py` |
| Ingest task key | `ingest_files` |
| Ingest notebook | `notebooks/01_ingest_bronze.py` |
| Bronze load task key | `load_bidmc_bronze` |
| Bronze load notebook | `notebooks/02_load_bidmc_bronze.py` |
| Dependencies | `ingest_files` depends on `download_bidmc_sample`; `load_bidmc_bronze` depends on `ingest_files` |
| Compute | Serverless job compute available in the Free Edition account |
| Maximum concurrent runs | 1 |
| Schedule | Off during initial development |
| Retries | 0 for the first run; enable one retry after replay safety is confirmed |
| `download_bidmc_sample.target_path` | `/Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0` |
| `ingest_files.raw_path` | `/Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0` |
| `ingest_files.manifest_table` | `thesis_biomed.bronze.file_manifest` |
| `ingest_files.file_limit` | `5` for the initial subset; change to `0` to process all matching files. Notebook default is `0`. |
| `load_bidmc_bronze.manifest_table` | `thesis_biomed.bronze.file_manifest` |
| `run_id` widgets | Optional; leave blank to let each notebook generate its own run ID. |

The Bronze loader reads the two sample CSVs as strings (preserving source values), checks expected columns and basic time/value quality, then merges rows into `thesis_biomed.bronze.bidmc_signals` and `thesis_biomed.bronze.bidmc_numerics`. Its `source_row_number` is derived from source time and a row hash; Delta `MERGE` makes reruns idempotent. It writes rule outcomes to `thesis_biomed.ops.dq_rule_results` and row/count metrics to `thesis_biomed.ops.pipeline_runs`. Hard-rule failures are recorded and fail the task, which prevents downstream tasks from running. This initial task records the evidence but does not yet move failed rows into quarantine.

The download notebook fetches two public sample CSVs for BIDMC record 01 (signals and numerics) from PhysioNet into the Volume. It verifies that the response looks like CSV, computes SHA-256, and does not overwrite an existing file if its content differs. The task needs outbound internet access and Volume write permissions. The dataset is distributed under the Open Data Commons Attribution License v1.0; retain attribution and the [dataset citation and terms](https://physionet.org/content/bidmc/1.0.0/).

The notebook inventories `.csv` files inside the Volume landing folder and uses Spark SQL `MERGE` to insert unseen file identities. It treats an exact source-key/checksum match as a replay and stops if a source key appears more than once with different checksums. It records metadata only; raw payloads remain in the Volume.

## First-run checks

1. Run with `file_limit=5` and confirm the notebook reports five or fewer files and the expected target table.
2. Inspect the manifest rows and raw URIs.
3. Re-run against the same files. Expected result: zero inserted rows and all files counted as exact replays.
4. Confirm that the notebook fails visibly if an existing relative path has a different checksum.
5. After the initial subset succeeds, increase the file limit or set it to `0` for the full BIDMC drop.

## Step 3: reconcile loaded Bronze rows

After `load_bidmc_bronze` succeeds, run this in SQL Editor. Each input row must be either newly inserted or recognized as an exact replay, and the table must have one row per source row number:

```sql
SELECT source_file_name,
       COUNT(*) AS bronze_rows,
       COUNT(DISTINCT source_row_number) AS distinct_source_rows,
       COUNT(DISTINCT file_checksum) AS file_versions
FROM thesis_biomed.bronze.bidmc_signals
GROUP BY source_file_name;

SELECT source_file_name,
       COUNT(*) AS bronze_rows,
       COUNT(DISTINCT source_row_number) AS distinct_source_rows,
       COUNT(DISTINCT file_checksum) AS file_versions
FROM thesis_biomed.bronze.bidmc_numerics
GROUP BY source_file_name;
```

`bronze_rows` should equal `distinct_source_rows`, with one checksum per file. The task output and `ops.pipeline_runs.metrics_json` show rows read, newly inserted, and exact replay rows skipped. On a rerun, inserted rows should be zero and replay rows should equal source rows.

## Step 4: inspect data-quality results

The load task evaluates required columns, non-empty input, numeric time values, invalid channel values, missing channel values, and duplicate time values. View the latest task audit to get its run ID and status:

```sql
SELECT run_id, run_status, accepted_rows, exact_replays_skipped,
       error_summary, metrics_json
FROM thesis_biomed.ops.pipeline_runs
WHERE task_key = 'load_bidmc_bronze'
ORDER BY started_at DESC
LIMIT 1;
```

Use that `run_id` to inspect rule-level evidence. A `passed = false` warning does not fail the task; a failed `FAIL`-severity rule is recorded and then fails the task so later stages are gated.

```sql
SELECT record_id, modality, rule_id, severity, passed,
       observed_value, threshold_value, failure_reason
FROM thesis_biomed.ops.dq_rule_results
WHERE run_id = '<load task run_id>'
ORDER BY record_id, severity, rule_id;
```

Review warnings before fixing thresholds. This is a technical-quality screen, not a clinical validity assessment. Quarantine routing is a later stage: until that is added, failed rows remain in raw/Bronze for traceability and are not ready for Silver processing.

## Official documentation

- [Create and manage scheduled notebook jobs](https://docs.databricks.com/aws/en/jobs/notebook-jobs)
- [Run serverless jobs](https://docs.databricks.com/aws/en/jobs/run-serverless-jobs)
- [Read files from a Unity Catalog Volume](https://docs.databricks.com/aws/en/query)
