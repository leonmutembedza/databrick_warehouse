# First Databricks Workflow setup

Use a manually triggered Job for the first BIDMC batch. Add a schedule only after a replay has been checked.

## Before creating the Job

1. In Databricks, create a catalog and schema for the thesis and a Unity Catalog Volume for raw source data. Run [the bootstrap SQL](sql/bootstrap.sql) after replacing `YOUR_CATALOG` with the catalog name. Create the raw Volume from Catalog Explorer if it does not already exist.
2. Upload the extracted BIDMC release to:

   ```text
   /Volumes/<catalog>/<schema>/<volume>/landing/bidmc/1.0.0/
   ```

3. Add `notebooks/01_ingest_bronze.py` to the Databricks workspace or open this project as a Git folder.
4. Confirm that the account can read the Volume and create managed Delta tables in the target schema.

## Job configuration

Create a new Workflow Job with one notebook task:

| Setting | Value |
|---|---|
| Task key | `ingest_bidmc_bronze` |
| Notebook | `notebooks/01_ingest_bronze.py` |
| Compute | Serverless job compute available in the Free Edition account |
| Maximum concurrent runs | 1 |
| Schedule | Off during initial development |
| Retries | 0 for the first run; enable one retry after replay safety is confirmed |
| `raw_path` parameter | Actual Volume path ending in `/landing/bidmc/1.0.0` |
| `manifest_table` parameter | `<catalog>.bronze.file_manifest` |
| `run_id` parameter | Job run ID, or leave blank for a manual run |
| `file_limit` parameter | `5` for the initial subset; use `0` only after the subset is validated |

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
