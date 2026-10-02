-- Run in a Databricks SQL editor after replacing the catalog name.
-- Create the catalog separately if the workspace does not already provide one.
-- Keep the raw Unity Catalog Volume separate from these managed Delta tables.

CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.bronze;
CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.silver;
CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.gold;
CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.quarantine;
CREATE SCHEMA IF NOT EXISTS YOUR_CATALOG.ops;

CREATE TABLE IF NOT EXISTS YOUR_CATALOG.bronze.file_manifest (
  record_id STRING NOT NULL,
  subject_id_hash STRING,
  modality STRING NOT NULL,
  source_dataset STRING NOT NULL,
  source_version STRING NOT NULL,
  source_record_key STRING NOT NULL,
  event_timestamp STRING,
  time_precision STRING,
  raw_uri STRING NOT NULL,
  file_checksum STRING NOT NULL,
  file_format STRING NOT NULL,
  source_metadata MAP<STRING, STRING> NOT NULL,
  ingestion_run_id STRING NOT NULL,
  ingested_at STRING NOT NULL,
  schema_version STRING NOT NULL,
  pipeline_version STRING NOT NULL,
  license_reference STRING,
  deidentification_status STRING NOT NULL
) USING DELTA;

CREATE TABLE IF NOT EXISTS YOUR_CATALOG.ops.pipeline_runs (
  run_id STRING NOT NULL,
  parent_job_run_id STRING,
  task_key STRING,
  source_dataset STRING,
  source_version STRING,
  run_date DATE,
  pipeline_version STRING NOT NULL,
  schema_version STRING NOT NULL,
  rule_version STRING,
  run_status STRING NOT NULL,
  started_at TIMESTAMP NOT NULL,
  finished_at TIMESTAMP,
  source_files_seen BIGINT,
  manifest_rows_inserted BIGINT,
  exact_replays_skipped BIGINT,
  changed_payload_conflicts BIGINT,
  accepted_rows BIGINT,
  quarantined_rows BIGINT,
  error_summary STRING,
  metrics_json STRING
) USING DELTA;

CREATE TABLE IF NOT EXISTS YOUR_CATALOG.ops.dq_rule_results (
  run_id STRING NOT NULL,
  record_id STRING NOT NULL,
  modality STRING NOT NULL,
  rule_id STRING NOT NULL,
  rule_version STRING NOT NULL,
  severity STRING NOT NULL,
  passed BOOLEAN NOT NULL,
  observed_value STRING,
  threshold_value STRING,
  failure_reason STRING,
  pipeline_version STRING NOT NULL,
  evaluated_at TIMESTAMP NOT NULL
) USING DELTA;
