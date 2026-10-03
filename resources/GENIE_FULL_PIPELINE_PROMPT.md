# Genie prompt: combine modality landing and Bronze tasks

Paste this into the Databricks assistant/Genie Code surface that can edit Workflows, with the repository Git folder open. The connected Genie MCP in this assistant is read-only data Q&A and cannot perform these Job edits itself.

```text
Update the existing BIDMC Databricks Workflow Job in this workspace to create one unified, batch-only multi-task pipeline for all thesis dataset branches. Preserve the current BIDMC job and its task history. Do not delete/recreate the job, do not add Kafka/streaming/Auto Loader, and do not schedule it.

Every task in this design must be a Databricks Notebook task pointing at the Git notebook source. Do not use Python script tasks: the task notebooks use notebook-scoped dbutils widgets and the BIDMC wrappers use a standalone %run notebook cell.

Repository: Git folder for leonmutembedza/databrick_warehouse, branch main.
Catalog: thesis_biomed. Existing layer schemas and Volume: thesis_biomed.bronze.raw_files.
Use /Volumes/thesis_biomed/bronze/raw_files/landing/<dataset>/<version>/ for every landing path.

Keep the existing BIDMC task chain and parallel modality fan-out:
- download_bidmc_sample: notebooks/00_download_bidmc_sample.py
- ingest_files: notebooks/01_ingest_bronze.py, depends on download_bidmc_sample
- extract_bidmc_ecg: notebooks/02_extract_bidmc_ecg.py, depends on ingest_files
- extract_bidmc_ppg: notebooks/02_extract_bidmc_ppg.py, depends on ingest_files
- extract_bidmc_respiration: notebooks/02_extract_bidmc_respiration.py, depends on ingest_files
Use the existing BIDMC landing path /Volumes/thesis_biomed/bronze/raw_files/landing/bidmc/1.0.0 and file_limit=5 for a bounded development run.

Add three independent source branches to the SAME Job. Each branch must have land -> register files -> extract to Bronze dependencies; the three branches and the BIDMC branch can run concurrently. Configure maximum concurrent runs=1 and conservative available serverless job compute.

PTB-XL ECG branch:
- land_ptbxl_ecg -> notebooks/10_land_ptbxl_ecg.py
  target_path=/Volumes/thesis_biomed/bronze/raw_files/landing/ptb-xl/1.0.3, version=1.0.3, max_records=5.
- register_ptbxl_files -> notebooks/13_register_landed_files.py; depends on land_ptbxl_ecg.
  raw_path=<same PTB-XL path>, dataset=ptb-xl, dataset_version=1.0.3,
  license_reference=https://physionet.org/content/ptb-xl/1.0.3/.
- extract_ptbxl_ecg_bronze -> notebooks/14_extract_ptbxl_bronze.py; depends on register_ptbxl_files.
  max_records=5. Add the PyPI task library wfdb to this task only.
  Output: thesis_biomed.bronze.ptbxl_ecg_records.

ICBHI respiratory-audio branch:
- land_icbhi_audio -> notebooks/11_land_icbhi_audio.py.
  Remove the obsolete icbhi_archive_url job parameter and source_archive_url task parameter.
  target_path=/Volumes/thesis_biomed/bronze/raw_files/landing/icbhi-2017/kaggle_vbookshelf,
  kaggle_dataset=vbookshelf/respiratory-sound-database,
  secret_scope=thesis, username_secret_key=kaggle_username, api_key_secret_key=kaggle_key,
  max_recordings=5. Add the PyPI task library kaggle to this landing task only.
  The script downloads the Kaggle dataset ZIP, unpacks it in temporary Volume staging,
  then lands only the requested WAV/TXT pairs under files/ and removes staging data.
- register_icbhi_files -> notebooks/13_register_landed_files.py; depends on land_icbhi_audio.
  raw_path=<same ICBHI path>, dataset=icbhi-2017,
  dataset_version=kaggle-vbookshelf-snapshot,
  source_dataset_id=vbookshelf/respiratory-sound-database,
  license_reference=https://www.kaggle.com/datasets/vbookshelf/respiratory-sound-database.
- extract_icbhi_audio_bronze -> notebooks/15_extract_icbhi_bronze.py; depends on register_icbhi_files.
  source_version=kaggle-vbookshelf-snapshot,
  kaggle_dataset_id=vbookshelf/respiratory-sound-database, max_recordings=5.
  The registration task must validate source_metadata.json and persist Kaggle mirror provenance;
  extraction must verify the source dataset ID/version and WAV/TXT checksums from the manifest.
  Outputs: thesis_biomed.bronze.icbhi_audio_records and thesis_biomed.bronze.icbhi_cycle_annotations.
  IMPORTANT: Kaggle currently lists the mirror license as Unknown. Preserve that caveat in
  metadata and do not represent it as license-cleared for research/publication without review.

RSNA chest-X-ray branch:
- land_rsna_chest_xray -> notebooks/12_land_rsna_chest_xray.py.
  target_path=/Volumes/thesis_biomed/bronze/raw_files/landing/rsna-pneumonia/stage_2,
  secret_scope=thesis, username_secret_key=kaggle_username, api_key_secret_key=kaggle_key.
  Add the PyPI task library kaggle to this landing task only. Keep download_image_archives=false by default; this avoids an automatic multi-GB download.
- register_rsna_files -> notebooks/13_register_landed_files.py; depends on land_rsna_chest_xray.
  raw_path=<same RSNA path>, dataset=rsna-pneumonia, dataset_version=stage_2,
  license_reference=https://www.kaggle.com/competitions/rsna-pneumonia-detection-challenge/rules.
- extract_rsna_chest_xray_bronze -> notebooks/16_extract_rsna_bronze.py; depends on register_rsna_files.
  max_images=5. Add the PyPI task library pydicom to this task only.
  Output: thesis_biomed.bronze.rsna_chest_xray_records.

Add a final validate_bronze_outputs task using notebooks/17_validate_bronze_outputs.py, depending on every enabled modality extraction task. Its required_tables parameter must list all six primary outputs: thesis_biomed.bronze.bidmc_ecg, thesis_biomed.bronze.bidmc_ppg, thesis_biomed.bronze.bidmc_respiration, thesis_biomed.bronze.ptbxl_ecg_records, thesis_biomed.bronze.icbhi_audio_records, and thesis_biomed.bronze.rsna_chest_xray_records. Once all branches are enabled, also include thesis_biomed.bronze.icbhi_cycle_annotations.

IMPORTANT ENABLEMENT / TEST RULES:
- Keep the existing BIDMC branch enabled.
- Create and save all branches, but disable ICBHI and RSNA landing/registration/extraction tasks until both Kaggle secrets exist in the thesis scope. ICBHI must use the Kaggle mirror ID and the distinct kaggle-vbookshelf-snapshot source version; keep it gated pending user review of the mirror's Unknown license status and full-download staging/storage needs. RSNA additionally requires accepting the competition rules and explicitly approving its multi-GB DICOM image download. Disable dependent tasks in those branches too.
- PTB-XL can be enabled with the bounded 5-record setting above. Do not increase record limits or turn on full RSNA archives without user review.
- Do not run the whole multi-dataset Job now. Save the DAG and report which tasks are enabled/disabled, the per-task libraries, and any unresolved setup requirements. Do not print secret values or include them in task parameters.
- Do not create fake-success tasks. Keep modality branches independent; a failure in one source must not cause another source's Bronze tables to be overwritten.
```
