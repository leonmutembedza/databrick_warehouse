# Batch workflow task layout

This is the intended DAG for the thesis pipeline. It is batch-only; it does not use Kafka or streaming. Keep one catalog (`thesis_biomed`) and shared layer schemas, while writing modality-specific Bronze/Silver tables.

## Executable first run (data currently landed)

```text
download_bidmc_sample
        |
   ingest_files
    /    |     \
extract_ecg extract_ppg extract_respiration   # parallel fan-out
    |          |          |
validate_ecg validate_ppg validate_respiration
    |          |          |
silver_ecg  silver_ppg  silver_respiration
    \__________|__________/
         cross_modal_gold (later)
```

The extraction tasks use separate notebook entrypoints and write to `thesis_biomed.bronze.bidmc_ecg`, `thesis_biomed.bronze.bidmc_ppg`, and `thesis_biomed.bronze.bidmc_respiration`. Each branch's validation and Silver task should depend only on that branch's extraction/validation task. `cross_modal_gold` must wait for all required Silver branches.

The BIDMC branch and the additional landing/manifest/Bronze notebooks are now in the repository. Validation/Silver/Gold transformations are still later stages; do not create placeholder tasks that report success without doing work.

## Full modality expansion

The following independent source branches use the modality-specific landing and extraction scripts below. Keep gated branches disabled until access prerequisites are met and a bounded test is reviewed:

| Branch | Input | Extract task | Own downstream chain |
|---|---|---|---|
| BIDMC ECG | CSV signal channel | `02_extract_bidmc_ecg.py` | ECG validation → ECG Silver → ECG features |
| BIDMC PPG | CSV signal channel | `02_extract_bidmc_ppg.py` | PPG validation → PPG Silver → PPG features |
| BIDMC respiration | CSV signal channel | `02_extract_bidmc_respiration.py` | Respiration validation → Silver → features |
| PTB-XL ECG | WFDB | format-specific WFDB extractor | ECG validation → Silver → features |
| ICBHI respiratory audio | WAV + TXT | audio/annotation extractor | audio validation → Silver → audio features |
| RSNA chest X-ray | DICOM + CSV labels | DICOM/label extractor | image validation → Silver → image features |

The concrete landing and Bronze task entrypoints are `10_land_ptbxl_ecg.py` → `14_extract_ptbxl_bronze.py`, `11_land_icbhi_audio.py` → `15_extract_icbhi_bronze.py`, and `12_land_rsna_chest_xray.py` → `16_extract_rsna_bronze.py`; each landing branch runs `13_register_landed_files.py` between landing and decoding. A full-run smoke check is `17_validate_bronze_outputs.py`.

PTB-XL is pinned to v1.0.3 because PhysioNet reports that v1.0.0 files are no longer available. Its first landing defaults to five 100-Hz records. ICBHI now uses the Kaggle mirror `vbookshelf/respiratory-sound-database`, authenticated with the same `thesis` secret scope used by RSNA; add the `kaggle` package to its landing task only. The mirror's Kaggle page lists its license as Unknown, so treat it as a development source until terms and content are verified. A five-record limit bounds the files persisted to the landing folder but does not bound the full Kaggle mirror download/unpack. RSNA uses Kaggle credentials from Databricks Secrets and leaves the full image archive download disabled by default; those archives are about 4 GB. Keep ICBHI and RSNA gated until credentials, data-use terms, and storage needs are reviewed. See [the full-pipeline Genie prompt](GENIE_FULL_PIPELINE_PROMPT.md).

PTB-XL ECG is a separate source branch even though ECG is also in BIDMC: the source format and dataset contract differ. Do not merge their raw records into one unqualified table. The shared ECG Silver contract can be harmonized later with explicit source/provenance columns.

The manifest task is the synchronization point within each branch. Extraction branches can run concurrently because they write separate modality tables. On Free Edition, retain a low task concurrency and bounded record counts; PTB-XL's first landing defaults to five records, ICBHI to five recordings (after downloading/unpacking the full Kaggle dataset into temporary Volume staging), and RSNA image download is opt-in because its archives total about 4 GB. Increase limits only after measuring job runtime, storage, and warehouse availability. Keep `wfdb`, `kaggle`, and `pydicom` isolated to the tasks that need them.

## Idempotency and acceptance checks

- Inventory is complete before any extraction begins.
- Re-running the same input must insert zero additional rows in each modality table.
- Same source identity with a changed file checksum must fail for review, not silently overwrite.
- Reconcile source rows, newly inserted rows, and exact replays per modality.
- Do not schedule recurring runs until these replay checks pass.
- Dataset access/license restrictions and format-specific dependencies must be confirmed before enabling PTB-XL, ICBHI, or RSNA branches.
