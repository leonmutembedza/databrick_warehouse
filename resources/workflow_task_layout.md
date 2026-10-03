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

Only the download, inventory, and three BIDMC extraction notebooks exist as runnable tasks today. Validation/Silver/Gold stages are DAG design, not implemented tasks; do not create placeholder tasks that report success without doing work.

## Full modality expansion

When the respective files have been landed and parsers tested, add these independent source branches after their own inventory tasks:

| Branch | Input | Extract task | Own downstream chain |
|---|---|---|---|
| BIDMC ECG | CSV signal channel | `02_extract_bidmc_ecg.py` | ECG validation → ECG Silver → ECG features |
| BIDMC PPG | CSV signal channel | `02_extract_bidmc_ppg.py` | PPG validation → PPG Silver → PPG features |
| BIDMC respiration | CSV signal channel | `02_extract_bidmc_respiration.py` | Respiration validation → Silver → features |
| PTB-XL ECG | WFDB | format-specific WFDB extractor | ECG validation → Silver → features |
| ICBHI respiratory audio | WAV + TXT | audio/annotation extractor | audio validation → Silver → audio features |
| RSNA chest X-ray | DICOM + CSV labels | DICOM/label extractor | image validation → Silver → image features |

PTB-XL ECG is a separate source branch even though ECG is also in BIDMC: the source format and dataset contract differ. Do not merge their raw records into one unqualified table. The shared ECG Silver contract can be harmonized later with explicit source/provenance columns.

The manifest task is the synchronization point before fan-out. Extraction branches can run concurrently because they write separate modality tables. On Free Edition, begin with only the three BIDMC branches; they fit within the five concurrent-task limit. Increase concurrency only after measuring job runtime and warehouse availability.

## Idempotency and acceptance checks

- Inventory is complete before any extraction begins.
- Re-running the same input must insert zero additional rows in each modality table.
- Same source identity with a changed file checksum must fail for review, not silently overwrite.
- Reconcile source rows, newly inserted rows, and exact replays per modality.
- Do not schedule recurring runs until these replay checks pass.
- Dataset access/license restrictions and format-specific dependencies must be confirmed before enabling PTB-XL, ICBHI, or RSNA branches.
