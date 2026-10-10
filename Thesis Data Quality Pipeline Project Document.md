# Design and Implementation of a Data Quality Pipeline for Multimodal Biomedical Data

**Project execution document**  
Working baseline · Updated 3 October 2026

### Implementation status

The implementation has progressed beyond the original foundation-only stage. The user reports that the catalog schemas were bootstrapped, two BIDMC CSVs were ingested successfully, the modality extraction tasks succeeded after fixing serverless Spark initialization and task dependencies, the multi-branch Databricks Workflow was configured, the updated code was pulled into the Databricks Git folder, and Kaggle username/key credentials were added to the `thesis` secret scope. These workspace/job outcomes are user-reported; this document does not claim to have independently queried Databricks today. The ICBHI Kaggle landing task has not yet been run with the newly configured credentials. See the repository [README](README.md) for the implementation layout.

### Progress log · 3 October 2026

#### Completed or reported successful

- Resolved the Kaggle source URL problem for ICBHI by switching the landing implementation to the Kaggle-hosted mirror `vbookshelf/respiratory-sound-database`; this replaces the previous direct archive URL approach.
- Updated the ICBHI landing and Bronze extraction scripts, dataset registry, and Workflow task instructions. The landing task reads credentials from Databricks Secrets and targets `/Volumes/thesis_biomed/bronze/raw_files/landing/icbhi-2017/kaggle_vbookshelf`.
- Committed and pushed the ICBHI mirror changes to GitHub `main` in commit `ca4d8e5` (`Integrate ICBHI Kaggle mirror landing`). The user reports pulling those changes into the Databricks Git folder.
- The user reports adding the Kaggle credentials to the Databricks secret scope `thesis`, under keys `kaggle_username` and `kaggle_key`. Secret values are intentionally not recorded here. Their availability to the job has not yet been confirmed by a successful task run.
- Earlier pipeline milestones: catalog schemas were created from the bootstrap SQL; a BIDMC landing/ingest run succeeded for two CSV files; the multi-branch batch Workflow was configured with 15 tasks, four modality branches (physiological signals, respiratory audio, and chest X-ray, with ECG and PPG processed separately), and a final validation task. The user also reported successful extraction runs after the Spark initialization and dependency fixes.

#### Remaining work and next execution sequence

1. Review the Kaggle mirror’s data-use terms before research use or publication. Kaggle lists the mirror’s license as **Unknown**; retain this as a blocker to treating the mirror as an approved thesis source until terms are clarified.
2. In the Databricks Job, verify `land_icbhi_audio` points to the pulled Git version of `notebooks/11_land_icbhi_audio.py`, uses the `thesis` / `kaggle_username` / `kaggle_key` secret references, and has the `kaggle` PyPI library attached to this task only. Keep `max_recordings=5` for the initial test.
3. Run only `land_icbhi_audio`. The five-record setting limits the WAV/annotation pairs retained, not the dataset download: the task downloads and unpacks the full Kaggle mirror in temporary Volume staging before retaining the bounded subset. Check run logs, staging cleanup, the landing manifest, and the five WAV/TXT pairs under the configured landing path.
4. If landing succeeds and source terms are acceptable, run `register_icbhi_files`, then `extract_icbhi_audio_bronze`. Confirm expected ICBHI Bronze tables, schemas, row counts, checksums, and run identifiers; record any failures and fix them before enabling the branch.
5. Keep the RSNA branch gated until Kaggle competition rules are accepted and the multi-gigabyte image download/storage impact is explicitly reviewed. Keep all-modality validation gated until every required branch has successful outputs.
6. Run the complete bounded batch Workflow after the branches are ready. Verify reconciliation, idempotent reruns, task-level dependencies, and the final validation results; capture the Databricks Job run ID and table counts for the thesis evidence log.

No streaming components are in scope: orchestration is batch-based, and Kafka is excluded.

This document records the current thesis scope and execution design. It consolidates the Notion thesis brief, planned pipeline tasks, Databricks Free Edition choice, and selected public datasets. The implementation is a repeatable batch lakehouse pipeline for assessing and preparing biomedical files for downstream analysis.

## 1. Thesis purpose and research framing

The thesis proposes a scalable data engineering pipeline that improves the quality, consistency, traceability, and usability of multimodal biomedical data. Its contribution is the engineering method: a common metadata contract, modality-specific validation, quarantine with evidence, standardized outputs, explainable scoring, and reproducible evaluation. It does not claim that image or signal heuristics establish clinical validity.

**Working research question:** How can a batch lakehouse pipeline detect and document technical quality issues across ECG and PPG signals, respiratory audio, and chest X-ray images, and how do its quality outputs affect dataset usability and a downstream baseline analysis?

### Objectives

- Design a lakehouse layout and shared record metadata contract for raw files and derived data.
- Ingest public ECG, PPG, respiratory-audio, and chest X-ray datasets in repeatable batches.
- Define versioned quality rules, standardize passing records, and retain failures in modality-specific quarantine tables.
- Produce modality-level features and an explainable record-level quality score.
- Measure pipeline correctness, data-quality outcomes, runtime, and, if feasible, the relationship between quality tiers and a downstream baseline task.

**Scope interpretation:** The earlier plan sometimes groups ECG and PPG as “signals” and sometimes lists three modalities. This document treats the project as three modality families—physiological signals (ECG and PPG), respiratory audio, and chest X-ray—with separate ECG and PPG rule sets. Confirm this scope before locking the final thesis title and evaluation design.

## 2. Architecture and technology decisions

Use Databricks Free Edition as the primary development and batch execution platform. Use Python/PySpark notebooks to run stages, Unity Catalog for organization and access where enabled, Volumes for raw unstructured files, and Delta tables for manifests and Bronze/Silver/Gold data. Databricks documents a medallion pattern and recommends Volumes for raw unstructured data ([Delta Lake architecture guidance](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/delta-lake)).

| Layer or service | Purpose and contents |
|---|---|
| Source datasets | Public files, dataset documentation, license/terms, and source checksums. |
| Unity Catalog Volume | Immutable raw files and staged source archives; keep binary payloads out of analytic tables. |
| Bronze Delta | One manifest row per source file/record, with source identity, checksum, raw URI, modality, run ID, and source metadata. |
| Silver Delta | Decoded, technically valid, standardized samples and normalized metadata. |
| Quarantine Delta | Rejected files/records with failure reasons, observed values, rule versions, and raw references. |
| Gold Delta | Per-modality quality features, score components, final score/tier, and analysis-ready views. |
| Databricks Workflows | Scheduled or manually triggered batch tasks, dependencies, retries, alerts, and run history. |
| Databricks SQL | Queries and dashboard views for pass rates, failures, scores, reconciliation, and runtime. |

**Core decision:** Implement with Delta tables in Databricks. The earlier Notion draft mentions Nessie and Dremio; those are not required in the current implementation baseline. Keep Dremio as a possible optional consumer only if a thesis requirement later makes it necessary. Avoid mixing table formats and catalogs during the core build.

**Batch model:** Each run processes a bounded dataset, dataset release, or input partition and then terminates. The workflow receives run parameters and can be safely rerun. Kafka, Structured Streaming, continuous triggers, and streaming checkpoints are outside the project scope.

## 3. Candidate datasets

| Data | Selected source and contents | Use and access notes |
|---|---|---|
| ECG | [PTB-XL, PhysioNet](https://physionet.org/content/ptb-xl/1.0.3/): 12-lead waveforms in WFDB formats, record headers, and extensive metadata. | Primary ECG ingestion and signal-quality dataset. Public anonymous dataset; cite the PhysioNet record and original paper. Check the selected version’s license and citation terms before redistribution. |
| PPG and reference signals | [BIDMC PPG and Respiration Dataset, PhysioNet](https://physionet.org/content/bidmc/1.0.0/): 53 eight-minute recordings, PPG, ECG, impedance respiration, numerics, and annotations; WFDB, CSV, and MAT formats. | Small first end-to-end batch. Use CSV to validate the pipeline quickly, then WFDB for format coverage. The dataset page reports about 208 MB uncompressed. |
| Respiratory audio | [ICBHI 2017 Respiratory Sound Database](https://bhichallenge.med.auth.gr/): respiratory recordings and cycle annotations, with associated demographic and diagnostic files. | Use WAV files and annotations to exercise audio decoding, audio-quality measures, metadata joins, and spectrogram feature extraction. The data is made available for research; verify official terms and redistribution limits before publication. |
| Chest X-ray | [RSNA Pneumonia Detection Challenge, Kaggle](https://www.kaggle.com/competitions/rsna-pneumonia-detection-challenge/data): DICOM images, training labels, detailed class information, and bounding boxes. | Start with a small subset for DICOM, pixel, and metadata validation; expand if storage and run time allow. Kaggle account access and acceptance of competition rules are required. The dataset page reports about 3.96 GB. |

Recommended order: BIDMC, ICBHI, a representative PTB-XL subset, then 500–1,000 RSNA images before deciding whether to process the full image collection. Record dataset version, source URL, download date, file count, bytes, checksums, license/terms, and citation in a dataset register. Do not commit dataset files or secrets to source control.

All proposed sources are research datasets, but “public” does not mean unrestricted. Check each current license or dataset agreement, cite the original dataset and paper, and record permitted uses. Use only published de-identified datasets for this project; do not place real patient data into the Free Edition workspace.

## 4. Data contract and medallion layout

The shared Bronze manifest describes the file and its processing provenance. Store subject identifiers only as the source’s de-identified/pseudonymous ID or a project-specific hash; do not add names or direct identifiers. Keep source-specific fields in a controlled metadata object and retain the original raw reference.

| Field | Meaning and rule |
|---|---|
| `record_id` | Stable project key derived from source dataset and source record key; document the derivation. |
| `subject_id_hash` | Pseudonymous subject/group key for split safety and grouping; never a direct identifier. |
| `modality` | Controlled value such as `ecg`, `ppg`, `respiratory_audio`, or `chest_xray`. |
| `source_dataset`, `source_version` | Dataset name and exact release/version. |
| `source_record_key`, `raw_uri`, `file_checksum` | Stable source identity, raw file location, and cryptographic file checksum. |
| `event_timestamp`, `time_precision` | Source event time if provided; otherwise null with the reason/precision documented. |
| `file_format`, `source_metadata` | Source encoding plus structured modality-specific metadata. |
| `ingestion_run_id`, `ingested_at` | Batch lineage and UTC ingestion time. |
| `schema_version`, `pipeline_version` | Version of the contract and processing code used. |
| `consent_or_license_reference`, `deidentification_status` | Dataset terms/citation key and source de-identification status. |

Suggested tables: `bronze.file_manifest`, `silver.ecg_samples`, `silver.ppg_samples`, `silver.audio_samples`, `silver.chest_xray_samples`, `quarantine.*`, `gold.sample_quality`, `gold.*_features`, `ops.pipeline_runs`, and `ops.dq_rule_results`. Partition only on useful low-cardinality fields, such as dataset or modality; avoid subject-level partitioning.

## 5. Quality gates and score design

A rule registry will make the quality gate auditable and adjustable. Each rule stores `rule_id`, modality, severity, threshold, `rule_version`, result, observed value, reason, `record_id`, `run_id`, code version, and evaluation time. Hard failures go to Quarantine. Warnings can pass to Silver but remain visible in rule results.

| Data type | Hard failures | Quality measures and warnings |
|---|---|---|
| ECG / PPG | Unreadable record; absent signal; empty or too-short waveform; invalid sampling rate; NaN/Inf values. | Missingness, amplitude range, clipping/saturation, baseline drift, duration, signal-to-noise proxy, lead/channel completeness. |
| Respiratory audio | Unreadable WAV; zero duration; invalid sample rate/channel count; non-finite samples. | Duration, silence fraction, clipping ratio, RMS level, background-noise/SNR proxy, spectrogram shape. |
| Chest X-ray | Unreadable DICOM/PNG; missing pixel array; invalid dimensions; unsupported pixel representation. | Bit depth, dimensions/aspect ratio, missing metadata, brightness/contrast, exposure outliers, entropy/sharpness proxies. |

These measures are proposed engineering checks, not clinical acceptance criteria. Set thresholds from dataset documentation and exploratory profiling, freeze them by rule version, and report false positives and limitations. Do not compare raw score values across modalities until the score is normalized and its meaning is justified.

Initial score hypothesis, to be tested rather than treated as established truth: completeness 30%, technical validity 30%, artifact/noise indicators 25%, metadata completeness 15%. Store all component scores and the rule/weight version alongside the final 0–100 score. Define quality tiers only after examining distributions and review examples.

## 6. Batch ingestion and orchestration

Each modality handler should implement the same batch contract: discover a bounded input set, verify format and source key, calculate checksum, record an immutable raw URI, append an idempotent Bronze manifest, and capture per-file outcomes. De-duplicate using source dataset, source record key, and checksum; make replay behavior explicit.

A Databricks Workflow Job should run these dependent tasks:

| Order | Task | Output or gate |
|---:|---|---|
| 1 | `ingest_ecg`, `ingest_ppg`, `ingest_audio`, `ingest_imaging` | Bronze manifest and per-file ingestion outcomes. |
| 2 | `bronze_reconciliation` | Source and Bronze counts, duplicate/skipped counts, immutable run manifest. |
| 3 | `validate_ecg_ppg`, `validate_audio`, `validate_imaging` | Rule results; accepted Silver records; rejected Quarantine records. |
| 4 | `silver_reconciliation` | Assert attempted = accepted + quarantined + explicitly skipped/deduplicated. |
| 5 | `extract_features` | Modality-specific Gold feature tables. |
| 6 | `score_quality` | Component and overall scores, tier, rule/score versions. |
| 7 | `publish_run_metrics` | Run status, row counts, pass/fail rates, durations, code/table versions. |
| 8 | `end_to_end_checks` | Schema contracts, reconciliation, idempotency regression checks; fail on critical errors. |

Pass `run_id`, `run_date`, `source_dataset`, `dataset_version`, `rule_version`, and `pipeline_version` into each task. Make writes idempotent before enabling retries. Use task dependencies to gate downstream work on upstream success. Begin with manual runs; add a daily schedule only when input availability and end-to-end behavior are stable. Record the failure stage, error summary, and remediation pointer. Keep the graph mostly sequential to respect Free Edition concurrency limits.

Free Edition constraints can change. Current Databricks documentation describes capped resources, including a five-concurrent-task job limit and one small SQL warehouse ([Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations)). Confirm current limits in the account before finalizing the workflow shape; this plan does not assume production-scale capacity.

## 7. Features, evaluation, and reproducibility

Gold should contain both shared quality information and modality-specific features. Candidate signal features include duration, sample rate, missingness, clipping, and signal-quality proxies. Audio features include duration, silence/clipping, RMS, sample-rate/channel metadata, and spectrogram summaries. Image features include dimensions, pixel representation, brightness/contrast, and sharpness/exposure proxies. Preserve raw references and processing versions throughout.

Evaluate pipeline behavior against a fixed dataset snapshot and fixed rule/code versions. Report accepted and quarantined proportions; counts and rates by modality/source; missingness and artifact distributions before and after processing; metadata completeness; duplicate rate; run time and volume; and reconciliation results. If labels permit, compare a simple downstream baseline across quality tiers to measure whether quality filtering changes usable performance. Avoid claims of clinical benefit from this engineering evaluation.

Build a small known-case fixture set containing valid, unreadable, missing, clipped, noisy, malformed, and duplicate inputs. Unit checks should cover each validator and score component; integration checks should cover all modality paths, retry/replay behavior, schema contracts, and count reconciliation. A final reported run should reference a source release, file manifest/checksums, Git commit, job run ID, rule version, and Delta table versions.

## 8. Execution sequence and completion criteria

| Phase | Work | Exit evidence |
|---|---|---|
| 1. Foundation | Create project layout; lock scope; dataset register; Bronze contract; `ops` audit schemas; parameter conventions. | Approved field definitions, source/license record, and schema contract. |
| 2. First batch | Load a BIDMC CSV subset; make checksum-based ingestion idempotent; persist the Bronze manifest. | Repeat run shows no duplicate accepted records; source/Bronze counts reconcile. |
| 3. Quality framework | Implement rule registry and ECG/PPG validators; add ICBHI audio and RSNA DICOM handlers. | Known-case fixtures exercise pass, warn, and quarantine paths for every data family. |
| 4. Silver and Gold | Standardize passing records; compute features and explainable score components. | Gold lineage links back to source, run, rules, and raw file. |
| 5. Workflow | Wire tasks and dependencies into a Databricks Workflow Job; add safe retries, logging, and alerts/report. | One manual end-to-end run succeeds; failure causes are visible and retries are safe. |
| 6. Analysis and thesis | Build SQL metrics views; conduct fixed-snapshot evaluation; write architecture, method, results, and limitations. | Reproducible result tables, diagrams, source citations, and documented limitations. |

Completion means a single batch can process all selected modality families from raw references through Gold; failures are retained with rule evidence; replays are safe; counts reconcile; and evaluation can be reproduced from recorded data, code, rule, and table versions.

## 9. Risks and decisions to resolve

- Confirm whether the final thesis treats ECG and PPG as separate modalities or as signal subtypes under a physiological-signal family.
- Confirm that selected dataset versions and terms allow the intended processing, publication of derived metrics, and any examples/screenshots.
- Measure available storage and job time in Free Edition; stage representative subsets until resource needs are understood.
- Set thresholds from documented dataset ranges and profiling; record them as engineering thresholds rather than clinical cutoffs.
- Decide whether downstream ML evaluation is feasible and methodologically appropriate; preserve patient-level grouping to prevent leakage.
- Update earlier Notion tasks that still mention Nessie and Dremio so the execution checklist reflects the Databricks/Delta baseline.

## 10. Sources and references

1. Thesis Project and Planned Steps to Multimodal Pipeline, Notion workspace pages, accessed 2 October 2026: [Thesis Project](https://app.notion.com/p/3a03c7189191801c9d21d28b29505a5a) and [Planned Steps](https://app.notion.com/p/3ba3c718919181119315d0374e3a83ea).
2. Databricks, [Delta Lake architecture recommendations](https://docs.databricks.com/aws/en/lakehouse-architecture/deployment-guide/delta-lake).
3. Databricks, [Free Edition limitations](https://docs.databricks.com/aws/en/getting-started/free-edition-limitations).
4. Wagner et al., [PTB-XL, a large publicly available electrocardiography dataset](https://physionet.org/content/ptb-xl/1.0.3/), PhysioNet.
5. Pimentel et al., [BIDMC PPG and Respiration Dataset](https://physionet.org/content/bidmc/1.0.0/), PhysioNet.
6. [ICBHI 2017 Respiratory Sound Database](https://bhichallenge.med.auth.gr/), official challenge site.
7. [RSNA Pneumonia Detection Challenge dataset](https://www.kaggle.com/competitions/rsna-pneumonia-detection-challenge/data) and [competition rules](https://www.kaggle.com/competitions/rsna-pneumonia-detection-challenge/rules), Kaggle.

---

**Document maintenance:** Update this working baseline when dataset versions, schema, thresholds, workflow behavior, evaluation results, or architecture decisions change. Keep experimental results separate from planned methods until a run has produced evidence.
