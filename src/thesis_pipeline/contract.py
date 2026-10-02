"""Shared Bronze manifest contract and deterministic record identifiers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime
from enum import StrEnum
import hashlib
import json
import re
from typing import Any, Mapping


class Modality(StrEnum):
    PHYSIOLOGICAL_SIGNALS = "physiological_signals"
    ECG = "ecg"
    PPG = "ppg"
    RESPIRATION = "respiration"
    RESPIRATORY_AUDIO = "respiratory_audio"
    CHEST_XRAY = "chest_xray"


@dataclass(frozen=True, slots=True)
class BronzeRecord:
    record_id: str
    modality: str
    source_dataset: str
    source_version: str
    source_record_key: str
    raw_uri: str
    file_checksum: str
    file_format: str
    ingestion_run_id: str
    ingested_at: str
    schema_version: str = "1.0.0"
    pipeline_version: str = "0.1.0"
    source_metadata: Mapping[str, Any] = field(default_factory=dict)
    subject_id_hash: str | None = None
    event_timestamp: str | None = None
    time_precision: str | None = None
    license_reference: str | None = None
    deidentification_status: str = "unknown"

    def to_dict(self) -> dict[str, Any]:
        """Return a serialization-ready copy with metadata materialized."""
        result = asdict(self)
        result["source_metadata"] = dict(self.source_metadata)
        return result


_SHA256_RE = re.compile(r"^[a-fA-F0-9]{64}$")
_REQUIRED_TEXT_FIELDS = (
    "record_id",
    "source_dataset",
    "source_version",
    "source_record_key",
    "raw_uri",
    "file_format",
    "ingestion_run_id",
    "schema_version",
    "pipeline_version",
)
_ALLOWED_DEIDENTIFICATION = {"source_deidentified", "pseudonymized", "unknown"}


def make_record_id(source_dataset: str, source_version: str, source_record_key: str) -> str:
    """Create a stable ID from a dataset release and its source record key."""
    parts = (source_dataset.strip(), source_version.strip(), source_record_key.strip())
    if any(not part for part in parts):
        raise ValueError("source_dataset, source_version, and source_record_key must be non-empty")
    stable_key = "\x1f".join(parts).encode("utf-8")
    return hashlib.sha256(stable_key).hexdigest()[:32]


def _valid_datetime(value: str, field_name: str, errors: list[str], required: bool) -> None:
    if not value:
        if required:
            errors.append(f"{field_name} is required")
        return
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{field_name} must be ISO-8601")
        return
    if parsed.tzinfo is None:
        errors.append(f"{field_name} must include a timezone")


def validate_bronze_record(record: BronzeRecord | Mapping[str, Any]) -> list[str]:
    """Return all contract violations; an empty list means the record is valid."""
    data = record.to_dict() if isinstance(record, BronzeRecord) else dict(record)
    errors: list[str] = []

    for name in _REQUIRED_TEXT_FIELDS:
        if not isinstance(data.get(name), str) or not data[name].strip():
            errors.append(f"{name} must be a non-empty string")

    if data.get("modality") not in {item.value for item in Modality}:
        errors.append("modality must be one of: " + ", ".join(item.value for item in Modality))
    if not isinstance(data.get("file_checksum"), str) or not _SHA256_RE.fullmatch(data["file_checksum"]):
        errors.append("file_checksum must be a 64-character SHA-256 hex digest")
    if not isinstance(data.get("source_metadata"), Mapping):
        errors.append("source_metadata must be an object/mapping")
    else:
        try:
            json.dumps(data["source_metadata"], allow_nan=False)
        except (TypeError, ValueError):
            errors.append("source_metadata must contain JSON-serializable finite values")

    _valid_datetime(data.get("ingested_at", ""), "ingested_at", errors, required=True)
    if data.get("event_timestamp") is not None:
        _valid_datetime(data["event_timestamp"], "event_timestamp", errors, required=True)

    if data.get("deidentification_status") not in _ALLOWED_DEIDENTIFICATION:
        errors.append("deidentification_status must be source_deidentified, pseudonymized, or unknown")
    return errors
