"""Small deterministic helpers for replay-safe batch manifests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Mapping


@dataclass(frozen=True, slots=True)
class Fingerprint:
    source_dataset: str
    source_record_key: str
    file_checksum: str

    @property
    def record_key(self) -> tuple[str, str]:
        return self.source_dataset, self.source_record_key

    @property
    def exact_key(self) -> tuple[str, str, str]:
        return self.source_dataset, self.source_record_key, self.file_checksum.lower()


@dataclass(frozen=True, slots=True)
class BatchClassification:
    new_records: tuple[Fingerprint, ...]
    exact_replays: tuple[Fingerprint, ...]
    changed_payloads: tuple[Fingerprint, ...]


@dataclass(frozen=True, slots=True)
class BatchReconciliation:
    attempted: int
    accepted: int
    quarantined: int
    skipped_duplicates: int

    @property
    def accounted_for(self) -> int:
        return self.accepted + self.quarantined + self.skipped_duplicates

    @property
    def balanced(self) -> bool:
        return self.attempted == self.accounted_for

    def validate(self) -> None:
        values = (self.attempted, self.accepted, self.quarantined, self.skipped_duplicates)
        if any(value < 0 for value in values):
            raise ValueError("reconciliation counts must be non-negative")
        if not self.balanced:
            raise ValueError(
                "batch counts do not reconcile: "
                f"attempted={self.attempted}, accepted={self.accepted}, "
                f"quarantined={self.quarantined}, skipped_duplicates={self.skipped_duplicates}"
            )


def classify_batch(
    incoming: Iterable[Fingerprint],
    existing: Iterable[Fingerprint],
) -> BatchClassification:
    """Classify exact replays, changed payloads, and unseen source records.

    A changed checksum for the same dataset and source key is surfaced as a
    conflict for review. It is never silently overwritten or treated as a
    second valid version inside a locked dataset release.
    """
    existing_rows = tuple(existing)
    exact_existing = {item.exact_key for item in existing_rows}
    checksums_by_record: dict[tuple[str, str], set[str]] = {}
    for item in existing_rows:
        checksums_by_record.setdefault(item.record_key, set()).add(item.file_checksum.lower())

    new_records: list[Fingerprint] = []
    exact_replays: list[Fingerprint] = []
    changed_payloads: list[Fingerprint] = []
    seen_incoming: set[tuple[str, str, str]] = set()

    for item in incoming:
        if item.exact_key in seen_incoming or item.exact_key in exact_existing:
            exact_replays.append(item)
        elif checksums_by_record.get(item.record_key):
            changed_payloads.append(item)
        else:
            new_records.append(item)
            checksums_by_record[item.record_key] = {item.file_checksum.lower()}
        seen_incoming.add(item.exact_key)

    return BatchClassification(tuple(new_records), tuple(exact_replays), tuple(changed_payloads))
