"""Core data models for stratified reason-bucketed sample collection."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
from typing import Any


def utc_now_iso() -> str:
    """Return current UTC timestamp in ISO-8601 format."""
    return datetime.now(timezone.utc).isoformat()


def compute_fingerprint(data: bytes) -> str:
    """Compute a canonical SHA-256 hex digest for deduplication."""
    return hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class BucketKey:
    """Stratification key for grouping samples by document type and blocker reason."""

    kind: str
    single_blocker: bool
    primary_key: str
    reason_code: str

    def canonical(self) -> str:
        """Return a stable string representation."""
        scope = "single" if self.single_blocker else "multi"
        return f"{self.kind}|{scope}|{self.primary_key}|{self.reason_code}"

    def to_dict(self) -> dict[str, Any]:
        return {
            "kind": self.kind,
            "single_blocker": self.single_blocker,
            "primary_key": self.primary_key,
            "reason_code": self.reason_code,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> BucketKey:
        return cls(
            kind=str(payload["kind"]),
            single_blocker=bool(payload["single_blocker"]),
            primary_key=str(payload["primary_key"]),
            reason_code=str(payload["reason_code"]),
        )


@dataclass(frozen=True)
class SampleEvent:
    """Incoming candidate sample to evaluate for capture."""

    sample_id: str
    bucket: BucketKey
    fingerprint: str
    size_bytes: int
    timestamp: str = field(default_factory=utc_now_iso)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class SampleRecord:
    """Stored sample metadata tracked by SamplerState and SampleStore."""

    sample_id: str
    bucket: BucketKey
    fingerprint: str
    size_bytes: int
    timestamp: str
    pinned: bool = False
    artifact_relpath: str | None = None
    layout_relpath: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "sample_id": self.sample_id,
            "bucket": self.bucket.to_dict(),
            "fingerprint": self.fingerprint,
            "size_bytes": self.size_bytes,
            "timestamp": self.timestamp,
            "pinned": self.pinned,
            "artifact_relpath": self.artifact_relpath,
            "layout_relpath": self.layout_relpath,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> SampleRecord:
        size_bytes = int(payload["size_bytes"])
        if size_bytes < 0:
            raise ValueError("size_bytes must be non-negative")
        return cls(
            sample_id=str(payload["sample_id"]),
            bucket=BucketKey.from_dict(payload["bucket"]),
            fingerprint=str(payload["fingerprint"]),
            size_bytes=size_bytes,
            timestamp=str(payload["timestamp"]),
            pinned=bool(payload.get("pinned", False)),
            artifact_relpath=payload.get("artifact_relpath"),
            layout_relpath=payload.get("layout_relpath"),
            metadata=dict(payload.get("metadata") or {}),
        )


@dataclass(frozen=True)
class SamplerState:
    """Immutable snapshot of currently stored samples and pinned identifiers."""

    records: tuple[SampleRecord, ...] = ()
    pinned_fingerprints: frozenset[str] = frozenset()
    pinned_sample_ids: frozenset[str] = frozenset()

    def is_pinned(self, record: SampleRecord) -> bool:
        """Return True if a record is protected from eviction."""
        return (
            record.pinned
            or record.fingerprint in self.pinned_fingerprints
            or record.sample_id in self.pinned_sample_ids
        )

    @property
    def total_bytes(self) -> int:
        return sum(r.size_bytes for r in self.records)

    @property
    def fingerprints(self) -> frozenset[str]:
        return frozenset(r.fingerprint for r in self.records)

    def records_in_bucket(self, bucket: BucketKey) -> list[SampleRecord]:
        return [r for r in self.records if r.bucket == bucket]


@dataclass(frozen=True)
class CaptureDecision:
    """Pure, deterministic decision returned by should_capture()."""

    capture: bool
    reason: str
    evict_ids: tuple[str, ...] = ()
    effective_bucket_limit: int = 0
    matched_target: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "capture": self.capture,
            "reason": self.reason,
            "evict_ids": list(self.evict_ids),
            "effective_bucket_limit": self.effective_bucket_limit,
            "matched_target": self.matched_target,
        }


@dataclass(frozen=True)
class RecordOutcome:
    """Result of SampleStore.safe_record() including non-fatal warnings."""

    decision: CaptureDecision
    stored: bool
    evicted_ids: tuple[str, ...] = ()
    warning: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "decision": self.decision.to_dict(),
            "stored": self.stored,
            "evicted_ids": list(self.evicted_ids),
            "warning": self.warning,
        }
