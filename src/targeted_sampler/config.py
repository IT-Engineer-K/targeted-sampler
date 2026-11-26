"""Configuration and dynamic target-rule loading for targeted-sampler."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from pathlib import Path
from typing import Any

from .models import BucketKey

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CaptureConfig:
    """Global sampling and disk-budget settings."""

    bucket_limit: int = 15
    single_blocker_bucket_limit: int | None = None
    max_total_bytes: int = 200 * 1024 * 1024  # 200 MB
    single_blocker_only: bool = False

    def __post_init__(self) -> None:
        if self.bucket_limit < 0:
            raise ValueError("bucket_limit must be >= 0")
        if self.single_blocker_bucket_limit is not None and self.single_blocker_bucket_limit < 0:
            raise ValueError("single_blocker_bucket_limit must be >= 0")
        if self.max_total_bytes <= 0:
            raise ValueError("max_total_bytes must be > 0")

    def default_limit_for(self, bucket: BucketKey) -> int:
        if bucket.single_blocker and self.single_blocker_bucket_limit is not None:
            return self.single_blocker_bucket_limit
        return self.bucket_limit


@dataclass(frozen=True)
class TargetRule:
    """Dynamic priority/force-capture override for matching buckets.

    Any pattern field set to ``"*"`` or ``None`` acts as a wildcard.
    """

    name: str
    kind: str | None = None
    single_blocker: bool | None = None
    primary_key: str | None = None
    reason_code: str | None = None
    bucket_limit: int | None = None
    force_capture: bool = False
    priority: int = 100
    enabled: bool = True

    def matches(self, bucket: BucketKey) -> bool:
        if not self.enabled:
            return False
        if self.kind not in (None, "*") and self.kind != bucket.kind:
            return False
        if self.single_blocker is not None and self.single_blocker != bucket.single_blocker:
            return False
        if self.primary_key not in (None, "*") and self.primary_key != bucket.primary_key:
            return False
        if self.reason_code not in (None, "*") and self.reason_code != bucket.reason_code:
            return False
        return True

    @classmethod
    def from_dict(cls, payload: dict[str, Any], index: int = 0) -> TargetRule:
        bucket_limit_raw = payload.get("bucket_limit")
        bucket_limit = int(bucket_limit_raw) if bucket_limit_raw is not None else None
        if bucket_limit is not None and bucket_limit < 0:
            raise ValueError("TargetRule.bucket_limit must be >= 0")
        sb = payload.get("single_blocker")
        if sb is not None and not isinstance(sb, bool):
            raise ValueError("TargetRule.single_blocker must be bool or null")
        return cls(
            name=str(payload.get("name") or f"target-{index}"),
            kind=payload.get("kind"),
            single_blocker=sb,
            primary_key=payload.get("primary_key"),
            reason_code=payload.get("reason_code"),
            bucket_limit=bucket_limit,
            force_capture=bool(payload.get("force_capture", False)),
            priority=int(payload.get("priority", 100)),
            enabled=bool(payload.get("enabled", True)),
        )


def load_targets(path: str | Path | None) -> tuple[TargetRule, ...]:
    """Load dynamic capture targets from a JSON file.

    Returns an empty tuple on missing or corrupt files (fail-safe).
    Supported schemas:
      - ``{"targets": [{...}, ...]}``
      - ``[{...}, ...]``
    """
    if path is None:
        return ()
    target_path = Path(path)
    if not target_path.exists():
        return ()
    try:
        raw = json.loads(target_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load capture targets from %s: %s", target_path, exc)
        return ()

    items = raw.get("targets") if isinstance(raw, dict) else raw
    if not isinstance(items, list):
        logger.warning("Invalid capture targets format in %s: expected list", target_path)
        return ()

    rules: list[TargetRule] = []
    for idx, entry in enumerate(items):
        if not isinstance(entry, dict):
            logger.warning("Skipping non-object target rule at index %d in %s", idx, target_path)
            continue
        try:
            rules.append(TargetRule.from_dict(entry, index=idx))
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping invalid target rule at index %d in %s: %s", idx, target_path, exc)
    return tuple(rules)


def load_pinned_ids(path: str | Path | None) -> tuple[frozenset[str], frozenset[str]]:
    """Load pinned fingerprints and sample IDs from an external JSON file.

    Supports flexible schemas so upstream label/regression registries can be
    plugged in without coupling:
      - ``{"pinned_fingerprints": [...], "pinned_sample_ids": [...]}``
      - ``{"labels": [{"sample_id": ..., "fingerprint": ...}, ...]}``
      - ``[{"sample_id": ..., "fingerprint": ...}, ...]``
    Returns ``(pinned_fingerprints, pinned_sample_ids)``; fails safe to empty sets.
    """
    if path is None:
        return (frozenset(), frozenset())
    pin_path = Path(path)
    if not pin_path.exists():
        return (frozenset(), frozenset())
    try:
        raw = json.loads(pin_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        logger.warning("Failed to load pinned IDs from %s: %s", pin_path, exc)
        return (frozenset(), frozenset())

    fingerprints: set[str] = set()
    sample_ids: set[str] = set()

    if isinstance(raw, dict):
        for fp in raw.get("pinned_fingerprints", []):
            if isinstance(fp, str) and fp:
                fingerprints.add(fp)
        for sid in raw.get("pinned_sample_ids", []):
            if isinstance(sid, str) and sid:
                sample_ids.add(sid)
        entries = raw.get("labels") or raw.get("items") or raw.get("records")
        if isinstance(entries, list):
            _collect_from_entries(entries, fingerprints, sample_ids)
        elif isinstance(entries, dict):
            for key, val in entries.items():
                if isinstance(key, str) and key:
                    sample_ids.add(key)
                if isinstance(val, dict):
                    _collect_entry(val, fingerprints, sample_ids)
    elif isinstance(raw, list):
        _collect_from_entries(raw, fingerprints, sample_ids)

    return (frozenset(fingerprints), frozenset(sample_ids))


def _collect_from_entries(
    entries: list[Any], fingerprints: set[str], sample_ids: set[str]
) -> None:
    for item in entries:
        if isinstance(item, dict):
            _collect_entry(item, fingerprints, sample_ids)
        elif isinstance(item, str) and item:
            sample_ids.add(item)


def _collect_entry(
    item: dict[str, Any], fingerprints: set[str], sample_ids: set[str]
) -> None:
    fp = item.get("fingerprint") or item.get("sha256")
    if isinstance(fp, str) and fp:
        fingerprints.add(fp)
    sid = item.get("sample_id") or item.get("id")
    if isinstance(sid, str) and sid:
        sample_ids.add(sid)
