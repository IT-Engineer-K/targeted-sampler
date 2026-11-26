"""Pure, deterministic capture and eviction decision logic."""

from __future__ import annotations

from typing import Sequence

from .config import CaptureConfig, TargetRule
from .models import BucketKey, CaptureDecision, SampleEvent, SampleRecord, SamplerState


def _match_best_target(
    bucket: BucketKey, targets: Sequence[TargetRule]
) -> TargetRule | None:
    matched = [rule for rule in targets if rule.matches(bucket)]
    if not matched:
        return None
    # Higher priority wins; ties broken by declaration order.
    matched.sort(key=lambda r: -r.priority)
    return matched[0]


def _effective_bucket_limit(
    bucket: BucketKey, config: CaptureConfig, target: TargetRule | None
) -> int:
    if target is not None and target.bucket_limit is not None:
        return target.bucket_limit
    return config.default_limit_for(bucket)


def _is_targeted(bucket: BucketKey, targets: Sequence[TargetRule]) -> bool:
    return _match_best_target(bucket, targets) is not None


def should_capture(
    event: SampleEvent,
    state: SamplerState,
    config: CaptureConfig | None = None,
    targets: Sequence[TargetRule] = (),
) -> CaptureDecision:
    """Decide whether to capture ``event`` and which unpinned records to evict.

    This function is completely pure (no filesystem, clock, or network side effects).
    """
    cfg = config or CaptureConfig()
    target = _match_best_target(event.bucket, targets)
    limit = _effective_bucket_limit(event.bucket, cfg, target)
    target_name = target.name if target is not None else None

    if event.size_bytes < 0:
        return CaptureDecision(
            capture=False,
            reason="invalid_size_bytes",
            effective_bucket_limit=limit,
            matched_target=target_name,
        )

    if event.size_bytes > cfg.max_total_bytes:
        return CaptureDecision(
            capture=False,
            reason="single_sample_exceeds_disk_budget",
            effective_bucket_limit=limit,
            matched_target=target_name,
        )

    # Deduplicate by SHA-256 fingerprint
    if event.fingerprint in state.fingerprints:
        return CaptureDecision(
            capture=False,
            reason="duplicate_fingerprint",
            effective_bucket_limit=limit,
            matched_target=target_name,
        )

    # Optional single-blocker-only mode unless overridden by target force_capture
    if cfg.single_blocker_only and not event.bucket.single_blocker:
        if target is None or not target.force_capture:
            return CaptureDecision(
                capture=False,
                reason="non_single_blocker_filtered",
                effective_bucket_limit=limit,
                matched_target=target_name,
            )

    if limit == 0 and (target is None or not target.force_capture):
        return CaptureDecision(
            capture=False,
            reason="bucket_limit_zero",
            effective_bucket_limit=limit,
            matched_target=target_name,
        )

    evict_ids: list[str] = []
    evicted_set: set[str] = set()

    # Step 1: Per-bucket ring buffer cap (only unpinned records count toward eviction;
    # pinned records are NEVER evicted).
    bucket_records = state.records_in_bucket(event.bucket)
    unpinned_in_bucket = sorted(
        (r for r in bucket_records if not state.is_pinned(r)),
        key=lambda r: (r.timestamp, r.sample_id),
    )

    if limit > 0 and len(unpinned_in_bucket) >= limit:
        # Ring buffer: make room for the incoming newest sample by evicting oldest unpinned
        excess = len(unpinned_in_bucket) - limit + 1
        for victim in unpinned_in_bucket[:excess]:
            evict_ids.append(victim.sample_id)
            evicted_set.add(victim.sample_id)

    # Step 2: Enforce global disk capacity cap (never evicting pinned samples).
    remaining_records = [r for r in state.records if r.sample_id not in evicted_set]
    projected_bytes = sum(r.size_bytes for r in remaining_records) + event.size_bytes

    if projected_bytes > cfg.max_total_bytes:
        candidates = [r for r in remaining_records if not state.is_pinned(r)]
        # Eviction order:
        # 1. Non-targeted, non-single-blocker oldest first
        # 2. Non-targeted, single-blocker oldest first
        # 3. Targeted oldest first (never evicting from same bucket unless necessary)
        candidates.sort(
            key=lambda r: (
                1 if _is_targeted(r.bucket, targets) else 0,
                1 if r.bucket.single_blocker else 0,
                1 if r.bucket == event.bucket else 0,
                r.timestamp,
                r.sample_id,
            )
        )
        for victim in candidates:
            if projected_bytes <= cfg.max_total_bytes:
                break
            evict_ids.append(victim.sample_id)
            evicted_set.add(victim.sample_id)
            projected_bytes -= victim.size_bytes

    if projected_bytes > cfg.max_total_bytes:
        return CaptureDecision(
            capture=False,
            reason="disk_budget_exhausted_by_pinned",
            evict_ids=(),
            effective_bucket_limit=limit,
            matched_target=target_name,
        )

    if target is not None and target.force_capture:
        reason = "capture_target_forced"
    elif target is not None:
        reason = "capture_target_priority"
    elif evict_ids:
        reason = "capture_with_rotation"
    else:
        reason = "capture_within_bucket_limit"

    return CaptureDecision(
        capture=True,
        reason=reason,
        evict_ids=tuple(evict_ids),
        effective_bucket_limit=limit,
        matched_target=target_name,
    )


def plan_prune(
    state: SamplerState,
    config: CaptureConfig | None = None,
    targets: Sequence[TargetRule] = (),
) -> tuple[str, ...]:
    """Compute which unpinned sample IDs should be evicted to satisfy current caps."""
    cfg = config or CaptureConfig()
    evict_ids: list[str] = []
    evicted_set: set[str] = set()

    # Per-bucket caps
    buckets: dict[BucketKey, list[SampleRecord]] = {}
    for rec in state.records:
        buckets.setdefault(rec.bucket, []).append(rec)

    for bucket, records in buckets.items():
        target = _match_best_target(bucket, targets)
        limit = _effective_bucket_limit(bucket, cfg, target)
        unpinned = sorted(
            (r for r in records if not state.is_pinned(r)),
            key=lambda r: (r.timestamp, r.sample_id),
        )
        if len(unpinned) > limit:
            for victim in unpinned[: len(unpinned) - limit]:
                evict_ids.append(victim.sample_id)
                evicted_set.add(victim.sample_id)

    # Global disk budget
    remaining = [r for r in state.records if r.sample_id not in evicted_set]
    total_bytes = sum(r.size_bytes for r in remaining)
    if total_bytes > cfg.max_total_bytes:
        candidates = [r for r in remaining if not state.is_pinned(r)]
        candidates.sort(
            key=lambda r: (
                1 if _is_targeted(r.bucket, targets) else 0,
                1 if r.bucket.single_blocker else 0,
                r.timestamp,
                r.sample_id,
            )
        )
        for victim in candidates:
            if total_bytes <= cfg.max_total_bytes:
                break
            evict_ids.append(victim.sample_id)
            evicted_set.add(victim.sample_id)
            total_bytes -= victim.size_bytes

    return tuple(evict_ids)
