# Requirements Specification: `targeted-sampler`

## 1. Purpose

Provide a reusable, domain-agnostic stratified sample collection module that eliminates artifact starvation caused by global first-come-first-served caps, while strictly protecting pinned/audited samples and bounding total disk usage.

## 2. Core Data Model

- **BucketKey**: `(kind: str, single_blocker: bool, primary_key: str, reason_code: str)`
  - Canonical string format: `{kind}|{"single"|"multi"}|{primary_key}|{reason_code}`
- **SampleEvent**: incoming candidate event with `sample_id`, `bucket`, `fingerprint` (SHA-256 hex), `size_bytes`, `timestamp`, and optional `metadata`.
- **TargetRule**: dynamic per-bucket or wildcard override (`bucket_limit`, `priority`, `force_capture`, `enabled`).
- **CaptureDecision**: pure decision output containing `capture: bool`, `reason: str`, `evict_ids: tuple[str, ...]`, `effective_bucket_limit: int`, and `matched_target: str | None`.

## 3. Functional Requirements (MVP)

1. **Per-bucket ring buffer**: Retain up to `bucket_limit` newest unpinned samples per `BucketKey`. Exceeding a global historical event count (e.g., >200 events) must not prevent new or targeted buckets from capturing up to their respective limits.
2. **Dynamic target configuration**: Load optional JSON target overrides (`capture_targets.json`) allowing higher quotas or forced capture for active investigation keys. Missing or corrupt target files must fail safe to defaults.
3. **Global disk-budget rotation**: Enforce `max_total_bytes` by evicting oldest unpinned samples (prioritizing non-targeted over-cap buckets, then oldest unpinned non-targeted samples, then oldest unpinned samples). If `max_total_bytes` cannot be met without evicting pinned samples, skip capture with reason `disk_budget_exhausted_by_pinned`.
4. **Pinning protection**: Fingerprints or sample IDs registered as pinned (e.g., audited/labeled or referenced by regression tests) must never be evicted under any bucket-cap or disk-rotation scenario.
5. **SHA-256 deduplication**: Reject duplicate events whose `fingerprint` already exists in the store (`reason="duplicate_fingerprint"`).
6. **Pure decision API**: `should_capture(event, state, config, targets) -> CaptureDecision` must have zero side effects and be 100% deterministic.
7. **Corrupt metadata resilience**: Corrupt JSON metadata files on disk must be quarantined/ignored with structured warnings without crashing store initialization or capture.
8. **Zero external I/O & non-blocking worker safety**: No network calls; `safe_record` catches storage I/O exceptions and returns a structured skip outcome.

## 4. Non-Goals

- Does not modify upstream decision engines, submission logic, or external databases.
- Does not store or transmit secrets, credentials, or domain-specific business rules.

## 5. Acceptance Criteria

1. After more than 200 events across earlier buckets, new reason buckets and targeted buckets continue to capture up to their bucket limits.
2. Under aggressive rotation and tight disk caps, zero pinned/labeled samples are ever deleted.
3. Boundary tests pass for disk-capacity exhaustion, corrupt metadata files, duplicate SHA-256 fingerprints, and I/O failure isolation.
