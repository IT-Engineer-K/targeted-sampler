# targeted-sampler

A lightweight, deterministic, zero-dependency Python library and CLI for **stratified, reason-bucketed sample collection** in document processing and automated review pipelines.

## Why?

Simple FIFO / global-cap sample collectors stop recording once an early limit (e.g. 200 items) is reached, starving newly emerging error reasons or high-priority single-blocker cases of real artifacts needed for evaluation and regression testing.

`targeted-sampler` solves this with:

- **Per-bucket ring buffers** keyed by `(kind, single_blocker, primary_key, reason_code)`
- **Dynamic target overrides** (`capture_targets.json`) for priority quotas and forced capture
- **Pinning protection** so audited/labeled or regression-referenced samples are never evicted
- **SHA-256 fingerprint deduplication**
- **Global disk-budget rotation** that evicts oldest unpinned samples safely
- **Pure decision API** (`should_capture(event, state, config, targets) -> CaptureDecision`) for deterministic unit testing
- **Fail-safe storage wrapper** (`SampleStore.safe_record(...)`) that never crashes caller workers on I/O errors or disk limits

## Quick Start

```python
from targeted_sampler import (
    BucketKey,
    CaptureConfig,
    SampleEvent,
    SampleStore,
    compute_fingerprint,
)

store = SampleStore(root_dir="./samples", config=CaptureConfig(bucket_limit=15, max_total_bytes=50 * 1024 * 1024))
payload = b"synthetic-image-bytes"

event = SampleEvent(
    sample_id="doc-001",
    bucket=BucketKey(
        kind="receipt",
        single_blocker=True,
        primary_key="items",
        reason_code="sum_mismatch",
    ),
    fingerprint=compute_fingerprint(payload),
    size_bytes=len(payload),
)

outcome = store.safe_record(event, artifact_bytes=payload, layout={"lines": []})
print(outcome.decision.capture, outcome.decision.reason)
```

## CLI Usage

```bash
# Inspect bucket distribution and disk usage
targeted-sampler stats --root ./samples --format table
targeted-sampler stats --root ./samples --format json

# Verify metadata integrity and report corrupt entries
targeted-sampler verify --root ./samples --format json

# Enforce disk/bucket caps without touching pinned samples
targeted-sampler prune --root ./samples --max-mb 100 --format json
```

## Running Tests

```bash
python -m unittest discover -s tests -v
```

## Security & Privacy Boundaries

- **Zero network access**: operates strictly on local storage.
- **No domain-specific coupling**: business identifiers, pricing, and external labels are injected at runtime via local configuration outside version control.
- **Safe failure**: storage errors and corrupt metadata files are isolated and logged without halting caller pipelines.
