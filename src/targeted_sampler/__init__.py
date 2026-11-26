"""Stratified, reason-bucketed sample collector with pinning and disk-budget rotation."""

from .config import CaptureConfig, TargetRule, load_targets, load_pinned_ids
from .decision import should_capture
from .models import (
    BucketKey,
    CaptureDecision,
    RecordOutcome,
    SampleEvent,
    SampleRecord,
    SamplerState,
    compute_fingerprint,
)
from .store import SampleStore

__all__ = [
    "BucketKey",
    "CaptureConfig",
    "CaptureDecision",
    "RecordOutcome",
    "SampleEvent",
    "SampleRecord",
    "SampleStore",
    "SamplerState",
    "TargetRule",
    "compute_fingerprint",
    "load_pinned_ids",
    "load_targets",
    "should_capture",
]

__version__ = "0.1.0"
