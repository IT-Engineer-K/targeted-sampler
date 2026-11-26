"""Unit tests for pure capture and eviction decision logic."""

from __future__ import annotations

import unittest

from targeted_sampler import (
    BucketKey,
    CaptureConfig,
    SampleEvent,
    SampleRecord,
    SamplerState,
    TargetRule,
    compute_fingerprint,
    should_capture,
)
from targeted_sampler.decision import plan_prune


def _make_record(
    idx: int,
    bucket: BucketKey,
    size_bytes: int = 100,
    pinned: bool = False,
) -> SampleRecord:
    ts = f"2026-11-26T00:{idx // 60:02d}:{idx % 60:02d}+00:00"
    return SampleRecord(
        sample_id=f"rec-{idx:04d}",
        bucket=bucket,
        fingerprint=compute_fingerprint(f"payload-{idx}".encode("utf-8")),
        size_bytes=size_bytes,
        timestamp=ts,
        pinned=pinned,
    )


class TestShouldCapture(unittest.TestCase):
    def test_captures_new_and_targeted_buckets_after_200_earlier_events(self) -> None:
        """Acceptance criterion 1: >200 earlier events never starve new or targeted buckets."""
        cfg = CaptureConfig(bucket_limit=15, max_total_bytes=10 * 1024 * 1024)
        state = SamplerState()

        # Simulate 250 events across 5 early buckets (50 each -> capped at 15 per bucket)
        for i in range(250):
            bucket = BucketKey("receipt", True, "total", f"early_reason_{i % 5}")
            payload = f"early-{i}".encode("utf-8")
            event = SampleEvent(
                sample_id=f"early-{i:04d}",
                bucket=bucket,
                fingerprint=compute_fingerprint(payload),
                size_bytes=1024,
                timestamp=f"2026-11-26T01:{i // 60:02d}:{i % 60:02d}+00:00",
            )
            decision = should_capture(event, state, config=cfg)
            self.assertTrue(decision.capture)
            evict_set = set(decision.evict_ids)
            next_records = [r for r in state.records if r.sample_id not in evict_set]
            next_records.append(
                SampleRecord(
                    sample_id=event.sample_id,
                    bucket=event.bucket,
                    fingerprint=event.fingerprint,
                    size_bytes=event.size_bytes,
                    timestamp=event.timestamp,
                )
            )
            state = SamplerState(records=tuple(next_records))

        # Each of the 5 early buckets is bounded at 15 (75 total, not 200 global freeze)
        self.assertEqual(len(state.records), 75)

        # Now a brand-new reason bucket and a targeted bucket must both capture up to their limits
        new_bucket = BucketKey("receipt", True, "items", "sum_mismatch")
        target_bucket = BucketKey("invoice", True, "tax", "rate_conflict")
        targets = (
            TargetRule(
                name="investigate-tax",
                kind="invoice",
                primary_key="tax",
                reason_code="rate_conflict",
                bucket_limit=20,
                force_capture=True,
            ),
        )

        for j in range(15):
            ev = SampleEvent(
                sample_id=f"new-{j:03d}",
                bucket=new_bucket,
                fingerprint=compute_fingerprint(f"new-{j}".encode("utf-8")),
                size_bytes=1024,
                timestamp=f"2026-11-26T05:00:{j:02d}+00:00",
            )
            dec = should_capture(ev, state, config=cfg, targets=targets)
            self.assertTrue(dec.capture)
            self.assertEqual(dec.evict_ids, ())
            state = SamplerState(
                records=state.records
                + (
                    SampleRecord(
                        sample_id=ev.sample_id,
                        bucket=ev.bucket,
                        fingerprint=ev.fingerprint,
                        size_bytes=ev.size_bytes,
                        timestamp=ev.timestamp,
                    ),
                )
            )

        for k in range(20):
            ev = SampleEvent(
                sample_id=f"tgt-{k:03d}",
                bucket=target_bucket,
                fingerprint=compute_fingerprint(f"tgt-{k}".encode("utf-8")),
                size_bytes=1024,
                timestamp=f"2026-11-26T06:00:{k:02d}+00:00",
            )
            dec = should_capture(ev, state, config=cfg, targets=targets)
            self.assertTrue(dec.capture)
            self.assertEqual(dec.matched_target, "investigate-tax")
            self.assertEqual(dec.effective_bucket_limit, 20)
            state = SamplerState(
                records=state.records
                + (
                    SampleRecord(
                        sample_id=ev.sample_id,
                        bucket=ev.bucket,
                        fingerprint=ev.fingerprint,
                        size_bytes=ev.size_bytes,
                        timestamp=ev.timestamp,
                    ),
                )
            )

        self.assertEqual(len(state.records_in_bucket(new_bucket)), 15)
        self.assertEqual(len(state.records_in_bucket(target_bucket)), 20)

    def test_pinned_samples_are_never_evicted_under_bucket_or_disk_rotation(self) -> None:
        """Acceptance criterion 2: zero pinned/labeled samples evicted during rotation."""
        bucket = BucketKey("receipt", True, "items", "missing_line")
        pinned_rec_flag = _make_record(0, bucket, size_bytes=400, pinned=True)
        pinned_rec_fp = _make_record(1, bucket, size_bytes=400, pinned=False)
        pinned_rec_id = _make_record(2, bucket, size_bytes=400, pinned=False)
        unpinned_1 = _make_record(3, bucket, size_bytes=400, pinned=False)
        unpinned_2 = _make_record(4, bucket, size_bytes=400, pinned=False)

        state = SamplerState(
            records=(pinned_rec_flag, pinned_rec_fp, pinned_rec_id, unpinned_1, unpinned_2),
            pinned_fingerprints=frozenset({pinned_rec_fp.fingerprint}),
            pinned_sample_ids=frozenset({pinned_rec_id.sample_id}),
        )
        cfg = CaptureConfig(bucket_limit=2, max_total_bytes=1700)

        incoming = SampleEvent(
            sample_id="incoming-001",
            bucket=bucket,
            fingerprint=compute_fingerprint(b"incoming-payload"),
            size_bytes=400,
            timestamp="2026-11-26T09:00:00+00:00",
        )
        decision = should_capture(incoming, state, config=cfg)
        self.assertTrue(decision.capture)
        # Must evict BOTH unpinned records to fit 1200 (pinned) + 400 (incoming) <= 1700
        self.assertEqual(set(decision.evict_ids), {unpinned_1.sample_id, unpinned_2.sample_id})
        for protected_id in (pinned_rec_flag.sample_id, pinned_rec_fp.sample_id, pinned_rec_id.sample_id):
            self.assertNotIn(protected_id, decision.evict_ids)

    def test_disk_budget_exhausted_by_pinned_skips_safely(self) -> None:
        bucket = BucketKey("receipt", True, "total", "ambiguous")
        pinned = _make_record(0, bucket, size_bytes=900, pinned=True)
        state = SamplerState(records=(pinned,))
        cfg = CaptureConfig(bucket_limit=10, max_total_bytes=1000)

        incoming = SampleEvent(
            sample_id="too-tight",
            bucket=bucket,
            fingerprint=compute_fingerprint(b"new-200"),
            size_bytes=200,
        )
        decision = should_capture(incoming, state, config=cfg)
        self.assertFalse(decision.capture)
        self.assertEqual(decision.reason, "disk_budget_exhausted_by_pinned")
        self.assertEqual(decision.evict_ids, ())

    def test_duplicate_fingerprint_is_rejected(self) -> None:
        bucket = BucketKey("receipt", True, "date", "format_error")
        existing = _make_record(0, bucket, size_bytes=128)
        state = SamplerState(records=(existing,))

        dup_event = SampleEvent(
            sample_id="different-id",
            bucket=bucket,
            fingerprint=existing.fingerprint,
            size_bytes=128,
        )
        decision = should_capture(dup_event, state)
        self.assertFalse(decision.capture)
        self.assertEqual(decision.reason, "duplicate_fingerprint")

    def test_single_sample_exceeding_max_budget_is_rejected(self) -> None:
        cfg = CaptureConfig(bucket_limit=5, max_total_bytes=500)
        event = SampleEvent(
            sample_id="huge",
            bucket=BucketKey("invoice", True, "total", "overflow"),
            fingerprint=compute_fingerprint(b"huge"),
            size_bytes=501,
        )
        decision = should_capture(event, SamplerState(), config=cfg)
        self.assertFalse(decision.capture)
        self.assertEqual(decision.reason, "single_sample_exceeds_disk_budget")

    def test_plan_prune_respects_pins_and_caps(self) -> None:
        bucket = BucketKey("receipt", False, "store", "low_conf")
        r0 = _make_record(0, bucket, size_bytes=300, pinned=True)
        r1 = _make_record(1, bucket, size_bytes=300, pinned=False)
        r2 = _make_record(2, bucket, size_bytes=300, pinned=False)
        r3 = _make_record(3, bucket, size_bytes=300, pinned=False)
        state = SamplerState(records=(r0, r1, r2, r3))
        cfg = CaptureConfig(bucket_limit=2, max_total_bytes=700)

        evict = plan_prune(state, config=cfg)
        self.assertNotIn(r0.sample_id, evict)
        self.assertEqual(set(evict), {r1.sample_id, r2.sample_id})


if __name__ == "__main__":
    unittest.main()
