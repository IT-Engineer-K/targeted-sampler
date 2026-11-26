"""Integration tests for SampleStore, corrupt metadata isolation, and CLI."""

from __future__ import annotations

import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

from targeted_sampler import (
    BucketKey,
    CaptureConfig,
    SampleEvent,
    SampleStore,
    compute_fingerprint,
)
from targeted_sampler import cli


class TestSampleStoreAndCLI(unittest.TestCase):
    def test_store_rotation_protects_pinned_and_handles_corrupt_metadata(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "samples"
            targets_file = Path(tmp) / "capture_targets.json"
            pinned_file = Path(tmp) / "pinned_labels.json"

            targets_file.write_text(
                json.dumps(
                    {
                        "targets": [
                            {
                                "name": "focus-items",
                                "kind": "receipt",
                                "single_blocker": True,
                                "primary_key": "items",
                                "bucket_limit": 4,
                                "force_capture": True,
                            }
                        ]
                    }
                ),
                encoding="utf-8",
            )

            pinned_payload = b"labeled-reference-sample"
            pinned_fp = compute_fingerprint(pinned_payload)
            pinned_file.write_text(
                json.dumps({"labels": [{"sample_id": "pin-001", "fingerprint": pinned_fp}]}),
                encoding="utf-8",
            )

            cfg = CaptureConfig(bucket_limit=2, max_total_bytes=5000)
            store = SampleStore(
                root_dir=root,
                config=cfg,
                targets_path=targets_file,
                pinned_path=pinned_file,
            )

            bucket = BucketKey("receipt", True, "items", "sum_mismatch")
            # Record pinned sample first
            pin_event = SampleEvent(
                sample_id="pin-001",
                bucket=bucket,
                fingerprint=pinned_fp,
                size_bytes=len(pinned_payload),
                timestamp="2026-11-26T00:00:01+00:00",
            )
            out = store.safe_record(
                pin_event,
                artifact_bytes=pinned_payload,
                artifact_ext=".bin",
                layout={"lines": [{"text": "ITEM_A", "bbox": [0, 0, 10, 10]}]},
            )
            self.assertTrue(out.stored)

            # Inject a corrupt metadata file directly on disk
            (root / "meta" / "broken.json").write_text("{not-valid-json", encoding="utf-8")

            # Record 6 more unpinned samples -> target bucket_limit is 4 unpinned, plus 1 pinned
            for i in range(6):
                raw = f"sample-bytes-{i}".encode("utf-8")
                ev = SampleEvent(
                    sample_id=f"u-{i:03d}",
                    bucket=bucket,
                    fingerprint=compute_fingerprint(raw),
                    size_bytes=len(raw),
                    timestamp=f"2026-11-26T00:01:{i:02d}+00:00",
                )
                res = store.safe_record(ev, artifact_bytes=raw)
                self.assertTrue(res.stored)
                self.assertEqual(res.decision.matched_target, "focus-items")

            # Duplicate fingerprint must be skipped
            dup_res = store.safe_record(pin_event, artifact_bytes=pinned_payload)
            self.assertFalse(dup_res.stored)
            self.assertEqual(dup_res.decision.reason, "duplicate_fingerprint")

            # Verify pinned sample still exists on disk and 4 newest unpinned remain
            state, corrupt = store.scan()
            self.assertEqual(len(corrupt), 1)
            ids = {r.sample_id for r in state.records}
            self.assertIn("pin-001", ids)
            self.assertEqual(ids, {"pin-001", "u-002", "u-003", "u-004", "u-005"})
            self.assertTrue((root / "artifacts" / "pin-001.bin").exists())
            self.assertTrue((root / "layouts" / "pin-001.layout.json").exists())

            # Verify CLI stats and verify commands work cleanly
            buf = io.StringIO()
            with mock.patch("sys.stdout", buf):
                rc = cli.main(["stats", "--root", str(root), "--pinned", str(pinned_file), "--format", "json"])
            self.assertEqual(rc, 0)
            stats_json = json.loads(buf.getvalue())
            self.assertEqual(stats_json["total_samples"], 5)
            self.assertEqual(stats_json["pinned_samples"], 1)
            self.assertEqual(stats_json["corrupt_metadata_count"], 1)

            buf2 = io.StringIO()
            with mock.patch("sys.stdout", buf2):
                rc_verify = cli.main(["verify", "--root", str(root), "--format", "json"])
            self.assertEqual(rc_verify, 1)  # 1 corrupt metadata file detected
            verify_json = json.loads(buf2.getvalue())
            self.assertFalse(verify_json["ok"])
            self.assertEqual(len(verify_json["corrupt_metadata_files"]), 1)

    def test_corrupt_targets_file_falls_back_safely(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp) / "samples"
            bad_targets = Path(tmp) / "capture_targets.json"
            bad_targets.write_text("{corrupt", encoding="utf-8")

            store = SampleStore(root_dir=root, targets_path=bad_targets)
            self.assertEqual(store.active_targets(), ())

    def test_io_error_during_write_never_raises_to_worker(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            store = SampleStore(root_dir=Path(tmp) / "samples")
            ev = SampleEvent(
                sample_id="io-fail-001",
                bucket=BucketKey("receipt", True, "total", "no_candidate"),
                fingerprint=compute_fingerprint(b"abc"),
                size_bytes=3,
            )
            with mock.patch.object(SampleStore, "_atomic_write_bytes", side_effect=OSError("disk full")):
                outcome = store.safe_record(ev, artifact_bytes=b"abc")
            self.assertFalse(outcome.stored)
            self.assertEqual(outcome.decision.reason, "storage_io_error")
            self.assertIsNotNone(outcome.warning)


if __name__ == "__main__":
    unittest.main()
