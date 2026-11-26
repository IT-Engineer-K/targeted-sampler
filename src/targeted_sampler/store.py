"""Fail-safe local filesystem store for stratified samples."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import logging
import os
from pathlib import Path
import re
import tempfile
from typing import Any, Iterator

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None  # type: ignore[assignment]

try:
    import msvcrt
except ImportError:  # pragma: no cover - POSIX
    msvcrt = None  # type: ignore[assignment]

from .config import CaptureConfig, TargetRule, load_pinned_ids, load_targets
from .decision import plan_prune, should_capture
from .models import CaptureDecision, RecordOutcome, SampleEvent, SampleRecord, SamplerState

logger = logging.getLogger(__name__)

_SAFE_NAME_RE = re.compile(r"[^A-Za-z0-9._-]+")


def _safe_filename(raw: str) -> str:
    cleaned = _SAFE_NAME_RE.sub("_", raw).strip("._")
    if not cleaned:
        cleaned = "sample"
    if cleaned != raw:
        digest = hashlib.sha256(raw.encode("utf-8")).hexdigest()[:8]
        return f"{cleaned}_{digest}"
    return cleaned


class SampleStore:
    """Manages stratified sample artifacts, metadata, and safe rotation on disk.

    Directory layout under ``root_dir``:
      - ``meta/<sample_id>.json``
      - ``artifacts/<sample_id><ext>``
      - ``layouts/<sample_id>.layout.json``
    """

    def __init__(
        self,
        root_dir: str | Path,
        config: CaptureConfig | None = None,
        targets_path: str | Path | None = None,
        pinned_path: str | Path | None = None,
        targets: tuple[TargetRule, ...] | None = None,
        pinned_fingerprints: frozenset[str] = frozenset(),
        pinned_sample_ids: frozenset[str] = frozenset(),
    ) -> None:
        self.root_dir = Path(root_dir)
        self.meta_dir = self.root_dir / "meta"
        self.artifacts_dir = self.root_dir / "artifacts"
        self.layouts_dir = self.root_dir / "layouts"
        self.config = config or CaptureConfig()
        self.targets_path = Path(targets_path) if targets_path is not None else None
        self.pinned_path = Path(pinned_path) if pinned_path is not None else None
        self._explicit_targets = targets
        self._explicit_pinned_fps = frozenset(pinned_fingerprints)
        self._explicit_pinned_ids = frozenset(pinned_sample_ids)

    def _ensure_dirs(self) -> None:
        self.meta_dir.mkdir(parents=True, exist_ok=True)
        self.artifacts_dir.mkdir(parents=True, exist_ok=True)
        self.layouts_dir.mkdir(parents=True, exist_ok=True)

    @contextmanager
    def _store_lock(self) -> Iterator[None]:
        self.root_dir.mkdir(parents=True, exist_ok=True)
        lock_path = self.root_dir / ".store.lock"
        with lock_path.open("a+b") as lock_file:
            if fcntl is not None:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
            elif msvcrt is not None:
                lock_file.seek(0)
                msvcrt.locking(lock_file.fileno(), msvcrt.LK_LOCK, 1)
            try:
                yield
            finally:
                if fcntl is not None:
                    fcntl.flock(lock_file.fileno(), fcntl.LOCK_UN)
                elif msvcrt is not None:
                    lock_file.seek(0)
                    msvcrt.locking(lock_file.fileno(), msvcrt.LK_UNLCK, 1)

    def _resolve_within_root(self, relpath: str) -> Path | None:
        root_resolved = self.root_dir.resolve()
        candidate = (self.root_dir / relpath).resolve()
        if candidate == root_resolved or not candidate.is_relative_to(root_resolved):
            logger.warning("Refusing path outside store root: %s", relpath)
            return None
        return candidate

    def active_targets(self) -> tuple[TargetRule, ...]:
        if self._explicit_targets is not None:
            return self._explicit_targets
        return load_targets(self.targets_path)

    def active_pins(self) -> tuple[frozenset[str], frozenset[str]]:
        file_fps, file_ids = load_pinned_ids(self.pinned_path)
        return (
            self._explicit_pinned_fps | file_fps,
            self._explicit_pinned_ids | file_ids,
        )

    def scan(self) -> tuple[SamplerState, tuple[str, ...]]:
        """Load current state from disk, isolating any corrupt metadata files."""
        records: list[SampleRecord] = []
        corrupt_files: list[str] = []

        if self.meta_dir.exists():
            for meta_file in sorted(self.meta_dir.glob("*.json")):
                try:
                    payload = json.loads(meta_file.read_text(encoding="utf-8"))
                    if not isinstance(payload, dict):
                        raise ValueError("metadata root must be a JSON object")
                    records.append(SampleRecord.from_dict(payload))
                except (OSError, json.JSONDecodeError, KeyError, TypeError, ValueError) as exc:
                    logger.warning("Ignoring corrupt metadata file %s: %s", meta_file, exc)
                    corrupt_files.append(str(meta_file))

        pinned_fps, pinned_ids = self.active_pins()
        state = SamplerState(
            records=tuple(records),
            pinned_fingerprints=pinned_fps,
            pinned_sample_ids=pinned_ids,
        )
        return state, tuple(corrupt_files)

    def load_state(self) -> SamplerState:
        state, _ = self.scan()
        return state

    def safe_record(
        self,
        event: SampleEvent,
        artifact_bytes: bytes | None = None,
        artifact_ext: str = ".bin",
        layout: dict[str, Any] | None = None,
        pinned: bool = False,
    ) -> RecordOutcome:
        """Evaluate and persist a sample without ever raising I/O exceptions to callers."""
        try:
            self._ensure_dirs()
            with self._store_lock():
                state = self.load_state()
                targets = self.active_targets()
                decision = should_capture(event, state, config=self.config, targets=targets)

                if not decision.capture:
                    return RecordOutcome(decision=decision, stored=False)

                # Write new sample BEFORE evicting old ones so write failure never loses data.
                self._write_sample(
                    event=event,
                    artifact_bytes=artifact_bytes,
                    artifact_ext=artifact_ext,
                    layout=layout,
                    pinned=pinned,
                )
                evicted = self._evict_records(decision.evict_ids, state)
                return RecordOutcome(decision=decision, stored=True, evicted_ids=evicted)
        except OSError as exc:
            msg = f"Storage I/O error while recording {event.sample_id}: {exc}"
            logger.warning(msg)
            fallback = CaptureDecision(capture=False, reason="storage_io_error")
            return RecordOutcome(decision=fallback, stored=False, warning=msg)
        except Exception as exc:  # noqa: BLE001 — worker safety boundary
            msg = f"Unexpected sampler error while recording {event.sample_id}: {exc}"
            logger.warning(msg)
            fallback = CaptureDecision(capture=False, reason="unexpected_error")
            return RecordOutcome(decision=fallback, stored=False, warning=msg)

    def prune(self) -> tuple[str, ...]:
        """Enforce bucket and disk caps on disk without touching pinned samples."""
        with self._store_lock():
            state = self.load_state()
            targets = self.active_targets()
            to_evict = plan_prune(state, config=self.config, targets=targets)
            return self._evict_records(to_evict, state)

    def stats(self) -> dict[str, Any]:
        """Return structured bucket and disk usage summary."""
        state, corrupt = self.scan()
        buckets: dict[str, dict[str, Any]] = {}
        pinned_count = 0
        for rec in state.records:
            key = rec.bucket.canonical()
            entry = buckets.setdefault(
                key,
                {
                    "bucket": rec.bucket.to_dict(),
                    "total": 0,
                    "pinned": 0,
                    "unpinned": 0,
                    "bytes": 0,
                },
            )
            is_pin = state.is_pinned(rec)
            entry["total"] += 1
            entry["bytes"] += rec.size_bytes
            if is_pin:
                entry["pinned"] += 1
                pinned_count += 1
            else:
                entry["unpinned"] += 1

        return {
            "root_dir": str(self.root_dir),
            "total_samples": len(state.records),
            "pinned_samples": pinned_count,
            "unpinned_samples": len(state.records) - pinned_count,
            "total_bytes": state.total_bytes,
            "max_total_bytes": self.config.max_total_bytes,
            "corrupt_metadata_count": len(corrupt),
            "buckets": buckets,
        }

    def verify(self) -> dict[str, Any]:
        """Inspect store health: corrupt metadata and missing artifact files."""
        state, corrupt = self.scan()
        missing_artifacts: list[str] = []
        missing_layouts: list[str] = []
        for rec in state.records:
            if rec.artifact_relpath:
                art_path = self._resolve_within_root(rec.artifact_relpath)
                if art_path is None or not art_path.exists():
                    missing_artifacts.append(rec.sample_id)
            if rec.layout_relpath:
                lay_path = self._resolve_within_root(rec.layout_relpath)
                if lay_path is None or not lay_path.exists():
                    missing_layouts.append(rec.sample_id)
        return {
            "ok": not corrupt and not missing_artifacts and not missing_layouts,
            "valid_samples": len(state.records),
            "corrupt_metadata_files": list(corrupt),
            "missing_artifacts": missing_artifacts,
            "missing_layouts": missing_layouts,
        }

    def _write_sample(
        self,
        event: SampleEvent,
        artifact_bytes: bytes | None,
        artifact_ext: str,
        layout: dict[str, Any] | None,
        pinned: bool,
    ) -> None:
        safe_id = _safe_filename(event.sample_id)
        ext = artifact_ext if artifact_ext.startswith(".") else f".{artifact_ext}"
        if "/" in ext or "\\" in ext or ".." in ext:
            raise ValueError(f"Invalid artifact extension: {artifact_ext!r}")

        meta_path = self.meta_dir / f"{safe_id}.json"
        if meta_path.exists():
            raise FileExistsError(f"Refusing to overwrite existing metadata file: {meta_path.name}")

        created_paths: list[Path] = []
        try:
            artifact_rel: str | None = None
            if artifact_bytes is not None:
                artifact_path = self.artifacts_dir / f"{safe_id}{ext}"
                self._atomic_write_bytes(artifact_path, artifact_bytes)
                created_paths.append(artifact_path)
                artifact_rel = str(artifact_path.relative_to(self.root_dir)).replace("\\", "/")

            layout_rel: str | None = None
            if layout is not None:
                layout_path = self.layouts_dir / f"{safe_id}.layout.json"
                layout_bytes = json.dumps(layout, ensure_ascii=False, indent=2).encode("utf-8")
                self._atomic_write_bytes(layout_path, layout_bytes)
                created_paths.append(layout_path)
                layout_rel = str(layout_path.relative_to(self.root_dir)).replace("\\", "/")

            record = SampleRecord(
                sample_id=event.sample_id,
                bucket=event.bucket,
                fingerprint=event.fingerprint,
                size_bytes=event.size_bytes,
                timestamp=event.timestamp,
                pinned=pinned,
                artifact_relpath=artifact_rel,
                layout_relpath=layout_rel,
                metadata=event.metadata,
            )
            meta_bytes = json.dumps(record.to_dict(), ensure_ascii=False, indent=2).encode("utf-8")
            self._atomic_write_bytes(meta_path, meta_bytes)
            created_paths.append(meta_path)
        except Exception:
            for path in reversed(created_paths):
                self._unlink_quiet(path)
            raise

    def _evict_records(
        self, sample_ids: tuple[str, ...], state: SamplerState
    ) -> tuple[str, ...]:
        if not sample_ids:
            return ()
        by_id = {r.sample_id: r for r in state.records}
        evicted: list[str] = []
        for sid in sample_ids:
            rec = by_id.get(sid)
            if rec is None:
                continue
            # Defense-in-depth: never delete a pinned record even if requested.
            if state.is_pinned(rec):
                logger.warning("Refusing to evict pinned sample %s", sid)
                continue
            safe_id = _safe_filename(sid)
            meta_candidate = self._resolve_within_root(f"meta/{safe_id}.json")
            if meta_candidate is not None:
                self._unlink_quiet(meta_candidate)
            if rec.artifact_relpath:
                art_candidate = self._resolve_within_root(rec.artifact_relpath)
                if art_candidate is not None:
                    self._unlink_quiet(art_candidate)
            if rec.layout_relpath:
                lay_candidate = self._resolve_within_root(rec.layout_relpath)
                if lay_candidate is not None:
                    self._unlink_quiet(lay_candidate)
            evicted.append(sid)
        return tuple(evicted)

    @staticmethod
    def _atomic_write_bytes(path: Path, data: bytes) -> None:
        tmp_name: str | None = None
        try:
            with tempfile.NamedTemporaryFile(
                mode="wb",
                dir=path.parent,
                prefix=f".{path.name}.",
                suffix=".tmp",
                delete=False,
            ) as tmp_file:
                tmp_name = tmp_file.name
                tmp_file.write(data)
                tmp_file.flush()
            os.replace(tmp_name, path)
            tmp_name = None
        finally:
            if tmp_name is not None:
                try:
                    os.unlink(tmp_name)
                except OSError:
                    pass

    @staticmethod
    def _unlink_quiet(path: Path) -> None:
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Failed to remove %s during eviction: %s", path, exc)
