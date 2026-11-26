"""Command-line interface for inspecting and maintaining a SampleStore."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Sequence

from .config import CaptureConfig
from .store import SampleStore


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="targeted-sampler",
        description="Inspect, verify, and prune stratified reason-bucketed sample stores.",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    for cmd_name, help_text in (
        ("stats", "Show per-bucket sample counts and disk usage."),
        ("verify", "Verify metadata integrity and artifact references."),
        ("prune", "Enforce bucket limits and disk budget without deleting pinned samples."),
    ):
        p = sub.add_parser(cmd_name, help=help_text)
        p.add_argument("--root", required=True, help="Path to sample store directory.")
        p.add_argument("--targets", default=None, help="Optional path to capture_targets.json.")
        p.add_argument("--pinned", default=None, help="Optional path to pinned labels JSON.")
        p.add_argument("--bucket-limit", type=int, default=15, help="Default per-bucket limit.")
        p.add_argument("--max-mb", type=float, default=200.0, help="Global disk budget in MB.")
        p.add_argument(
            "--format",
            choices=("table", "json"),
            default="table",
            help="Output format (table or json).",
        )
    return parser


def _render_stats_table(data: dict) -> str:
    lines = [
        f"Root: {data['root_dir']}",
        f"Samples: {data['total_samples']} (pinned={data['pinned_samples']}, unpinned={data['unpinned_samples']})",
        f"Disk: {data['total_bytes']} / {data['max_total_bytes']} bytes (corrupt_meta={data['corrupt_metadata_count']})",
        "",
        f"{'BUCKET':<52} {'TOTAL':>5} {'PIN':>4} {'BYTES':>10}",
        "-" * 75,
    ]
    for key, info in sorted(data["buckets"].items()):
        lines.append(f"{key:<52} {info['total']:>5} {info['pinned']:>4} {info['bytes']:>10}")
    return "\n".join(lines)


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    cfg = CaptureConfig(
        bucket_limit=args.bucket_limit,
        max_total_bytes=max(1, int(args.max_mb * 1024 * 1024)),
    )
    store = SampleStore(
        root_dir=args.root,
        config=cfg,
        targets_path=args.targets,
        pinned_path=args.pinned,
    )

    if args.command == "stats":
        result = store.stats()
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(_render_stats_table(result))
        return 0

    if args.command == "verify":
        result = store.verify()
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            status = "OK" if result["ok"] else "ISSUES FOUND"
            print(
                f"Verify: {status} (valid={result['valid_samples']}, "
                f"corrupt={len(result['corrupt_metadata_files'])}, "
                f"missing_artifacts={len(result['missing_artifacts'])})"
            )
        return 0 if result["ok"] else 1

    if args.command == "prune":
        evicted = store.prune()
        result = {"evicted_count": len(evicted), "evicted_ids": list(evicted)}
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            print(f"Pruned {len(evicted)} unpinned sample(s).")
        return 0

    return 2


if __name__ == "__main__":
    sys.exit(main())
