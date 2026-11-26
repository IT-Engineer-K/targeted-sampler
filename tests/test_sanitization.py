"""Repository confidentiality & sanitization gate tests.

Ensures no client/service identifiers, external URLs, or real PII/business
secrets leak into repository documentation, source code, or tests.
"""

from __future__ import annotations

import os
from pathlib import Path
import re
import unittest

REPO_ROOT = Path(__file__).resolve().parents[1]


def _build_forbidden_patterns() -> tuple[re.Pattern[str], ...]:
    patterns: list[re.Pattern[str]] = [
        re.compile(r"https?://", re.IGNORECASE),
        re.compile(r"\bT\d{13}\b"),  # Qualified invoice registration number pattern
        re.compile(r"\b0\d{1,4}-\d{1,4}-\d{4}\b"),  # Japanese phone number pattern
    ]
    extra_terms = os.environ.get("SANITIZATION_FORBIDDEN_TERMS", "")
    for raw_term in extra_terms.split(","):
        term = raw_term.strip()
        if term:
            patterns.append(re.compile(re.escape(term), re.IGNORECASE))
    return tuple(patterns)


class TestRepositorySanitization(unittest.TestCase):
    def test_no_forbidden_business_terms_or_pii_in_repo(self) -> None:
        forbidden_patterns = _build_forbidden_patterns()
        violations: list[str] = []
        for path in REPO_ROOT.rglob("*"):
            if not path.is_file():
                continue
            if any(part in {".git", "__pycache__", ".pytest_cache"} for part in path.parts):
                continue
            if path.suffix not in {".py", ".md", ".toml", ".txt", ".json", ".yaml", ".yml"}:
                continue
            content = path.read_text(encoding="utf-8", errors="ignore")
            for pattern in forbidden_patterns:
                if pattern.search(content):
                    rel = path.relative_to(REPO_ROOT)
                    violations.append(f"{rel} matched {pattern.pattern}")
        self.assertEqual(violations, [], f"Sanitization violations found: {violations}")


if __name__ == "__main__":
    unittest.main()
