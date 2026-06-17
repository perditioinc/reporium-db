"""Validate the local seed dataset shape before a substrate run.

Cheap guardrail so a malformed seed fails fast with a clear message instead of
surfacing as a confusing pipeline error. $0, stdlib only.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

SEED = Path("/src/local/seed/repos.json")

REQUIRED = {"nameWithOwner", "name"}
ALLOWED_PUSHED_TOKENS = {"RECENT", "WEEKLY", "MONTHLY"}


def main() -> None:
    if not SEED.exists():
        print(f"[validate-seed] FAIL: seed not found at {SEED}")
        sys.exit(1)

    raw = json.loads(SEED.read_text())
    repos = raw.get("repos")
    if not isinstance(repos, list) or not repos:
        print("[validate-seed] FAIL: 'repos' must be a non-empty array")
        sys.exit(1)

    names = set()
    for i, r in enumerate(repos):
        missing = REQUIRED - r.keys()
        if missing:
            print(f"[validate-seed] FAIL: repo #{i} missing fields {sorted(missing)}")
            sys.exit(1)
        if r["nameWithOwner"] in names:
            print(f"[validate-seed] FAIL: duplicate nameWithOwner {r['nameWithOwner']}")
            sys.exit(1)
        names.add(r["nameWithOwner"])
        pushed = r.get("pushedAt")
        if pushed is not None and pushed not in ALLOWED_PUSHED_TOKENS and "T" not in str(pushed):
            print(f"[validate-seed] FAIL: repo {r['nameWithOwner']} has bad pushedAt {pushed!r}")
            sys.exit(1)

    print(f"[validate-seed] OK: {len(repos)} seed repos valid")


if __name__ == "__main__":
    main()
