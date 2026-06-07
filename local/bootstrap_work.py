"""Prepare a clean /work scratch tree from the read-only /src mount.

Copies the application source (reporium_db), the local scaffolding (local), and
the seed into /work, and guarantees a CLEAN slate: no pre-existing data/,
snapshot/, checkpoints/, schedule.json, or *_cache so each substrate run is a
true clean-state run (the analogue of "all migrations apply to a fresh DB").

Idempotent: safe to call before every step. Source at /src is never modified.
"""

from __future__ import annotations

import shutil
from pathlib import Path

SRC = Path("/src")
WORK = Path("/work")

# Application + local scaffolding the pipeline needs at runtime.
COPY_TREES = ["reporium_db", "local"]
# Generated / stateful artifacts that must NOT be carried over from the
# committed checkout, so every run starts clean.
RESET = [
    "data",
    "snapshot",
    "checkpoints",
    "schedule.json",
    "pending_enrichment.json",
    "LAST_RUN.md",
    "README.md",
]


def main() -> None:
    WORK.mkdir(parents=True, exist_ok=True)

    for tree in COPY_TREES:
        dst = WORK / tree
        if dst.exists():
            shutil.rmtree(dst)
        shutil.copytree(SRC / tree, dst)

    # Wipe any stale generated state in /work from a prior run.
    for item in RESET:
        p = WORK / item
        if p.is_dir():
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()

    print("[bootstrap] clean /work prepared (no data/, schedule.json, snapshot/, checkpoints/)")


if __name__ == "__main__":
    main()
