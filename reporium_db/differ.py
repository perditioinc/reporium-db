"""Diff computation between today's fetch and yesterday's snapshot."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from .models import DatasetDiff, RepoMetadata

logger = logging.getLogger(__name__)

MAX_SNAPSHOTS = 7


def _load_index(path: Path) -> dict:
    """Load an index.json file, returning empty structure on failure."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not read index at %s: %s", path, exc)
        return {}


def _save_snapshot(index_path: Path, snapshot_dir: Path) -> None:
    """Copy current index.json to snapshot/YYYY-MM-DD.json and prune old snapshots.

    Keeps only the last MAX_SNAPSHOTS files.
    """
    if not index_path.exists():
        return
    snapshot_dir.mkdir(parents=True, exist_ok=True)
    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    dest = snapshot_dir / f"{date_str}.json"
    tmp = dest.with_suffix(".tmp")
    tmp.write_bytes(index_path.read_bytes())
    os.replace(tmp, dest)
    logger.info("Saved snapshot to %s", dest)

    # Prune oldest snapshots beyond MAX_SNAPSHOTS
    snapshots = sorted(snapshot_dir.glob("*.json"))
    for old in snapshots[:-MAX_SNAPSHOTS]:
        old.unlink()
        logger.info("Pruned old snapshot: %s", old)


def _repo_signature(repo: RepoMetadata) -> tuple[Optional[str], tuple[str, ...]]:
    """Return the fields that mark a repo as 'updated' when changed."""
    return repo.description, tuple(sorted(repo.topics))


def compute_diff(
    today: list[RepoMetadata],
    data_dir: Path,
    snapshot_dir: Path,
) -> DatasetDiff:
    """Compare today's repos against the last snapshot and write pending_enrichment.json.

    Before writing new data, saves the current index.json as a dated snapshot.

    Args:
        today: Freshly fetched repos.
        data_dir: Directory containing index.json and pending_enrichment.json.
        snapshot_dir: Directory for dated snapshot files.

    Returns:
        DatasetDiff with new, removed, updated, and unchanged counts.
    """
    index_path = data_dir / "index.json"
    _save_snapshot(index_path, snapshot_dir)

    # Build yesterday's lookup from the stored full repo list (if any)
    yesterday_full_path = data_dir / "_repos_cache.json"
    if yesterday_full_path.exists():
        try:
            yesterday_raw: dict[str, dict] = json.loads(yesterday_full_path.read_text())
        except Exception as exc:  # noqa: BLE001
            logger.warning("Could not load yesterday cache: %s", exc)
            yesterday_raw = {}
    else:
        yesterday_raw = {}

    today_map = {r.nameWithOwner: r for r in today}
    today_names = set(today_map)
    yesterday_names = set(yesterday_raw)

    new_repos = sorted(today_names - yesterday_names)
    removed_repos = sorted(yesterday_names - today_names)
    updated_repos = []
    unchanged_count = 0

    for name in today_names & yesterday_names:
        repo = today_map[name]
        prev = yesterday_raw[name]
        prev_sig = (prev.get("description"), tuple(sorted(prev.get("topics", []))))
        if _repo_signature(repo) != prev_sig:
            updated_repos.append(name)
        else:
            unchanged_count += 1

    # Persist today's repos as the new cache for tomorrow's diff
    cache_data = {
        r.nameWithOwner: {
            "description": r.description,
            "topics": r.topics,
        }
        for r in today
    }
    tmp = yesterday_full_path.with_suffix(".tmp")
    tmp.write_text(json.dumps(cache_data))
    os.replace(tmp, yesterday_full_path)

    # Write pending_enrichment.json for reporium-ingestion
    enrichment_repos = [{"name_with_owner": n, "reason": "new_repo"} for n in new_repos] + [
        {"name_with_owner": n, "reason": "updated_repo"} for n in updated_repos
    ]

    pending_path = data_dir / "pending_enrichment.json"
    pending_tmp = pending_path.with_suffix(".tmp")
    pending_tmp.write_text(
        json.dumps(
            {"generated_at": datetime.now(timezone.utc).isoformat(), "repos": enrichment_repos},
            indent=2,
        )
    )
    os.replace(pending_tmp, pending_path)

    logger.info(
        "Diff: %d new, %d removed, %d updated, %d unchanged",
        len(new_repos),
        len(removed_repos),
        len(updated_repos),
        unchanged_count,
    )
    return DatasetDiff(
        new_repos=new_repos,
        removed_repos=removed_repos,
        updated_repos=updated_repos,
        unchanged_count=unchanged_count,
    )
