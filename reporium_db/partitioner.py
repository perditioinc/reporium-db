"""Write repos to partitioned JSON files — all writes are atomic."""

from __future__ import annotations

import json
import logging
import math
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .models import RepoMetadata

logger = logging.getLogger(__name__)

RECENT_DAYS = 7
RECENT_MAX = 500
TOP_STARRED_COUNT = 100
FULL_PARTITION_SIZE = 10_000


def _atomic_write(path: Path, data) -> None:
    """Write JSON atomically: write to .tmp then os.replace."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str))
    os.replace(tmp, path)


def _build_index(repos: list[RepoMetadata]) -> dict:
    """Build the index.json structure from all repos."""
    categories: dict[str, int] = {}
    languages: dict[str, int] = {}

    for r in repos:
        for topic in r.topics:
            categories[topic] = categories.get(topic, 0) + 1
        if r.primaryLanguage:
            languages[r.primaryLanguage] = languages.get(r.primaryLanguage, 0) + 1

    return {
        "meta": {
            "total": len(repos),
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "version": "1.0.0",
        },
        "categories": dict(sorted(categories.items(), key=lambda x: -x[1])),
        "languages": dict(sorted(languages.items(), key=lambda x: -x[1])),
    }


def _write_recent(repos: list[RepoMetadata], data_dir: Path) -> None:
    """Write repos pushed within the last RECENT_DAYS, capped at RECENT_MAX."""
    cutoff = datetime.now(timezone.utc) - timedelta(days=RECENT_DAYS)
    recent = []
    for r in repos:
        if not r.pushedAt:
            continue
        try:
            pushed = datetime.fromisoformat(r.pushedAt.replace("Z", "+00:00"))
        except ValueError:
            continue
        if pushed >= cutoff:
            recent.append(r)

    recent.sort(key=lambda r: r.pushedAt or "", reverse=True)
    recent = recent[:RECENT_MAX]
    _atomic_write(data_dir / "recent.json", [r.__dict__ for r in recent])
    logger.info("Wrote %d recent repos", len(recent))


def _write_top_starred(repos: list[RepoMetadata], data_dir: Path) -> None:
    """Write top repos sorted by star count."""
    top = sorted(repos, key=lambda r: r.stars, reverse=True)[:TOP_STARRED_COUNT]
    _atomic_write(data_dir / "top_starred.json", [r.__dict__ for r in top])
    logger.info("Wrote %d top-starred repos", len(top))


def _write_by_category(repos: list[RepoMetadata], data_dir: Path) -> None:
    """Write one JSON file per topic/category."""
    by_cat: dict[str, list[dict]] = {}
    for r in repos:
        for topic in r.topics:
            by_cat.setdefault(topic, []).append(r.__dict__)

    cat_dir = data_dir / "by_category"
    for cat, items in by_cat.items():
        safe = cat.replace("/", "_").replace(" ", "_")
        _atomic_write(cat_dir / f"{safe}.json", items)
    logger.info("Wrote %d category files", len(by_cat))


def _write_by_language(repos: list[RepoMetadata], data_dir: Path) -> None:
    """Write one JSON file per primary language."""
    by_lang: dict[str, list[dict]] = {}
    for r in repos:
        lang = r.primaryLanguage or "unknown"
        by_lang.setdefault(lang, []).append(r.__dict__)

    lang_dir = data_dir / "by_language"
    for lang, items in by_lang.items():
        safe = lang.replace("/", "_").replace(" ", "_")
        _atomic_write(lang_dir / f"{safe}.json", items)
    logger.info("Wrote %d language files", len(by_lang))


def _write_full_partitions(repos: list[RepoMetadata], data_dir: Path) -> None:
    """Write full dataset in FULL_PARTITION_SIZE-repo chunks."""
    full_dir = data_dir / "full"
    num_parts = max(1, math.ceil(len(repos) / FULL_PARTITION_SIZE))
    for i in range(num_parts):
        chunk = repos[i * FULL_PARTITION_SIZE : (i + 1) * FULL_PARTITION_SIZE]
        filename = f"repos_{i:04d}.json"
        _atomic_write(full_dir / filename, [r.__dict__ for r in chunk])
    logger.info("Wrote %d full partition file(s)", num_parts)


def write_partitioned(repos: list[RepoMetadata], data_dir: Path) -> dict:
    """Write all partitioned output files atomically and return the index.

    Args:
        repos: All fetched repositories.
        data_dir: Root data directory (typically ./data).

    Returns:
        The index dict written to index.json.
    """
    import time

    t0 = time.monotonic()

    index = _build_index(repos)
    _atomic_write(data_dir / "index.json", index)

    _write_recent(repos, data_dir)
    _write_top_starred(repos, data_dir)
    _write_by_category(repos, data_dir)
    _write_by_language(repos, data_dir)
    _write_full_partitions(repos, data_dir)

    elapsed = time.monotonic() - t0
    logger.info("Partition write complete: %d repos in %.2fs", len(repos), elapsed)
    return index
