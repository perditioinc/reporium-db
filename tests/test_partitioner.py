"""Tests for reporium_db.partitioner."""

from __future__ import annotations

import json

from reporium_db.partitioner import (
    FULL_PARTITION_SIZE,
    write_partitioned,
)
from tests.conftest import make_repo


def test_partitioner_index_counts(tmp_path):
    """index.json contains correct total and language counts."""
    repos = [
        make_repo("a", language="Python"),
        make_repo("b", language="Python"),
        make_repo("c", language="Go"),
    ]
    index = write_partitioned(repos, tmp_path)

    assert index["meta"]["total"] == 3
    assert index["languages"]["Python"] == 2
    assert index["languages"]["Go"] == 1


def test_partitioner_index_written(tmp_path):
    """index.json is written to disk correctly."""
    repos = [make_repo("a")]
    write_partitioned(repos, tmp_path)

    index = json.loads((tmp_path / "index.json").read_text())
    assert index["meta"]["total"] == 1


def test_partitioner_top_starred(tmp_path):
    """top_starred.json is sorted by stars descending."""
    repos = [
        make_repo("low", stars=10),
        make_repo("high", stars=500),
        make_repo("mid", stars=100),
    ]
    write_partitioned(repos, tmp_path)

    top = json.loads((tmp_path / "top_starred.json").read_text())
    assert top[0]["stars"] == 500
    assert top[1]["stars"] == 100


def test_partitioner_recent_filters_old(tmp_path):
    """recent.json excludes repos not pushed in the last 7 days."""
    repos = [
        make_repo("new", pushed_at="2026-03-16T00:00:00Z"),
        make_repo("old", pushed_at="2025-01-01T00:00:00Z"),
    ]
    write_partitioned(repos, tmp_path)

    recent = json.loads((tmp_path / "recent.json").read_text())
    names = [r["name"] for r in recent]
    assert "new" in names
    assert "old" not in names


def test_partitioner_by_language(tmp_path):
    """by_language/ contains one file per language."""
    repos = [
        make_repo("a", language="Python"),
        make_repo("b", language="Go"),
    ]
    write_partitioned(repos, tmp_path)

    assert (tmp_path / "by_language" / "Python.json").exists()
    assert (tmp_path / "by_language" / "Go.json").exists()


def test_partitioner_by_category(tmp_path):
    """by_category/ contains one file per topic."""
    repos = [make_repo("a", topics=["llm", "rag"])]
    write_partitioned(repos, tmp_path)

    assert (tmp_path / "by_category" / "llm.json").exists()
    assert (tmp_path / "by_category" / "rag.json").exists()


def test_partitioner_full_single_file(tmp_path):
    """Small dataset produces exactly one full partition file."""
    repos = [make_repo(f"repo-{i}") for i in range(5)]
    write_partitioned(repos, tmp_path)

    parts = list((tmp_path / "full").glob("*.json"))
    assert len(parts) == 1


def test_partitioner_full_multiple_files(tmp_path):
    """Large dataset is split into multiple partition files."""
    # Use FULL_PARTITION_SIZE + 1 repos to force 2 files
    repos = [make_repo(f"r{i}", owner=f"u{i}") for i in range(FULL_PARTITION_SIZE + 1)]
    write_partitioned(repos, tmp_path)

    parts = list((tmp_path / "full").glob("*.json"))
    assert len(parts) == 2


def test_partitioner_atomic_no_tmp_remaining(tmp_path):
    """No .tmp files remain after write_partitioned completes."""
    repos = [make_repo("a")]
    write_partitioned(repos, tmp_path)

    tmp_files = list(tmp_path.rglob("*.tmp"))
    assert tmp_files == []
