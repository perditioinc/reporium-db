"""Tests for reporium_db.differ."""

from __future__ import annotations

import json

from reporium_db.differ import compute_diff
from tests.conftest import make_repo


def test_differ_all_new(tmp_path):
    """All repos are new when there is no existing cache."""
    repos = [make_repo("a"), make_repo("b")]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert len(diff.new_repos) == 2
    assert len(diff.removed_repos) == 0
    assert len(diff.updated_repos) == 0
    assert diff.unchanged_count == 0


def test_differ_no_change(tmp_path):
    """Repos with same description/topics show as unchanged."""
    repos = [make_repo("a", description="same", topics=["ai"])]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    # Seed the cache as if it ran yesterday
    cache = {"testuser/a": {"description": "same", "topics": ["ai"]}}
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert diff.unchanged_count == 1
    assert len(diff.new_repos) == 0
    assert len(diff.updated_repos) == 0


def test_differ_updated_description(tmp_path):
    """Repo with changed description shows as updated."""
    repos = [make_repo("a", description="new description", topics=["ai"])]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    cache = {"testuser/a": {"description": "old description", "topics": ["ai"]}}
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert "testuser/a" in diff.updated_repos
    assert diff.unchanged_count == 0


def test_differ_updated_topics(tmp_path):
    """Repo with changed topics shows as updated."""
    repos = [make_repo("a", description="desc", topics=["ai", "new-topic"])]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    cache = {"testuser/a": {"description": "desc", "topics": ["ai"]}}
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert "testuser/a" in diff.updated_repos


def test_differ_removed_repo(tmp_path):
    """Repos in yesterday's cache but not today show as removed."""
    repos = [make_repo("a")]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    cache = {
        "testuser/a": {"description": "desc", "topics": []},
        "testuser/old": {"description": "desc", "topics": []},
    }
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert "testuser/old" in diff.removed_repos


def test_differ_mixed(tmp_path):
    """Mix of new, updated, unchanged, and removed repos."""
    repos = [
        make_repo("new-repo"),
        make_repo("unchanged", description="same", topics=["t1"]),
        make_repo("updated", description="new desc", topics=["t1"]),
    ]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    cache = {
        "testuser/unchanged": {"description": "same", "topics": ["t1"]},
        "testuser/updated": {"description": "old desc", "topics": ["t1"]},
        "testuser/removed": {"description": "gone", "topics": []},
    }
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    diff = compute_diff(repos, data_dir, tmp_path / "snapshot")

    assert "testuser/new-repo" in diff.new_repos
    assert "testuser/updated" in diff.updated_repos
    assert "testuser/removed" in diff.removed_repos
    assert diff.unchanged_count == 1


def test_differ_writes_pending_enrichment(tmp_path):
    """pending_enrichment.json lists new and updated repos."""
    repos = [
        make_repo("brand-new"),
        make_repo("changed", description="updated"),
    ]
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    cache = {"testuser/changed": {"description": "original", "topics": []}}
    (data_dir / "_repos_cache.json").write_text(json.dumps(cache))

    compute_diff(repos, data_dir, tmp_path / "snapshot")

    pending = json.loads((data_dir / "pending_enrichment.json").read_text())
    names = [r["name_with_owner"] for r in pending["repos"]]
    assert "testuser/brand-new" in names
    assert "testuser/changed" in names


def test_differ_saves_snapshot(tmp_path):
    """compute_diff saves a dated snapshot of the previous index.json."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    snapshot_dir = tmp_path / "snapshot"

    # Pre-seed an index so there's something to snapshot
    (data_dir / "index.json").write_text(json.dumps({"meta": {"total": 5}}))

    compute_diff([make_repo("a")], data_dir, snapshot_dir)

    snapshots = list(snapshot_dir.glob("*.json"))
    assert len(snapshots) == 1
