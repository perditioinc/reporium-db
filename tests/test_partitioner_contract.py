"""Schema / contract tests for partitioned output.

Four downstream services read this directory verbatim (reporium-api,
reporium-ingestion, reporium-dataset, reporium-metrics). These tests pin the
*contract* - the exact keys and value types those consumers rely on - so a
field rename or a dropped key in partitioner/models trips a test instead of
silently breaking a consumer in production. They use offline fixtures only
(make_repo + tmp_path); no network, no live data artifacts.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone

from reporium_db.models import RepoMetadata
from reporium_db.partitioner import write_partitioned
from tests.conftest import make_repo

# The full field set every emitted repo dict must carry. Locked here so that
# adding/removing a RepoMetadata field is a deliberate, test-visible change.
EXPECTED_REPO_FIELDS = {
    "nameWithOwner",
    "name",
    "description",
    "stars",
    "forks",
    "primaryLanguage",
    "pushedAt",
    "updatedAt",
    "createdAt",
    "isArchived",
    "isFork",
    "isEmpty",
    "topics",
    "licenseName",
    "openIssues",
    "defaultBranch",
    "parentRepo",
    "parentStars",
    "parentForks",
    "isPrivate",
}


def test_repo_field_set_matches_model():
    """The expected field set tracks the RepoMetadata dataclass exactly.

    Guards the assertion below from going stale: if a field is added to the
    model without updating EXPECTED_REPO_FIELDS, this fails first and tells the
    author to update the contract intentionally.
    """
    model_fields = {f.name for f in dataclasses.fields(RepoMetadata)}
    assert model_fields == EXPECTED_REPO_FIELDS


# -- index.json contract -------------------------------------------------------


def test_index_top_level_keys(tmp_path):
    """index.json has exactly the meta/categories/languages top-level keys."""
    index = write_partitioned([make_repo("a")], tmp_path)
    assert set(index.keys()) == {"meta", "categories", "languages"}


def test_index_meta_contract(tmp_path):
    """meta carries total (int), version (str), and an ISO-8601 last_updated."""
    write_partitioned([make_repo("a"), make_repo("b")], tmp_path)
    index = json.loads((tmp_path / "index.json").read_text())

    meta = index["meta"]
    assert set(meta.keys()) == {"total", "last_updated", "version"}
    assert meta["total"] == 2
    assert isinstance(meta["total"], int)
    assert meta["version"] == "1.0.0"
    # last_updated must round-trip through datetime.fromisoformat.
    parsed = datetime.fromisoformat(meta["last_updated"])
    assert parsed.tzinfo is not None  # timezone-aware (UTC)


def test_index_category_and_language_counts_are_ints(tmp_path):
    """categories/languages map names to positive int counts, sorted descending."""
    repos = [
        make_repo("a", language="Python", topics=["llm", "rag"]),
        make_repo("b", language="Python", topics=["llm"]),
        make_repo("c", language="Go", topics=["rag"]),
    ]
    index = write_partitioned(repos, tmp_path)

    assert index["languages"] == {"Python": 2, "Go": 1}
    assert index["categories"]["llm"] == 2
    assert index["categories"]["rag"] == 2
    for v in list(index["languages"].values()) + list(index["categories"].values()):
        assert isinstance(v, int) and v > 0
    # Languages are sorted by descending count (Python before Go).
    assert list(index["languages"].keys())[0] == "Python"


# -- per-repo record contract across every output file -------------------------


def _all_repo_records(data_dir):
    """Yield every repo dict written to recent/top_starred/by_*/full."""
    files = [
        data_dir / "recent.json",
        data_dir / "top_starred.json",
    ]
    files += list((data_dir / "by_category").glob("*.json"))
    files += list((data_dir / "by_language").glob("*.json"))
    files += list((data_dir / "full").glob("*.json"))
    for f in files:
        for rec in json.loads(f.read_text()):
            yield f.name, rec


def test_every_emitted_record_has_full_field_set(tmp_path):
    """Every repo dict in every partition file carries the full model field set."""
    recent_date = (datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    repos = [
        make_repo("a", language="Python", topics=["llm"], pushed_at=recent_date),
        make_repo("b", language="Go", topics=["rag"], pushed_at=recent_date),
    ]
    write_partitioned(repos, tmp_path)

    seen_any = False
    for fname, rec in _all_repo_records(tmp_path):
        seen_any = True
        assert set(rec.keys()) == EXPECTED_REPO_FIELDS, f"field drift in {fname}"
    assert seen_any, "no repo records were emitted"


def test_full_partition_is_lossless_round_trip(tmp_path):
    """The full/ partition reconstructs RepoMetadata objects equal to the input."""
    recent_date = (datetime.now(timezone.utc)).strftime("%Y-%m-%dT%H:%M:%SZ")
    original = [
        make_repo("a", stars=42, language="Python", topics=["llm", "rag"], pushed_at=recent_date),
        make_repo("b", stars=7, language="Rust", topics=[], pushed_at=recent_date),
    ]
    write_partitioned(original, tmp_path)

    part = json.loads((tmp_path / "full" / "repos_0000.json").read_text())
    reconstructed = [RepoMetadata(**rec) for rec in part]

    assert reconstructed == original


def test_output_is_valid_utf8_json(tmp_path):
    """All written files parse as JSON (atomic .tmp files are never left behind)."""
    write_partitioned([make_repo("a", topics=["ml"])], tmp_path)
    for f in tmp_path.rglob("*.json"):
        json.loads(f.read_text())  # raises if malformed
    assert list(tmp_path.rglob("*.tmp")) == []


# -- filename-sanitisation contract (path safety for consumers) ----------------


def test_category_filename_sanitised(tmp_path):
    """Topics with slashes/spaces are written to filesystem-safe filenames."""
    write_partitioned([make_repo("a", topics=["machine learning", "a/b"])], tmp_path)

    cat_dir = tmp_path / "by_category"
    assert (cat_dir / "machine_learning.json").exists()
    assert (cat_dir / "a_b.json").exists()
    # No path traversal / nested dirs created from the slash.
    assert not (cat_dir / "a").exists()


def test_language_unknown_bucket(tmp_path):
    """Repos with no primaryLanguage land in by_language/unknown.json."""
    repo = make_repo("a", language="Python")
    repo.primaryLanguage = None
    write_partitioned([repo], tmp_path)

    assert (tmp_path / "by_language" / "unknown.json").exists()
