"""Partition-output schema/contract tests for reporium_db.partitioner.

The Platform Fit section of the README declares four downstream consumers that
read these files directly:

  * reporium-api      -> data/ (index.json, by_language/, by_category/, full/)
  * reporium-ingestion-> pending_enrichment.json (covered in test_differ)
  * reporium-dataset  -> index.json
  * reporium-metrics  -> data/index.json

These tests assert the EMITTED JSON honors that contract using offline fixtures
only (tmp_path). They check the index envelope shape, the per-repo record
schema in every list output, deterministic sort orders, and category/language
fan-out -- the things a consumer parses and would break on if silently changed.

No data/snapshot/checkpoint artifacts are committed; everything lives in tmp_path.
"""

from __future__ import annotations

import json
from dataclasses import fields

from reporium_db.models import RepoMetadata
from reporium_db.partitioner import (
    TOP_STARRED_COUNT,
    write_partitioned,
)
from tests.conftest import make_repo

# The canonical per-repo record the partitioner emits is the full dataclass
# field set. Consumers index on these exact keys; dropping or renaming one is a
# breaking change. We derive the expected key set from the model so this test
# tracks the model but still fails loudly if a field is silently omitted on
# serialization.
EXPECTED_REPO_KEYS = {f.name for f in fields(RepoMetadata)}

# Keys the API/dataset/metrics surfaces explicitly depend on. Spelled out (not
# just derived) so a rename in the model is caught as an intentional contract
# change, not auto-accepted.
CORE_CONTRACT_KEYS = {
    "nameWithOwner",
    "name",
    "description",
    "stars",
    "forks",
    "primaryLanguage",
    "pushedAt",
    "topics",
    "isPrivate",
}


def _load(path):
    return json.loads(path.read_text())


def test_index_envelope_contract(tmp_path):
    """index.json has the meta/categories/languages envelope consumers expect."""
    repos = [
        make_repo("a", language="Python", topics=["llm", "rag"]),
        make_repo("b", language="Go", topics=["llm"]),
    ]
    write_partitioned(repos, tmp_path)
    index = _load(tmp_path / "index.json")

    # Top-level keys.
    assert set(index.keys()) == {"meta", "categories", "languages"}

    # meta sub-contract: total + version + an ISO last_updated string.
    meta = index["meta"]
    assert set(meta.keys()) == {"total", "last_updated", "version"}
    assert meta["total"] == 2
    assert meta["version"] == "1.0.0"
    assert isinstance(meta["last_updated"], str) and meta["last_updated"]

    # Aggregations are name->count maps with correct counts.
    assert index["languages"] == {"Python": 1, "Go": 1}
    assert index["categories"]["llm"] == 2
    assert index["categories"]["rag"] == 1


def test_index_aggregations_sorted_descending_by_count(tmp_path):
    """categories and languages are emitted sorted by count descending.

    Consumers render these as ranked facets; order is part of the contract.
    """
    repos = [
        make_repo("a", language="Python", topics=["common", "common", "rare"]),
        make_repo("b", language="Python", topics=["common"]),
        make_repo("c", language="Go", topics=["common"]),
    ]
    write_partitioned(repos, tmp_path)
    index = _load(tmp_path / "index.json")

    lang_counts = list(index["languages"].values())
    cat_counts = list(index["categories"].values())
    assert lang_counts == sorted(lang_counts, reverse=True)
    assert cat_counts == sorted(cat_counts, reverse=True)
    # Most common language first.
    assert next(iter(index["languages"])) == "Python"


def test_full_partition_record_schema(tmp_path):
    """Every record in full/ carries the complete per-repo key set."""
    repos = [make_repo("a"), make_repo("b")]
    write_partitioned(repos, tmp_path)

    records = _load(tmp_path / "full" / "repos_0000.json")
    assert len(records) == 2
    for rec in records:
        assert set(rec.keys()) == EXPECTED_REPO_KEYS
        assert CORE_CONTRACT_KEYS.issubset(rec.keys())


def test_recent_and_top_starred_record_schema(tmp_path):
    """recent.json and top_starred.json records honor the same per-repo schema."""
    from datetime import datetime, timedelta, timezone

    fresh = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    repos = [
        make_repo("hot", stars=900, pushed_at=fresh),
        make_repo("warm", stars=10, pushed_at=fresh),
    ]
    write_partitioned(repos, tmp_path)

    for fname in ("recent.json", "top_starred.json"):
        records = _load(tmp_path / fname)
        assert records, f"{fname} should not be empty"
        for rec in records:
            assert CORE_CONTRACT_KEYS.issubset(rec.keys())
            # Star count must be a JSON number (consumers sort/filter on it).
            assert isinstance(rec["stars"], int)


def test_top_starred_is_sorted_and_capped(tmp_path):
    """top_starred.json is strictly star-descending and capped at the limit."""
    repos = [make_repo(f"r{i}", owner=f"u{i}", stars=i) for i in range(TOP_STARRED_COUNT + 25)]
    write_partitioned(repos, tmp_path)

    top = _load(tmp_path / "top_starred.json")
    assert len(top) == TOP_STARRED_COUNT
    star_seq = [r["stars"] for r in top]
    assert star_seq == sorted(star_seq, reverse=True)
    # The single highest-star repo is present and first.
    assert star_seq[0] == TOP_STARRED_COUNT + 24


def test_by_language_and_category_fanout_contract(tmp_path):
    """by_language/ and by_category/ emit one file per key, each a record list.

    A repo with no primaryLanguage lands in unknown.json (consumers rely on the
    'unknown' bucket existing rather than the repo vanishing).
    """
    repos = [
        make_repo("p", language="Python", topics=["llm"]),
        make_repo("g", language="Go", topics=["llm", "infra"]),
        make_repo("n", language=None, topics=[]),
    ]
    write_partitioned(repos, tmp_path)

    # Languages: one file each, unknown bucket for the None-language repo.
    py = _load(tmp_path / "by_language" / "Python.json")
    go = _load(tmp_path / "by_language" / "Go.json")
    unknown = _load(tmp_path / "by_language" / "unknown.json")
    assert [r["name"] for r in py] == ["p"]
    assert [r["name"] for r in go] == ["g"]
    assert [r["name"] for r in unknown] == ["n"]

    # Categories: llm appears in two repos, infra in one.
    llm = _load(tmp_path / "by_category" / "llm.json")
    infra = _load(tmp_path / "by_category" / "infra.json")
    assert {r["name"] for r in llm} == {"p", "g"}
    assert {r["name"] for r in infra} == {"g"}

    # Every record across the fan-out honors the core schema.
    for rec in py + go + unknown + llm + infra:
        assert CORE_CONTRACT_KEYS.issubset(rec.keys())


def test_emitted_json_is_valid_and_total_consistent_across_files(tmp_path):
    """index.meta.total equals the record count in the full partitions.

    Cross-file consistency invariant: a consumer that trusts index.total must
    find that many records in full/. Guards against a partitioner that writes a
    stale or mismatched total.
    """
    repos = [make_repo(f"r{i}", owner=f"u{i}") for i in range(7)]
    write_partitioned(repos, tmp_path)

    index = _load(tmp_path / "index.json")
    full_records = []
    for part in sorted((tmp_path / "full").glob("repos_*.json")):
        full_records.extend(_load(part))

    assert index["meta"]["total"] == len(full_records) == 7
