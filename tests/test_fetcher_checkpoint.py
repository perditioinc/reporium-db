"""Cross-run checkpoint-cache resilience tests for reporium_db.fetcher.

These tests target the documented production failure mode (run 26023064535):
a sustained GitHub-side 502 window that outlasts the per-request retry budget.
The durable fix is the cross-run checkpoint: a failed run must leave a
checkpoint on disk pointing at the *next* cursor so the following run resumes
forward instead of re-fetching page 1 (which would duplicate repos).

All GraphQL traffic is mocked with respx; asyncio.sleep is patched so retry
backoff does not slow the suite. No network, no real tokens.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import pytest
import respx

from reporium_db.config import Config
from reporium_db.fetcher import GRAPHQL_URL, _MAX_RETRIES, fetch_all_repos

TEST_CONFIG = Config(
    gh_token="test-token",
    gh_username="testuser",
    concurrency_graphql=5,
    rate_limit_threshold=0.8,
    checkpoint_interval=1000,
    nightly_tier_days=30,
    weekly_tier_days=365,
)


def _page(nodes: list[dict], has_next: bool, cursor: str = "c1", remaining: int = 4000) -> dict:
    return {
        "data": {
            "repositoryOwner": {
                "repositories": {
                    "pageInfo": {"hasNextPage": has_next, "endCursor": cursor},
                    "nodes": nodes,
                }
            },
            "rateLimit": {"remaining": remaining, "resetAt": "2026-03-17T06:00:00Z", "cost": 1},
        }
    }


def _node(name: str = "repo") -> dict:
    return {
        "nameWithOwner": f"testuser/{name}",
        "name": name,
        "description": "desc",
        "stargazerCount": 5,
        "forkCount": 1,
        "primaryLanguage": {"name": "Python"},
        "pushedAt": "2026-03-01T00:00:00Z",
        "updatedAt": "2026-03-01T00:00:00Z",
        "createdAt": "2025-01-01T00:00:00Z",
        "isArchived": False,
        "isFork": False,
        "isEmpty": False,
        "parent": None,
        "repositoryTopics": {"nodes": []},
        "licenseInfo": {"name": "MIT"},
        "issues": {"totalCount": 0},
        "defaultBranchRef": {"name": "main"},
    }


@respx.mock
async def test_checkpoint_removed_on_clean_success(tmp_path):
    """A fully successful single-process run leaves no checkpoint on disk.

    The unlink-on-success path must clear the checkpoint so a *new* nightly
    starts cold (page 1) rather than wrongly resuming a finished run.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(200, json=_page([_node("a")], has_next=False))
    )

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a"]
    assert meta["resumed"] is False
    assert not ckpt.exists(), "checkpoint must be unlinked after a clean finish"


@respx.mock
async def test_checkpoint_persisted_after_first_page_before_failure(tmp_path):
    """A multi-page run that fails on page 2 leaves a checkpoint at page 1's cursor.

    This is the cross-run durability invariant: page 1 succeeds and checkpoints
    the *advanced* cursor (endCursor of page 1); page 2 then 502s past the retry
    budget and the run raises. Because the failure path does NOT unlink, the
    checkpoint survives pointing at page 2 -- the next run resumes forward.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"

    page1 = _page([_node("p1a"), _node("p1b")], has_next=True, cursor="CURSOR_PAGE2")
    # page 1 OK, then unlimited 502s for page 2 -> exhausts retries -> raises.
    side_effects = [httpx.Response(200, json=page1)] + [
        httpx.Response(502) for _ in range(_MAX_RETRIES + 5)
    ]
    route = respx.post(GRAPHQL_URL)
    route.side_effect = side_effects

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt), patch("asyncio.sleep"):
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await fetch_all_repos(TEST_CONFIG)

    assert excinfo.value.response.status_code == 502
    # Checkpoint must survive the failure (not unlinked) and point forward.
    assert ckpt.exists(), "failed multi-page run must preserve its checkpoint"
    saved = json.loads(ckpt.read_text())
    assert saved["last_cursor"] == "CURSOR_PAGE2"
    assert saved["repos_processed"] == 2


@respx.mock
async def test_resume_continues_from_checkpoint_cursor(tmp_path):
    """A second run reads the checkpoint and sends its cursor as the 'after' arg.

    Asserts real resume behavior: the resumed run's first GraphQL request carries
    the checkpoint cursor (not None), proving it does not re-fetch from page 1.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    ckpt.parent.mkdir(parents=True)
    from datetime import datetime, timezone

    ckpt.write_text(
        json.dumps(
            {
                "started_at": datetime.now(timezone.utc).isoformat(),
                "last_cursor": "CURSOR_PAGE2",
                "repos_processed": 2,
            }
        )
    )

    captured: list[dict] = []

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content)["variables"])
        return httpx.Response(200, json=_page([_node("p2a")], has_next=False))

    respx.post(GRAPHQL_URL).mock(side_effect=_capture)

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt), patch("asyncio.sleep"):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert meta["resumed"] is True
    assert [r.name for r in repos] == ["p2a"]
    # The very first request of the resumed run must use the checkpoint cursor.
    assert captured[0]["after"] == "CURSOR_PAGE2"
    # Clean finish on resume clears the checkpoint.
    assert not ckpt.exists()


@respx.mock
async def test_two_run_resume_is_idempotent_no_duplicate_merge(tmp_path):
    """End-to-end cross-run merge: run A (page 1) fails on page 2, run B resumes
    and finishes page 2. The merged repo set across both runs has NO duplicates
    and equals page1 + page2 exactly.

    This is the core idempotency invariant: resuming from the advanced cursor
    means page 1's repos are never re-emitted in run B.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"

    # ---- Run A: page 1 succeeds (checkpoints CURSOR_PAGE2), page 2 hard-fails.
    page1 = _page([_node("alpha"), _node("beta")], has_next=True, cursor="CURSOR_PAGE2")
    route_a = respx.post(GRAPHQL_URL)
    route_a.side_effect = [httpx.Response(200, json=page1)] + [
        httpx.Response(502) for _ in range(_MAX_RETRIES + 3)
    ]

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt), patch("asyncio.sleep"):
        with pytest.raises(httpx.HTTPStatusError):
            await fetch_all_repos(TEST_CONFIG)

    run_a_partial = json.loads(ckpt.read_text())
    assert run_a_partial["last_cursor"] == "CURSOR_PAGE2"

    # ---- Run B: resumes from CURSOR_PAGE2, fetches page 2, finishes.
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(
            200, json=_page([_node("gamma"), _node("delta")], has_next=False)
        )
    )

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt), patch("asyncio.sleep"):
        repos_b, meta_b = await fetch_all_repos(TEST_CONFIG)

    assert meta_b["resumed"] is True
    # Run B only emits page-2 repos; page-1 repos are NOT re-fetched.
    names_b = [r.name for r in repos_b]
    assert names_b == ["gamma", "delta"]

    # Cross-run merged set = page1 (from run A's checkpoint progress) + page2.
    merged = {"alpha", "beta"} | set(names_b)
    assert merged == {"alpha", "beta", "gamma", "delta"}
    # Idempotent: no page-1 repo leaked into run B.
    assert "alpha" not in names_b and "beta" not in names_b
