"""Checkpoint-cache resilience tests for the GraphQL fetcher.

These cover the durable-resume contract that the nightly sync depends on:

  * a sustained GitHub-side 502 exhausts the retry budget and raises, but
    leaves the checkpoint on disk so the NEXT run resumes instead of cold
    restarting (the live failure mode documented in fetcher.py);
  * resuming from a checkpoint starts at the saved cursor (the NEXT page), so
    the merge across two runs is idempotent and never double-counts;
  * the checkpoint is written with the post-advance cursor on every page;
  * a transient lock on the checkpoint at cleanup time does NOT fail an
    otherwise-successful sync (cross-platform robustness, Windows in CI).

All GraphQL traffic is mocked with respx; no network, keys, or cloud are used.
The autouse ``isolate_checkpoint`` fixture (conftest) pins CHECKPOINT_FILE to a
per-test tmp path, so nothing here writes a checkpoint into the repo tree.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import httpx
import pytest
import respx

import reporium_db.fetcher as fetcher
from reporium_db.config import Config
from reporium_db.fetcher import _MAX_RETRIES, GRAPHQL_URL, fetch_all_repos

TEST_CONFIG = Config(
    gh_token="test-token",
    gh_username="testuser",
    concurrency_graphql=5,
    rate_limit_threshold=0.8,
    checkpoint_interval=1000,
    nightly_tier_days=30,
    weekly_tier_days=365,
)


def _page(nodes, has_next, cursor="c1", remaining=4000):
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


def _node(name):
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


# -- checkpoint persistence on sustained-502 (durable resume across runs) ------


@respx.mock
async def test_sustained_502_leaves_resumable_checkpoint(tmp_path):
    """A sustained 502 raises but leaves the checkpoint for the NEXT run.

    Run 1 fetches page 1 (advancing + saving the checkpoint with the page-2
    cursor) then hits an unrecoverable 502 on page 2. The checkpoint file must
    survive the raise so a follow-up run can resume - this is the documented
    durable-resume fix, and the whole point of writing the checkpoint per page.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    page1 = _page([_node("a")], has_next=True, cursor="page2-cursor")

    responses = [httpx.Response(200, json=page1)]
    responses += [httpx.Response(502)] * (_MAX_RETRIES + 2)  # exhaust page-2 budget
    route = respx.post(GRAPHQL_URL)
    route.side_effect = responses

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        with patch("asyncio.sleep"):
            with pytest.raises(httpx.HTTPStatusError) as excinfo:
                await fetch_all_repos(TEST_CONFIG)

    assert excinfo.value.response.status_code == 502
    # The checkpoint must STILL be on disk (resume path), pointing at page 2.
    assert ckpt.exists(), "checkpoint must survive a hard 502 for the next run"
    saved = json.loads(ckpt.read_text())
    assert saved["last_cursor"] == "page2-cursor"
    assert saved["repos_processed"] == 1


@respx.mock
async def test_resume_starts_at_saved_cursor_no_double_count(tmp_path):
    """Resuming uses the saved cursor as ``after`` and merges idempotently.

    Run 1 saved a checkpoint at ``page2-cursor`` after persisting page 1's repo
    in a *prior* process. Run 2 (this one) must POST with after=page2-cursor and
    return ONLY page-2 repos - it must not re-fetch page 1. The combined view
    across runs therefore has no duplicates: the merge is idempotent.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    ckpt.parent.mkdir(parents=True)
    ckpt.write_text(
        json.dumps(
            {
                # < 24h old so the checkpoint is honored
                "started_at": datetime.now(timezone.utc).isoformat(),
                "last_cursor": "page2-cursor",
                "repos_processed": 1,
            }
        )
    )

    captured = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        captured["after"] = body["variables"]["after"]
        return httpx.Response(200, json=_page([_node("b")], has_next=False))

    respx.post(GRAPHQL_URL).mock(side_effect=_handler)

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert meta["resumed"] is True
    # The first (only) request resumed from the saved cursor, not from None.
    assert captured["after"] == "page2-cursor"
    # Only page-2 repos are fetched this run; page 1 is NOT re-fetched.
    assert [r.name for r in repos] == ["b"]
    # Successful completion clears the checkpoint.
    assert not ckpt.exists()


@respx.mock
async def test_checkpoint_saved_with_post_advance_cursor(tmp_path):
    """Each saved checkpoint stores the NEXT page's cursor, not the current one.

    Saving the pre-advance cursor would re-fetch the just-processed page on
    resume and double-count. We assert the checkpoint written mid-stream points
    at page 2 (endCursor of page 1), proving the advance-then-save ordering.
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    page1 = _page([_node("a")], has_next=True, cursor="cursor-after-page1")
    page2 = _page([_node("b")], has_next=False, cursor="cursor-after-page2")

    saved_cursors = []
    real_save = fetcher._save_checkpoint

    def _spy_save(started_at, cursor, count):
        saved_cursors.append(cursor)
        return real_save(started_at, cursor, count)

    route = respx.post(GRAPHQL_URL)
    route.side_effect = [httpx.Response(200, json=page1), httpx.Response(200, json=page2)]

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        with patch.object(fetcher, "_save_checkpoint", _spy_save):
            repos, _ = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    # Exactly one checkpoint save happened (after page 1, before page 2),
    # storing the post-advance cursor.
    assert saved_cursors == ["cursor-after-page1"]


@respx.mock
async def test_stale_checkpoint_over_24h_is_ignored(tmp_path):
    """A checkpoint older than 24h is discarded; the run starts cold (after=None)."""
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    ckpt.parent.mkdir(parents=True)
    old = datetime.now(timezone.utc) - timedelta(hours=30)
    ckpt.write_text(
        json.dumps(
            {"started_at": old.isoformat(), "last_cursor": "stale", "repos_processed": 999}
        )
    )

    captured = {}

    def _handler(request: httpx.Request) -> httpx.Response:
        captured["after"] = json.loads(request.content)["variables"]["after"]
        return httpx.Response(200, json=_page([_node("fresh")], has_next=False))

    respx.post(GRAPHQL_URL).mock(side_effect=_handler)

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert meta["resumed"] is False
    assert captured["after"] is None  # cold start, stale cursor not used
    assert [r.name for r in repos] == ["fresh"]


# -- cleanup robustness: a locked checkpoint must not fail a green sync --------


@respx.mock
async def test_locked_checkpoint_at_cleanup_does_not_fail_sync(tmp_path):
    """A transient PermissionError on the final unlink is swallowed (green stays green).

    Windows briefly share-locks files during AV/indexing; the cleanup unlink is
    best-effort (a leftover < 24h checkpoint is resumable). A successful fetch
    must NOT be turned red by a momentary lock on cleanup. We drive TWO pages so
    a checkpoint is actually written to disk and the cleanup unlink is exercised
    (a single-page fetch never writes one, so it would not test cleanup at all).
    """
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=_page([_node("a")], has_next=True, cursor="c2")),
        httpx.Response(200, json=_page([_node("b")], has_next=False)),
    ]

    real_unlink = type(ckpt).unlink

    def _locked_unlink(self, *args, **kwargs):
        # Only the cleanup path targets the checkpoint file; raise there.
        if str(self) == str(ckpt):
            raise PermissionError("WinError 32: file is being used by another process")
        return real_unlink(self, *args, **kwargs)

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        with patch.object(type(ckpt), "unlink", _locked_unlink):
            # Must NOT raise despite the cleanup unlink failing.
            repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    assert meta["api_calls"] == 2
    # The checkpoint is intentionally left on disk (resumable) rather than
    # crashing the run - it will be aged out next run.
    assert ckpt.exists()


@respx.mock
async def test_missing_checkpoint_at_cleanup_is_silent(tmp_path):
    """A FileNotFoundError on cleanup (already removed) is swallowed silently."""
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=_page([_node("a")], has_next=True, cursor="c2")),
        httpx.Response(200, json=_page([_node("b")], has_next=False)),
    ]

    real_unlink = type(ckpt).unlink

    def _gone_unlink(self, *args, **kwargs):
        if str(self) == str(ckpt):
            raise FileNotFoundError("already gone")
        return real_unlink(self, *args, **kwargs)

    with patch.object(fetcher, "CHECKPOINT_FILE", ckpt):
        with patch.object(type(ckpt), "unlink", _gone_unlink):
            repos, _ = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
