"""Cursor-pagination edge-case tests for reporium_db.fetch_all_repos.

Covers the boundary conditions of the GraphQL cursor loop that the existing
single/two-page happy-path tests do not exercise:

  * empty page (zero nodes, hasNextPage False)  -> zero repos, one call
  * empty first page that still has a next page  -> follows cursor, no crash
  * last page (hasNextPage False) terminates the loop with no extra request
  * a None endCursor on a non-final page is forwarded verbatim as 'after'
  * the cursor sent on page N+1 is exactly the endCursor returned by page N

All traffic is mocked with respx; no network. asyncio.sleep is patched where
throttle/backoff could otherwise run.
"""

from __future__ import annotations

import json
from unittest.mock import patch

import httpx
import respx

from reporium_db.config import Config
from reporium_db.fetcher import GRAPHQL_URL, fetch_all_repos

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


def _node(name="repo"):
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
async def test_empty_single_page_returns_no_repos():
    """An owner with zero repos: one call, empty result, no crash."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(200, json=_page([], has_next=False, cursor=None))
    )

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert repos == []
    assert meta["api_calls"] == 1


@respx.mock
async def test_empty_first_page_still_follows_next_cursor():
    """An empty page with hasNextPage True must still advance to the next page."""
    page1 = _page([], has_next=True, cursor="c2")
    page2 = _page([_node("late")], has_next=False)
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["late"]
    assert meta["api_calls"] == 2


@respx.mock
async def test_last_page_terminates_without_extra_request():
    """hasNextPage False stops the loop -- exactly the pages served are called."""
    page1 = _page([_node("a")], has_next=True, cursor="c2")
    page2 = _page([_node("b")], has_next=False, cursor="c3")
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    assert meta["api_calls"] == 2
    # respx records exactly the two calls; no speculative third request.
    assert route.call_count == 2


@respx.mock
async def test_cursor_forwarded_matches_previous_end_cursor():
    """The 'after' arg on each page equals the prior page's endCursor.

    First page must be requested with after=None; the second with the exact
    endCursor the first page returned. This pins the cursor-threading contract.
    """
    captured: list = []

    pages = [
        httpx.Response(200, json=_page([_node("a")], has_next=True, cursor="CURSOR_X")),
        httpx.Response(200, json=_page([_node("b")], has_next=False, cursor="CURSOR_Y")),
    ]
    call_index = {"i": 0}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content)["variables"]["after"])
        resp = pages[call_index["i"]]
        call_index["i"] += 1
        return resp

    respx.post(GRAPHQL_URL).mock(side_effect=_capture)

    repos, _ = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    assert captured == [None, "CURSOR_X"]


@respx.mock
async def test_none_end_cursor_on_nonfinal_page_is_forwarded_verbatim():
    """A malformed page that says hasNextPage True but endCursor None must not
    crash; the None cursor is forwarded as 'after' (GitHub treats it as start).

    This guards the loop against a None-cursor edge instead of raising.
    """
    captured: list = []
    pages = [
        httpx.Response(200, json=_page([_node("a")], has_next=True, cursor=None)),
        httpx.Response(200, json=_page([_node("b")], has_next=False, cursor="end")),
    ]
    call_index = {"i": 0}

    def _capture(request: httpx.Request) -> httpx.Response:
        captured.append(json.loads(request.content)["variables"]["after"])
        resp = pages[call_index["i"]]
        call_index["i"] += 1
        return resp

    respx.post(GRAPHQL_URL).mock(side_effect=_capture)

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    assert meta["api_calls"] == 2
    # Second request forwarded the None cursor verbatim (no fabricated value).
    assert captured[1] is None
