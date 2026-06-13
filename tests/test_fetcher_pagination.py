"""Cursor-pagination edge cases for the GraphQL fetcher.

Pagination correctness is what keeps the merge idempotent at scale: the loop
must stop on hasNextPage=False, advance via endCursor, and never duplicate or
drop repos across page boundaries. These cover the edges the happy-path tests
skip: an empty owner, a trailing empty page, a null endCursor on the last page,
private-repo filtering spread across pages, and the api_calls/cursor accounting.

All traffic is respx-mocked; no network. The autouse isolate_checkpoint fixture
keeps every checkpoint write under tmp_path.
"""

from __future__ import annotations

import json

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


def _node(name, is_private=False):
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
        "isPrivate": is_private,
        "parent": None,
        "repositoryTopics": {"nodes": []},
        "licenseInfo": {"name": "MIT"},
        "issues": {"totalCount": 0},
        "defaultBranchRef": {"name": "main"},
    }


@respx.mock
async def test_empty_owner_single_empty_page():
    """An owner with zero repos returns one empty page and no repos."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(200, json=_page([], has_next=False, cursor=None))
    )

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert repos == []
    assert meta["api_calls"] == 1
    assert meta["resumed"] is False


@respx.mock
async def test_trailing_empty_page_is_consumed():
    """A non-empty page followed by an empty final page yields only real repos.

    GitHub can report hasNextPage=True then return an empty terminal page; the
    loop must still terminate cleanly and not invent or drop repos.
    """
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=_page([_node("a"), _node("b")], has_next=True, cursor="c2")),
        httpx.Response(200, json=_page([], has_next=False, cursor="c3")),
    ]

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b"]
    assert meta["api_calls"] == 2


@respx.mock
async def test_cursor_threaded_through_each_page():
    """Each request after the first uses the prior page's endCursor as ``after``."""
    afters = []

    pages = iter(
        [
            _page([_node("a")], has_next=True, cursor="cursor-1"),
            _page([_node("b")], has_next=True, cursor="cursor-2"),
            _page([_node("c")], has_next=False, cursor="cursor-3"),
        ]
    )

    def _handler(request: httpx.Request) -> httpx.Response:
        afters.append(json.loads(request.content)["variables"]["after"])
        return httpx.Response(200, json=next(pages))

    respx.post(GRAPHQL_URL).mock(side_effect=_handler)

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["a", "b", "c"]
    # First page starts cold; subsequent pages thread the prior endCursor.
    assert afters == [None, "cursor-1", "cursor-2"]
    assert meta["api_calls"] == 3


@respx.mock
async def test_null_end_cursor_on_final_page_does_not_paginate():
    """hasNextPage=False with a null endCursor terminates after one page."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(200, json=_page([_node("only")], has_next=False, cursor=None))
    )

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["only"]
    assert meta["api_calls"] == 1


@respx.mock
async def test_private_repos_filtered_across_pages():
    """Private repos are dropped on every page; public ones survive in order."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(
            200,
            json=_page(
                [_node("pub1"), _node("secret1", is_private=True)],
                has_next=True,
                cursor="c2",
            ),
        ),
        httpx.Response(
            200,
            json=_page(
                [_node("secret2", is_private=True), _node("pub2")],
                has_next=False,
            ),
        ),
    ]

    repos, _ = await fetch_all_repos(TEST_CONFIG)

    names = [r.name for r in repos]
    assert names == ["pub1", "pub2"]
    assert "secret1" not in names and "secret2" not in names


@respx.mock
async def test_all_private_page_yields_no_repos_but_counts_call():
    """A page of only private repos contributes zero repos but still counts the call."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(
            200,
            json=_page([_node("p", is_private=True)], has_next=False),
        )
    )

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert repos == []
    assert meta["api_calls"] == 1
