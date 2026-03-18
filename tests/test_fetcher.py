"""Tests for reporium_db.fetcher."""

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


def _page(nodes: list[dict], has_next: bool, cursor: str = "c1", remaining: int = 4000) -> dict:
    """Build a mock GraphQL response page."""
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
    """Build a minimal repo GraphQL node."""
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
        "repositoryTopics": {"nodes": []},
        "licenseInfo": {"name": "MIT"},
        "issues": {"totalCount": 0},
        "defaultBranchRef": {"name": "main"},
    }


@respx.mock
async def test_fetch_single_page():
    """Fetches a single page and returns correct repos."""
    respx.post(GRAPHQL_URL).mock(return_value=httpx.Response(200, json=_page([_node("a")], False)))

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert len(repos) == 1
    assert repos[0].name == "a"
    assert meta["api_calls"] == 1
    assert meta["resumed"] is False


@respx.mock
async def test_fetch_two_pages():
    """Follows cursor pagination across multiple pages."""
    page1 = _page([_node("a"), _node("b")], has_next=True, cursor="c2")
    page2 = _page([_node("c")], has_next=False, cursor="c3")

    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert len(repos) == 3
    assert meta["api_calls"] == 2


@respx.mock
async def test_fetch_retries_429():
    """Retries on HTTP 429 and eventually succeeds."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(429),
        httpx.Response(429),
        httpx.Response(200, json=_page([_node("x")], False)),
    ]

    with patch("asyncio.sleep"):  # skip actual sleep in tests
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert len(repos) == 1


@respx.mock
async def test_fetch_retries_502():
    """Retries on HTTP 502 and eventually succeeds."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(502),
        httpx.Response(200, json=_page([_node("y")], False)),
    ]

    with patch("asyncio.sleep"):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert len(repos) == 1


@respx.mock
async def test_fetch_checkpoint_resume(tmp_path):
    """Resumes from a valid checkpoint file."""
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    ckpt.parent.mkdir(parents=True)
    ckpt.write_text(
        json.dumps(
            {
                "started_at": "2026-03-17T05:00:00+00:00",
                "last_cursor": "cursor-prev",
                "repos_processed": 100,
            }
        )
    )

    respx.post(GRAPHQL_URL).mock(return_value=httpx.Response(200, json=_page([_node("z")], False)))

    with patch("reporium_db.fetcher.CHECKPOINT_FILE", ckpt):
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert meta["resumed"] is True
    assert len(repos) == 1


@respx.mock
async def test_fetch_throttles_on_low_rate_limit():
    """Calls asyncio.sleep when rate limit drops below threshold."""
    # remaining=500 triggers throttle (threshold=0.8, so throttle when remaining < 1000)
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(200, json=_page([_node("a")], False, remaining=500))
    )

    with patch("asyncio.sleep") as mock_sleep:
        await fetch_all_repos(TEST_CONFIG)

    mock_sleep.assert_called()
