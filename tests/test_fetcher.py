"""Tests for reporium_db.fetcher."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import patch

import httpx
import pytest
import respx

from reporium_db.config import Config
from reporium_db.fetcher import (
    _BACKOFF_CAP_SECONDS,
    _MAX_RETRIES,
    GRAPHQL_URL,
    _backoff_seconds,
    _retry_delay_seconds,
    fetch_all_repos,
)

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


def _node(name: str = "repo", is_fork: bool = False, parent: dict | None = None) -> dict:
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
        "isFork": is_fork,
        "isEmpty": False,
        "parent": parent,
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
async def test_fetch_retries_secondary_rate_limit_403():
    """Retries on GitHub secondary-rate-limit 403s and eventually succeeds."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(
            403,
            headers={"Retry-After": "3"},
            text="You have exceeded a secondary rate limit. Please wait a few minutes before you try again.",
        ),
        httpx.Response(200, json=_page([_node("retry-403")], False)),
    ]

    with patch("asyncio.sleep") as mock_sleep:
        repos, meta = await fetch_all_repos(TEST_CONFIG)

    assert len(repos) == 1
    assert repos[0].name == "retry-403"
    mock_sleep.assert_called_with(3.0)


@respx.mock
async def test_fetch_does_not_retry_non_rate_limit_403():
    """Fails fast on permanent 403s that do not signal throttling."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(403, text="Resource not accessible by integration")
    )

    with patch("asyncio.sleep") as mock_sleep:
        try:
            await fetch_all_repos(TEST_CONFIG)
        except httpx.HTTPStatusError as exc:
            assert exc.response.status_code == 403
        else:
            raise AssertionError("Expected HTTPStatusError for non-retryable 403")

    mock_sleep.assert_not_called()


@respx.mock
async def test_fetch_checkpoint_resume(tmp_path):
    """Resumes from a valid checkpoint file."""
    ckpt = tmp_path / "checkpoints" / "current_run.json"
    ckpt.parent.mkdir(parents=True)
    ckpt.write_text(
        json.dumps(
            {
                "started_at": datetime.now(timezone.utc).isoformat(),
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


# --------------------------------------------------------------------------
# Backoff / retry-budget unit tests (table-driven)
# --------------------------------------------------------------------------


@pytest.mark.parametrize("attempt", [0, 1, 2, 3, 5, 8, 12])
def test_backoff_full_jitter_within_bounds(attempt):
    """Full-jitter backoff is always in [0, min(cap, 2**attempt)] and capped."""
    ceiling = min(_BACKOFF_CAP_SECONDS, 2.0**attempt)
    # Sample many draws; every draw must respect the bounds.
    for _ in range(200):
        delay = _backoff_seconds(attempt)
        assert 0.0 <= delay <= ceiling
        assert delay <= _BACKOFF_CAP_SECONDS


def test_backoff_is_randomized_not_constant():
    """Full jitter must actually vary (de-synchronise retries), not be fixed."""
    samples = {round(_backoff_seconds(6), 4) for _ in range(50)}
    assert len(samples) > 1, "backoff should be jittered, got a constant value"


@pytest.mark.parametrize(
    "header,attempt,expect_exact,expect_max",
    [
        # Retry-After honored verbatim when within the cap.
        ({"Retry-After": "3"}, 0, 3.0, None),
        ({"Retry-After": "45"}, 9, 45.0, None),
        # Hostile/huge Retry-After is clamped to the cap, never stalls the job.
        ({"Retry-After": "99999"}, 0, _BACKOFF_CAP_SECONDS, None),
        # No Retry-After => full-jitter path, bounded by the cap.
        ({}, 0, None, _BACKOFF_CAP_SECONDS),
        ({}, 20, None, _BACKOFF_CAP_SECONDS),
    ],
)
def test_retry_delay_seconds_table(header, attempt, expect_exact, expect_max):
    """_retry_delay_seconds honors Retry-After (clamped) else jittered backoff."""
    resp = httpx.Response(502, headers=header)
    delay = _retry_delay_seconds(resp, attempt)
    if expect_exact is not None:
        assert delay == expect_exact
    if expect_max is not None:
        assert 0.0 <= delay <= expect_max


@respx.mock
async def test_fetch_502_then_200_recovers():
    """A single 502 followed by 200 recovers without surfacing an error."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(502),
        httpx.Response(200, json=_page([_node("ok")], False)),
    ]
    with patch("asyncio.sleep"):
        repos, meta = await fetch_all_repos(TEST_CONFIG)
    assert [r.name for r in repos] == ["ok"]
    assert meta["api_calls"] == 1


@respx.mock
async def test_fetch_repeated_502_exhausts_and_raises():
    """Sustained 502 (longer than the retry budget) raises a clear 502 error.

    This is the live failure mode (run 26023064535): a GitHub-side 502 window
    that outlasts every retry. We must fail loud (non-zero exit) — never a
    silent green — and the surfaced error must be the actual 502.
    """
    # _MAX_RETRIES retries after the first try => _MAX_RETRIES + 1 total 502s,
    # plus extras to be safe; respx repeats the last side_effect.
    respx.post(GRAPHQL_URL).mock(return_value=httpx.Response(502))

    with patch("asyncio.sleep"):
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await fetch_all_repos(TEST_CONFIG)

    assert excinfo.value.response.status_code == 502


@respx.mock
async def test_fetch_502_budget_count_is_max_retries_plus_one():
    """Exactly _MAX_RETRIES retries are attempted (first try + _MAX_RETRIES)."""
    # One fewer 502 than the budget then a 200 => must succeed.
    side_effects = [httpx.Response(502)] * _MAX_RETRIES
    side_effects.append(httpx.Response(200, json=_page([_node("recovered")], False)))
    route = respx.post(GRAPHQL_URL)
    route.side_effect = side_effects

    with patch("asyncio.sleep"):
        repos, _ = await fetch_all_repos(TEST_CONFIG)

    assert [r.name for r in repos] == ["recovered"]


@respx.mock
async def test_fetch_503_504_are_retryable():
    """503 and 504 (Cloud-front / gateway timeouts) are retried like 502."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.Response(503),
        httpx.Response(504),
        httpx.Response(200, json=_page([_node("gw")], False)),
    ]
    with patch("asyncio.sleep"):
        repos, _ = await fetch_all_repos(TEST_CONFIG)
    assert [r.name for r in repos] == ["gw"]


@respx.mock
async def test_fetch_non_retryable_4xx_passes_through_immediately():
    """A non-throttling 401/404 is NOT retried — fails fast, no sleeps."""
    respx.post(GRAPHQL_URL).mock(
        return_value=httpx.Response(401, text="Bad credentials")
    )
    with patch("asyncio.sleep") as mock_sleep:
        with pytest.raises(httpx.HTTPStatusError) as excinfo:
            await fetch_all_repos(TEST_CONFIG)
    assert excinfo.value.response.status_code == 401
    mock_sleep.assert_not_called()


@respx.mock
async def test_fetch_request_error_then_success_recovers():
    """A transient transport error (timeout/conn reset) is retried then recovers."""
    route = respx.post(GRAPHQL_URL)
    route.side_effect = [
        httpx.ConnectError("connection reset by peer"),
        httpx.Response(200, json=_page([_node("net")], False)),
    ]
    with patch("asyncio.sleep"):
        repos, _ = await fetch_all_repos(TEST_CONFIG)
    assert [r.name for r in repos] == ["net"]


@respx.mock
async def test_fetch_persistent_request_error_raises():
    """A transport error that never clears raises (loud fail, not silent)."""
    respx.post(GRAPHQL_URL).mock(side_effect=httpx.ConnectError("dns failure"))
    with patch("asyncio.sleep"):
        with pytest.raises(httpx.ConnectError):
            await fetch_all_repos(TEST_CONFIG)
