"""GitHub GraphQL fetcher with pagination, checkpointing, and rate-limit throttling."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import httpx

from .config import Config
from .models import RepoMetadata

logger = logging.getLogger(__name__)

GRAPHQL_URL = "https://api.github.com/graphql"
CHECKPOINT_FILE = Path("checkpoints/current_run.json")
_RATE_LIMIT_TOTAL = 5000  # GitHub authenticated GraphQL points/hour

QUERY = """
query($login: String!, $first: Int!, $after: String) {
  repositoryOwner(login: $login) {
    repositories(first: $first, after: $after, ownerAffiliations: OWNER) {
      pageInfo { hasNextPage endCursor }
      nodes {
        nameWithOwner name description stargazerCount forkCount
        primaryLanguage { name }
        pushedAt updatedAt createdAt
        isArchived isFork isEmpty
        repositoryTopics(first: 10) { nodes { topic { name } } }
        licenseInfo { name }
        issues(states: [OPEN]) { totalCount }
        defaultBranchRef { name }
      }
    }
  }
  rateLimit { remaining resetAt cost }
}
"""


def _parse_repo(node: dict[str, Any]) -> RepoMetadata:
    """Parse a single GraphQL repository node into a RepoMetadata dataclass."""
    return RepoMetadata(
        nameWithOwner=node["nameWithOwner"],
        name=node["name"],
        description=node.get("description"),
        stars=node["stargazerCount"],
        forks=node["forkCount"],
        primaryLanguage=(node["primaryLanguage"]["name"] if node.get("primaryLanguage") else None),
        pushedAt=node.get("pushedAt"),
        updatedAt=node.get("updatedAt"),
        createdAt=node["createdAt"],
        isArchived=node["isArchived"],
        isFork=node["isFork"],
        isEmpty=node["isEmpty"],
        topics=[n["topic"]["name"] for n in node.get("repositoryTopics", {}).get("nodes", [])],
        licenseName=(node["licenseInfo"]["name"] if node.get("licenseInfo") else None),
        openIssues=node["issues"]["totalCount"],
        defaultBranch=(node["defaultBranchRef"]["name"] if node.get("defaultBranchRef") else None),
    )


def _load_checkpoint() -> Optional[dict[str, Any]]:
    """Load checkpoint if it exists and is less than 24 hours old."""
    if not CHECKPOINT_FILE.exists():
        return None
    try:
        data = json.loads(CHECKPOINT_FILE.read_text())
        started = datetime.fromisoformat(data["started_at"])
        age_seconds = (datetime.now(timezone.utc) - started).total_seconds()
        if age_seconds < 86400:
            return data
        logger.info("Checkpoint is %ds old — starting fresh", int(age_seconds))
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not load checkpoint: %s", exc)
    return None


def _save_checkpoint(started_at: str, cursor: Optional[str], count: int) -> None:
    """Atomically save a checkpoint to disk."""
    CHECKPOINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    tmp = CHECKPOINT_FILE.with_suffix(".tmp")
    tmp.write_text(
        json.dumps({"started_at": started_at, "last_cursor": cursor, "repos_processed": count})
    )
    os.replace(tmp, CHECKPOINT_FILE)


async def _graphql_request(
    client: httpx.AsyncClient,
    token: str,
    variables: dict[str, Any],
    attempt: int = 0,
) -> dict[str, Any]:
    """Execute a GraphQL POST with exponential backoff on 429/502/503 (max 4 attempts)."""
    headers = {"Authorization": f"bearer {token}", "Content-Type": "application/json"}
    try:
        resp = await client.post(
            GRAPHQL_URL,
            json={"query": QUERY, "variables": variables},
            headers=headers,
            timeout=30,
        )
    except httpx.RequestError as exc:
        if attempt >= 3:
            raise
        wait = 2**attempt + 0.1 * attempt
        logger.warning("Request error (attempt %d): %s — retry in %.1fs", attempt + 1, exc, wait)
        await asyncio.sleep(wait)
        return await _graphql_request(client, token, variables, attempt + 1)

    if resp.status_code in (429, 502, 503):
        if attempt >= 3:
            resp.raise_for_status()
        wait = 2**attempt + 0.1 * attempt
        logger.warning("HTTP %d (attempt %d) — retry in %.1fs", resp.status_code, attempt + 1, wait)
        await asyncio.sleep(wait)
        return await _graphql_request(client, token, variables, attempt + 1)

    resp.raise_for_status()
    return resp.json()


async def fetch_all_repos(config: Config) -> tuple[list[RepoMetadata], dict[str, Any]]:
    """Fetch all repositories for the configured user/org via GraphQL pagination.

    Supports checkpoint resume, rate-limit throttling, and exponential-backoff retry.

    Returns:
        A tuple of (repos, meta) where meta contains api_calls, rate_info, and resumed flag.
    """
    t0 = time.monotonic()
    checkpoint = _load_checkpoint()
    cursor: Optional[str] = checkpoint["last_cursor"] if checkpoint else None
    repos: list[RepoMetadata] = []
    resumed = checkpoint is not None
    started_at = checkpoint["started_at"] if checkpoint else datetime.now(timezone.utc).isoformat()
    api_calls = 0
    rate_info: dict[str, Any] = {}

    if resumed:
        logger.info(
            "Resuming checkpoint: %d repos already processed", checkpoint["repos_processed"]
        )

    async with httpx.AsyncClient() as client:
        while True:
            variables: dict[str, Any] = {
                "login": config.gh_username,
                "first": 100,
                "after": cursor,
            }
            data = await _graphql_request(client, config.gh_token, variables)
            api_calls += 1

            if "errors" in data:
                logger.error("GraphQL errors: %s", data["errors"])

            rate_info = data["data"]["rateLimit"]
            remaining = rate_info["remaining"]
            throttle_threshold = _RATE_LIMIT_TOTAL * (1 - config.rate_limit_threshold)

            if remaining < throttle_threshold:
                logger.warning(
                    "Rate limit throttle: %d remaining (threshold=%d) — sleeping 5s",
                    remaining,
                    int(throttle_threshold),
                )
                await asyncio.sleep(5)

            page = data["data"]["repositoryOwner"]["repositories"]
            repos.extend(_parse_repo(n) for n in page["nodes"])

            if len(repos) % config.checkpoint_interval < 100:
                _save_checkpoint(started_at, cursor, len(repos))

            logger.info(
                "Fetched %d repos (api_calls=%d, rate_remaining=%d)",
                len(repos),
                api_calls,
                remaining,
            )

            if not page["pageInfo"]["hasNextPage"]:
                break
            cursor = page["pageInfo"]["endCursor"]

    if CHECKPOINT_FILE.exists():
        CHECKPOINT_FILE.unlink()

    elapsed = time.monotonic() - t0
    logger.info("Fetch complete: %d repos in %.1fs (%d API calls)", len(repos), elapsed, api_calls)
    return repos, {"api_calls": api_calls, "rate_info": rate_info, "resumed": resumed}
