"""Shared pytest fixtures for reporium-db tests."""

from __future__ import annotations

import pytest

from reporium_db.models import RepoMetadata


def make_repo(
    name: str = "test-repo",
    owner: str = "testuser",
    stars: int = 10,
    pushed_at: str = "2026-03-01T00:00:00Z",
    description: str = "A test repo",
    topics: list[str] | None = None,
    language: str = "Python",
    is_fork: bool = False,
) -> RepoMetadata:
    """Factory for RepoMetadata test fixtures."""
    if topics is None:
        topics = ["ai", "tools"]
    return RepoMetadata(
        nameWithOwner=f"{owner}/{name}",
        name=name,
        description=description,
        stars=stars,
        forks=0,
        primaryLanguage=language,
        pushedAt=pushed_at,
        updatedAt=pushed_at,
        createdAt="2025-01-01T00:00:00Z",
        isArchived=False,
        isFork=is_fork,
        isEmpty=False,
        topics=topics,
        licenseName="MIT",
        openIssues=0,
        defaultBranch="main",
        parentRepo=None,
        parentStars=None,
        parentForks=None,
    )


@pytest.fixture
def sample_repos() -> list[RepoMetadata]:
    """Three repos with different star counts for sort/partition tests."""
    return [
        make_repo("repo-a", stars=100, language="Python"),
        make_repo("repo-b", stars=50, language="TypeScript"),
        make_repo("repo-c", stars=200, language="Python"),
    ]


@pytest.fixture
def graphql_page_factory():
    """Return a factory that builds a mock GraphQL page response."""

    def _make(
        repos: list[dict],
        has_next: bool = False,
        cursor: str = "cursor-abc",
        remaining: int = 4000,
    ) -> dict:
        nodes = [
            {
                "nameWithOwner": r.get("nameWithOwner", "user/repo"),
                "name": r.get("name", "repo"),
                "description": r.get("description"),
                "stargazerCount": r.get("stars", 0),
                "forkCount": r.get("forks", 0),
                "primaryLanguage": ({"name": r["language"]} if r.get("language") else None),
                "pushedAt": r.get("pushedAt", "2026-03-01T00:00:00Z"),
                "updatedAt": r.get("pushedAt", "2026-03-01T00:00:00Z"),
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
            for r in repos
        ]
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

    return _make
