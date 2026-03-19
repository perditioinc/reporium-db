"""Data models for reporium-db."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class RepoMetadata:
    """Metadata for a single GitHub repository."""

    nameWithOwner: str
    name: str
    description: Optional[str]
    stars: int
    forks: int
    primaryLanguage: Optional[str]
    pushedAt: Optional[str]
    updatedAt: Optional[str]
    createdAt: str
    isArchived: bool
    isFork: bool
    isEmpty: bool
    topics: list[str]
    licenseName: Optional[str]
    openIssues: int
    defaultBranch: Optional[str]
    parentRepo: Optional[str] = None
    parentStars: Optional[int] = None
    parentForks: Optional[int] = None


@dataclass
class DatasetDiff:
    """Result of comparing today's repos against yesterday's snapshot."""

    new_repos: list[str]
    removed_repos: list[str]
    updated_repos: list[str]
    unchanged_count: int


@dataclass
class SyncRun:
    """Statistics for a single sync run."""

    started_at: str
    completed_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    total_fetched: int = 0
    checked: int = 0
    skipped_schedule: int = 0
    new_repos: int = 0
    updated_repos: int = 0
    api_calls_used: int = 0
    rate_limit_remaining: int = 0
    errors: list[str] = field(default_factory=list)
    checkpoint_resumed: bool = False


@dataclass
class ScheduleEntry:
    """Scheduling metadata for a single repo."""

    repo_name: str
    last_checked: str
    tier: str
    upstream_pushed_at: Optional[str] = None
