"""Configuration loaded from environment variables."""

from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv

load_dotenv()


@dataclass
class Config:
    """All runtime configuration for reporium-db."""

    gh_token: str
    gh_username: str
    concurrency_graphql: int
    rate_limit_threshold: float
    checkpoint_interval: int
    nightly_tier_days: int
    weekly_tier_days: int


def load_config() -> Config:
    """Load configuration from environment variables.

    Raises:
        ValueError: If a required variable is missing.
    """
    token = os.getenv("GH_TOKEN")
    if not token:
        raise ValueError("GH_TOKEN environment variable is required")

    username = os.getenv("GH_USERNAME")
    if not username:
        raise ValueError("GH_USERNAME environment variable is required")

    return Config(
        gh_token=token,
        gh_username=username,
        concurrency_graphql=int(os.getenv("CONCURRENCY_GRAPHQL", "20")),
        rate_limit_threshold=float(os.getenv("RATE_LIMIT_THRESHOLD", "0.8")),
        checkpoint_interval=int(os.getenv("CHECKPOINT_INTERVAL", "1000")),
        nightly_tier_days=int(os.getenv("NIGHTLY_TIER_DAYS", "30")),
        weekly_tier_days=int(os.getenv("WEEKLY_TIER_DAYS", "365")),
    )
