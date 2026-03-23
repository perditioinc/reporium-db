"""Security tests — ensure private repos never enter the pipeline."""
from reporium_db.models import RepoMetadata


def test_repo_metadata_has_is_private_field():
    """RepoMetadata must have isPrivate field to track privacy status."""
    repo = RepoMetadata(
        nameWithOwner="test/repo", name="repo", description="test",
        stars=0, forks=0, primaryLanguage="Python", pushedAt=None,
        updatedAt=None, createdAt="2026-01-01", isArchived=False,
        isFork=True, isEmpty=False, topics=[], licenseName=None,
        openIssues=0, defaultBranch="main", isPrivate=True,
    )
    assert repo.isPrivate is True


def test_private_repo_defaults_to_false():
    """isPrivate must default to False if not specified."""
    repo = RepoMetadata(
        nameWithOwner="test/repo", name="repo", description="test",
        stars=0, forks=0, primaryLanguage="Python", pushedAt=None,
        updatedAt=None, createdAt="2026-01-01", isArchived=False,
        isFork=True, isEmpty=False, topics=[], licenseName=None,
        openIssues=0, defaultBranch="main",
    )
    assert repo.isPrivate is False
