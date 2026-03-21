# Changelog

## [1.1.0] - 2026-03-21

### Added
- Event publishing: `publish_event(EventType.DB_SYNCED)` after each sync run via reporium-events
- reporium-events dependency for Pub/Sub integration
- Build counter in nightly workflow (`BUILD` file incremented each run)
- Reporium suite badges in README template
- Perditio-devkit reusable test failure workflow

### Changed
- Nightly workflow commit message format: `build($BUILD): nightly update YYYY-MM-DD`

## [1.0.0] - 2026-03-17

### Added
- Nightly GitHub metadata sync via GraphQL batch fetch (9 calls for 826 repos)
- Partitioned JSON output: index.json, by_language, by_category, full/repos_NNNN.json
- Schedule-based tiering: nightly for active, weekly for moderate, monthly for inactive
- Diff computation: tracks new and updated repos between runs
- LAST_RUN.md generated after each sync with real metrics
