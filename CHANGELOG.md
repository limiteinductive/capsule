# Changelog

## 0.0.2 - 2026-05-06

### Added

- Added `capsule queue`, a text and JSON overview for queue counts,
  claimable work, active capsules, accepted capsules, and deploy-verification
  state.
- Added an integration test covering `capsule queue` text and JSON output.

### Changed

- Raised the declared MSRV to Rust 1.85 and added a locked MSRV CI job.
- Updated the Rust dependency group (`thiserror`, `rusqlite`, `toml`, and
  compatible transitive dependencies) while keeping `time` below its Rust 1.88
  boundary.
- Updated release workflow actions.

## 0.0.1 - 2026-05-06

Initial binary release.
