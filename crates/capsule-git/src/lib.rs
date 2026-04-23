//! Git wire integration. See `DESIGN.md` §7.1.2 (land) and §3.1 (publication contract).
//!
//! Shells out to `git` for portability over libgit2 — `--force-with-lease` and
//! atomic multi-ref push semantics are best preserved by the canonical CLI.
//!
//! Implementation pending.
