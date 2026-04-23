//! SQLite-backed capsule store. See `DESIGN.md` §4 (data model) and §7.1 (protocols).
//!
//! Implementation pending. v0 will provide:
//! - `Store::open(path)` / `Store::init(path)`
//! - `claim` / `heartbeat` / `attest` / `land` / `reconcile` (each one DB transaction)
//! - All transitions atomic via a single `tx` per operation.
