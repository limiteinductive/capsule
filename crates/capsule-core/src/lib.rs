//! Pure types and protocol logic for capsule. No I/O, no DB, no git.
//!
//! See `DESIGN.md` §4 (data model) and §7 (invariants and protocols).

pub mod id;
pub mod model;
pub mod path;
pub mod sha;

pub use model::*;
