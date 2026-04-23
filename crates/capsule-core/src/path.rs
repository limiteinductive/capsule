//! Path canonicalization and prefix-overlap. See DESIGN.md §7.0.
//!
//! POSIX, case-sensitive NFC, path-component-wise prefix overlap.
//! `src/foo` overlaps `src/foo/bar.rs` but not `src/foobar`.

use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CanonicalPath(String);

#[derive(Debug, Error)]
pub enum CanonicalizeError {
    #[error("path is empty")]
    Empty,
    #[error("path is absolute (must be repo-relative)")]
    Absolute,
    #[error("path contains '..' component")]
    DotDot,
}

impl CanonicalPath {
    /// Canonicalize a repo-relative path:
    /// - reject empty / absolute / contains `..`
    /// - normalize separators to `/`
    /// - drop empty components and `.` components
    /// - strip trailing `/`
    /// - NFC normalization is applied per spec; here we accept input as-is
    ///   (assume callers feed UTF-8 NFC). Re-normalization can be added later.
    pub fn new(input: &str) -> Result<Self, CanonicalizeError> {
        if input.is_empty() {
            return Err(CanonicalizeError::Empty);
        }
        if input.starts_with('/') {
            return Err(CanonicalizeError::Absolute);
        }
        let parts: Vec<&str> = input
            .split(|c| c == '/' || c == '\\')
            .filter(|p| !p.is_empty() && *p != ".")
            .collect();
        if parts.iter().any(|p| *p == "..") {
            return Err(CanonicalizeError::DotDot);
        }
        if parts.is_empty() {
            return Err(CanonicalizeError::Empty);
        }
        Ok(Self(parts.join("/")))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Path-component-wise prefix overlap. `src/foo` overlaps `src/foo/bar.rs`
    /// (one is a prefix of the other when split on `/`), but not `src/foobar`.
    pub fn overlaps(&self, other: &Self) -> bool {
        let a: Vec<&str> = self.0.split('/').collect();
        let b: Vec<&str> = other.0.split('/').collect();
        let n = a.len().min(b.len());
        a[..n] == b[..n]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cp(s: &str) -> CanonicalPath {
        CanonicalPath::new(s).unwrap()
    }

    #[test]
    fn rejects_empty() {
        assert!(matches!(
            CanonicalPath::new(""),
            Err(CanonicalizeError::Empty)
        ));
    }

    #[test]
    fn rejects_absolute() {
        assert!(matches!(
            CanonicalPath::new("/abs"),
            Err(CanonicalizeError::Absolute)
        ));
    }

    #[test]
    fn rejects_dotdot() {
        assert!(matches!(
            CanonicalPath::new("a/../b"),
            Err(CanonicalizeError::DotDot)
        ));
    }

    #[test]
    fn strips_dot_and_empty() {
        assert_eq!(cp("./a//b/").as_str(), "a/b");
    }

    #[test]
    fn overlap_component_wise() {
        assert!(cp("src/foo").overlaps(&cp("src/foo/bar.rs")));
        assert!(cp("src/foo/bar.rs").overlaps(&cp("src/foo")));
        assert!(!cp("src/foo").overlaps(&cp("src/foobar")));
        assert!(!cp("src/foo").overlaps(&cp("src/bar")));
        assert!(cp("src/foo").overlaps(&cp("src/foo")));
    }

    #[test]
    fn case_sensitive() {
        assert!(!cp("src/Foo").overlaps(&cp("src/foo")));
    }
}
