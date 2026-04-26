//! Path canonicalization and prefix-overlap. See DESIGN.md §7.0.
//!
//! POSIX, case-sensitive NFC, path-component-wise prefix overlap.
//! `src/foo` overlaps `src/foo/bar.rs` but not `src/foobar`.

use serde::{Deserialize, Serialize};
use thiserror::Error;
use unicode_normalization::UnicodeNormalization;

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
    /// Canonicalize a repo-relative path: reject empty/absolute/`..`, normalize
    /// separators to `/`, drop empty and `.` components, strip trailing `/`,
    /// apply Unicode NFC to each component (DESIGN.md §7.0).
    ///
    /// Single-pass build into one `String` buffer: components are NFC-extended
    /// inline rather than collected into an intermediate `Vec<String>` and
    /// `join`-ed. The `..` check sits inside the loop because ASCII dots are
    /// NFC-stable, so seeing `".."` pre-NFC is equivalent to seeing it post-NFC.
    pub fn new(input: &str) -> Result<Self, CanonicalizeError> {
        if input.is_empty() {
            return Err(CanonicalizeError::Empty);
        }
        if input.starts_with('/') {
            return Err(CanonicalizeError::Absolute);
        }
        let mut out = String::with_capacity(input.len()); // hint only; NFC may grow
        let mut had_any = false;
        for p in input
            .split(['/', '\\'])
            .filter(|p| !p.is_empty() && *p != ".")
        {
            if p == ".." {
                return Err(CanonicalizeError::DotDot);
            }
            if had_any {
                out.push('/');
            }
            had_any = true;
            out.extend(p.nfc());
        }
        if !had_any {
            return Err(CanonicalizeError::Empty);
        }
        Ok(Self(out))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// True iff any pair `(a, b)` in `lhs × rhs` overlaps. Empty inputs return
    /// false (consistent with `iter().any` on an empty iterator).
    pub fn any_overlap(lhs: &[Self], rhs: &[Self]) -> bool {
        lhs.iter().any(|a| rhs.iter().any(|b| a.overlaps(b)))
    }

    /// Path-component-wise prefix overlap. `src/foo` overlaps `src/foo/bar.rs`
    /// (one is a prefix of the other when split on `/`), but not `src/foobar`.
    ///
    /// Allocation-free by construction: `zip` stops at the shorter iterator,
    /// so this compares exactly `min(len(a), len(b))` components — same answer
    /// as an explicit prefix check, no intermediate `Vec`s. Called per
    /// (in-flight × claimed) scope pair on every `claim`, so the saving
    /// matters at scale.
    pub fn overlaps(&self, other: &Self) -> bool {
        self.0
            .split('/')
            .zip(other.0.split('/'))
            .all(|(a, b)| a == b)
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

    #[test]
    fn overlap_self_with_self() {
        // Identity: a path always overlaps itself, regardless of depth.
        assert!(cp("a").overlaps(&cp("a")));
        assert!(cp("a/b/c").overlaps(&cp("a/b/c")));
    }

    #[test]
    fn overlap_disjoint_at_first_component() {
        assert!(!cp("a/b").overlaps(&cp("c/d")));
    }

    #[test]
    fn any_overlap_detects_cross_list_match() {
        // Match exists across the lists at non-aligned indices: a[0] overlaps b[1].
        let a = vec![cp("src/foo"), cp("docs")];
        let b = vec![cp("tests"), cp("src/foo/bar.rs")];
        assert!(CanonicalPath::any_overlap(&a, &b));
    }

    #[test]
    fn any_overlap_empty_scopes_never_overlap() {
        let empty: Vec<CanonicalPath> = vec![];
        assert!(!CanonicalPath::any_overlap(&empty, &empty));
        assert!(!CanonicalPath::any_overlap(&[cp("src")], &empty));
        assert!(!CanonicalPath::any_overlap(&empty, &[cp("src")]));
    }

    #[test]
    fn any_overlap_disjoint_lists() {
        let a = vec![cp("src"), cp("docs")];
        let b = vec![cp("tests"), cp("README.md")];
        assert!(!CanonicalPath::any_overlap(&a, &b));
    }

    #[test]
    fn nfc_normalizes_decomposed_to_composed() {
        // 'é' as decomposed (e + combining acute) vs precomposed.
        let decomposed = "src/cafe\u{0301}";
        let composed = "src/caf\u{00e9}";
        let a = CanonicalPath::new(decomposed).unwrap();
        let b = CanonicalPath::new(composed).unwrap();
        assert_eq!(a.as_str(), b.as_str());
        assert!(a.overlaps(&b));
    }
}
