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
        let mut out = String::with_capacity(input.len());
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

    /// Identity: a path always overlaps itself, regardless of depth.
    #[test]
    fn overlap_self_with_self() {
        assert!(cp("a").overlaps(&cp("a")));
        assert!(cp("a/b/c").overlaps(&cp("a/b/c")));
    }

    #[test]
    fn overlap_disjoint_at_first_component() {
        assert!(!cp("a/b").overlaps(&cp("c/d")));
    }

    /// Match exists across the lists at non-aligned indices: a[0] overlaps b[1].
    #[test]
    fn any_overlap_detects_cross_list_match() {
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

    /// The doc-comment on `new` says "normalize separators to `/`" — the
    /// implementation splits on both `/` and `\` so a Windows-style path
    /// pasted into a scope arg canonicalizes losslessly. Pin both pure and
    /// mixed-separator inputs so a future tightening to `/`-only is a
    /// deliberate change (it would silently regress canonicalization for
    /// any caller relying on the wider split set).
    #[test]
    fn backslash_separator_normalized_to_slash() {
        assert_eq!(cp(r"a\b\c").as_str(), "a/b/c");
        assert_eq!(cp(r"a/b\c").as_str(), "a/b/c");
    }

    /// Inputs made only of `.` and separators canonicalize to no components
    /// and must surface as `Empty` rather than a stored `""`. The
    /// `"//"` precedence assertion pins that `Absolute` (early guard)
    /// wins over `Empty` (late guard) when both would otherwise fire.
    #[test]
    fn rejects_inputs_that_strip_to_zero_components() {
        for input in [".", "./", "././", "./././", r"\", r".\."] {
            let got = CanonicalPath::new(input);
            assert!(
                matches!(got, Err(CanonicalizeError::Empty)),
                "expected Empty for {input:?}, got {got:?}"
            );
        }
        assert!(matches!(
            CanonicalPath::new("//"),
            Err(CanonicalizeError::Absolute)
        ));
    }

    /// Pin the public JSON shape: `CanonicalPath` is a bare string, not
    /// a wrapped object. `Capsule.scope_prefixes` rides through `--json`
    /// to agents — a refactor that switched the inner type to a struct
    /// (or wrapped this in an enum) would silently break that contract.
    #[test]
    fn canonical_path_json_wire_shape_is_bare_string() {
        let p = CanonicalPath::new("src/foo").unwrap();
        assert_eq!(serde_json::to_string(&p).unwrap(), r#""src/foo""#);
        let parsed: CanonicalPath = serde_json::from_str(r#""src/foo""#).unwrap();
        assert_eq!(parsed.as_str(), "src/foo");
    }

    /// 'é' as decomposed (e + combining acute) vs precomposed. Asserting
    /// only `a == b` would also pass an NFD impl (both inputs decompose);
    /// pin the stored form to NFC so a refactor to `.nfd()` is a deliberate
    /// change rather than a silent regression that lets visually equivalent
    /// paths persist under different bytes and bypass byte-wise overlap.
    #[test]
    fn nfc_normalizes_decomposed_to_composed() {
        let decomposed = "src/cafe\u{0301}";
        let composed = "src/caf\u{00e9}";
        let a = CanonicalPath::new(decomposed).unwrap();
        let b = CanonicalPath::new(composed).unwrap();
        assert_eq!(a.as_str(), composed);
        assert_eq!(b.as_str(), composed);
        assert!(a.overlaps(&b));
    }
}
