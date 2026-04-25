//! Capsule id validation. Capsule ids feed into git ref names
//! (`refs/heads/capsules/<id>/a<N>` and `refs/heads/capsule-witness/<id>/a<N>`),
//! so they must be a single safe ref-name component.
//!
//! Conservative whitelist: ASCII alphanumeric, `-`, `_`, `.`, between 1 and 128
//! bytes, no leading/trailing `.`, no `..`. UUIDs satisfy this trivially.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum IdError {
    #[error("capsule id is empty")]
    Empty,
    #[error("capsule id too long (max 128, got {0})")]
    TooLong(usize),
    #[error("capsule id contains invalid character at byte {0}")]
    InvalidChar(usize),
    #[error("capsule id has leading or trailing '.'")]
    EdgeDot,
    #[error("capsule id contains '..'")]
    DoubleDot,
    #[error("capsule id ends with '.lock' (forbidden in git ref components)")]
    LockSuffix,
}

pub fn validate(id: &str) -> Result<(), IdError> {
    if id.is_empty() {
        return Err(IdError::Empty);
    }
    if id.len() > 128 {
        return Err(IdError::TooLong(id.len()));
    }
    if id.starts_with('.') || id.ends_with('.') {
        return Err(IdError::EdgeDot);
    }
    if id.contains("..") {
        return Err(IdError::DoubleDot);
    }
    // Git ref-name rule: no slash-separated component may end with ".lock".
    // Capsule id is one such component (`refs/heads/capsules/<id>/a<N>`), so
    // a `.lock` suffix here would surface as an opaque push failure at land
    // time. Reject up-front. Verified with `git check-ref-format`.
    if id.ends_with(".lock") {
        return Err(IdError::LockSuffix);
    }
    for (i, b) in id.bytes().enumerate() {
        let ok = b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.';
        if !ok {
            return Err(IdError::InvalidChar(i));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn uuid_ok() {
        validate("5ed0a728-19d2-42b8-8bec-6f33ed9d22f1").unwrap();
    }

    #[test]
    fn slug_ok() {
        validate("my_capsule.v1").unwrap();
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(validate(""), Err(IdError::Empty));
    }

    #[test]
    fn slash_rejected() {
        assert!(matches!(validate("a/b"), Err(IdError::InvalidChar(_))));
    }

    #[test]
    fn space_rejected() {
        assert!(matches!(validate("a b"), Err(IdError::InvalidChar(_))));
    }

    #[test]
    fn dotdot_rejected() {
        assert_eq!(validate("a..b"), Err(IdError::DoubleDot));
    }

    #[test]
    fn leading_dot_rejected() {
        assert_eq!(validate(".a"), Err(IdError::EdgeDot));
    }

    #[test]
    fn unicode_rejected() {
        assert!(matches!(validate("café"), Err(IdError::InvalidChar(_))));
    }

    #[test]
    fn lock_suffix_rejected() {
        // Verified: `git check-ref-format refs/heads/capsules/foo.lock/a1`
        // exits non-zero (per-component .lock-suffix is forbidden).
        assert_eq!(validate("foo.lock"), Err(IdError::LockSuffix));
        assert_eq!(validate("a.lock"), Err(IdError::LockSuffix));
    }

    #[test]
    fn locks_plural_ok() {
        // Only the exact ".lock" suffix is banned; ".locks" is fine.
        validate("foo.locks").unwrap();
    }

    #[test]
    fn lock_substring_in_middle_ok() {
        // `.lock` only matters at the end of the component.
        validate("foo.lock.bar").unwrap();
    }
}
