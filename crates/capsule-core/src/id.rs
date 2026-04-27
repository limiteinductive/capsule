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
    let bytes = id.as_bytes();
    let len = bytes.len();
    if len == 0 {
        return Err(IdError::Empty);
    }
    if len > 128 {
        return Err(IdError::TooLong(len));
    }
    if bytes[0] == b'.' || bytes[len - 1] == b'.' {
        return Err(IdError::EdgeDot);
    }
    if id.ends_with(".lock") {
        return Err(IdError::LockSuffix);
    }
    let mut prev_dot = false;
    for (i, &b) in bytes.iter().enumerate() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' => prev_dot = false,
            b'.' => {
                if prev_dot {
                    return Err(IdError::DoubleDot);
                }
                prev_dot = true;
            }
            _ => return Err(IdError::InvalidChar(i)),
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

    /// `EdgeDot` covers both ends — the impl checks `bytes[0]` AND
    /// `bytes[len - 1]`; this pins the trailing branch independently.
    #[test]
    fn trailing_dot_rejected() {
        assert_eq!(validate("a."), Err(IdError::EdgeDot));
    }

    /// 128 is the inclusive upper bound (`len > 128` rejects). Pin both
    /// sides so flipping `>` to `>=` fails the test instead of silently
    /// shrinking the accepted-id space by one byte.
    #[test]
    fn length_boundary_128_ok_129_rejected() {
        validate(&"a".repeat(128)).unwrap();
        assert_eq!(validate(&"a".repeat(129)), Err(IdError::TooLong(129)));
    }

    #[test]
    fn unicode_rejected() {
        assert!(matches!(validate("café"), Err(IdError::InvalidChar(_))));
    }

    /// Verified: `git check-ref-format refs/heads/capsules/foo.lock/a1`
    /// exits non-zero (per-component .lock-suffix is forbidden).
    #[test]
    fn lock_suffix_rejected() {
        assert_eq!(validate("foo.lock"), Err(IdError::LockSuffix));
        assert_eq!(validate("a.lock"), Err(IdError::LockSuffix));
    }

    /// Only the exact ".lock" suffix is banned; ".locks" is fine.
    #[test]
    fn locks_plural_ok() {
        validate("foo.locks").unwrap();
    }

    /// `.lock` only matters at the end of the component.
    #[test]
    fn lock_substring_in_middle_ok() {
        validate("foo.lock.bar").unwrap();
    }

    /// Single-pass validation reports the first defect positionally:
    /// a `/` (invalid) at byte 1 wins over a `..` later in the string.
    /// Either error is correct for boundary rejection; this test pins
    /// the chosen ordering so future refactors don't drift.
    #[test]
    fn invalid_char_before_dotdot_reports_invalid_char() {
        assert_eq!(validate("a/..b"), Err(IdError::InvalidChar(1)));
    }

    /// Pin cross-branch error precedence for ambiguous ids so refactors
    /// don't silently change which `IdError` callers see: EdgeDot beats
    /// LockSuffix (`.lock` has a leading `.` and matches the `.lock`
    /// suffix check), and LockSuffix beats DoubleDot (`a..lock` matches
    /// the `.lock` suffix check and also contains `..`).
    #[test]
    fn precedence_edgedot_then_locksuffix_then_doubledot() {
        assert_eq!(validate(".lock"), Err(IdError::EdgeDot));
        assert_eq!(validate("a..lock"), Err(IdError::LockSuffix));
    }
}
