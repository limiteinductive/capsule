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
}
