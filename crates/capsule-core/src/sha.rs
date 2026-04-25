//! Git object-id (SHA) validation. Capsule currently targets SHA-1 git
//! (DESIGN.md §3.1), so a verified_sha is exactly 40 lowercase hex chars —
//! `git rev-parse` and `git ls-remote` both emit that form. Validating up-front
//! at `attest` time turns "garbage in `verified_sha`" from an opaque `git push`
//! failure at land time into an actionable error at the protocol boundary.

use thiserror::Error;

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ShaError {
    #[error("sha must be 40 hex characters, got {0}")]
    BadLength(usize),
    #[error("sha contains non-hex character at byte {0}")]
    NonHex(usize),
    #[error("sha must be lowercase hex (uppercase/mixed case forbidden)")]
    NotLowercase,
}

pub fn validate(s: &str) -> Result<(), ShaError> {
    if s.len() != 40 {
        return Err(ShaError::BadLength(s.len()));
    }
    for (i, b) in s.bytes().enumerate() {
        if b.is_ascii_digit() || (b'a'..=b'f').contains(&b) {
            continue;
        }
        if (b'A'..=b'F').contains(&b) {
            return Err(ShaError::NotLowercase);
        }
        return Err(ShaError::NonHex(i));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lowercase_40_hex_ok() {
        validate("0123456789abcdef0123456789abcdef01234567").unwrap();
    }

    #[test]
    fn zero_oid_ok() {
        validate("0000000000000000000000000000000000000000").unwrap();
    }

    #[test]
    fn short_rejected() {
        assert_eq!(validate("abc"), Err(ShaError::BadLength(3)));
    }

    #[test]
    fn long_rejected() {
        // 64-char SHA-256 form — accept later if we move off SHA-1.
        let long = "0".repeat(64);
        assert_eq!(validate(&long), Err(ShaError::BadLength(64)));
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(validate(""), Err(ShaError::BadLength(0)));
    }

    #[test]
    fn uppercase_rejected_with_dedicated_variant() {
        // Distinct error helps the caller suggest .to_lowercase() rather than
        // hunting for a "non-hex" character that's actually fine modulo case.
        assert_eq!(
            validate("0123456789ABCDEF0123456789abcdef01234567"),
            Err(ShaError::NotLowercase)
        );
    }

    #[test]
    fn non_hex_rejected() {
        let mut s = String::from("0123456789abcdef0123456789abcdef01234567");
        s.replace_range(5..6, "g");
        assert_eq!(validate(&s), Err(ShaError::NonHex(5)));
    }
}
