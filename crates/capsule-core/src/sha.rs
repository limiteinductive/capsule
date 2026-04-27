//! Git object-id (SHA) validation. Capsule currently targets SHA-1 git
//! (DESIGN.md §3.1), so any sha on the wire is exactly 40 lowercase hex
//! chars — `git rev-parse` and `git ls-remote` both emit that form.
//!
//! Called at every protocol boundary that hands a sha to git: `claim`
//! (`base_sha` → `git worktree add` + `LandPush` prior-base) and `attest`
//! (`verified_sha` → `git push <sha>:refs/heads/...`). Validating up-front
//! turns "garbage sha" from an opaque `git push` failure at land time into
//! an actionable error at the boundary; the SQL CHECK constraints can't
//! express the 40-lowercase-hex shape.

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

pub const fn validate(s: &str) -> Result<(), ShaError> {
    if s.len() != 40 {
        return Err(ShaError::BadLength(s.len()));
    }
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'0'..=b'9' | b'a'..=b'f' => {}
            b'A'..=b'F' => return Err(ShaError::NotLowercase),
            _ => return Err(ShaError::NonHex(i)),
        }
        i += 1;
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

    /// Pin BadLength on the 64-char SHA-256 form so loosening the validator
    /// when we move off SHA-1 is a deliberate change, not a silent regression.
    #[test]
    fn long_rejected() {
        let long = "0".repeat(64);
        assert_eq!(validate(&long), Err(ShaError::BadLength(64)));
    }

    #[test]
    fn empty_rejected() {
        assert_eq!(validate(""), Err(ShaError::BadLength(0)));
    }

    /// Uppercase hex must surface as `NotLowercase`, not a generic non-hex
    /// error: callers can then suggest `.to_lowercase()` instead of sending
    /// the user hunting for a "non-hex" character that's actually fine modulo
    /// case.
    #[test]
    fn uppercase_rejected_with_dedicated_variant() {
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

    #[test]
    fn bad_length_outranks_per_byte_defects() {
        // Callers turn ShaError variants into actionable messages; for
        // malformed lengths, length is the primary issue regardless of
        // byte contents.
        assert_eq!(validate("ABCDEF"), Err(ShaError::BadLength(6)));
        assert_eq!(validate("abcg"), Err(ShaError::BadLength(4)));
    }
}
