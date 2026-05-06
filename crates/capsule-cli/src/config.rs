//! Per-store CLI config (`<store_dir>/config.toml`). Currently only the
//! `serialize_paths.required` list (PROPOSAL §3.2) — paths that, when
//! touched in a verified diff, must be covered by the capsule's declared
//! `scope_prefixes`. Default ships the common lockfiles preloaded; per-repo
//! override empties or replaces the list.

use std::path::Path;

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use serde::Deserialize;

pub const DEFAULT_REQUIRED: &[&str] = &[
    "Cargo.lock",
    "package-lock.json",
    "pnpm-lock.yaml",
    "yarn.lock",
    "go.sum",
    "uv.lock",
];

#[derive(Debug, Clone, Default, Deserialize)]
pub struct CapsuleConfig {
    #[serde(default)]
    pub serialize_paths: SerializePaths,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SerializePaths {
    #[serde(default)]
    pub required: Vec<String>,
}

impl Default for SerializePaths {
    fn default() -> Self {
        Self {
            required: default_required_lockfiles(),
        }
    }
}

/// The built-in lockfile preload. Mirrors PROPOSAL §3.2 — common ecosystems
/// (Rust / npm / pnpm / Yarn / Go / Python uv). Repos whose lockfile lives
/// elsewhere override via `<store_dir>/config.toml`.
fn default_required_lockfiles() -> Vec<String> {
    DEFAULT_REQUIRED.iter().map(|s| (*s).to_string()).collect()
}

/// Load `<dir>/config.toml`. Missing file → defaults. Malformed → error
/// (do not silently fall back to defaults — a broken config is more likely
/// to be a typo than an intentional reset).
pub fn load(dir: &Path) -> Result<CapsuleConfig> {
    let path = dir.join("config.toml");
    if !path.exists() {
        return Ok(CapsuleConfig::default());
    }
    let body =
        std::fs::read_to_string(&path).with_context(|| format!("reading {}", path.display()))?;
    let cfg: CapsuleConfig =
        toml::from_str(&body).with_context(|| format!("parsing {}", path.display()))?;
    Ok(cfg)
}

/// Canonicalize the configured `serialize_paths.required` strings. Surfaces
/// canonicalization errors at config-load time rather than on each attest.
pub fn canonicalize_required(cfg: &CapsuleConfig) -> Result<Vec<CanonicalPath>> {
    let required: Vec<CanonicalPath> = cfg
        .serialize_paths
        .required
        .iter()
        .map(|s| {
            CanonicalPath::new(s)
                .with_context(|| format!("invalid serialize_paths.required entry {s:?}"))
        })
        .collect::<Result<_>>()?;
    Ok(dedup(required))
}

fn dedup(v: Vec<CanonicalPath>) -> Vec<CanonicalPath> {
    let mut out = Vec::with_capacity(v.len());
    for p in v {
        if !out.contains(&p) {
            out.push(p);
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_parse() {
        let cfg = CapsuleConfig::default();
        let required = canonicalize_required(&cfg).unwrap();
        assert_eq!(required.len(), DEFAULT_REQUIRED.len());
    }

    #[test]
    fn load_returns_defaults_when_no_file() {
        let tmp = tempfile::tempdir().unwrap();
        let cfg = load(tmp.path()).unwrap();
        let required = canonicalize_required(&cfg).unwrap();
        assert_eq!(required.len(), DEFAULT_REQUIRED.len());
    }

    #[test]
    fn load_parses_user_config_replacing_defaults() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "[serialize_paths]\nrequired = [\"Cargo.lock\", \"db/migrations/\"]\n",
        )
        .unwrap();

        let cfg = load(tmp.path()).unwrap();
        let required = canonicalize_required(&cfg).unwrap();
        assert_eq!(required.len(), 2);
        assert_eq!(required[0].as_str(), "Cargo.lock");
        assert_eq!(required[1].as_str(), "db/migrations");
    }

    #[test]
    fn load_empty_required_disables_lint() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::write(
            tmp.path().join("config.toml"),
            "[serialize_paths]\nrequired = []\n",
        )
        .unwrap();

        let cfg = load(tmp.path()).unwrap();
        let required = canonicalize_required(&cfg).unwrap();
        assert!(required.is_empty());
    }

    #[test]
    fn canonicalize_required_rejects_invalid_path_entry() {
        let cfg = CapsuleConfig {
            serialize_paths: SerializePaths {
                required: vec!["../escape".into()],
            },
        };

        let err = canonicalize_required(&cfg).unwrap_err();
        assert!(err.to_string().contains("escape"), "{err}");
    }

    #[test]
    fn canonicalize_required_dedups_canonical_duplicates() {
        let cfg = CapsuleConfig {
            serialize_paths: SerializePaths {
                required: vec![
                    "Cargo.lock".into(),
                    "Cargo.lock".into(),
                    "./Cargo.lock".into(),
                ],
            },
        };

        let required = canonicalize_required(&cfg).unwrap();
        assert_eq!(required.len(), 1);
        assert_eq!(required[0].as_str(), "Cargo.lock");
    }
}
