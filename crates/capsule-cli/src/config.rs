//! Per-store CLI config (`<store_dir>/config.toml`). Currently only the
//! `serialize_paths.required` list (PROPOSAL §3.2) — paths that, when
//! touched in a verified diff, must be covered by the capsule's declared
//! `scope_prefixes`. Default ships the common lockfiles preloaded; per-repo
//! override empties or replaces the list.

use std::path::Path;

use anyhow::{Context, Result};
use capsule_core::path::CanonicalPath;
use serde::Deserialize;

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
    vec![
        "Cargo.lock".into(),
        "package-lock.json".into(),
        "pnpm-lock.yaml".into(),
        "yarn.lock".into(),
        "go.sum".into(),
        "uv.lock".into(),
    ]
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
    cfg.serialize_paths
        .required
        .iter()
        .map(|s| {
            CanonicalPath::new(s)
                .with_context(|| format!("invalid serialize_paths.required entry {s:?}"))
        })
        .collect()
}
