use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server_url: String,
    pub api_key: String,
    pub token: String,
    pub expires_at: String,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default)]
    pub last_sync_at: Option<String>,
}

fn default_concurrency() -> usize {
    20
}

impl Config {
    pub fn is_token_expired(&self) -> bool {
        if let Ok(exp) = DateTime::parse_from_rfc3339(&self.expires_at) {
            let now = Utc::now();
            let exp_utc = exp.with_timezone(&Utc);
            // Refresh if less than 5 minutes remaining
            (exp_utc - now).num_seconds() < 300
        } else {
            true
        }
    }
}

fn config_path() -> anyhow::Result<PathBuf> {
    let dir = dirs::config_dir()
        .ok_or_else(|| anyhow::anyhow!("Cannot find config directory"))?
        .join("s3ec");
    std::fs::create_dir_all(&dir)?;
    Ok(dir.join("config.toml"))
}

pub fn load() -> anyhow::Result<Config> {
    let path = config_path()?;
    if !path.exists() {
        anyhow::bail!("Not logged in. Run `s3ec login --server <url> --api-key <key>` first");
    }
    let content = std::fs::read_to_string(&path)?;
    Ok(toml::from_str(&content)?)
}

pub fn save(cfg: &Config) -> anyhow::Result<()> {
    let path = config_path()?;
    let content = toml::to_string(cfg)?;
    std::fs::write(&path, content)?;
    Ok(())
}

#[expect(dead_code)]
pub fn server_url() -> anyhow::Result<String> {
    let cfg = load()?;
    Ok(cfg.server_url.trim_end_matches('/').to_string())
}

#[expect(dead_code)]
pub fn token() -> anyhow::Result<String> {
    let cfg = load()?;
    Ok(cfg.token)
}