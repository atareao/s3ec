use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Serialize, Deserialize)]
pub struct Config {
    pub server_url: String,
    pub token: String,
    pub expires_at: String,
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

pub fn server_url() -> anyhow::Result<String> {
    let cfg = load()?;
    Ok(cfg.server_url.trim_end_matches('/').to_string())
}

pub fn token() -> anyhow::Result<String> {
    let cfg = load()?;
    Ok(cfg.token)
}