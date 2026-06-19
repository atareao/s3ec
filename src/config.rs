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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Utc, Duration};

    fn make_config(expires_at: &str) -> Config {
        Config {
            server_url: "https://example.com".into(),
            api_key: "key".into(),
            token: "token".into(),
            expires_at: expires_at.into(),
            concurrency: 10,
            last_sync_at: None,
        }
    }

    #[test]
    fn default_concurrency_value() {
        assert_eq!(default_concurrency(), 20);
    }

    #[test]
    fn token_expired_when_parse_fails() {
        let cfg = make_config("not-a-date");
        assert!(cfg.is_token_expired());
    }

    #[test]
    fn token_expired_when_empty() {
        let cfg = make_config("");
        assert!(cfg.is_token_expired());
    }

    #[test]
    fn token_not_expired_far_future() {
        let future = Utc::now() + Duration::hours(2);
        let cfg = make_config(&future.to_rfc3339());
        assert!(!cfg.is_token_expired());
    }

    #[test]
    fn token_expired_within_five_minutes() {
        let soon = Utc::now() + Duration::seconds(30);
        let cfg = make_config(&soon.to_rfc3339());
        assert!(cfg.is_token_expired());
    }

    #[test]
    fn token_expired_in_the_past() {
        let past = Utc::now() - Duration::hours(1);
        let cfg = make_config(&past.to_rfc3339());
        assert!(cfg.is_token_expired());
    }

    #[test]
    fn token_expired_at_exactly_five_minutes() {
        let boundary = Utc::now() + Duration::seconds(299);
        let cfg = make_config(&boundary.to_rfc3339());
        assert!(cfg.is_token_expired());
    }

    #[test]
    fn token_valid_at_just_over_five_minutes() {
        let future = Utc::now() + Duration::seconds(301);
        let cfg = make_config(&future.to_rfc3339());
        assert!(!cfg.is_token_expired());
    }

    #[test]
    fn config_roundtrip_serialization() {
        let cfg = Config {
            server_url: "https://server.test".into(),
            api_key: "test-api-key".into(),
            token: "test-token".into(),
            expires_at: Utc::now().to_rfc3339(),
            concurrency: 5,
            last_sync_at: Some(Utc::now().to_rfc3339()),
        };

        let toml_str = toml::to_string(&cfg).unwrap();
        let deserialized: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(deserialized.server_url, cfg.server_url);
        assert_eq!(deserialized.api_key, cfg.api_key);
        assert_eq!(deserialized.token, cfg.token);
        assert_eq!(deserialized.expires_at, cfg.expires_at);
        assert_eq!(deserialized.concurrency, cfg.concurrency);
        assert_eq!(deserialized.last_sync_at, cfg.last_sync_at);
    }

    #[test]
    fn config_default_concurrency_on_missing_field() {
        let toml_str = r#"
server_url = "https://server.test"
api_key = "key"
token = "tok"
expires_at = "2025-01-01T00:00:00Z"
"#;
        let cfg: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(cfg.concurrency, 20);
        assert!(cfg.last_sync_at.is_none());
    }
}
