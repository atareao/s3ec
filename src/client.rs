use chrono::Utc;
use reqwest::Client;
use serde_json::Value;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::Arc;
use tokio::fs;
use tokio::sync::Semaphore;

use crate::config::{self, Config};

pub async fn login(server_url: &str, api_key: &str) -> anyhow::Result<()> {
    let client = Client::new();
    let resp = client
        .post(format!(
            "{}/api/auth/login",
            server_url.trim_end_matches('/')
        ))
        .json(&serde_json::json!({ "api_key": api_key }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::bail!("Login failed ({}): {}", status, body);
    }

    let data: Value = resp.json().await?;
    let cfg = Config {
        server_url: server_url.trim_end_matches('/').to_string(),
        api_key: api_key.to_string(),
        token: data["token"].as_str().unwrap_or_default().to_string(),
        expires_at: data["expires_at"].as_str().unwrap_or_default().to_string(),
        concurrency: 20,
        last_sync_at: None,
    };
    config::save(&cfg)?;
    println!("Logged in. Token expires at {}", cfg.expires_at);
    Ok(())
}

pub async fn refresh_token(cfg: &Config) -> anyhow::Result<()> {
    let client = Client::new();
    let resp = client
        .post(format!("{}/api/auth/login", cfg.server_url))
        .json(&serde_json::json!({ "api_key": &cfg.api_key }))
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::bail!("Token refresh failed ({}): {}", status, body);
    }

    let data: Value = resp.json().await?;
    let mut new_cfg = cfg.clone();
    new_cfg.token = data["token"].as_str().unwrap_or_default().to_string();
    new_cfg.expires_at = data["expires_at"].as_str().unwrap_or_default().to_string();
    config::save(&new_cfg)?;
    tracing::info!("Token refreshed, expires at {}", new_cfg.expires_at);
    Ok(())
}

pub async fn ensure_valid_token() -> anyhow::Result<()> {
    let cfg = config::load()?;
    if cfg.is_token_expired() {
        refresh_token(&cfg).await?;
    }
    Ok(())
}

fn build_client() -> Client {
    Client::new()
}

async fn get(endpoint: &str) -> anyhow::Result<reqwest::Response> {
    let cfg = config::load()?;
    let resp = build_client()
        .get(format!("{}{}", cfg.server_url, endpoint))
        .bearer_auth(&cfg.token)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::bail!("Server error ({}): {}", status, body);
    }
    Ok(resp)
}

pub async fn upload(file_path: &str, remote_path: Option<&str>) -> anyhow::Result<()> {
    let cfg = config::load()?;
    let path = Path::new(file_path);
    let file_name = path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("unknown");

    let meta = fs::metadata(path).await?;
    if meta.len() == 0 {
        anyhow::bail!("File is empty, skipping upload");
    }

    let file_bytes = fs::read(path).await?;

    let upload_meta = serde_json::json!({
        "path": remote_path.unwrap_or(""),
        "mode": Some(meta.permissions().mode() as i64),
        "mtime": meta.modified().ok().map(|t| {
            let dt: chrono::DateTime<Utc> = t.into();
            dt.to_rfc3339()
        }),
    });

    let part = reqwest::multipart::Part::bytes(file_bytes)
        .file_name(file_name.to_string())
        .mime_str(
            mime_guess::from_path(file_name)
                .first_or_octet_stream()
                .as_ref(),
        )?;
    let metadata_part = reqwest::multipart::Part::bytes(upload_meta.to_string().into_bytes())
        .mime_str("application/json")?;

    let form = reqwest::multipart::Form::new()
        .part("file", part)
        .text("file_name", file_name.to_string())
        .part("metadata", metadata_part);

    let resp = build_client()
        .put(format!("{}/api/files/upload", cfg.server_url))
        .bearer_auth(&cfg.token)
        .multipart(form)
        .send()
        .await?;

    let status = resp.status();
    if status == reqwest::StatusCode::NO_CONTENT {
        println!("Uploaded: {} (unchanged)", file_name);
        return Ok(());
    }

    let body: Value = resp.json().await?;

    if status.is_success() {
        println!("Uploaded: {} (id: {})", file_name, body["id"]);
    } else {
        anyhow::bail!("Upload failed ({}): {}", status, body);
    }
    Ok(())
}

pub async fn get_file_info(id: &str) -> anyhow::Result<Value> {
    let resp = get(&format!("/api/files/{}", id)).await?;
    Ok(resp.json().await?)
}

pub async fn download(id: &str, output: Option<&str>) -> anyhow::Result<()> {
    let resp = get(&format!("/api/files/{}", id)).await?;
    let file_info: Value = resp.json().await?;
    let file_name = output
        .map(|s| s.to_string())
        .unwrap_or_else(|| file_info["name"].as_str().unwrap_or("download").to_string());

    let resp = get(&format!("/api/files/{}/download", id)).await?;
    let bytes = resp.bytes().await?;

    if let Some(parent) = Path::new(&file_name).parent() {
        fs::create_dir_all(parent).await?;
    }
    fs::write(&file_name, &bytes).await?;

    if let Some(mode) = file_info["mode"].as_i64()
        && let Ok(m) = std::fs::metadata(&file_name)
    {
        let mut perms = m.permissions();
        perms.set_mode(mode as u32);
        let _ = std::fs::set_permissions(&file_name, perms);
    }
    if let Some(mtime_str) = file_info["mtime"].as_str()
        && let Ok(mtime) = chrono::DateTime::parse_from_rfc3339(mtime_str)
    {
        let ts = filetime::FileTime::from_unix_time(mtime.timestamp(), 0);
        let _ = filetime::set_file_times(&file_name, ts, ts);
    }

    println!("Downloaded: {} ({} bytes)", file_name, bytes.len());
    Ok(())
}

pub async fn fetch_remote_files() -> anyhow::Result<Vec<Value>> {
    let cfg = config::load()?;
    let client = Client::new();
    let limit: i64 = 1000;

    let first_resp = client
        .get(format!(
            "{}/api/files?limit={}&offset=0",
            cfg.server_url, limit
        ))
        .bearer_auth(&cfg.token)
        .send()
        .await?;
    if !first_resp.status().is_success() {
        anyhow::bail!("Failed to fetch remote files: {}", first_resp.status());
    }

    let total: i64 = first_resp
        .headers()
        .get("x-total-count")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);

    let first_page: Vec<Value> = first_resp.json().await?;
    let mut all_files = first_page;

    tracing::debug!(
        total,
        limit,
        page_size = all_files.len(),
        "fetch_remote_files: first page"
    );

    let remaining = total.saturating_sub(limit);
    if remaining <= 0 {
        return Ok(all_files);
    }

    let semaphore = Arc::new(Semaphore::new(cfg.concurrency));
    let num_pages = (total + limit - 1) / limit;
    let mut handles: Vec<(i64, tokio::task::JoinHandle<anyhow::Result<Vec<Value>>>)> = Vec::new();

    for page in 1..num_pages {
        let offset = page * limit;
        let sem = Arc::clone(&semaphore);
        let cl = client.clone();
        let server_url = cfg.server_url.clone();
        let token = cfg.token.clone();

        handles.push((
            offset,
            tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();
                let resp = cl
                    .get(format!(
                        "{}/api/files?limit={}&offset={}",
                        server_url, limit, offset
                    ))
                    .bearer_auth(&token)
                    .send()
                    .await?;
                if !resp.status().is_success() {
                    anyhow::bail!("Page fetch failed: {}", resp.status());
                }
                let files: Vec<Value> = resp.json().await?;
                Ok(files)
            }),
        ));
    }

    let mut failed_offsets = Vec::new();
    for (offset, handle) in handles {
        match handle.await {
            Ok(Ok(files)) => {
                tracing::debug!(
                    offset,
                    count = files.len(),
                    "fetch_remote_files: page fetched"
                );
                all_files.extend(files);
            }
            Ok(Err(e)) => {
                tracing::error!(offset, "Failed to fetch page: {e}");
                failed_offsets.push(offset);
            }
            Err(e) => {
                tracing::error!("Task join error at offset {offset}: {e}");
                failed_offsets.push(offset);
            }
        }
    }

    for offset in &failed_offsets {
        tracing::info!(offset, "Retrying page fetch");
        match client
            .get(format!(
                "{}/api/files?limit={}&offset={}",
                cfg.server_url, limit, offset
            ))
            .bearer_auth(&cfg.token)
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => match resp.json::<Vec<Value>>().await {
                Ok(files) => {
                    tracing::debug!(
                        offset,
                        count = files.len(),
                        "fetch_remote_files: retry success"
                    );
                    all_files.extend(files);
                }
                Err(e) => tracing::error!(offset, "Retry parse error: {e}"),
            },
            Ok(resp) => tracing::error!(offset, "Retry failed: {}", resp.status()),
            Err(e) => tracing::error!(offset, "Retry error: {e}"),
        }
    }

    Ok(all_files)
}

pub async fn list(
    path: Option<&str>,
    search: Option<&str>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> anyhow::Result<()> {
    let mut query = Vec::new();
    if let Some(p) = path {
        query.push(format!("path={}", urlencoding::encode(p)));
    }
    if let Some(s) = search {
        query.push(format!("search={}", urlencoding::encode(s)));
    }
    if let Some(l) = limit {
        query.push(format!("limit={}", l));
    }
    if let Some(o) = offset {
        query.push(format!("offset={}", o));
    }
    let qs = if query.is_empty() {
        String::new()
    } else {
        format!("?{}", query.join("&"))
    };

    let resp = get(&format!("/api/files{}", qs)).await?;
    let files: Vec<Value> = resp.json().await?;

    if files.is_empty() {
        println!("No files found.");
    } else {
        for f in &files {
            let size = f["size"].as_i64().unwrap_or(0);
            let size_str = if size > 1024 * 1024 {
                format!("{:.1} MB", size as f64 / (1024.0 * 1024.0))
            } else if size > 1024 {
                format!("{:.1} KB", size as f64 / 1024.0)
            } else {
                format!("{} B", size)
            };
            let display_path = match f["path"].as_str() {
                Some(p) if !p.is_empty() => format!("{}/", p),
                _ => String::new(),
            };
            println!(
                "{}  {}  {}  {}{}",
                f["id"].as_str().unwrap_or("?"),
                size_str,
                f["updated_at"].as_str().unwrap_or(""),
                display_path,
                f["name"].as_str().unwrap_or("")
            );
        }
    }
    Ok(())
}

pub async fn info(id: &str) -> anyhow::Result<()> {
    let resp = get(&format!("/api/files/{}", id)).await?;
    let file: Value = resp.json().await?;
    println!("{}", serde_json::to_string_pretty(&file)?);
    Ok(())
}

pub async fn delete_by_path(name: &str, remote_path: Option<&str>) -> anyhow::Result<()> {
    let cfg = config::load()?;
    let client = build_client();

    let mut query = String::from("limit=100");
    if let Some(p) = remote_path
        && !p.is_empty()
    {
        query.push_str(&format!("&path={}", urlencoding::encode(p)));
    }
    let url = format!("{}/api/files?{}", cfg.server_url, query);

    let resp = client.get(&url).bearer_auth(&cfg.token).send().await?;

    if !resp.status().is_success() {
        return Ok(());
    }

    let files: Vec<Value> = resp.json().await?;
    for file in files {
        if file["name"].as_str() == Some(name)
            && let Some(id) = file["id"].as_str()
        {
            return rm(id).await;
        }
    }

    tracing::warn!("No remote file found for deleted local file: {}", name);
    Ok(())
}

pub async fn rm(id: &str) -> anyhow::Result<()> {
    let cfg = config::load()?;
    let resp = build_client()
        .delete(format!("{}/api/files/{}", cfg.server_url, id))
        .bearer_auth(&cfg.token)
        .send()
        .await?;
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await?;
        anyhow::bail!("Delete failed ({}): {}", status, body);
    }
    println!("Deleted file: {}", id);
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Mutex;
    use crate::config;

    static CONFIG_LOCK: Mutex<()> = Mutex::new(());

    struct TestConfig {
        _lock: std::sync::MutexGuard<'static, ()>,
        backup: Option<String>,
        cfg_path: PathBuf,
    }

    impl TestConfig {
        fn setup() -> Self {
            let lock = CONFIG_LOCK.lock().unwrap();
            let cfg_path = dirs::config_dir()
                .expect("config dir")
                .join("s3ec")
                .join("config.toml");

            if let Some(parent) = cfg_path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }

            let backup = std::fs::read_to_string(&cfg_path).ok();

            let cfg = config::Config {
                server_url: "https://test.example.com".into(),
                api_key: "test-key".into(),
                token: "test-token".into(),
                expires_at: "2099-01-01T00:00:00Z".into(),
                concurrency: 5,
                last_sync_at: None,
            };
            config::save(&cfg).expect("failed to save test config");
            TestConfig { _lock: lock, backup, cfg_path }
        }
    }

    impl Drop for TestConfig {
        fn drop(&mut self) {
            match &self.backup {
                Some(content) => {
                    let _ = std::fs::write(&self.cfg_path, content);
                }
                None => {
                    let _ = std::fs::remove_file(&self.cfg_path);
                }
            }
        }
    }

    #[tokio::test]
    async fn upload_rejects_empty_file() {
        let _cfg = TestConfig::setup();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.txt");
        tokio::fs::write(&path, b"").await.unwrap();

        let result = super::upload(&path.to_string_lossy(), None).await;
        assert!(result.is_err());
        let err = result.unwrap_err().to_string();
        assert!(err.contains("File is empty"), "expected 'File is empty', got: {}", err);
    }

    #[tokio::test]
    async fn upload_rejects_nonexistent_file() {
        let _cfg = TestConfig::setup();
        let result = super::upload("/tmp/s3ec-nonexistent-test-file-xxxxx", None).await;
        assert!(result.is_err());
    }
}
