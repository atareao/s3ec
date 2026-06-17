use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use chrono::Utc;
use reqwest::Client;
use serde_json::Value;
use tokio::fs;

use crate::config::{self, Config};

pub async fn login(server_url: &str, api_key: &str) -> anyhow::Result<()> {
    let client = Client::new();
    let resp = client
        .post(format!("{}/api/auth/login", server_url.trim_end_matches('/')))
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

    let data = fs::read(path).await?;

    let upload_meta = serde_json::json!({
        "path": remote_path.unwrap_or(""),
        "mode": Some(meta.permissions().mode() as i64),
        "mtime": meta.modified().ok().map(|t| {
            let dt: chrono::DateTime<Utc> = t.into();
            dt.to_rfc3339()
        }),
    });

    let part = reqwest::multipart::Part::bytes(data)
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

pub async fn download(id: &str, output: Option<&str>) -> anyhow::Result<()> {
    let resp = get(&format!("/api/files/{}", id)).await?;
    let file_info: Value = resp.json().await?;
    let file_name = output
        .map(|s| s.to_string())
        .unwrap_or_else(|| file_info["name"].as_str().unwrap_or("download").to_string());

    let resp = get(&format!("/api/files/{}/download", id)).await?;
    let bytes = resp.bytes().await?;
    fs::write(&file_name, &bytes).await?;

    if let Some(mode) = file_info["mode"].as_i64() {
        if let Ok(m) = std::fs::metadata(&file_name) {
            let mut perms = m.permissions();
            perms.set_mode(mode as u32);
            let _ = std::fs::set_permissions(&file_name, perms);
        }
    }
    if let Some(mtime_str) = file_info["mtime"].as_str() {
        if let Ok(mtime) = chrono::DateTime::parse_from_rfc3339(mtime_str) {
            let ts = filetime::FileTime::from_unix_time(mtime.timestamp(), 0);
            let _ = filetime::set_file_times(&file_name, ts, ts);
        }
    }

    println!("Downloaded: {} ({} bytes)", file_name, bytes.len());
    Ok(())
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