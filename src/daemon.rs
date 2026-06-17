use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use chrono::Utc;
use inotify::{EventMask, Inotify, WatchMask};
use tokio::fs;
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::client;

type FileMap = Arc<Mutex<HashMap<String, String>>>;

pub async fn run(watch_dir: &str, debounce_ms: u64) -> anyhow::Result<()> {
    let dir = watch_dir.to_string();
    let file_map: FileMap = Arc::new(Mutex::new(HashMap::new()));

    tracing::info!("Starting daemon, watching: {}", dir);
    tracing::info!("Debounce: {}ms", debounce_ms);

    if let Err(e) = sync_dir(watch_dir).await {
        tracing::warn!("Initial sync failed: {}", e);
    }

    let wm = file_map.clone();
    let wdir = dir.clone();
    let wdeb = debounce_ms;
    let watcher = tokio::spawn(async move {
        if let Err(e) = watcher_loop(&wdir, wdeb, wm).await {
            tracing::error!("Watcher error: {}", e);
        }
    });

    let sdir = dir.clone();
    let sse = tokio::spawn(async move {
        if let Err(e) = sync_history(&sdir).await {
            tracing::warn!("History sync failed: {}", e);
        }
        loop {
            if let Err(e) = sse_loop(&sdir).await {
                tracing::warn!("SSE error: {}, reconnecting in 5s", e);
                sleep(Duration::from_secs(5)).await;
            }
        }
    });

    tokio::select! {
        _ = watcher => {},
        _ = sse => {},
    }

    Ok(())
}

async fn watcher_loop(dir: &str, debounce_ms: u64, _file_map: FileMap) -> anyhow::Result<()> {
    let mut inotify = Inotify::init()?;
    inotify
        .watches()
        .add(Path::new(dir), WatchMask::CREATE | WatchMask::MODIFY | WatchMask::DELETE)?;

    let mut buffer = [0u8; 4096];
    let debounce = Duration::from_millis(debounce_ms);

    loop {
        let events = inotify.read_events_blocking(&mut buffer)?;
        for event in events {
            if event.mask.contains(EventMask::CREATE) || event.mask.contains(EventMask::MODIFY) {
                if let Some(name) = event.name.and_then(|n| n.to_str()) {
                    if name.starts_with('.') {
                        continue;
                    }
                    let path = format!("{}/{}", dir, name);
                    tracing::info!("Change detected: {}", path);
                    sleep(debounce).await;
                    let max_retries = 10;
                    let mut retry = 0;
                    loop {
                        match client::upload(&path, None).await {
                            Ok(()) => break,
                            Err(e) => {
                                let retryable = e.to_string().contains("File is empty");
                                if retry >= max_retries || !retryable {
                                    tracing::warn!("Upload failed for {}: {}", path, e);
                                    break;
                                }
                                let backoff = Duration::from_millis(debounce_ms * (1 << retry));
                                tracing::info!("File empty, retrying {} in {:?}...", path, backoff);
                                retry += 1;
                                sleep(backoff).await;
                            }
                        }
                    }
                }
            }
            if event.mask.contains(EventMask::DELETE) {
                if let Some(name) = event.name.and_then(|n| n.to_str()) {
                    if name.starts_with('.') {
                        continue;
                    }
                    tracing::info!("Delete detected: {}", name);
                }
            }
        }
    }
}

async fn sync_history(dir: &str) -> anyhow::Result<()> {
    client::ensure_valid_token().await?;
    let cfg = crate::config::load()?;
    let now = Utc::now().to_rfc3339();
    let url = format!("{}/api/events/history?since={}", cfg.server_url, now);
    let client = reqwest::Client::new();
    let resp = client.get(&url).bearer_auth(&cfg.token).send().await?;
    if !resp.status().is_success() {
        return Ok(());
    }
    let events: Vec<serde_json::Value> = resp.json().await?;
    for ev in &events {
        handle_event(ev, dir).await;
    }
    Ok(())
}

async fn sse_loop(dir: &str) -> anyhow::Result<()> {
    use futures_util::StreamExt;

    client::ensure_valid_token().await?;
    let cfg = crate::config::load()?;
    let client = reqwest::Client::new();
    let url = format!("{}/api/events", cfg.server_url);
    let resp = client.get(&url).bearer_auth(&cfg.token).send().await?;

    let status = resp.status();
    if status == reqwest::StatusCode::UNAUTHORIZED {
        // Token expired, refresh and try again next loop
        return Err(anyhow::anyhow!("401 Unauthorized"));
    }

    let mut buf = String::new();
    let mut stream = resp.bytes_stream();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        buf.push_str(&String::from_utf8_lossy(&chunk));

        while let Some(pos) = buf.find("\n\n") {
            let event_block = buf[..pos].to_string();
            buf = buf[pos + 2..].to_string();

            for line in event_block.lines() {
                if let Some(data) = line.strip_prefix("data:") {
                    if let Ok(event) = serde_json::from_str::<serde_json::Value>(data.trim()) {
                        handle_event(&event, dir).await;
                    }
                }
            }
        }
    }

    Ok(())
}

async fn handle_event(event: &serde_json::Value, dir: &str) {
    let event_type = event["type"].as_str().unwrap_or("");
    let payload = &event["payload"];

    match event_type {
        "file_created" | "file_updated" => {
            let file_name = payload["name"].as_str().unwrap_or("unknown");
            let path = payload["path"].as_str().unwrap_or("");
            let dest = if path.is_empty() {
                format!("{}/{}", dir, file_name)
            } else {
                format!("{}/{}/{}", dir, path, file_name)
            };

            if let Some(id) = event["resource_id"].as_str() {
                if !Path::new(&dest).exists() {
                    tracing::info!("Remote {}: downloading {} as {}", event_type, id, dest);
                    if let Err(e) = client::download(id, Some(&dest)).await {
                        tracing::warn!("Download failed: {}", e);
                    }
                } else {
                    tracing::info!(
                        "Remote {}: {} already exists locally, skipping",
                        event_type,
                        file_name
                    );
                }
            }
        }
        "file_deleted" => {
            let file_name = payload["name"].as_str().unwrap_or("unknown");
            let path = payload["path"].as_str().unwrap_or("");
            let dest = if path.is_empty() {
                format!("{}/{}", dir, file_name)
            } else {
                format!("{}/{}/{}", dir, path, file_name)
            };
            if Path::new(&dest).exists() {
                tracing::info!("Remote delete: {}", dest);
                let _ = tokio::fs::remove_file(&dest).await;
            }
        }
        _ => {}
    }
}

pub async fn sync_dir(dir: &str) -> anyhow::Result<()> {
    client::ensure_valid_token().await?;
    let dir_path = Path::new(dir);
    if !dir_path.exists() {
        anyhow::bail!("Directory does not exist: {}", dir);
    }
    tracing::info!("Initial sync of {}", dir);
    sync_recursive(dir_path, "").await
}

async fn sync_recursive(dir: &Path, prefix: &str) -> anyhow::Result<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with('.') {
            continue;
        }
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            let sub_prefix = if prefix.is_empty() {
                name_str.to_string()
            } else {
                format!("{}/{}", prefix, name_str)
            };
            Box::pin(sync_recursive(&path, &sub_prefix)).await?;
        } else if file_type.is_file() {
            let remote_path = if prefix.is_empty() {
                None
            } else {
                Some(prefix)
            };
            tracing::info!("Initial sync: uploading {}", path.display());
            if let Err(e) = client::upload(path.to_str().unwrap(), remote_path).await {
                tracing::warn!("Initial sync: failed to upload {}: {}", path.display(), e);
            }
        }
    }
    Ok(())
}