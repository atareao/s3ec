use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use chrono::Utc;
use inotify::{EventMask, Inotify, WatchMask};
use tokio::sync::Mutex;
use tokio::time::{sleep, Duration};

use crate::client;

type FileMap = Arc<Mutex<HashMap<String, String>>>;

pub async fn run(watch_dir: &str, debounce_ms: u64) -> anyhow::Result<()> {
    let dir = watch_dir.to_string();
    let file_map: FileMap = Arc::new(Mutex::new(HashMap::new()));

    tracing::info!("Starting daemon, watching: {}", dir);
    tracing::info!("Debounce: {}ms", debounce_ms);

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
                    if let Err(e) = client::upload(&path, Some(dir)).await {
                        tracing::warn!("Upload failed for {}: {}", path, e);
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

    let cfg = crate::config::load()?;
    let client = reqwest::Client::new();
    let url = format!("{}/api/events", cfg.server_url);
    let resp = client.get(&url).bearer_auth(&cfg.token).send().await?;

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