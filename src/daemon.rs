use chrono::{DateTime, Utc};
use inotify::{EventMask, Inotify, WatchMask};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::SystemTime;
use tokio::fs;
use tokio::sync::Semaphore;
use tokio::time::{Duration, sleep};

use crate::client;
use crate::config;

pub async fn run(watch_dir: &str, debounce_ms: u64) -> anyhow::Result<()> {
    let dir = watch_dir.to_string();
    let root = Path::new(&dir).canonicalize()?;

    let sync_start = Utc::now().to_rfc3339();

    let downloading: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));

    tracing::info!("Starting daemon, watching: {}", dir);
    tracing::info!("Debounce: {}ms", debounce_ms);

    match config::load().ok().and_then(|c| c.last_sync_at) {
        Some(since) => {
            tracing::info!("Incremental sync since: {}", since);
            if let Err(e) = incremental_sync(watch_dir, &since, &downloading).await {
                tracing::warn!("Incremental sync failed: {}", e);
            }
        }
        None => {
            tracing::info!("Full initial sync");
            if let Err(e) = sync_dir(watch_dir).await {
                tracing::warn!("Initial sync failed: {}", e);
            }
        }
    }

    if let Ok(mut cfg) = config::load() {
        cfg.last_sync_at = Some(sync_start.clone());
        let _ = config::save(&cfg);
    }

    let wdir = dir.clone();
    let wdeb = debounce_ms;
    let wroot = root.clone();
    let wdownloading = downloading.clone();
    let watcher = tokio::spawn(async move {
        if let Err(e) = watcher_loop(&wdir, wdeb, &wroot, wdownloading).await {
            tracing::error!("Watcher error: {}", e);
        }
    });

    let sdir = dir.clone();
    let sse_since = sync_start;
    let sdownloading = downloading.clone();
    let sse = tokio::spawn(async move {
        if let Err(e) = sync_history(&sdir, &sse_since, sdownloading.clone()).await {
            tracing::warn!("History sync failed: {}", e);
        }
        loop {
            if let Err(e) = sse_loop(&sdir, sdownloading.clone()).await {
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

fn add_watches_recursive(
    inotify: &mut Inotify,
    watch_dirs: &mut HashMap<inotify::WatchDescriptor, std::path::PathBuf>,
    dir: &Path,
) -> anyhow::Result<()> {
    let wd = inotify.watches().add(
        dir,
        WatchMask::CREATE | WatchMask::MODIFY | WatchMask::DELETE | WatchMask::MOVED_TO,
    )?;
    watch_dirs.insert(wd, dir.to_path_buf());

    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(ftype) = entry.file_type()
                && ftype.is_dir()
            {
                let name = entry.file_name();
                let name_str = name.to_string_lossy();
                if name_str.starts_with('.') {
                    continue;
                }
                if let Err(e) = add_watches_recursive(inotify, watch_dirs, &entry.path()) {
                    tracing::warn!("Failed to watch {}: {}", entry.path().display(), e);
                }
            }
        }
    }

    Ok(())
}

async fn watcher_loop(_dir: &str, debounce_ms: u64, root: &Path, downloading: Arc<Mutex<HashSet<String>>>) -> anyhow::Result<()> {
    let mut inotify = Inotify::init()?;
    let mut watch_dirs: HashMap<inotify::WatchDescriptor, std::path::PathBuf> = HashMap::new();

    add_watches_recursive(&mut inotify, &mut watch_dirs, root)?;
    tracing::info!("Watching {} directories with inotify", watch_dirs.len());

    let mut buffer = [0u8; 65536];
    let debounce = Duration::from_millis(debounce_ms);

    loop {
        let events = inotify.read_events_blocking(&mut buffer)?;
        for event in events {
            let ev_dir = match watch_dirs.get(&event.wd) {
                Some(d) => d.clone(),
                None => continue,
            };

            let is_create = event.mask.contains(EventMask::CREATE);
            let is_modify = event.mask.contains(EventMask::MODIFY);
            let is_delete = event.mask.contains(EventMask::DELETE);
            let is_moved_to = event.mask.contains(EventMask::MOVED_TO);

            if (is_create || is_modify || is_moved_to)
                && let Some(name) = event.name.and_then(|n| n.to_str())
            {
                if name.starts_with('.') {
                    continue;
                }
                let full_path = ev_dir.join(name);
                let path_str = full_path.to_string_lossy().to_string();

                if (is_create || is_moved_to)
                    && let Ok(meta) = tokio::fs::metadata(&full_path).await
                    && meta.is_dir()
                {
                    if let Err(e) = add_watches_recursive(&mut inotify, &mut watch_dirs, &full_path)
                    {
                        tracing::warn!("Failed to watch new dir {}: {}", path_str, e);
                    }
                    continue;
                }

                tracing::info!("Change detected: {}", path_str);

                if downloading.lock().unwrap().contains(&path_str) {
                    continue;
                }

                sleep(debounce).await;
                let rel_path = full_path
                    .strip_prefix(root)
                    .ok()
                    .and_then(|p| p.parent())
                    .and_then(|p| {
                        let s = p.to_string_lossy().to_string();
                        if s.is_empty() { None } else { Some(s) }
                    });
                let max_retries = 10;
                let mut retry = 0;
                loop {
                    match client::upload(&path_str, rel_path.as_deref()).await {
                        Ok(()) => break,
                        Err(e) => {
                            let err_msg = e.to_string();
                            let empty_file = err_msg.contains("File is empty");
                            let server_error = err_msg.contains("Upload failed");
                            if retry >= max_retries {
                                tracing::warn!("Upload failed for {}: {}", path_str, e);
                                break;
                            }
                            if !empty_file && !server_error {
                                tracing::warn!("Upload failed for {}: {}", path_str, e);
                                break;
                            }
                            let backoff = Duration::from_millis(if empty_file {
                                debounce_ms * (1 << retry)
                            } else {
                                1000
                            });
                            tracing::info!(
                                "Upload failed, retrying {} in {:?}...",
                                path_str,
                                backoff
                            );
                            retry += 1;
                            sleep(backoff).await;
                        }
                    }
                }
            }
            if is_delete && let Some(name) = event.name.and_then(|n| n.to_str()) {
                if name.starts_with('.') {
                    continue;
                }
                let full_path = ev_dir.join(name);
                let rel_path = full_path
                    .strip_prefix(root)
                    .ok()
                    .and_then(|p| p.parent())
                    .and_then(|p| {
                        let s = p.to_string_lossy().to_string();
                        if s.is_empty() { None } else { Some(s) }
                    });
                tracing::info!("Delete detected: {}/{}", ev_dir.display(), name);

                let del_path_str = full_path.to_string_lossy().to_string();
                if downloading.lock().unwrap().contains(&del_path_str) {
                    continue;
                }

                if let Err(e) = client::delete_by_path(name, rel_path.as_deref()).await {
                    tracing::warn!("Failed to notify server about deleted file {}: {}", name, e);
                }
            }
        }
    }
}

async fn sync_history(dir: &str, since: &str, downloading: Arc<Mutex<HashSet<String>>>) -> anyhow::Result<()> {
    client::ensure_valid_token().await?;
    let cfg = crate::config::load()?;
    let url = format!("{}/api/events/history?since={}", cfg.server_url, since);
    let client = reqwest::Client::new();
    let resp = client.get(&url).bearer_auth(&cfg.token).send().await?;
    if !resp.status().is_success() {
        return Ok(());
    }
    let events: Vec<serde_json::Value> = resp.json().await?;
    for ev in &events {
        handle_event(ev, dir, &downloading).await;
    }
    Ok(())
}

async fn sse_loop(dir: &str, downloading: Arc<Mutex<HashSet<String>>>) -> anyhow::Result<()> {
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
                if let Some(data) = line.strip_prefix("data:")
                    && let Ok(event) = serde_json::from_str::<serde_json::Value>(data.trim())
                {
                    handle_event(&event, dir, &downloading).await;
                }
            }
        }
    }

    Ok(())
}

async fn handle_event(event: &serde_json::Value, dir: &str, downloading: &Arc<Mutex<HashSet<String>>>) {
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
                    downloading.lock().unwrap().insert(dest.clone());
                    tracing::info!("Remote {}: downloading {} as {}", event_type, id, dest);
                    if let Err(e) = client::download(id, Some(&dest)).await {
                        tracing::warn!("Download failed: {}", e);
                    }
                    downloading.lock().unwrap().remove(&dest);
                } else if event_type == "file_updated" {
                    match resolve_conflict(id, &dest).await {
                        Ok(ConflictAction::Touch) => {
                            tracing::info!(
                                "Remote {}: content identical, syncing mtime for {}",
                                event_type,
                                dest
                            );
                            let file_info = client::get_file_info(id).await;
                            if let Ok(info) = file_info
                                && let Some(mtime_str) = info["mtime"].as_str()
                                && let Ok(mtime) = chrono::DateTime::parse_from_rfc3339(mtime_str)
                            {
                                let ts = filetime::FileTime::from_unix_time(mtime.timestamp(), 0);
                                let _ = filetime::set_file_times(&dest, ts, ts);
                            }
                        }
                        Ok(ConflictAction::Overwrite) => {
                            downloading.lock().unwrap().insert(dest.clone());
                            tracing::info!(
                                "Remote {}: remote is newer, overwriting {}",
                                event_type,
                                dest
                            );
                            if let Err(e) = client::download(id, Some(&dest)).await {
                                tracing::warn!("Download failed: {}", e);
                            }
                            downloading.lock().unwrap().remove(&dest);
                        }
                        Ok(ConflictAction::Conflict(conflict_path)) => {
                            downloading.lock().unwrap().insert(conflict_path.clone());
                            tracing::info!(
                                "Remote {}: local is newer, saving conflict as {}",
                                event_type,
                                conflict_path
                            );
                            if let Err(e) = client::download(id, Some(&conflict_path)).await {
                                tracing::warn!("Download failed: {}", e);
                            }
                            downloading.lock().unwrap().remove(&conflict_path);
                        }
                        Ok(ConflictAction::Skip) | Err(_) => {
                            tracing::info!(
                                "Remote {}: {} already exists locally, skipping",
                                event_type,
                                file_name
                            );
                        }
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
                downloading.lock().unwrap().insert(dest.clone());
                tracing::info!("Remote delete: {}", dest);
                let _ = tokio::fs::remove_file(&dest).await;
                downloading.lock().unwrap().remove(&dest);
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
    tracing::info!("Bidirectional sync of {}", dir);

    let cfg = crate::config::load()?;
    let semaphore = Arc::new(Semaphore::new(cfg.concurrency));

    let remote_files = client::fetch_remote_files().await?;
    let mut remote_map: HashMap<String, &serde_json::Value> = HashMap::new();
    for f in &remote_files {
        let name = f["name"].as_str().unwrap_or("");
        let path = f["path"].as_str().unwrap_or("");
        let key = if path.is_empty() {
            name.to_string()
        } else {
            format!("{}/{}", path, name)
        };
        remote_map.insert(key, f);
    }

    let mut local_keys: HashSet<String> = HashSet::new();
    let files_to_upload = sync_local(dir_path, dir_path, &remote_map, &mut local_keys).await?;

    let mut handles = Vec::new();
    for (file_path, remote_path) in files_to_upload {
        let sem = semaphore.clone();
        handles.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();
            if let Err(e) = client::upload(&file_path, remote_path.as_deref()).await {
                tracing::warn!("Failed to upload {}: {}", file_path, e);
            }
        }));
    }
    for handle in handles {
        let _ = handle.await;
    }

    let mut handles = Vec::new();
    for (key, f) in &remote_map {
        if local_keys.contains(key) {
            continue;
        }
        let dest = format!("{}/{}", dir, key);
        if let Some(id) = f["id"].as_str() {
            let id = id.to_string();
            if !Path::new(&dest).exists() {
                if let Some(parent) = Path::new(&dest).parent() {
                    fs::create_dir_all(parent).await?;
                }
                let sem = semaphore.clone();
                handles.push(tokio::spawn(async move {
                    let _permit = sem.acquire().await.unwrap();
                    tracing::info!("Downloading remote file: {}", dest);
                    if let Err(e) = client::download(&id, Some(&dest)).await {
                        tracing::warn!("Failed to download {}: {}", dest, e);
                    }
                }));
            }
        }
    }
    for handle in handles {
        let _ = handle.await;
    }

    tracing::info!("🚀 Sincronización completa");

    Ok(())
}

async fn sync_local(
    base: &Path,
    dir: &Path,
    remote_map: &HashMap<String, &serde_json::Value>,
    local_keys: &mut HashSet<String>,
) -> anyhow::Result<Vec<(String, Option<String>)>> {
    let mut files_to_upload = Vec::new();
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
            let sub = Box::pin(sync_local(base, &path, remote_map, local_keys)).await?;
            files_to_upload.extend(sub);
        } else if file_type.is_file() {
            let rel_path = path.strip_prefix(base).unwrap();
            let key = rel_path.to_string_lossy().to_string();
            local_keys.insert(key.clone());

            let remote_path = rel_path.parent().and_then(|p| {
                let s = p.to_string_lossy();
                if s.is_empty() {
                    None
                } else {
                    Some(s.to_string())
                }
            });

            let needs_upload = match remote_map.get(&key) {
                Some(remote) => {
                    let meta = fs::metadata(&path).await?;
                    let local_size = meta.len() as i64;
                    let local_mtime = meta.modified().ok();
                    let remote_size = (*remote)["size"].as_i64().unwrap_or(-1);
                    let remote_mtime = (*remote)["mtime"].as_str();

                    match (local_mtime, remote_mtime) {
                        (Some(lm), Some(rm)) if !rm.is_empty() && mtime_equalish(lm, rm) => false,
                        _ => {
                            if local_size != remote_size {
                                true
                            } else {
                                match (*remote)["checksum_sha256"].as_str() {
                                    Some(remote_hash) if !remote_hash.is_empty() => {
                                        let local_hash = sha256_of_file(&path).await?;
                                        local_hash != remote_hash
                                    }
                                    _ => true,
                                }
                            }
                        }
                    }
                }
                None => true,
            };

            if needs_upload {
                files_to_upload.push((path.to_string_lossy().to_string(), remote_path));
            }
        }
    }
    Ok(files_to_upload)
}

async fn sha256_of_file(path: &Path) -> anyhow::Result<String> {
    use sha2::Digest;
    let mut file = tokio::fs::File::open(path).await?;
    let mut hasher = sha2::Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = tokio::io::AsyncReadExt::read(&mut file, &mut buf).await?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hex::encode(hasher.finalize()))
}

fn mtime_equalish(local: SystemTime, remote_mtime: &str) -> bool {
    if let Ok(rm) = chrono::DateTime::parse_from_rfc3339(remote_mtime) {
        let rm_sys: SystemTime = rm.with_timezone(&Utc).into();
        let diff = if local > rm_sys {
            local.duration_since(rm_sys)
        } else {
            rm_sys.duration_since(local)
        };
        diff.is_ok_and(|d| d.as_secs() < 2)
    } else {
        false
    }
}

enum ConflictAction {
    Touch,
    Overwrite,
    Conflict(String),
    Skip,
}

async fn resolve_conflict(id: &str, dest: &str) -> anyhow::Result<ConflictAction> {
    let file_info = client::get_file_info(id).await?;
    let remote_mtime = file_info["mtime"].as_str().unwrap_or("");
    let remote_hash = file_info["checksum_sha256"].as_str().unwrap_or("");

    let local_hash = sha256_of_file(Path::new(dest)).await?;

    if !remote_hash.is_empty() && local_hash == remote_hash {
        return Ok(ConflictAction::Touch);
    }

    let local_meta = tokio::fs::metadata(dest).await?;
    let local_mtime = local_meta.modified()?;

    if let Ok(rm) = chrono::DateTime::parse_from_rfc3339(remote_mtime) {
        let remote_sys: SystemTime = rm.with_timezone(&Utc).into();
        if remote_sys > local_mtime {
            return Ok(ConflictAction::Overwrite);
        }
        let conflict_path = format!(
            "{}.{}.conflict",
            dest,
            rm.format("%Y-%m-%dT%H%M%S%.3f")
        );
        Ok(ConflictAction::Conflict(conflict_path))
    } else {
        Ok(ConflictAction::Skip)
    }
}

async fn incremental_sync(dir: &str, since: &str, downloading: &Arc<Mutex<HashSet<String>>>) -> anyhow::Result<()> {
    let since_dt = DateTime::parse_from_rfc3339(since)?;
    let since_sys: SystemTime = since_dt.with_timezone(&Utc).into();

    tracing::info!("Uploading local files changed since {}", since);
    walk_and_upload_changed(Path::new(dir), Path::new(dir), &since_sys).await?;

    tracing::info!("Applying remote changes since {}", since);
    sync_history(dir, since, downloading.clone()).await?;

    tracing::info!("Incremental sync complete");
    Ok(())
}

async fn walk_and_upload_changed(
    base: &Path,
    dir: &Path,
    since: &SystemTime,
) -> anyhow::Result<()> {
    let mut entries = fs::read_dir(dir).await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        let name = entry.file_name();
        if name.to_string_lossy().starts_with('.') {
            continue;
        }
        let file_type = entry.file_type().await?;
        if file_type.is_dir() {
            Box::pin(walk_and_upload_changed(base, &path, since)).await?;
        } else if file_type.is_file() {
            let meta = fs::metadata(&path).await?;
            if let Ok(mtime) = meta.modified()
                && mtime > *since
            {
                let rel_path = path.strip_prefix(base).ok().and_then(|p| {
                    p.parent().and_then(|parent| {
                        let s = parent.to_string_lossy();
                        if s.is_empty() {
                            None
                        } else {
                            Some(s.to_string())
                        }
                    })
                });
                let path_str = path.to_string_lossy().to_string();
                tracing::info!("Uploading changed file: {}", path_str);
                if let Err(e) = client::upload(&path_str, rel_path.as_deref()).await {
                    tracing::warn!("Failed to upload {}: {}", path_str, e);
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, Duration};

    #[test]
    fn mtime_equalish_exact_match() {
        let now = SystemTime::now();
        let rfc = {
            let dt: chrono::DateTime<Utc> = now.into();
            dt.to_rfc3339()
        };
        assert!(mtime_equalish(now, &rfc));
    }

    #[test]
    fn mtime_equalish_within_one_second() {
        let local = SystemTime::now();
        let remote = local + Duration::from_secs(1);
        let rfc = {
            let dt: chrono::DateTime<Utc> = remote.into();
            dt.to_rfc3339()
        };
        assert!(mtime_equalish(local, &rfc));
    }

    #[test]
    fn mtime_equalish_over_two_seconds() {
        let local = SystemTime::now();
        let remote = local + Duration::from_secs(3);
        let rfc = {
            let dt: chrono::DateTime<Utc> = remote.into();
            dt.to_rfc3339()
        };
        assert!(!mtime_equalish(local, &rfc));
    }

    #[test]
    fn mtime_equalish_invalid_remote_string() {
        assert!(!mtime_equalish(SystemTime::now(), "not-a-date"));
    }

    #[test]
    fn mtime_equalish_empty_string() {
        assert!(!mtime_equalish(SystemTime::now(), ""));
    }

    #[test]
    fn mtime_equalish_exactly_two_seconds() {
        let local = SystemTime::now();
        let remote = local + Duration::from_secs(2);
        let rfc = {
            let dt: chrono::DateTime<Utc> = remote.into();
            dt.to_rfc3339()
        };
        assert!(!mtime_equalish(local, &rfc));
    }

    #[test]
    fn mtime_equalish_sub_second_diff() {
        let local = SystemTime::now();
        let remote = local + Duration::from_millis(500);
        let rfc = {
            let dt: chrono::DateTime<Utc> = remote.into();
            dt.to_rfc3339()
        };
        assert!(mtime_equalish(local, &rfc));
    }

    #[tokio::test]
    async fn sha256_of_empty_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("empty.txt");
        tokio::fs::write(&path, b"").await.unwrap();

        let hash = sha256_of_file(&path).await.unwrap();
        assert_eq!(
            hash,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[tokio::test]
    async fn sha256_of_known_content() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("hello.txt");
        tokio::fs::write(&path, b"hello world\n").await.unwrap();

        let hash = sha256_of_file(&path).await.unwrap();
        assert_eq!(
            hash,
            "a948904f2f0f479b8f8197694b30184b0d2ed1c1cd2a1ec0fb85d299a192a447"
        );
    }

    #[tokio::test]
    async fn sha256_of_large_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("large.bin");
        let content = vec![0xABu8; 100_000];
        tokio::fs::write(&path, &content).await.unwrap();

        let hash = sha256_of_file(&path).await.unwrap();
        let expected = {
            use sha2::Digest;
            let mut hasher = sha2::Sha256::new();
            hasher.update(&vec![0xABu8; 100_000]);
            hex::encode(hasher.finalize())
        };
        assert_eq!(hash, expected);
    }
}
