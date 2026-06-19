<div align="center">

# s3ec — S3 Event Client

[![Crates.io](https://img.shields.io/crates/v/s3ec?style=flat-square)](https://crates.io/crates/s3ec)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2024-dea584?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org)

File sync daemon and CLI for an S3-compatible event server.

Watch a directory, upload local changes automatically, and receive remote file events via SSE for bidirectional sync.

</div>

## Features

- **Daemon mode** — watch a directory with inotify, debounce file changes, upload with retry
- **Initial & incremental sync** — full sync on first run, incremental sync on subsequent runs
- **Real-time remote events** — SSE client that downloads/removes files when remote changes happen
- **Conflict resolution** — automatic handling of concurrent local and remote modifications
- **Download loop prevention** — coordinated inotify/SSE event filtering avoids re-upload cycles
- **Token auth** — login with API key, automatic token refresh before expiry
- **Metadata preservation** — file permissions and modification times stored and restored

## Installation

```bash
cargo install s3ec
```

Or build from source:

```bash
git clone https://github.com/atareao/s3ec
cd s3ec
cargo build --release
```

## Usage

### Login

```bash
s3ec login --server https://your-server.com --api-key <key>
```

Config is saved to `~/.config/s3ec/config.toml`.

### Commands

| Command | Description |
|---|---|
| `upload <file>` | Upload a file |
| `upload <file> --path remote/dir` | Upload to a remote subdirectory |
| `download <id>` | Download a file by ID |
| `download <id> -o output.txt` | Download to a specific path |
| `ls` | List files |
| `ls --path docs --search report --limit 10` | Filtered list |
| `info <id>` | Show file metadata |
| `rm <id>` | Delete a file |
| `sync -w <dir>` | One-shot bidirectional sync |
| `daemon -w <dir>` | Start the file watcher daemon |

### Daemon

```bash
s3ec daemon -w /path/to/watch --debounce 500
```

- Uploads local file changes to the server with retry on server errors
- Receives remote events via SSE and downloads/removes files accordingly
- Skips empty files immediately (no retry for zero-byte files)
- Prevents download loops by filtering inotify events triggered by own downloads
- Runs an initial full sync on first startup, incremental sync on subsequent runs
- Configurable concurrency (default: 20) and debounce delay (default: 500ms)

### One-shot Sync

```bash
s3ec sync -w /path/to/dir
```

Bidirectional sync: uploads local files missing or changed on the server, and downloads remote files not present locally.

### Systemd Service

A systemd unit file is provided in `contrib/s3ec.service`:

```bash
sudo cp contrib/s3ec.service /etc/systemd/system/
# Edit ExecStart to set your watch directory
sudo systemctl daemon-reload
sudo systemctl enable --now s3ec
```

## Configuration

Created automatically by `s3ec login` at `~/.config/s3ec/config.toml`:

| Field | Description |
|---|---|
| `server_url` | API server base URL |
| `api_key` | API key for authentication |
| `token` | JWT token (auto-refreshed) |
| `expires_at` | Token expiry timestamp (RFC 3339) |
| `concurrency` | Max concurrent uploads/downloads (default: 20) |
| `last_sync_at` | Timestamp of last sync (set by `sync` and `daemon`) |

## Architecture

```
┌──────────────┐   SSE events   ┌──────────────┐
│   Server     │◄──────────────►│   s3ec CLI   │
│  (REST API)  │   HTTP upload  │  (daemon)    │
└──────────────┘    download    └──────┬───────┘
                                       │ inotify
                                       ▼
                                  ┌──────────┐
                                  │ Watched  │
                                  │ Directory│
                                  └──────────┘
```