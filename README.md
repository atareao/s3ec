<div align="center">

# s3ec — S3 Event Client

[![Crates.io](https://img.shields.io/crates/v/s3ec?style=flat-square)](https://crates.io/crates/s3ec)
[![License](https://img.shields.io/badge/License-MIT-blue?style=flat-square)](LICENSE)
[![Rust](https://img.shields.io/badge/Rust-2024-dea584?style=flat-square&logo=rust&logoColor=white)](https://www.rust-lang.org)
[![CI](https://img.shields.io/badge/CI-Passing-green?style=flat-square)](#)

File sync daemon and CLI for an S3-compatible event server. Watch a directory and automatically upload file changes, and receive remote file events via SSE.

</div>

## Features

- **Daemon mode** — watch a directory with inotify, debounce changes, and upload with exponential backoff
- **Initial sync** — upload all existing files on startup (`s3ec sync -w <dir>`)
- **Real-time remote events** — SSE client that downloads/removes files when remote changes happen
- **Bidirectional sync** — local changes push upstream, remote changes pull downstream
- **Token auth** — login with API key, automatic token refresh before expiry
- **Metadata preservation** — file permissions and modification times are stored and restored

## Installation

```bash
cargo install s3ec
```

Or build from source:

```bash
git clone https://github.com/anomalyco/s3ec
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
| `download <id>` | Download a file by ID |
| `ls` | List files (supports `--path`, `--search`, `--limit`, `--offset`) |
| `info <id>` | Show file metadata |
| `rm <id>` | Delete a file |
| `sync -w <dir>` | One-shot upload of all files in a directory |
| `daemon -w <dir>` | Start the file watcher daemon |

### Daemon

The daemon watches a directory and syncs bidirectionally:

```bash
s3ec daemon -w /path/to/watch --debounce 500
```

- Uploads local file changes to the server
- Receives remote events via SSE and downloads/removes files accordingly
- Retries empty files (still being written) with exponential backoff up to 10 times
- Runs an initial sync of all existing files on startup

### Systemd Service

A systemd unit file is provided in `contrib/s3ec.service`:

```bash
sudo cp contrib/s3ec.service /etc/systemd/system/
# Edit ExecStart to set your watch directory
sudo systemctl daemon-reload
sudo systemctl enable --now s3ec
```

## Configuration

The config file at `~/.config/s3ec/config.toml` is managed automatically by the `login` command and contains:

- `server_url` — the API server base URL
- `api_key` — the API key for authentication
- `token` — the JWT token (auto-refreshed)
- `expires_at` — token expiry timestamp

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