# S3EC — S3 Event Client

## Overview

Single-crate Rust CLI (edition 2024) that syncs files between a local directory and an S3-compatible event server. Not a workspace.

## Quick reference

```bash
cargo build                    # debug build
cargo build --release          # release build
cargo run -- login --server <url> --api-key <key>
cargo fmt --check              # format check (no custom config)
cargo clippy -- -D warnings    # lint
cargo test                     # run tests
```

## Architecture

| Module | Responsibility |
|--------|---------------|
| `src/cli.rs` | Clap derive CLI (subcommands: login, upload, download, ls, info, rm, daemon, sync) |
| `src/client.rs` | HTTP client: login, token refresh, multipart upload, download, list, info, delete |
| `src/config.rs` | TOML config file management at `~/.config/s3ec/config.toml` |
| `src/daemon.rs` | Inotify watcher, SSE event loop, initial sync, retry with exponential backoff |
| `src/error.rs` | Error types |
| `src/main.rs` | Tokio entrypoint, command dispatch |

## Key behavior

- Config is stored at `~/.config/s3ec/config.toml` — auto-created on first login.
- Token auto-refreshes when less than 5 minutes from expiry.
- Daemon uses inotify (CREATE/MODIFY/DELETE) with configurable debounce.
- Empty files are retried with exponential backoff (up to 10×) to handle inotify race conditions.
- `sync` subcommand does a one-shot recursive upload of all files in a directory.
- SSE receives real-time remote events and downloads/removes files accordingly.
- `systemd` service file at `contrib/s3ec.service`.

## Style / conventions

- Uses `Rust 2024` edition.
- No `rustfmt.toml` or `clippy.toml` — all defaults.
- Imports: `std::*` first, then external crates, then `crate::*`.
- Tracing `info!` for user-facing events, `warn!` for failures, `error!` for unrecoverable errors.
- `anyhow::bail!` for early returns on error conditions.