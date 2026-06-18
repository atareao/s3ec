use clap::{Parser, Subcommand};
use chrono::Utc;

#[derive(Parser)]
#[command(name = "s3ec", about = "S3 Event Client")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    Login {
        #[arg(long)]
        server: String,
        #[arg(long)]
        api_key: String,
    },
    Upload {
        file: String,
        #[arg(long)]
        path: Option<String>,
    },
    Download {
        id: String,
        #[arg(short = 'o')]
        output: Option<String>,
    },
    #[command(name = "ls")]
    List {
        #[arg(long)]
        path: Option<String>,
        #[arg(long)]
        search: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        offset: Option<i64>,
    },
    Info {
        id: String,
    },
    Rm {
        id: String,
    },
    Daemon {
        #[arg(long, short = 'w')]
        watch: String,
        #[arg(long, alias = "debounce", default_value = "500")]
        debounce_ms: u64,
    },
    #[command(name = "sync")]
    Sync {
        #[arg(long, short = 'w')]
        watch: String,
    },
}

impl Commands {
    pub async fn execute(self) -> anyhow::Result<()> {
        use Commands::*;
        match self {
            Login { server, api_key } => crate::client::login(&server, &api_key).await,
            Upload { file, path } => crate::client::upload(&file, path.as_deref()).await,
            Download { id, output } => crate::client::download(&id, output.as_deref()).await,
            List { path, search, limit, offset } => {
                crate::client::list(path.as_deref(), search.as_deref(), limit, offset).await
            }
            Info { id } => crate::client::info(&id).await,
            Rm { id } => crate::client::rm(&id).await,
            Daemon { watch, debounce_ms } => crate::daemon::run(&watch, debounce_ms).await,
            Sync { watch } => {
                crate::daemon::sync_dir(&watch).await?;
                if let Ok(mut cfg) = crate::config::load() {
                    cfg.last_sync_at = Some(Utc::now().to_rfc3339());
                    let _ = crate::config::save(&cfg);
                }
                Ok(())
            }
        }
    }
}