use clap::{Parser, Subcommand};

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
        #[arg(long)]
        watch: String,
        #[arg(long, default_value = "500")]
        debounce_ms: u64,
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
        }
    }
}