use thiserror::Error;

#[derive(Debug, Error)]
#[expect(dead_code)]
pub enum AppError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Server error: {status} {body}")]
    Server { status: reqwest::StatusCode, body: String },
    #[error("{0}")]
    Other(String),
}