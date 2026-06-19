use thiserror::Error;

#[derive(Debug, Error)]
#[allow(dead_code)]
pub enum AppError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Server error: {status} {body}")]
    Server {
        status: reqwest::StatusCode,
        body: String,
    },
    #[error("{0}")]
    Other(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use reqwest::StatusCode;

    #[test]
    fn display_io_error() {
        let err = AppError::Io(std::io::Error::new(std::io::ErrorKind::NotFound, "file not found"));
        let msg = err.to_string();
        assert!(msg.contains("IO error") || msg.contains("file not found"));
    }

    #[test]
    fn display_server_error() {
        let err = AppError::Server {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            body: "server error".into(),
        };
        let msg = err.to_string();
        assert!(msg.contains("500"));
        assert!(msg.contains("server error"));
    }

    #[test]
    fn display_other_error() {
        let err = AppError::Other("something went wrong".into());
        assert_eq!(err.to_string(), "something went wrong");
    }

    #[test]
    fn debug_format() {
        let err = AppError::Other("test".into());
        let debug = format!("{:?}", err);
        assert!(debug.contains("Other") || debug.contains("test"));
    }

    #[test]
    fn from_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::PermissionDenied, "permission denied");
        let app_err: AppError = io_err.into();
        assert!(app_err.to_string().contains("IO error"));
    }
}
