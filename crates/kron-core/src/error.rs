use thiserror::Error;

#[derive(Debug, Error)]
pub enum KronError {
    #[error("invalid cron expression: {0}")]
    InvalidCron(String),

    #[error("invalid timezone: {0}")]
    InvalidTimezone(String),

    #[error("timer not found: {0}")]
    TimerNotFound(String),

    #[error("log I/O error: {0}")]
    LogIo(#[from] std::io::Error),

    #[error("log serialization error: {0}")]
    LogSerde(#[from] serde_json::Error),

    #[error("corrupt log line {line} in {path}: {source}")]
    CorruptLog {
        path: String,
        line: usize,
        source: serde_json::Error,
    },

    #[error("engine already started")]
    AlreadyStarted,

    #[error("engine has already been shut down")]
    AlreadyStopped,

    #[error("shutdown timed out after {timeout_ms}ms")]
    ShutdownTimeout { timeout_ms: u64 },

    #[error("data directory is already locked: {path}")]
    DataDirLocked { path: String },

    #[error("invalid snapshot: {0}")]
    InvalidSnapshot(String),

    #[error("IPC unavailable: {0}")]
    IpcUnavailable(String),
}
