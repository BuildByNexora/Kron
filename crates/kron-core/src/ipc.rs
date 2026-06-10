use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::engine::Engine;
use crate::error::KronError;
use crate::event::Event;
use crate::log::AppendOnlyLog;
use crate::snapshot;
use crate::timer::{TimerId, TimerSummary};

const MAX_IPC_LINE_BYTES: u64 = 1024 * 1024;

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "cmd", rename_all = "snake_case")]
pub enum IpcRequest {
    Auth {
        token: String,
        inner: Box<IpcRequest>,
    },
    Status {
        name: String,
    },
    List,
    History {
        name: String,
        limit: Option<usize>,
    },
    Shutdown,
    Compact,
    Doctor,
    RuntimeStatus,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
pub enum IpcResponse {
    Ok { data: serde_json::Value },
    Error { message: String },
}

#[cfg(unix)]
pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.sock")
}

#[cfg(not(unix))]
pub fn socket_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.sock")
}

pub fn start_server(engine: Arc<Engine>) -> Result<std::thread::JoinHandle<()>, KronError> {
    prepare_ipc_files(engine.data_dir())?;
    let token = read_or_create_token(engine.data_dir())?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        use std::os::unix::net::UnixListener;

        let path = socket_path(engine.data_dir());
        let _ = std::fs::remove_file(&path);
        let listener = UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        listener.set_nonblocking(true)?;
        let tcp = start_tcp_server(Arc::clone(&engine), token.clone())?;

        Ok(std::thread::spawn(move || {
            let _tcp = tcp;
            loop {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let engine = Arc::clone(&engine);
                        let token = token.clone();
                        std::thread::spawn(move || {
                            let _ = handle_stream(engine, stream, &token);
                        });
                    }
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        if !path.exists() {
                            break;
                        }
                        std::thread::sleep(Duration::from_millis(50));
                    }
                    Err(_) => break,
                }
            }
        }))
    }

    #[cfg(not(unix))]
    {
        Ok(tcp)
    }
}

pub fn request(data_dir: &Path, request: &IpcRequest) -> Result<IpcResponse, KronError> {
    let token = read_token(data_dir)?;
    let request = IpcRequest::Auth {
        token,
        inner: Box::new(clone_request(request)?),
    };

    #[cfg(unix)]
    {
        use std::os::unix::net::UnixStream;

        let path = socket_path(data_dir);
        if let Ok(mut stream) = UnixStream::connect(&path) {
            let mut line = serde_json::to_string(&request)?;
            line.push('\n');
            stream.write_all(line.as_bytes())?;
            stream.flush()?;

            let mut reader = BufReader::new(stream);
            let response = read_limited_line(&mut reader)?;
            let response = serde_json::from_str(&response)?;
            return Ok(response);
        }
    }

    let endpoint = read_tcp_endpoint(data_dir)?;
    let mut stream = TcpStream::connect(endpoint)?;
    let mut line = serde_json::to_string(&request)?;
    line.push('\n');
    stream.write_all(line.as_bytes())?;
    stream.flush()?;
    let mut reader = BufReader::new(stream);
    let response = read_limited_line(&mut reader)?;
    Ok(serde_json::from_str(&response)?)
}

fn clone_request(request: &IpcRequest) -> Result<IpcRequest, KronError> {
    Ok(serde_json::from_value(serde_json::to_value(request)?)?)
}

fn handle_stream<S: std::io::Read + std::io::Write>(
    engine: Arc<Engine>,
    mut stream: S,
    expected_token: &str,
) -> Result<(), KronError> {
    let mut reader = BufReader::new(&mut stream);
    let line = read_limited_line(&mut reader)?;
    drop(reader);

    let request: IpcRequest = serde_json::from_str(&line)?;
    let request = match request {
        IpcRequest::Auth { token, inner }
            if secure_eq(token.as_bytes(), expected_token.as_bytes()) =>
        {
            *inner
        }
        IpcRequest::Auth { .. } => {
            let mut encoded = serde_json::to_string(&IpcResponse::Error {
                message: "invalid IPC token".to_string(),
            })?;
            encoded.push('\n');
            stream.write_all(encoded.as_bytes())?;
            return Ok(());
        }
        _ => {
            let mut encoded = serde_json::to_string(&IpcResponse::Error {
                message: "missing IPC token".to_string(),
            })?;
            encoded.push('\n');
            stream.write_all(encoded.as_bytes())?;
            return Ok(());
        }
    };
    let response = match handle_request(engine, request) {
        Ok(data) => IpcResponse::Ok { data },
        Err(err) => IpcResponse::Error {
            message: err.to_string(),
        },
    };
    let mut encoded = serde_json::to_string(&response)?;
    encoded.push('\n');
    stream.write_all(encoded.as_bytes())?;
    stream.flush()?;
    Ok(())
}

fn handle_request(
    engine: Arc<Engine>,
    request: IpcRequest,
) -> Result<serde_json::Value, KronError> {
    match request {
        IpcRequest::Auth { .. } => Err(KronError::IpcUnavailable(
            "nested auth request is invalid".to_string(),
        )),
        IpcRequest::Status { name } => Ok(serde_json::to_value(engine.status(&name))?),
        IpcRequest::List => Ok(serde_json::to_value(engine.list())?),
        IpcRequest::History { name, limit } => {
            let entries = history(engine.data_dir(), &name, limit)?;
            Ok(serde_json::to_value(entries)?)
        }
        IpcRequest::Shutdown => {
            let engine = Arc::clone(&engine);
            std::thread::spawn(move || {
                if let Ok(runtime) = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    let _ = runtime.block_on(engine.shutdown(Duration::from_secs(5)));
                }
            });
            Ok(serde_json::json!({"shutdown": "requested"}))
        }
        IpcRequest::Compact => {
            engine.compact()?;
            Ok(serde_json::json!({"compacted": true}))
        }
        IpcRequest::Doctor | IpcRequest::RuntimeStatus => Ok(serde_json::json!({
            "data_dir": engine.data_dir(),
            "lock_path": engine.lock_path(),
            "socket_path": socket_path(engine.data_dir()),
            "timers": engine.list().len()
        })),
    }
}

fn prepare_ipc_files(data_dir: &Path) -> Result<(), KronError> {
    std::fs::create_dir_all(data_dir)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub fn token_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.token")
}

pub fn port_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.port")
}

pub fn read_or_create_token(data_dir: &Path) -> Result<String, KronError> {
    let path = token_path(data_dir);
    if path.exists() {
        return read_token(data_dir);
    }
    let token = generate_secret_token()?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&path)?;
    file.write_all(token.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
    }
    Ok(token)
}

pub fn read_token(data_dir: &Path) -> Result<String, KronError> {
    let mut token = String::new();
    File::open(token_path(data_dir))?.read_to_string(&mut token)?;
    Ok(token.trim().to_string())
}

fn start_tcp_server(
    engine: Arc<Engine>,
    token: String,
) -> Result<std::thread::JoinHandle<()>, KronError> {
    let host = std::env::var("KRON_IPC_HOST").unwrap_or_else(|_| "127.0.0.1".to_string());
    if !is_loopback_host(&host)
        && std::env::var("KRON_UNSAFE_ALLOW_REMOTE_IPC").as_deref() != Ok("1")
    {
        return Err(KronError::IpcUnavailable(format!(
            "refusing to bind IPC to non-loopback host {host}; set KRON_UNSAFE_ALLOW_REMOTE_IPC=1 only if you understand the risk"
        )));
    }
    let port = std::env::var("KRON_IPC_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .unwrap_or(0);
    let listener = TcpListener::bind((host.as_str(), port))?;
    let addr = listener.local_addr()?;
    listener.set_nonblocking(true)?;
    let port_path = port_path(engine.data_dir());
    std::fs::write(&port_path, format!("{}\n", addr))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&port_path, std::fs::Permissions::from_mode(0o600))?;
    }
    let stop_file = port_path;
    Ok(std::thread::spawn(move || loop {
        match listener.accept() {
            Ok((stream, _)) => {
                let engine = Arc::clone(&engine);
                let token = token.clone();
                std::thread::spawn(move || {
                    let _ = handle_stream(engine, stream, &token);
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                if !stop_file.exists() {
                    break;
                }
                std::thread::sleep(Duration::from_millis(50));
            }
            Err(_) => break,
        }
    }))
}

fn read_limited_line<R: BufRead>(reader: &mut R) -> Result<String, KronError> {
    let mut limited = reader.take(MAX_IPC_LINE_BYTES + 1);
    let mut line = String::new();
    let bytes = limited.read_line(&mut line)?;
    if bytes as u64 > MAX_IPC_LINE_BYTES || !line.ends_with('\n') {
        return Err(KronError::IpcUnavailable(
            "IPC request exceeded maximum line size".to_string(),
        ));
    }
    Ok(line)
}

pub fn generate_secret_token() -> Result<String, KronError> {
    let mut bytes = [0u8; 32];
    getrandom::getrandom(&mut bytes)
        .map_err(|err| KronError::IpcUnavailable(format!("failed to generate token: {err}")))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

pub fn secure_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    let mut diff = 0u8;
    for (a, b) in left.iter().zip(right.iter()) {
        diff |= a ^ b;
    }
    diff == 0
}

fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "localhost" | "::1")
}

fn read_tcp_endpoint(data_dir: &Path) -> Result<String, KronError> {
    let mut endpoint = String::new();
    File::open(port_path(data_dir))?.read_to_string(&mut endpoint)?;
    Ok(endpoint.trim().to_string())
}

pub fn history(
    data_dir: &Path,
    name: &str,
    limit: Option<usize>,
) -> Result<Vec<serde_json::Value>, KronError> {
    let log = AppendOnlyLog::open(snapshot::aof_path(data_dir))?;
    let entries = log.replay()?;
    let id = TimerId::new(name);
    let mut matching = Vec::new();

    for entry in entries {
        let matches_timer = match &entry.event {
            Event::TimerCreated { spec } | Event::TimerUpdated { spec } => spec.id == id,
            Event::TimerPaused { timer_id, .. }
            | Event::TimerResumed { timer_id, .. }
            | Event::TimerCancelled { timer_id, .. }
            | Event::RunDue { timer_id, .. }
            | Event::RunStarted { timer_id, .. }
            | Event::RunSucceeded { timer_id, .. }
            | Event::RunFailed { timer_id, .. }
            | Event::RunRetrying { timer_id, .. }
            | Event::RunDead { timer_id, .. } => timer_id == &id,
        };
        if matches_timer {
            matching.push(serde_json::to_value(&entry)?);
        }
    }
    if let Some(limit) = limit {
        let start = matching.len().saturating_sub(limit);
        Ok(matching.split_off(start))
    } else {
        Ok(matching)
    }
}

pub fn summary_to_json(summary: TimerSummary) -> Result<serde_json::Value, KronError> {
    Ok(serde_json::to_value(summary)?)
}
