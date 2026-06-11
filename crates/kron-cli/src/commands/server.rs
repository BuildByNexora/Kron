use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;

use clap::Subcommand;
use kron_core::ipc;
use kron_core::openraft_adapter::runtime::{self, OpenRaftCluster};
use serde::Deserialize;

#[derive(Subcommand)]
pub enum ServerCommand {
    Start {
        #[arg(long, default_value = "n1")]
        node_id: String,
        #[arg(long, default_value = "127.0.0.1:7379")]
        http: String,
        #[arg(long, default_value = "127.0.0.1:7380")]
        raft: String,
        #[arg(long)]
        leader_id: Option<String>,
        #[arg(long)]
        cluster_token: Option<String>,
    },
    Status,
    Shutdown,
    Join {
        #[arg(long)]
        node_id: String,
        #[arg(long)]
        http: String,
        #[arg(long)]
        raft: String,
    },
    Leave {
        #[arg(long)]
        node_id: String,
    },
}

pub fn run(command: ServerCommand, data_dir: &Path) -> Result<(), String> {
    match command {
        ServerCommand::Start {
            node_id,
            http,
            raft,
            leader_id,
            cluster_token,
        } => start(data_dir, node_id, http, raft, leader_id, cluster_token),
        ServerCommand::Status => {
            let value = request(
                data_dir,
                "GET",
                "/v1/cluster/status",
                serde_json::Value::Null,
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        ServerCommand::Shutdown => {
            let value = request(
                data_dir,
                "POST",
                "/v1/runtime/shutdown",
                serde_json::Value::Object(Default::default()),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        ServerCommand::Join {
            node_id,
            http,
            raft,
        } => {
            let value = request(
                data_dir,
                "POST",
                "/v1/cluster/join",
                serde_json::json!({
                    "node_id": node_id,
                    "http_addr": http,
                    "raft_addr": raft,
                }),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        ServerCommand::Leave { node_id } => {
            let value = request(
                data_dir,
                "POST",
                "/v1/cluster/leave",
                serde_json::json!({ "node_id": node_id }),
            )?;
            println!(
                "{}",
                serde_json::to_string_pretty(&value).map_err(|e| e.to_string())?
            );
            Ok(())
        }
    }
}

fn start(
    data_dir: &Path,
    node_id: String,
    http: String,
    raft: String,
    leader_id: Option<String>,
    cluster_token: Option<String>,
) -> Result<(), String> {
    let bootstrap_single_node = leader_id
        .as_deref()
        .map(|leader| leader == node_id)
        .unwrap_or(true);
    let token = match cluster_token {
        Some(token) => token,
        None => runtime::server_token(data_dir).map_err(|e| e.to_string())?,
    };
    println!("kron server listening on http://{http}");
    println!("token: {}", ipc::token_path(data_dir).display());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .map_err(|e| e.to_string())?;
    runtime.block_on(async move {
        let cluster =
            OpenRaftCluster::open(data_dir, node_id, http, raft, token, bootstrap_single_node)
                .await
                .map_err(|e| e.to_string())?;
        std::sync::Arc::new(cluster)
            .serve()
            .await
            .map_err(|e| e.to_string())
    })
}

pub fn request(
    data_dir: &Path,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> Result<serde_json::Value, String> {
    let endpoint = std::fs::read_to_string(data_dir.join("kron.http"))
        .map_err(|_| "server endpoint not found; is `kron server start` running?".to_string())?;
    let token = server_request_token(data_dir)?;
    let mut endpoint = endpoint.trim().to_string();
    let original_endpoint = normalize_endpoint(&endpoint);
    for attempt in 0..=1 {
        let (status, value) = request_once(&endpoint, token.trim(), method, path, &body)?;
        if (200..300).contains(&status) {
            let normalized_endpoint = normalize_endpoint(&endpoint);
            if normalized_endpoint != original_endpoint {
                if let Err(err) = std::fs::write(
                    data_dir.join("kron.http"),
                    format!("{normalized_endpoint}\n"),
                ) {
                    eprintln!(
                        "warning: request followed leader redirect to {normalized_endpoint}, \
                         but failed to update kron.http: {err}"
                    );
                }
            }
            return Ok(value);
        }
        if attempt == 0 && value.get("error").and_then(|v| v.as_str()) == Some("not_leader") {
            if let Some(leader_http) = value.get("leader_http").and_then(|v| v.as_str()) {
                if !leader_http.trim().is_empty() {
                    endpoint = normalize_endpoint(leader_http);
                    continue;
                }
            }
        }
        let message = value
            .get("error")
            .or_else(|| value.get("message"))
            .and_then(|value| value.as_str())
            .unwrap_or("server request failed");
        return Err(format!("HTTP {status}: {message}"));
    }
    Err("leader redirect retry failed".to_string())
}

fn request_once(
    endpoint: &str,
    token: &str,
    method: &str,
    path: &str,
    body: &serde_json::Value,
) -> Result<(u16, serde_json::Value), String> {
    let body = if body.is_null() {
        String::new()
    } else {
        serde_json::to_string(body).map_err(|e| e.to_string())?
    };
    let endpoint = normalize_endpoint(endpoint);
    let mut stream = TcpStream::connect(&endpoint).map_err(|e| e.to_string())?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        endpoint,
        token,
        body.len(),
        body
    )
    .map_err(|e| e.to_string())?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|e| e.to_string())?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| "invalid HTTP response".to_string())?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| "invalid HTTP status line".to_string())?;
    let value: serde_json::Value = serde_json::from_str(body).map_err(|e| e.to_string())?;
    Ok((status, value))
}

fn normalize_endpoint(endpoint: &str) -> String {
    endpoint
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string()
}

#[derive(Deserialize)]
struct CliTokenFile {
    tokens: Vec<CliTokenEntry>,
}

#[derive(Deserialize)]
struct CliTokenEntry {
    token: String,
    role: String,
}

fn server_request_token(data_dir: &Path) -> Result<String, String> {
    if let Ok(token) = std::env::var("KRON_TOKEN") {
        if !token.trim().is_empty() {
            return Ok(token);
        }
    }
    let legacy = ipc::token_path(data_dir);
    if legacy.exists() {
        return std::fs::read_to_string(legacy)
            .map(|token| token.trim().to_string())
            .map_err(|_| "server token not found".to_string());
    }
    let tokens_path = data_dir.join("kron.tokens.json");
    let content = std::fs::read_to_string(&tokens_path).map_err(|_| {
        "server token not found; set KRON_TOKEN or create kron.token/kron.tokens.json".to_string()
    })?;
    let token_file: CliTokenFile =
        serde_json::from_str(&content).map_err(|err| format!("invalid kron.tokens.json: {err}"))?;
    ["admin", "operator", "reader", "worker", "raft"]
        .into_iter()
        .find_map(|role| {
            token_file
                .tokens
                .iter()
                .find(|entry| entry.role == role)
                .map(|entry| entry.token.clone())
        })
        .ok_or_else(|| "kron.tokens.json does not contain a usable token".to_string())
}
