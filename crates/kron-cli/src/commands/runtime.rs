use std::path::Path;

use clap::Subcommand;
use kron_core::ipc::{self, IpcRequest, IpcResponse};

#[derive(Subcommand)]
pub enum RuntimeCommand {
    Status,
    Shutdown,
}

pub fn run(command: RuntimeCommand, data_dir: &Path) -> Result<(), String> {
    let request = match command {
        RuntimeCommand::Status => IpcRequest::RuntimeStatus,
        RuntimeCommand::Shutdown => IpcRequest::Shutdown,
    };
    match ipc::request(data_dir, &request).map_err(|e| e.to_string())? {
        IpcResponse::Ok { data } => {
            println!(
                "{}",
                serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?
            );
            Ok(())
        }
        IpcResponse::Error { message } => Err(message),
    }
}
