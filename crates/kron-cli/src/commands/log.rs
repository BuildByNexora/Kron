use std::path::Path;

use clap::Subcommand;
use kron_core::ipc::{self, IpcRequest, IpcResponse};
use kron_core::snapshot;

#[derive(Subcommand)]
pub enum LogCommand {
    Compact,
}

pub fn run(command: LogCommand, data_dir: &Path) -> Result<(), String> {
    match command {
        LogCommand::Compact => compact(data_dir),
    }
}

fn compact(data_dir: &Path) -> Result<(), String> {
    match ipc::request(data_dir, &IpcRequest::Compact) {
        Ok(IpcResponse::Ok { .. }) => {
            println!("compacted via active runtime");
            Ok(())
        }
        Ok(IpcResponse::Error { message }) => Err(message),
        Err(_) => {
            let _lock = snapshot::try_compaction_lock(data_dir).map_err(|e| e.to_string())?;
            snapshot::compact_read_only(data_dir).map_err(|e| e.to_string())?;
            println!("compacted");
            Ok(())
        }
    }
}
