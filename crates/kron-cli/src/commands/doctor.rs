use std::path::Path;

use kron_core::ipc::{self, IpcRequest, IpcResponse};
use kron_core::snapshot;

pub fn run(data_dir: &Path) -> Result<(), String> {
    if let Ok(response) = ipc::request(data_dir, &IpcRequest::Doctor) {
        match response {
            IpcResponse::Ok { data } => {
                println!(
                    "{}",
                    serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?
                );
                return Ok(());
            }
            IpcResponse::Error { message } => return Err(message),
        }
    }

    let state = snapshot::load_state(data_dir).map_err(|e| e.to_string())?;
    println!("runtime: inactive");
    println!("data_dir: {}", data_dir.display());
    println!("timers: {}", state.specs.len());
    println!("snapshot: {}", snapshot::snapshot_path(data_dir).exists());
    println!("aof: {}", snapshot::aof_path(data_dir).exists());
    Ok(())
}
