use std::path::Path;

use clap::Subcommand;
use kron_core::ipc::{self, IpcRequest, IpcResponse};
use kron_core::snapshot;
use kron_core::state::EngineState;
use kron_core::timer::{TimerId, TimerState, TimerSummary};

use crate::commands::server;

#[derive(Subcommand)]
pub enum JobCommand {
    List,
    Status {
        timer: String,
    },
    History {
        timer: String,
        #[arg(long)]
        limit: Option<usize>,
    },
}

pub fn run(command: JobCommand, data_dir: &Path) -> Result<(), String> {
    match command {
        JobCommand::List => list(data_dir),
        JobCommand::Status { timer } => status(data_dir, &timer),
        JobCommand::History { timer, limit } => history(data_dir, &timer, limit),
    }
}

fn list(data_dir: &Path) -> Result<(), String> {
    if data_dir.join("kron.http").exists() {
        let data = server::request(data_dir, "GET", "/v1/timers", serde_json::Value::Null)?;
        println!(
            "{}",
            serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    if let Ok(IpcResponse::Ok { data }) = ipc::request(data_dir, &IpcRequest::List) {
        let summaries: Vec<TimerSummary> =
            serde_json::from_value(data).map_err(|e| e.to_string())?;
        print_list(summaries);
        return Ok(());
    }
    let state = load_state(data_dir)?;
    let summaries = state
        .specs
        .keys()
        .filter_map(|id| state.summary(id))
        .collect();
    print_list(summaries);
    Ok(())
}

fn status(data_dir: &Path, timer: &str) -> Result<(), String> {
    if data_dir.join("kron.http").exists() {
        let data = server::request(
            data_dir,
            "GET",
            &format!("/v1/timers/{timer}"),
            serde_json::Value::Null,
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    if let Ok(IpcResponse::Ok { data }) =
        ipc::request(data_dir, &IpcRequest::Status { name: timer.into() })
    {
        let summary: Option<TimerSummary> =
            serde_json::from_value(data).map_err(|e| e.to_string())?;
        if let Some(summary) = summary {
            print_status(summary);
            return Ok(());
        }
    }
    let state = load_state(data_dir)?;
    let summary = state
        .summary(&TimerId::new(timer))
        .ok_or_else(|| format!("timer '{timer}' not found"))?;
    print_status(summary);
    Ok(())
}

fn history(data_dir: &Path, timer: &str, limit: Option<usize>) -> Result<(), String> {
    if data_dir.join("kron.http").exists() {
        let _ = limit;
        let data = server::request(
            data_dir,
            "GET",
            &format!("/v1/timers/{timer}/history"),
            serde_json::Value::Null,
        )?;
        println!(
            "{}",
            serde_json::to_string_pretty(&data).map_err(|e| e.to_string())?
        );
        return Ok(());
    }
    let entries = match ipc::request(
        data_dir,
        &IpcRequest::History {
            name: timer.into(),
            limit,
        },
    ) {
        Ok(IpcResponse::Ok { data }) => {
            serde_json::from_value::<Vec<serde_json::Value>>(data).map_err(|e| e.to_string())?
        }
        _ => ipc::history(data_dir, timer, limit).map_err(|e| e.to_string())?,
    };
    for entry in entries {
        println!(
            "{}",
            serde_json::to_string_pretty(&entry).map_err(|e| e.to_string())?
        );
    }
    Ok(())
}

fn load_state(data_dir: &Path) -> Result<EngineState, String> {
    snapshot::load_state(data_dir).map_err(|e| e.to_string())
}

fn print_list(mut summaries: Vec<TimerSummary>) {
    if summaries.is_empty() {
        println!("(no timers registered)");
        return;
    }
    summaries.sort_by(|a, b| a.id.0.cmp(&b.id.0));
    println!("{:<30} {:<12} NEXT RUN", "TIMER", "STATUS");
    println!("{}", "-".repeat(70));
    for s in summaries {
        let next = s
            .next_run_at
            .map(|t| t.format("%Y-%m-%d %H:%M UTC").to_string())
            .unwrap_or_else(|| "-".to_string());
        println!(
            "{:<30} {:<12} {}",
            s.id.as_str(),
            format_state(&s.state),
            next
        );
    }
}

fn print_status(s: TimerSummary) {
    println!("timer:       {}", s.id.as_str());
    println!("status:      {}", format_state(&s.state));
    println!(
        "fn:          {}",
        s.fn_name
            .map(|name| format!("{name} (registered)"))
            .unwrap_or_else(|| "- (not registered)".to_string())
    );
    if let Some(last) = s.last_run_at {
        println!("last_run:    {}", last.format("%Y-%m-%d %H:%M:%S UTC"));
    }
    if let Some(ms) = s.last_duration_ms {
        println!("duration:    {}ms", ms);
    }
    if let Some(status) = s.last_status {
        println!("last_status: {}", status);
    }
    if let Some(next) = s.next_run_at {
        println!("next_run:    {}", next.format("%Y-%m-%d %H:%M:%S UTC"));
    }
    println!("retries_7d:  {}", s.retries_last_7d);
}

fn format_state(s: &TimerState) -> &'static str {
    match s {
        TimerState::Scheduled => "scheduled",
        TimerState::Running => "running",
        TimerState::Retrying => "retrying",
        TimerState::Dead => "dead",
        TimerState::Orphaned => "orphaned",
        TimerState::Paused => "paused",
        TimerState::Cancelled => "cancelled",
    }
}
