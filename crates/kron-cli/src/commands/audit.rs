use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::thread;
use std::time::Duration;

use chrono::{DateTime, NaiveDate, Utc};
use clap::Subcommand;
use kron_core::audit::{self, AuditRecord};

#[derive(Subcommand)]
pub enum AuditCommand {
    Verify,
    Tail {
        #[arg(short, long, default_value_t = 20)]
        limit: usize,
        #[arg(long)]
        no_follow: bool,
    },
    Query {
        #[arg(long)]
        actor: Option<String>,
        #[arg(long)]
        action: Option<String>,
        #[arg(long)]
        from: Option<String>,
        #[arg(long)]
        to: Option<String>,
    },
}

pub fn run(command: AuditCommand, data_dir: &Path) -> Result<(), String> {
    match command {
        AuditCommand::Verify => verify(data_dir),
        AuditCommand::Tail { limit, no_follow } => tail(data_dir, limit, !no_follow),
        AuditCommand::Query {
            actor,
            action,
            from,
            to,
        } => query(data_dir, actor, action, from, to),
    }
}

fn verify(data_dir: &Path) -> Result<(), String> {
    match audit::verify(data_dir) {
        Ok(result) => {
            println!("Audit chain verified: {} records OK", result.records);
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn tail(data_dir: &Path, limit: usize, follow: bool) -> Result<(), String> {
    let path = audit::audit_path(data_dir);
    let records = read_records(data_dir)?;
    for record in records.iter().skip(records.len().saturating_sub(limit)) {
        print_record(record)?;
    }
    if !follow {
        return Ok(());
    }
    if !path.exists() {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent).map_err(|err| err.to_string())?;
        }
        OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(|err| err.to_string())?;
    }
    let mut file = File::open(&path).map_err(|err| err.to_string())?;
    file.seek(SeekFrom::End(0)).map_err(|err| err.to_string())?;
    let mut reader = BufReader::new(file);
    loop {
        let mut line = String::new();
        let bytes = reader.read_line(&mut line).map_err(|err| err.to_string())?;
        if bytes == 0 {
            thread::sleep(Duration::from_millis(500));
            continue;
        }
        if let Ok(record) = serde_json::from_str::<AuditRecord>(&line) {
            print_record(&record)?;
        }
    }
}

fn query(
    data_dir: &Path,
    actor: Option<String>,
    action: Option<String>,
    from: Option<String>,
    to: Option<String>,
) -> Result<(), String> {
    let from = parse_bound(from, false)?;
    let to = parse_bound(to, true)?;
    for record in read_records(data_dir)? {
        if actor.as_deref().is_some_and(|value| record.actor != value) {
            continue;
        }
        if action
            .as_deref()
            .is_some_and(|value| record.action != value)
        {
            continue;
        }
        if from.is_some_and(|value| record.ts < value) {
            continue;
        }
        if to.is_some_and(|value| record.ts > value) {
            continue;
        }
        print_record(&record)?;
    }
    Ok(())
}

fn read_records(data_dir: &Path) -> Result<Vec<AuditRecord>, String> {
    let path = audit::audit_path(data_dir);
    if !path.exists() {
        return Ok(Vec::new());
    }
    let file = File::open(path).map_err(|err| err.to_string())?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for line in reader.lines() {
        let line = line.map_err(|err| err.to_string())?;
        if line.trim().is_empty() {
            continue;
        }
        records.push(serde_json::from_str(&line).map_err(|err| err.to_string())?);
    }
    Ok(records)
}

fn print_record(record: &AuditRecord) -> Result<(), String> {
    println!(
        "{}",
        serde_json::to_string(record).map_err(|err| err.to_string())?
    );
    Ok(())
}

fn parse_bound(value: Option<String>, end_of_day: bool) -> Result<Option<DateTime<Utc>>, String> {
    let Some(value) = value else {
        return Ok(None);
    };
    if let Ok(dt) = DateTime::parse_from_rfc3339(&value) {
        return Ok(Some(dt.with_timezone(&Utc)));
    }
    let date = NaiveDate::parse_from_str(&value, "%Y-%m-%d")
        .map_err(|err| format!("invalid date `{value}`: {err}"))?;
    let time = if end_of_day {
        date.and_hms_opt(23, 59, 59)
    } else {
        date.and_hms_opt(0, 0, 0)
    }
    .ok_or_else(|| format!("invalid date `{value}`"))?;
    Ok(Some(DateTime::from_naive_utc_and_offset(time, Utc)))
}
