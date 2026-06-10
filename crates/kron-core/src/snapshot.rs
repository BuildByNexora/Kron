use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::error::KronError;
use crate::log::AppendOnlyLog;
use crate::state::{EngineState, LastRunInfo};
use crate::timer::{RunId, TimerId, TimerSpec, TimerState};

pub const SNAPSHOT_VERSION: u8 = 1;
pub const STORAGE_FORMAT_VERSION: u8 = 1;
pub const ENGINE_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Snapshot {
    pub v: u8,
    pub format_version: u8,
    pub engine_version: String,
    pub created_at: DateTime<Utc>,
    pub last_aof_offset: u64,
    pub specs: HashMap<TimerId, TimerSpec>,
    pub states: HashMap<TimerId, TimerState>,
    pub last_runs: HashMap<TimerId, LastRunInfoSnapshot>,
    pub next_runs: HashMap<TimerId, DateTime<Utc>>,
    pub retries_7d: HashMap<TimerId, u32>,
    pub pending_retries: HashMap<TimerId, PendingRetrySnapshot>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LastRunInfoSnapshot {
    pub run_id: RunId,
    pub finished_at: DateTime<Utc>,
    pub duration_ms: Option<u64>,
    pub status: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingRetrySnapshot {
    pub run_id: RunId,
    pub next_retry_at: DateTime<Utc>,
    pub attempt: u32,
}

impl Snapshot {
    pub fn from_state(state: &EngineState, last_aof_offset: u64) -> Self {
        Self {
            v: SNAPSHOT_VERSION,
            format_version: STORAGE_FORMAT_VERSION,
            engine_version: ENGINE_VERSION.to_string(),
            created_at: Utc::now(),
            last_aof_offset,
            specs: state.specs.clone(),
            states: state.states.clone(),
            last_runs: state
                .last_runs
                .iter()
                .map(|(id, run)| {
                    (
                        id.clone(),
                        LastRunInfoSnapshot {
                            run_id: run.run_id.clone(),
                            finished_at: run.finished_at,
                            duration_ms: run.duration_ms,
                            status: run.status.clone(),
                        },
                    )
                })
                .collect(),
            next_runs: state.next_runs.clone(),
            retries_7d: state.retries_7d.clone(),
            pending_retries: state
                .pending_retries
                .iter()
                .map(|(id, (run_id, next_retry_at, attempt))| {
                    (
                        id.clone(),
                        PendingRetrySnapshot {
                            run_id: run_id.clone(),
                            next_retry_at: *next_retry_at,
                            attempt: *attempt,
                        },
                    )
                })
                .collect(),
        }
    }

    pub fn into_state(self) -> Result<EngineState, KronError> {
        if self.v != SNAPSHOT_VERSION {
            return Err(KronError::InvalidSnapshot(format!(
                "unsupported snapshot version {}",
                self.v
            )));
        }
        if self.format_version != STORAGE_FORMAT_VERSION {
            return Err(KronError::InvalidSnapshot(format!(
                "unsupported storage format version {}",
                self.format_version
            )));
        }
        Ok(EngineState {
            specs: self.specs,
            states: self.states,
            fn_names: HashMap::new(),
            last_runs: self
                .last_runs
                .into_iter()
                .map(|(id, run)| {
                    (
                        id,
                        LastRunInfo {
                            run_id: run.run_id,
                            finished_at: run.finished_at,
                            duration_ms: run.duration_ms,
                            status: run.status,
                        },
                    )
                })
                .collect(),
            next_runs: self.next_runs,
            retries_7d: self.retries_7d,
            pending_retries: self
                .pending_retries
                .into_iter()
                .map(|(id, retry)| (id, (retry.run_id, retry.next_retry_at, retry.attempt)))
                .collect(),
        })
    }
}

pub fn snapshot_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.snapshot")
}

pub fn aof_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.aof")
}

pub fn load_state(data_dir: &Path) -> Result<EngineState, KronError> {
    let snapshot = snapshot_path(data_dir);
    if snapshot.exists() {
        let file = File::open(&snapshot)?;
        let snapshot: Snapshot = serde_json::from_reader(file).map_err(|source| {
            KronError::InvalidSnapshot(format!("cannot read {}: {}", snapshot.display(), source))
        })?;
        let offset = snapshot.last_aof_offset;
        let mut state = snapshot.into_state()?;
        let aof = aof_path(data_dir);
        let entries = if offset > std::fs::metadata(&aof).map(|meta| meta.len()).unwrap_or(0) {
            let old_aof = data_dir.join("kron.aof.old");
            if !old_aof.exists() {
                return Err(KronError::InvalidSnapshot(format!(
                    "snapshot points to AOF offset {offset}, but {} is shorter and {} is missing",
                    aof.display(),
                    old_aof.display()
                )));
            }
            let old_log = AppendOnlyLog::open(old_aof)?;
            let mut entries = old_log.replay_from(offset)?;
            let log = AppendOnlyLog::open(&aof)?;
            entries.extend(log.replay()?);
            entries
        } else {
            let log = AppendOnlyLog::open(&aof)?;
            log.replay_from(offset)?
        };
        state.replay(&entries);
        return Ok(state);
    }

    let log = AppendOnlyLog::open(aof_path(data_dir))?;
    let entries = log.replay()?;
    let mut state = EngineState::new();
    state.replay(&entries);
    Ok(state)
}

pub fn write_snapshot_atomic(
    data_dir: &Path,
    state: &EngineState,
    last_aof_offset: u64,
) -> Result<(), KronError> {
    std::fs::create_dir_all(data_dir)?;
    let tmp_path = data_dir.join("kron.snapshot.tmp");
    let final_path = snapshot_path(data_dir);
    let snapshot = Snapshot::from_state(state, last_aof_offset);

    {
        let mut file = File::create(&tmp_path)?;
        serde_json::to_writer_pretty(&mut file, &snapshot)?;
        file.write_all(b"\n")?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp_path, &final_path)?;
    sync_dir(data_dir)?;
    Ok(())
}

pub fn compact(data_dir: &Path, state: &EngineState) -> Result<(), KronError> {
    let aof = aof_path(data_dir);
    let offset = std::fs::metadata(&aof).map(|meta| meta.len()).unwrap_or(0);
    write_snapshot_atomic(data_dir, state, offset)?;
    if aof.exists() {
        let old = data_dir.join("kron.aof.old");
        let _ = std::fs::remove_file(&old);
        std::fs::rename(&aof, old)?;
    }
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&aof)?;
    file.sync_all()?;
    write_snapshot_atomic(data_dir, state, 0)?;
    sync_dir(data_dir)?;
    Ok(())
}

pub fn compact_read_only(data_dir: &Path) -> Result<(), KronError> {
    let state = load_state(data_dir)?;
    compact(data_dir, &state)
}

fn sync_dir(path: &Path) -> Result<(), KronError> {
    let dir = File::open(path)?;
    dir.sync_all()?;
    Ok(())
}

pub fn try_compaction_lock(data_dir: &Path) -> Result<File, KronError> {
    let lock_path = data_dir.join("kron.lock");
    let lock = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .truncate(false)
        .open(&lock_path)?;
    lock.try_lock_exclusive()
        .map_err(|_| KronError::DataDirLocked {
            path: lock_path.display().to_string(),
        })?;
    Ok(lock)
}
