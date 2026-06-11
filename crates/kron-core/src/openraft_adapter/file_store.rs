#![allow(clippy::result_large_err)]

use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Cursor, Read, Write};
use std::ops::{Bound, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use openraft::entry::EntryPayload;
use openraft::storage::{
    LogFlushed, RaftLogReader, RaftLogStorage, RaftSnapshotBuilder, RaftStateMachine,
};
use openraft::{
    ErrorSubject, ErrorVerb, LogId, LogState, Membership, Snapshot, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::cluster::{DistributedSummary, DistributedTimerSpec, NodeInfo, RaftCommand, WorkerRun};
use crate::openraft_adapter::{KronRaftResponse, KronTypeConfig};
use crate::schedule::Schedule;
use crate::timer::{RunId, TimerId, TimerState};

type StoreResult<T> = Result<T, StorageError<u64>>;
type KronEntry = openraft::Entry<KronTypeConfig>;
const STORE_FORMAT_VERSION: u32 = 1;
const LOG_SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;
const LOG_RECORD_MAGIC: &[u8; 8] = b"KRLOG001";
const LOG_RECORD_HEADER_BYTES: usize = 8 + 4 + 8 + 8 + 4 + 8;

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct PersistedRaftStore {
    vote: Option<Vote<u64>>,
    committed: Option<LogId<u64>>,
    last_purged_log_id: Option<LogId<u64>>,
    logs: BTreeMap<u64, KronEntry>,
    state_machine: PersistedStateMachine,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Manifest {
    format_version: u32,
    active_snapshot: Option<String>,
    last_purged_log_id: Option<LogId<u64>>,
    segments: Vec<SegmentMeta>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SegmentMeta {
    first_index: u64,
    last_index: u64,
    file: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct StoreState {
    state_machine: PersistedStateMachine,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedStateMachine {
    last_applied_log_id: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, openraft::BasicNode>,
    timers: BTreeMap<String, PersistedTimer>,
    next_runs: BTreeMap<String, chrono::DateTime<chrono::Utc>>,
    pending: VecDeque<WorkerRun>,
    active_runs: BTreeMap<String, PersistedActiveRun>,
    workers: BTreeMap<String, PersistedWorker>,
    nodes: BTreeMap<String, NodeInfo>,
    history: Vec<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedTimer {
    spec: DistributedTimerSpec,
    state: TimerState,
    fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedActiveRun {
    pub timer_id: TimerId,
    pub run_id: RunId,
    pub worker_id: String,
    pub fencing_token: u64,
    pub attempt: u32,
    pub lease_deadline: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedWorker {
    tasks: Vec<String>,
    lease_until: chrono::DateTime<chrono::Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RaftAppState {
    pub specs: BTreeMap<String, DistributedTimerSpec>,
    pub timers: BTreeMap<String, DistributedSummary>,
    pub history: Vec<serde_json::Value>,
    pub pending: VecDeque<WorkerRun>,
    pub active_runs: BTreeMap<String, PersistedActiveRun>,
    pub nodes: BTreeMap<String, NodeInfo>,
}

impl Default for PersistedStateMachine {
    fn default() -> Self {
        Self {
            last_applied_log_id: None,
            last_membership: StoredMembership::new(None, Membership::new(vec![], None)),
            timers: BTreeMap::new(),
            next_runs: BTreeMap::new(),
            pending: VecDeque::new(),
            active_runs: BTreeMap::new(),
            workers: BTreeMap::new(),
            nodes: BTreeMap::new(),
            history: Vec::new(),
        }
    }
}

#[derive(Clone)]
pub struct KronRaftFileStore {
    raft_dir: PathBuf,
    inner: Arc<Mutex<PersistedRaftStore>>,
}

impl KronRaftFileStore {
    #[allow(clippy::result_large_err)]
    pub fn open(data_dir: impl AsRef<Path>) -> StoreResult<Self> {
        let data_dir = data_dir.as_ref();
        std::fs::create_dir_all(data_dir).map_err(write_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(write_error)?;
        }
        if data_dir.join("kron.openraft.store.json").exists() {
            return Err(StorageError::from_io_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "alpha JSON Raft storage requires manual migration",
                ),
            ));
        }
        let raft_dir = data_dir.join("raft");
        std::fs::create_dir_all(raft_dir.join("log")).map_err(write_error)?;
        std::fs::create_dir_all(raft_dir.join("snapshots")).map_err(write_error)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&raft_dir, std::fs::Permissions::from_mode(0o700))
                .map_err(write_error)?;
        }
        let manifest = read_manifest(&raft_dir)?;
        let logs = read_log_segments(&raft_dir, &manifest)?;
        let state_machine = read_state(&raft_dir)?;
        let inner = PersistedRaftStore {
            vote: read_json_optional(&raft_dir.join("vote.json"))?,
            committed: read_json_optional(&raft_dir.join("committed.json"))?,
            last_purged_log_id: manifest.last_purged_log_id,
            logs,
            state_machine,
        };
        Ok(Self {
            raft_dir,
            inner: Arc::new(Mutex::new(inner)),
        })
    }

    pub fn app_state(&self) -> RaftAppState {
        let inner = self.inner.lock().unwrap();
        let timers = inner
            .state_machine
            .timers
            .iter()
            .map(|(id, timer)| {
                (
                    id.clone(),
                    DistributedSummary {
                        id: id.clone(),
                        tenant_id: timer.spec.tenant_id.clone(),
                        state: timer.state.clone(),
                        target: timer.spec.target.clone(),
                        next_run_at: inner.state_machine.next_runs.get(id).copied(),
                        last_status: inner
                            .state_machine
                            .history
                            .iter()
                            .rev()
                            .find(|entry| {
                                entry.get("timer").and_then(|v| v.as_str()) == Some(id.as_str())
                            })
                            .and_then(|entry| {
                                entry
                                    .get("type")
                                    .and_then(|v| v.as_str())
                                    .map(str::to_string)
                            }),
                        fencing_token: timer.fencing_token,
                    },
                )
            })
            .collect();
        RaftAppState {
            specs: inner
                .state_machine
                .timers
                .iter()
                .map(|(id, timer)| (id.clone(), timer.spec.clone()))
                .collect(),
            timers,
            history: inner.state_machine.history.clone(),
            pending: inner.state_machine.pending.clone(),
            active_runs: inner.state_machine.active_runs.clone(),
            nodes: inner.state_machine.nodes.clone(),
        }
    }

    #[allow(clippy::result_large_err)]
    fn persist(&self, inner: &PersistedRaftStore) -> StoreResult<()> {
        write_json_atomic(&self.raft_dir.join("vote.json"), &inner.vote)?;
        write_json_atomic(&self.raft_dir.join("committed.json"), &inner.committed)?;
        write_json_atomic(
            &self.raft_dir.join("state.json"),
            &StoreState {
                state_machine: inner.state_machine.clone(),
            },
        )?;
        rewrite_log_segments(&self.raft_dir, &inner.logs, inner.last_purged_log_id)?;
        sync_dir(&self.raft_dir)?;
        Ok(())
    }

    #[cfg(test)]
    #[allow(clippy::result_large_err)]
    fn append_for_test(&self, entries: impl IntoIterator<Item = KronEntry>) -> StoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            inner.logs.insert(entry.log_id.index, entry);
        }
        self.persist(&inner)
    }
}

impl RaftLogReader<KronTypeConfig> for KronRaftFileStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + std::fmt::Debug + Send>(
        &mut self,
        range: RB,
    ) -> StoreResult<Vec<KronEntry>> {
        let inner = self.inner.lock().unwrap();
        Ok(inner
            .logs
            .iter()
            .filter(|(index, _)| contains(&range, **index))
            .map(|(_, entry)| entry.clone())
            .collect())
    }
}

impl RaftLogStorage<KronTypeConfig> for KronRaftFileStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> StoreResult<LogState<KronTypeConfig>> {
        let inner = self.inner.lock().unwrap();
        Ok(LogState {
            last_purged_log_id: inner.last_purged_log_id,
            last_log_id: inner
                .logs
                .values()
                .last()
                .map(|entry| entry.log_id)
                .or(inner.last_purged_log_id),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> StoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.vote = Some(*vote);
        self.persist(&inner)
    }

    async fn read_vote(&mut self) -> StoreResult<Option<Vote<u64>>> {
        Ok(self.inner.lock().unwrap().vote)
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> StoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.committed = committed;
        self.persist(&inner)
    }

    async fn read_committed(&mut self) -> StoreResult<Option<LogId<u64>>> {
        Ok(self.inner.lock().unwrap().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<KronTypeConfig>,
    ) -> StoreResult<()>
    where
        I: IntoIterator<Item = KronEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().unwrap();
        for entry in entries {
            inner.logs.insert(entry.log_id.index, entry);
        }
        let result = self.persist(&inner).map_err(storage_to_io);
        callback.log_io_completed(result);
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> StoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.logs.retain(|index, _| *index < log_id.index);
        self.persist(&inner)
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> StoreResult<()> {
        let mut inner = self.inner.lock().unwrap();
        inner.logs.retain(|index, _| *index > log_id.index);
        inner.last_purged_log_id = Some(log_id);
        self.persist(&inner)
    }
}

impl RaftStateMachine<KronTypeConfig> for KronRaftFileStore {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> StoreResult<(
        Option<LogId<u64>>,
        StoredMembership<u64, openraft::BasicNode>,
    )> {
        let inner = self.inner.lock().unwrap();
        Ok((
            inner.state_machine.last_applied_log_id,
            inner.state_machine.last_membership.clone(),
        ))
    }

    async fn apply<I>(&mut self, entries: I) -> StoreResult<Vec<KronRaftResponse>>
    where
        I: IntoIterator<Item = KronEntry> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.inner.lock().unwrap();
        let mut responses = Vec::new();
        for entry in entries {
            inner.state_machine.last_applied_log_id = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => {}
                EntryPayload::Normal(request) => {
                    apply_kron_command(&mut inner.state_machine, request.command);
                }
                EntryPayload::Membership(membership) => {
                    inner.state_machine.last_membership =
                        StoredMembership::new(Some(entry.log_id), membership);
                }
            }
            responses.push(KronRaftResponse { applied: true });
        }
        self.persist(&inner)?;
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(&mut self) -> StoreResult<Box<Cursor<Vec<u8>>>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, openraft::BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> StoreResult<()> {
        let bytes = snapshot.into_inner();
        let state_machine: PersistedStateMachine =
            serde_json::from_slice(&bytes).map_err(|err| {
                StorageError::from_io_error(
                    ErrorSubject::Snapshot(None),
                    ErrorVerb::Read,
                    json_io(err),
                )
            })?;
        let mut inner = self.inner.lock().unwrap();
        inner.state_machine = state_machine;
        inner.state_machine.last_applied_log_id = meta.last_log_id;
        inner.state_machine.last_membership = meta.last_membership.clone();
        self.persist(&inner)
    }

    async fn get_current_snapshot(&mut self) -> StoreResult<Option<Snapshot<KronTypeConfig>>> {
        let inner = self.inner.lock().unwrap();
        let meta = SnapshotMeta {
            last_log_id: inner.state_machine.last_applied_log_id,
            last_membership: inner.state_machine.last_membership.clone(),
            snapshot_id: format!(
                "kron-{}",
                inner
                    .state_machine
                    .last_applied_log_id
                    .map(|id| id.index)
                    .unwrap_or(0)
            ),
        };
        let bytes = serde_json::to_vec(&inner.state_machine).map_err(|err| {
            StorageError::from_io_error(ErrorSubject::Snapshot(None), ErrorVerb::Read, json_io(err))
        })?;
        Ok(Some(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(bytes)),
        }))
    }
}

impl RaftSnapshotBuilder<KronTypeConfig> for KronRaftFileStore {
    async fn build_snapshot(&mut self) -> StoreResult<Snapshot<KronTypeConfig>> {
        self.get_current_snapshot().await?.ok_or_else(|| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                missing_snapshot_io(),
            )
        })
    }
}

fn apply_kron_command(state: &mut PersistedStateMachine, command: RaftCommand) {
    match command {
        RaftCommand::CreateTimer { spec, next_run_at }
        | RaftCommand::UpdateTimer { spec, next_run_at } => {
            let id = spec.id.as_str().to_string();
            state.timers.insert(
                id.clone(),
                PersistedTimer {
                    spec,
                    state: TimerState::Scheduled,
                    fencing_token: 0,
                },
            );
            if let Some(next) = next_run_at {
                state.next_runs.insert(id.clone(), next);
            } else {
                state.next_runs.remove(&id);
            }
            state.history.push(serde_json::json!({
                "type": "TIMER_CREATED",
                "timer": id,
                "tenant_id": state.timers.get(&id).and_then(|timer| timer.spec.tenant_id.clone()),
            }));
        }
        RaftCommand::CancelTimer { timer_id } => {
            set_state(state, &timer_id, TimerState::Cancelled);
            state.next_runs.remove(timer_id.as_str());
        }
        RaftCommand::PauseTimer { timer_id } => {
            set_state(state, &timer_id, TimerState::Paused);
            state.next_runs.remove(timer_id.as_str());
        }
        RaftCommand::ResumeTimer {
            timer_id,
            next_run_at,
        } => {
            set_state(state, &timer_id, TimerState::Scheduled);
            if let Some(next_run_at) = next_run_at {
                state
                    .next_runs
                    .insert(timer_id.as_str().to_string(), next_run_at);
            } else {
                state.next_runs.remove(timer_id.as_str());
            }
        }
        RaftCommand::ClaimRun {
            timer_id,
            run_id,
            worker_id,
            fencing_token,
            attempt,
            lease_deadline,
        } => {
            if state
                .active_runs
                .values()
                .any(|run| run.timer_id == timer_id)
            {
                return;
            }
            if let Some(timer) = state.timers.get_mut(timer_id.as_str()) {
                timer.state = TimerState::Running;
                timer.fencing_token = fencing_token;
            }
            if let Some(pos) = state.pending.iter().position(|run| run.run_id == run_id.0) {
                state.pending.remove(pos);
            }
            state.active_runs.insert(
                run_id.0.clone(),
                PersistedActiveRun {
                    timer_id: timer_id.clone(),
                    run_id: run_id.clone(),
                    worker_id: worker_id.clone(),
                    fencing_token,
                    attempt,
                    lease_deadline,
                },
            );
            state.history.push(serde_json::json!({
                "type": "RUN_CLAIMED",
                "timer": timer_id.as_str(),
                "tenant_id": timer_tenant(state, timer_id.as_str()),
                "run_id": run_id.0,
                "worker_id": worker_id,
                "fencing_token": fencing_token,
            }));
        }
        RaftCommand::CompleteRun {
            run_id,
            worker_id,
            fencing_token,
            completed_at,
        } => {
            let valid = state
                .active_runs
                .get(&run_id.0)
                .map(|active| {
                    active.worker_id == worker_id && active.fencing_token == fencing_token
                })
                .unwrap_or(false);
            if valid {
                let active = state
                    .active_runs
                    .remove(&run_id.0)
                    .expect("active run checked");
                set_state(state, &active.timer_id, TimerState::Scheduled);
                reschedule_timer(state, &active.timer_id, completed_at);
                state.history.push(serde_json::json!({
                    "type": "RUN_SUCCEEDED",
                    "timer": active.timer_id.as_str(),
                    "tenant_id": timer_tenant(state, active.timer_id.as_str()),
                    "run_id": run_id.0,
                    "worker_id": worker_id,
                    "fencing_token": active.fencing_token,
                }));
            }
        }
        RaftCommand::FailRun {
            run_id,
            worker_id,
            fencing_token,
            error,
            failed_at: _,
        } => {
            let valid = state
                .active_runs
                .get(&run_id.0)
                .map(|active| {
                    active.worker_id == worker_id && active.fencing_token == fencing_token
                })
                .unwrap_or(false);
            if valid {
                let active = state
                    .active_runs
                    .remove(&run_id.0)
                    .expect("active run checked");
                set_state(state, &active.timer_id, TimerState::Dead);
                state.history.push(serde_json::json!({
                    "type": "RUN_FAILED",
                    "timer": active.timer_id.as_str(),
                    "tenant_id": timer_tenant(state, active.timer_id.as_str()),
                    "run_id": run_id.0,
                    "worker_id": worker_id,
                    "error": error,
                    "fencing_token": active.fencing_token,
                }));
            }
        }
        RaftCommand::ExpireRunLease {
            run_id,
            replacement,
            expired_at: _,
        } => {
            if let Some(active) = state.active_runs.remove(&run_id.0) {
                state.history.push(serde_json::json!({
                    "type": "RUN_LEASE_EXPIRED",
                    "timer": active.timer_id.as_str(),
                    "tenant_id": timer_tenant(state, active.timer_id.as_str()),
                    "run_id": active.run_id.0,
                    "worker_id": active.worker_id,
                    "fencing_token": active.fencing_token,
                }));
                if let Some(replacement) = replacement {
                    if let Some(timer) = state.timers.get_mut(&replacement.timer) {
                        timer.state = TimerState::Retrying;
                        timer.fencing_token = replacement.fencing_token;
                    }
                    state.pending.push_back(replacement);
                } else {
                    set_state(state, &active.timer_id, TimerState::Dead);
                }
            }
        }
        RaftCommand::HeartbeatWorker {
            worker_id,
            lease_until,
        } => {
            if let Some(worker) = state.workers.get_mut(&worker_id) {
                worker.lease_until = lease_until;
            }
        }
        RaftCommand::RegisterWorker {
            worker_id,
            tasks,
            lease_until,
        } => {
            state
                .workers
                .insert(worker_id.clone(), PersistedWorker { tasks, lease_until });
            state.history.push(serde_json::json!({
                "type": "WORKER_REGISTERED",
                "worker_id": worker_id,
            }));
        }
        RaftCommand::UnregisterWorker { worker_id } => {
            state.workers.remove(&worker_id);
        }
        RaftCommand::AddNode { node } => {
            state.nodes.insert(node.node_id.clone(), node);
        }
        RaftCommand::RemoveNode { node_id } => {
            state.nodes.remove(&node_id);
        }
    }
}

fn timer_tenant(state: &PersistedStateMachine, timer_id: &str) -> Option<String> {
    state
        .timers
        .get(timer_id)
        .and_then(|timer| timer.spec.tenant_id.clone())
}

fn set_state(state: &mut PersistedStateMachine, timer_id: &TimerId, timer_state: TimerState) {
    if let Some(timer) = state.timers.get_mut(timer_id.as_str()) {
        timer.state = timer_state;
    }
}

fn reschedule_timer(
    state: &mut PersistedStateMachine,
    timer_id: &TimerId,
    after: chrono::DateTime<chrono::Utc>,
) {
    let Some(timer) = state.timers.get(timer_id.as_str()) else {
        return;
    };
    match timer.spec.schedule {
        Schedule::At { .. } | Schedule::After { .. } => {
            state.next_runs.remove(timer_id.as_str());
        }
        _ => {
            if let Ok(Some(next)) = timer
                .spec
                .schedule
                .next_run_after(after, &timer.spec.timezone)
            {
                state.next_runs.insert(timer_id.as_str().to_string(), next);
            } else {
                state.next_runs.remove(timer_id.as_str());
            }
        }
    }
}

fn read_manifest(raft_dir: &Path) -> StoreResult<Manifest> {
    let path = raft_dir.join("manifest.json");
    if !path.exists() {
        let manifest = Manifest {
            format_version: STORE_FORMAT_VERSION,
            active_snapshot: None,
            last_purged_log_id: None,
            segments: Vec::new(),
        };
        write_json_atomic(&path, &manifest)?;
        return Ok(manifest);
    }
    let manifest: Manifest = read_json(&path)?;
    if manifest.format_version != STORE_FORMAT_VERSION {
        return Err(StorageError::from_io_error(
            ErrorSubject::Store,
            ErrorVerb::Read,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "unsupported raft storage format version {}",
                    manifest.format_version
                ),
            ),
        ));
    }
    Ok(manifest)
}

fn read_state(raft_dir: &Path) -> StoreResult<PersistedStateMachine> {
    let path = raft_dir.join("state.json");
    if !path.exists() {
        return Ok(PersistedStateMachine::default());
    }
    let state: StoreState = read_json(&path)?;
    Ok(state.state_machine)
}

fn read_json<T: serde::de::DeserializeOwned>(path: &Path) -> StoreResult<T> {
    let mut content = String::new();
    File::open(path)
        .map_err(read_error)?
        .read_to_string(&mut content)
        .map_err(read_error)?;
    serde_json::from_str(&content).map_err(|err| {
        StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Read, json_io(err))
    })
}

fn read_json_optional<T: serde::de::DeserializeOwned>(path: &Path) -> StoreResult<Option<T>> {
    if !path.exists() {
        return Ok(None);
    }
    read_json(path)
}

fn write_json_atomic<T: serde::Serialize>(path: &Path, value: &T) -> StoreResult<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(write_error)?;
    }
    let tmp = path.with_extension("tmp");
    #[cfg(unix)]
    let mut file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(&tmp)
            .map_err(write_error)?
    };
    #[cfg(not(unix))]
    let mut file = File::create(&tmp).map_err(write_error)?;
    serde_json::to_writer_pretty(&mut file, value).map_err(|err| {
        StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, json_io(err))
    })?;
    file.write_all(b"\n").map_err(write_error)?;
    file.sync_all().map_err(write_error)?;
    std::fs::rename(&tmp, path).map_err(write_error)?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

fn read_log_segments(
    raft_dir: &Path,
    manifest: &Manifest,
) -> StoreResult<BTreeMap<u64, KronEntry>> {
    let mut logs = BTreeMap::new();
    for segment in &manifest.segments {
        let path = raft_dir.join("log").join(&segment.file);
        if !path.exists() {
            return Err(StorageError::from_io_error(
                ErrorSubject::Store,
                ErrorVerb::Read,
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    format!("missing raft log segment {}", path.display()),
                ),
            ));
        }
        let segment_logs = read_segment_file(&path)?;
        for entry in segment_logs {
            logs.insert(entry.log_id.index, entry);
        }
    }
    Ok(logs)
}

fn read_segment_file(path: &Path) -> StoreResult<Vec<KronEntry>> {
    let mut file = File::open(path).map_err(read_error)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(read_error)?;
    let mut offset = 0usize;
    let mut entries = Vec::new();
    while offset < bytes.len() {
        let remaining = bytes.len() - offset;
        if remaining < LOG_RECORD_HEADER_BYTES {
            if entries.is_empty() {
                return Err(corrupt_store("truncated raft log record header"));
            }
            break;
        }
        let start = offset;
        let magic = &bytes[offset..offset + 8];
        offset += 8;
        if magic != LOG_RECORD_MAGIC {
            return Err(corrupt_store("invalid raft log record magic"));
        }
        let version = read_u32(&bytes, &mut offset)?;
        if version != STORE_FORMAT_VERSION {
            return Err(corrupt_store("unsupported raft log record version"));
        }
        let term = read_u64(&bytes, &mut offset)?;
        let index = read_u64(&bytes, &mut offset)?;
        let checksum = read_u32(&bytes, &mut offset)?;
        let len = read_u64(&bytes, &mut offset)? as usize;
        if bytes.len().saturating_sub(offset) < len {
            if entries.is_empty() {
                return Err(corrupt_store("truncated raft log record payload"));
            }
            break;
        }
        let payload = &bytes[offset..offset + len];
        offset += len;
        if checksum32(payload) != checksum {
            return Err(corrupt_store("raft log record checksum mismatch"));
        }
        let entry: KronEntry = serde_json::from_slice(payload).map_err(|err| {
            StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Read, json_io(err))
        })?;
        if entry.log_id.index != index || entry.log_id.leader_id.term != term {
            return Err(corrupt_store("raft log record header does not match entry"));
        }
        if start == offset {
            return Err(corrupt_store("empty raft log record"));
        }
        entries.push(entry);
    }
    Ok(entries)
}

fn rewrite_log_segments(
    raft_dir: &Path,
    logs: &BTreeMap<u64, KronEntry>,
    last_purged_log_id: Option<LogId<u64>>,
) -> StoreResult<()> {
    let log_dir = raft_dir.join("log");
    std::fs::create_dir_all(&log_dir).map_err(write_error)?;
    for entry in std::fs::read_dir(&log_dir).map_err(read_error)? {
        let path = entry.map_err(read_error)?.path();
        if path.extension().and_then(|value| value.to_str()) == Some("seg") {
            std::fs::remove_file(path).map_err(write_error)?;
        }
    }
    let mut manifest = Manifest {
        format_version: STORE_FORMAT_VERSION,
        active_snapshot: None,
        last_purged_log_id,
        segments: Vec::new(),
    };
    let mut current_file: Option<File> = None;
    let mut current_first = 0u64;
    let mut current_last = 0u64;
    let mut current_size = 0u64;
    for entry in logs.values() {
        if current_file.is_none() || current_size >= LOG_SEGMENT_MAX_BYTES {
            if let Some(file) = current_file.take() {
                file.sync_all().map_err(write_error)?;
                let old = log_dir.join(segment_file_name(current_first, current_first));
                let final_name = segment_file_name(current_first, current_last);
                let final_path = log_dir.join(&final_name);
                if old != final_path {
                    std::fs::rename(old, &final_path).map_err(write_error)?;
                }
                manifest.segments.push(SegmentMeta {
                    first_index: current_first,
                    last_index: current_last,
                    file: final_name,
                });
            }
            current_first = entry.log_id.index;
            current_size = 0;
            current_file = Some(open_segment_file(
                &log_dir.join(segment_file_name(current_first, current_first)),
            )?);
        }
        current_last = entry.log_id.index;
        let encoded = encode_log_record(entry)?;
        if let Some(file) = current_file.as_mut() {
            file.write_all(&encoded).map_err(write_error)?;
        }
        current_size += encoded.len() as u64;
    }
    if let Some(file) = current_file.take() {
        file.sync_all().map_err(write_error)?;
        let old = log_dir.join(segment_file_name(current_first, current_first));
        let final_name = segment_file_name(current_first, current_last);
        let final_path = log_dir.join(&final_name);
        if old != final_path {
            std::fs::rename(old, &final_path).map_err(write_error)?;
        }
        manifest.segments.push(SegmentMeta {
            first_index: current_first,
            last_index: current_last,
            file: final_name,
        });
    }
    write_json_atomic(&raft_dir.join("manifest.json"), &manifest)?;
    sync_dir(&log_dir)?;
    Ok(())
}

fn open_segment_file(path: &Path) -> StoreResult<File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .mode(0o600)
            .open(path)
            .map_err(write_error)
    }
    #[cfg(not(unix))]
    {
        OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(write_error)
    }
}

fn encode_log_record(entry: &KronEntry) -> StoreResult<Vec<u8>> {
    let payload = serde_json::to_vec(entry).map_err(|err| {
        StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, json_io(err))
    })?;
    let mut out = Vec::with_capacity(LOG_RECORD_HEADER_BYTES + payload.len());
    out.extend_from_slice(LOG_RECORD_MAGIC);
    out.extend_from_slice(&STORE_FORMAT_VERSION.to_le_bytes());
    out.extend_from_slice(&entry.log_id.leader_id.term.to_le_bytes());
    out.extend_from_slice(&entry.log_id.index.to_le_bytes());
    out.extend_from_slice(&checksum32(&payload).to_le_bytes());
    out.extend_from_slice(&(payload.len() as u64).to_le_bytes());
    out.extend_from_slice(&payload);
    Ok(out)
}

fn read_u32(bytes: &[u8], offset: &mut usize) -> StoreResult<u32> {
    if bytes.len().saturating_sub(*offset) < 4 {
        return Err(corrupt_store("truncated u32"));
    }
    let mut raw = [0u8; 4];
    raw.copy_from_slice(&bytes[*offset..*offset + 4]);
    *offset += 4;
    Ok(u32::from_le_bytes(raw))
}

fn read_u64(bytes: &[u8], offset: &mut usize) -> StoreResult<u64> {
    if bytes.len().saturating_sub(*offset) < 8 {
        return Err(corrupt_store("truncated u64"));
    }
    let mut raw = [0u8; 8];
    raw.copy_from_slice(&bytes[*offset..*offset + 8]);
    *offset += 8;
    Ok(u64::from_le_bytes(raw))
}

fn checksum32(bytes: &[u8]) -> u32 {
    let mut hash = 0x811c9dc5u32;
    for byte in bytes {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(0x01000193);
    }
    hash
}

fn segment_file_name(first: u64, last: u64) -> String {
    format!("{first:016x}-{last:016x}.seg")
}

#[cfg(unix)]
fn sync_dir(path: &Path) -> StoreResult<()> {
    let dir = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(read_error)?;
    dir.sync_all().map_err(write_error)
}

#[cfg(not(unix))]
fn sync_dir(_path: &Path) -> StoreResult<()> {
    Ok(())
}

fn corrupt_store(message: &str) -> StorageError<u64> {
    StorageError::from_io_error(
        ErrorSubject::Store,
        ErrorVerb::Read,
        std::io::Error::new(std::io::ErrorKind::InvalidData, message.to_string()),
    )
}

fn contains<RB: RangeBounds<u64>>(range: &RB, value: u64) -> bool {
    let start_ok = match range.start_bound() {
        Bound::Included(start) => value >= *start,
        Bound::Excluded(start) => value > *start,
        Bound::Unbounded => true,
    };
    let end_ok = match range.end_bound() {
        Bound::Included(end) => value <= *end,
        Bound::Excluded(end) => value < *end,
        Bound::Unbounded => true,
    };
    start_ok && end_ok
}

fn read_error(err: std::io::Error) -> StorageError<u64> {
    StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Read, err)
}

fn write_error(err: std::io::Error) -> StorageError<u64> {
    StorageError::from_io_error(ErrorSubject::Store, ErrorVerb::Write, err)
}

fn json_io(err: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

fn missing_snapshot_io() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::NotFound, "snapshot missing")
}

fn storage_to_io(err: StorageError<u64>) -> std::io::Error {
    std::io::Error::other(err.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeSet;

    use chrono::Utc;
    use openraft::storage::{RaftLogReader, RaftLogStorage, RaftStateMachine};
    use openraft::CommittedLeaderId;
    use tempfile::tempdir;

    use crate::cluster::{DistributedTimerSpec, TimerTarget};
    use crate::retry::RetryPolicy;
    use crate::schedule::Schedule;
    use crate::timer::TimerId;

    fn log_id(index: u64) -> LogId<u64> {
        LogId::new(CommittedLeaderId::new(1, 1), index)
    }

    fn timer_spec(name: &str) -> DistributedTimerSpec {
        DistributedTimerSpec {
            id: TimerId::new(name),
            tenant_id: None,
            schedule: Schedule::Every { seconds: 60 },
            retry: RetryPolicy::default(),
            timezone: "UTC".to_string(),
            target: TimerTarget::WorkerTask {
                task: "send_digest".to_string(),
                payload: serde_json::json!({"list": "daily"}),
            },
            created_at: Utc::now(),
        }
    }

    fn create_timer_entry(index: u64, name: &str) -> KronEntry {
        let spec = timer_spec(name);
        let next_run_at = spec
            .schedule
            .next_run_after(spec.created_at, &spec.timezone)
            .unwrap();
        KronEntry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(crate::openraft_adapter::KronRaftRequest {
                command: RaftCommand::CreateTimer { spec, next_run_at },
            }),
        }
    }

    fn claim_entry(index: u64, timer: &str, run: &str, token: u64) -> KronEntry {
        KronEntry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(crate::openraft_adapter::KronRaftRequest {
                command: RaftCommand::ClaimRun {
                    timer_id: TimerId::new(timer),
                    run_id: crate::timer::RunId(run.to_string()),
                    worker_id: "worker-1".to_string(),
                    fencing_token: token,
                    attempt: 1,
                    lease_deadline: Utc::now() + chrono::Duration::seconds(30),
                },
            }),
        }
    }

    #[tokio::test]
    async fn appends_logs_and_reopens_from_disk() {
        let dir = tempdir().unwrap();
        let store = KronRaftFileStore::open(dir.path()).unwrap();
        let entry = create_timer_entry(1, "email_digest");

        store.append_for_test([entry.clone()]).unwrap();

        let mut reopened = KronRaftFileStore::open(dir.path()).unwrap();
        let state = reopened.get_log_state().await.unwrap();
        assert_eq!(state.last_log_id, Some(log_id(1)));

        let entries = reopened.try_get_log_entries(1..2).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, entry.log_id);
    }

    #[tokio::test]
    async fn applies_timer_commands_to_state_machine_and_snapshot() {
        let dir = tempdir().unwrap();
        let mut store = KronRaftFileStore::open(dir.path()).unwrap();

        let responses = store
            .apply([create_timer_entry(1, "email_digest")])
            .await
            .unwrap();
        assert_eq!(responses.len(), 1);
        assert!(responses[0].applied);

        let snapshot = store.get_current_snapshot().await.unwrap().unwrap();
        assert_eq!(snapshot.meta.last_log_id, Some(log_id(1)));
        let state: PersistedStateMachine =
            serde_json::from_slice(&snapshot.snapshot.into_inner()).unwrap();
        let timer = state.timers.get("email_digest").unwrap();
        assert_eq!(timer.state, TimerState::Scheduled);
        assert_eq!(timer.fencing_token, 0);
        assert_eq!(state.history.len(), 1);
    }

    #[tokio::test]
    async fn duplicate_claim_for_same_timer_is_noop() {
        let dir = tempdir().unwrap();
        let mut store = KronRaftFileStore::open(dir.path()).unwrap();

        store
            .apply([
                create_timer_entry(1, "email_digest"),
                claim_entry(2, "email_digest", "run_1", 1),
                claim_entry(3, "email_digest", "run_2", 2),
            ])
            .await
            .unwrap();

        let state = store.app_state();
        assert_eq!(state.active_runs.len(), 1);
        assert!(state.active_runs.contains_key("run_1"));
        assert!(!state.active_runs.contains_key("run_2"));
    }

    #[tokio::test]
    async fn membership_entries_update_state_machine_membership() {
        let dir = tempdir().unwrap();
        let mut store = KronRaftFileStore::open(dir.path()).unwrap();
        let membership = Membership::new(vec![BTreeSet::from([1, 2, 3])], None);
        let entry = KronEntry {
            log_id: log_id(2),
            payload: EntryPayload::Membership(membership.clone()),
        };

        store.apply([entry]).await.unwrap();

        let (last_applied, stored_membership) = store.applied_state().await.unwrap();
        assert_eq!(last_applied, Some(log_id(2)));
        assert_eq!(stored_membership.membership(), &membership);
    }

    #[tokio::test]
    async fn truncate_and_purge_persist_across_reopen() {
        let dir = tempdir().unwrap();
        let mut store = KronRaftFileStore::open(dir.path()).unwrap();
        store
            .append_for_test([
                create_timer_entry(1, "one"),
                create_timer_entry(2, "two"),
                create_timer_entry(3, "three"),
            ])
            .unwrap();

        store.truncate(log_id(3)).await.unwrap();
        store.purge(log_id(1)).await.unwrap();

        let mut reopened = KronRaftFileStore::open(dir.path()).unwrap();
        let entries = reopened.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, log_id(2));
        let log_state = reopened.get_log_state().await.unwrap();
        assert_eq!(log_state.last_purged_log_id, Some(log_id(1)));
        assert_eq!(log_state.last_log_id, Some(log_id(2)));
    }

    #[tokio::test]
    async fn segmented_store_writes_manifest_and_segments() {
        let dir = tempdir().unwrap();
        let store = KronRaftFileStore::open(dir.path()).unwrap();

        store
            .append_for_test([create_timer_entry(1, "one"), create_timer_entry(2, "two")])
            .unwrap();

        let manifest_path = dir.path().join("raft").join("manifest.json");
        let manifest: Manifest = read_json(&manifest_path).unwrap();
        assert_eq!(manifest.format_version, STORE_FORMAT_VERSION);
        assert_eq!(manifest.segments.len(), 1);
        assert_eq!(manifest.segments[0].first_index, 1);
        assert_eq!(manifest.segments[0].last_index, 2);
        assert!(dir
            .path()
            .join("raft")
            .join("log")
            .join(&manifest.segments[0].file)
            .exists());
    }

    #[test]
    fn open_rejects_legacy_json_raft_store() {
        let dir = tempdir().unwrap();
        std::fs::write(dir.path().join("kron.openraft.store.json"), "{}").unwrap();

        let err = match KronRaftFileStore::open(dir.path()) {
            Ok(_) => panic!("legacy JSON store should be rejected"),
            Err(err) => err,
        };
        assert!(err
            .to_string()
            .contains("alpha JSON Raft storage requires manual migration"));
    }

    #[tokio::test]
    async fn corrupt_middle_segment_record_fails_loudly() {
        let dir = tempdir().unwrap();
        let store = KronRaftFileStore::open(dir.path()).unwrap();
        store
            .append_for_test([create_timer_entry(1, "one"), create_timer_entry(2, "two")])
            .unwrap();

        let manifest: Manifest = read_json(&dir.path().join("raft").join("manifest.json")).unwrap();
        let path = dir
            .path()
            .join("raft")
            .join("log")
            .join(&manifest.segments[0].file);
        let mut bytes = std::fs::read(&path).unwrap();
        let first_record_len = {
            let mut offset = 8 + 4 + 8 + 8 + 4;
            read_u64(&bytes, &mut offset).unwrap() as usize + LOG_RECORD_HEADER_BYTES
        };
        bytes[first_record_len + 2] ^= 0xff;
        std::fs::write(&path, bytes).unwrap();

        let err = match KronRaftFileStore::open(dir.path()) {
            Ok(_) => panic!("corrupt segment should be rejected"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("magic")
                || err.to_string().contains("checksum")
                || err.to_string().contains("header")
        );
    }

    #[tokio::test]
    async fn truncated_final_segment_tail_is_ignored_deterministically() {
        let dir = tempdir().unwrap();
        let store = KronRaftFileStore::open(dir.path()).unwrap();
        store
            .append_for_test([create_timer_entry(1, "one"), create_timer_entry(2, "two")])
            .unwrap();

        let manifest: Manifest = read_json(&dir.path().join("raft").join("manifest.json")).unwrap();
        let path = dir
            .path()
            .join("raft")
            .join("log")
            .join(&manifest.segments[0].file);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(bytes.len() - 10);
        std::fs::write(&path, bytes).unwrap();

        let mut reopened = KronRaftFileStore::open(dir.path()).unwrap();
        let entries = reopened.try_get_log_entries(..).await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].log_id, log_id(1));
    }
}
