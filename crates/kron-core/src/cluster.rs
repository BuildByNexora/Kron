use std::collections::{HashMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use fs2::FileExt;
use serde::{Deserialize, Serialize};

use crate::error::KronError;
use crate::ipc;
use crate::retry::RetryPolicy;
use crate::schedule::Schedule;
use crate::timer::{RunId, TimerId, TimerState};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub node_id: String,
    pub http_addr: String,
    pub raft_addr: String,
    pub leader_id: String,
    pub peers: Vec<NodeInfo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    pub node_id: String,
    pub http_addr: String,
    pub raft_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JoinRequest {
    pub node_id: String,
    pub http_addr: String,
    pub raft_addr: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LeaveRequest {
    pub node_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TimerTarget {
    EmbeddedFn {
        name: String,
    },
    WorkerTask {
        task: String,
        payload: serde_json::Value,
    },
    HttpTarget {
        url: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedTimerSpec {
    pub id: TimerId,
    pub schedule: Schedule,
    pub retry: RetryPolicy,
    pub timezone: String,
    pub target: TimerTarget,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateTimerRequest {
    pub name: String,
    pub cron: Option<String>,
    pub every: Option<String>,
    pub after: Option<String>,
    pub at: Option<String>,
    pub timezone: Option<String>,
    pub max_attempts: Option<u32>,
    pub task: String,
    pub payload: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRegisterRequest {
    pub worker_id: String,
    pub tasks: Vec<String>,
    pub lease_seconds: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerPollRequest {
    pub worker_id: String,
    pub tasks: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunCompleteRequest {
    pub worker_id: String,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunFailRequest {
    pub worker_id: String,
    pub fencing_token: u64,
    pub error: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerRun {
    pub timer: String,
    pub run_id: String,
    pub task: String,
    pub payload: serde_json::Value,
    pub attempt: u32,
    pub fencing_token: u64,
    pub idempotency_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DistributedSummary {
    pub id: String,
    pub state: TimerState,
    pub target: TimerTarget,
    pub next_run_at: Option<DateTime<Utc>>,
    pub last_status: Option<String>,
    pub fencing_token: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RaftCommand {
    CreateTimer {
        spec: DistributedTimerSpec,
        next_run_at: Option<DateTime<Utc>>,
    },
    UpdateTimer {
        spec: DistributedTimerSpec,
        next_run_at: Option<DateTime<Utc>>,
    },
    CancelTimer {
        timer_id: TimerId,
    },
    PauseTimer {
        timer_id: TimerId,
    },
    ResumeTimer {
        timer_id: TimerId,
        next_run_at: Option<DateTime<Utc>>,
    },
    ClaimRun {
        timer_id: TimerId,
        run_id: RunId,
        worker_id: String,
        fencing_token: u64,
        attempt: u32,
        lease_deadline: DateTime<Utc>,
    },
    CompleteRun {
        run_id: RunId,
        worker_id: String,
        fencing_token: u64,
        completed_at: DateTime<Utc>,
    },
    FailRun {
        run_id: RunId,
        worker_id: String,
        fencing_token: u64,
        error: String,
        failed_at: DateTime<Utc>,
    },
    ExpireRunLease {
        run_id: RunId,
        replacement: Option<WorkerRun>,
        expired_at: DateTime<Utc>,
    },
    HeartbeatWorker {
        worker_id: String,
        lease_until: DateTime<Utc>,
    },
    RegisterWorker {
        worker_id: String,
        tasks: Vec<String>,
        lease_until: DateTime<Utc>,
    },
    UnregisterWorker {
        worker_id: String,
    },
    AddNode {
        node: NodeInfo,
    },
    RemoveNode {
        node_id: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommandEntry {
    v: u8,
    ts: DateTime<Utc>,
    command: RaftCommand,
}

#[derive(Debug, Clone)]
struct WorkerInfo {
    tasks: Vec<String>,
    lease_until: Instant,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ActiveRun {
    timer_id: TimerId,
    run_id: RunId,
    worker_id: String,
    fencing_token: u64,
    attempt: u32,
    lease_deadline: DateTime<Utc>,
}

struct ClusterState {
    config: ClusterConfig,
    timers: HashMap<TimerId, DistributedTimerSpec>,
    states: HashMap<TimerId, TimerState>,
    next_runs: HashMap<TimerId, DateTime<Utc>>,
    history: Vec<serde_json::Value>,
    workers: HashMap<String, WorkerInfo>,
    pending: VecDeque<WorkerRun>,
    active_runs: HashMap<RunId, ActiveRun>,
    fencing: HashMap<TimerId, u64>,
    leader: bool,
    shutting_down: bool,
}

impl ClusterState {
    fn new(config: ClusterConfig) -> Self {
        Self {
            leader: config.leader_id == config.node_id,
            config,
            timers: HashMap::new(),
            states: HashMap::new(),
            next_runs: HashMap::new(),
            history: Vec::new(),
            workers: HashMap::new(),
            pending: VecDeque::new(),
            active_runs: HashMap::new(),
            fencing: HashMap::new(),
            shutting_down: false,
        }
    }
}

pub struct ClusterEngine {
    data_dir: PathBuf,
    state: Arc<Mutex<ClusterState>>,
    command_log: Arc<Mutex<File>>,
    _lock_file: File,
}

impl ClusterEngine {
    pub fn open(
        data_dir: impl AsRef<Path>,
        node_id: impl Into<String>,
        http_addr: impl Into<String>,
        raft_addr: impl Into<String>,
    ) -> Result<Self, KronError> {
        Self::open_with_leader(data_dir, node_id, http_addr, raft_addr, None)
    }

    pub fn open_with_leader(
        data_dir: impl AsRef<Path>,
        node_id: impl Into<String>,
        http_addr: impl Into<String>,
        raft_addr: impl Into<String>,
        leader_id: Option<String>,
    ) -> Result<Self, KronError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&data_dir)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&data_dir, std::fs::Permissions::from_mode(0o700))?;
        }
        let lock_path = data_dir.join("kron.lock");
        #[cfg(unix)]
        let lock_file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .truncate(false)
                .mode(0o600)
                .open(&lock_path)?
        };
        #[cfg(not(unix))]
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(&lock_path)?;
        lock_file
            .try_lock_exclusive()
            .map_err(|_| KronError::DataDirLocked {
                path: lock_path.display().to_string(),
            })?;

        let node_id = node_id.into();
        let config = ClusterConfig {
            node_id: node_id.clone(),
            http_addr: http_addr.into(),
            raft_addr: raft_addr.into(),
            leader_id: leader_id.unwrap_or_else(|| node_id.clone()),
            peers: Vec::new(),
        };
        let config_path = data_dir.join("kron.cluster.json");
        std::fs::write(&config_path, serde_json::to_string_pretty(&config)? + "\n")?;

        let command_log = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(data_dir.join("kron.raft.aof"))?;
        let engine = Self {
            data_dir,
            state: Arc::new(Mutex::new(ClusterState::new(config))),
            command_log: Arc::new(Mutex::new(command_log)),
            _lock_file: lock_file,
        };
        engine.replay_commands()?;
        Ok(engine)
    }

    pub fn start_scheduler(self: &Arc<Self>) {
        let engine = Arc::clone(self);
        thread::spawn(move || loop {
            thread::sleep(Duration::from_millis(100));
            if engine.is_shutting_down() {
                break;
            }
            let _ = engine.enqueue_due_runs();
            engine.expire_workers();
        });
    }

    pub fn create_timer(
        &self,
        request: CreateTimerRequest,
    ) -> Result<DistributedSummary, KronError> {
        self.ensure_leader()?;
        let schedule = parse_request_schedule(&request)?;
        let id = TimerId::new(request.name);
        let spec = DistributedTimerSpec {
            id: id.clone(),
            schedule,
            retry: RetryPolicy {
                max_attempts: request.max_attempts.unwrap_or(3),
                ..Default::default()
            },
            timezone: request.timezone.unwrap_or_else(|| "UTC".to_string()),
            target: TimerTarget::WorkerTask {
                task: request.task,
                payload: request.payload.unwrap_or(serde_json::Value::Null),
            },
            created_at: Utc::now(),
        };
        let next_run_at = spec
            .schedule
            .next_run_after(spec.created_at, &spec.timezone)?;
        self.append_replicated_command(RaftCommand::CreateTimer {
            spec: spec.clone(),
            next_run_at,
        })?;
        let mut state = self.state.lock().unwrap();
        apply_create_timer(&mut state, spec)?;
        Ok(summary_for(&state, &id).expect("created timer summary exists"))
    }

    pub fn list(&self) -> Vec<DistributedSummary> {
        let state = self.state.lock().unwrap();
        state
            .timers
            .keys()
            .filter_map(|id| summary_for(&state, id))
            .collect()
    }

    pub fn status(&self, name: &str) -> Option<DistributedSummary> {
        let state = self.state.lock().unwrap();
        summary_for(&state, &TimerId::new(name))
    }

    pub fn history(&self, name: &str) -> Vec<serde_json::Value> {
        let state = self.state.lock().unwrap();
        state
            .history
            .iter()
            .filter(|entry| entry.get("timer").and_then(|v| v.as_str()) == Some(name))
            .cloned()
            .collect()
    }

    pub fn register_worker(
        &self,
        request: WorkerRegisterRequest,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        self.append_replicated_command(RaftCommand::RegisterWorker {
            worker_id: request.worker_id.clone(),
            tasks: request.tasks.clone(),
            lease_until: Utc::now()
                + chrono::Duration::seconds(request.lease_seconds.unwrap_or(30) as i64),
        })?;
        let mut state = self.state.lock().unwrap();
        state.workers.insert(
            request.worker_id.clone(),
            WorkerInfo {
                tasks: request.tasks,
                lease_until: Instant::now()
                    + Duration::from_secs(request.lease_seconds.unwrap_or(30)),
            },
        );
        state.history.push(serde_json::json!({
            "type": "WORKER_REGISTERED",
            "worker_id": request.worker_id,
            "ts": Utc::now(),
        }));
        Ok(serde_json::json!({"registered": true}))
    }

    pub fn heartbeat_worker(&self, worker_id: &str) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        let mut state = self.state.lock().unwrap();
        if let Some(worker) = state.workers.get_mut(worker_id) {
            worker.lease_until = Instant::now() + Duration::from_secs(30);
        }
        Ok(serde_json::json!({"ok": true}))
    }

    pub fn poll_worker(&self, request: WorkerPollRequest) -> Result<Option<WorkerRun>, KronError> {
        self.ensure_leader()?;
        self.heartbeat_worker(&request.worker_id)?;
        let deadline = Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(run) = self.claim_pending_for(&request.worker_id, &request.tasks)? {
                return Ok(Some(run));
            }
            if Instant::now() >= deadline || self.is_shutting_down() {
                return Ok(None);
            }
            thread::sleep(Duration::from_millis(100));
        }
    }

    pub fn complete_run(
        &self,
        run_id: &str,
        request: RunCompleteRequest,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        let run_id = RunId(run_id.to_string());
        let mut state = self.state.lock().unwrap();
        let active =
            validate_active_run(&state, &run_id, &request.worker_id, request.fencing_token)?;
        self.append_command(RaftCommand::CompleteRun {
            run_id: run_id.clone(),
            worker_id: request.worker_id,
            fencing_token: request.fencing_token,
            completed_at: Utc::now(),
        })?;
        state.active_runs.remove(&run_id);
        state
            .states
            .insert(active.timer_id.clone(), TimerState::Scheduled);
        state.history.push(serde_json::json!({
            "type": "RUN_SUCCEEDED",
            "timer": active.timer_id.as_str(),
            "run_id": run_id.0,
            "fencing_token": active.fencing_token,
            "ts": Utc::now(),
        }));
        reschedule_timer(&mut state, &active.timer_id);
        Ok(serde_json::json!({"ok": true}))
    }

    pub fn fail_run(
        &self,
        run_id: &str,
        request: RunFailRequest,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        let run_id = RunId(run_id.to_string());
        let mut state = self.state.lock().unwrap();
        let active =
            validate_active_run(&state, &run_id, &request.worker_id, request.fencing_token)?;
        self.append_command(RaftCommand::FailRun {
            run_id: run_id.clone(),
            worker_id: request.worker_id,
            fencing_token: request.fencing_token,
            error: request.error.clone(),
            failed_at: Utc::now(),
        })?;
        state.active_runs.remove(&run_id);
        state
            .states
            .insert(active.timer_id.clone(), TimerState::Dead);
        state.history.push(serde_json::json!({
            "type": "RUN_FAILED",
            "timer": active.timer_id.as_str(),
            "run_id": run_id.0,
            "error": request.error,
            "fencing_token": active.fencing_token,
            "ts": Utc::now(),
        }));
        Ok(serde_json::json!({"ok": true}))
    }

    pub fn cluster_status(&self) -> serde_json::Value {
        let state = self.state.lock().unwrap();
        serde_json::json!({
            "node_id": state.config.node_id,
            "leader_id": state.config.leader_id,
            "role": if state.leader { "leader" } else { "follower" },
            "timers": state.timers.len(),
            "workers": state.workers.len(),
            "peers": state.config.peers,
            "raft": "file-aof-quorum-alpha",
        })
    }

    pub fn join(&self, request: JoinRequest) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        let node = NodeInfo {
            node_id: request.node_id,
            http_addr: request.http_addr,
            raft_addr: request.raft_addr,
        };
        self.append_replicated_command(RaftCommand::AddNode { node: node.clone() })?;
        let mut state = self.state.lock().unwrap();
        apply_command(&mut state, &RaftCommand::AddNode { node })?;
        self.write_config(&state.config)?;
        Ok(serde_json::json!({"joined": true}))
    }

    pub fn leave(&self, request: LeaveRequest) -> Result<serde_json::Value, KronError> {
        self.ensure_leader()?;
        self.append_command(RaftCommand::RemoveNode {
            node_id: request.node_id.clone(),
        })?;
        let mut state = self.state.lock().unwrap();
        apply_command(
            &mut state,
            &RaftCommand::RemoveNode {
                node_id: request.node_id,
            },
        )?;
        self.write_config(&state.config)?;
        Ok(serde_json::json!({"left": true}))
    }

    pub fn shutdown(&self) {
        let mut state = self.state.lock().unwrap();
        state.shutting_down = true;
    }

    pub fn data_dir(&self) -> &Path {
        &self.data_dir
    }

    fn ensure_leader(&self) -> Result<(), KronError> {
        let state = self.state.lock().unwrap();
        if state.leader {
            Ok(())
        } else {
            Err(KronError::IpcUnavailable(format!(
                "not_leader: leader is {}",
                state.config.leader_id
            )))
        }
    }

    fn is_shutting_down(&self) -> bool {
        self.state.lock().unwrap().shutting_down
    }

    fn enqueue_due_runs(&self) -> Result<(), KronError> {
        let mut state = self.state.lock().unwrap();
        if !state.leader {
            return Ok(());
        }
        let now = Utc::now();
        let due: Vec<TimerId> = state
            .next_runs
            .iter()
            .filter_map(|(id, at)| if *at <= now { Some(id.clone()) } else { None })
            .collect();
        for id in due {
            if state.pending.iter().any(|run| run.timer == id.as_str())
                || state.active_runs.values().any(|run| run.timer_id == id)
            {
                continue;
            }
            if let Some(spec) = state.timers.get(&id).cloned() {
                if let TimerTarget::WorkerTask { task, payload } = spec.target {
                    let run_id = RunId::new();
                    let attempt = 1;
                    let idempotency_key =
                        format!("{}:{}", id.as_str(), state.next_runs[&id].to_rfc3339());
                    let token = *state.fencing.entry(id.clone()).or_insert(0) + 1;
                    state.fencing.insert(id.clone(), token);
                    state.pending.push_back(WorkerRun {
                        timer: id.as_str().to_string(),
                        run_id: run_id.0,
                        task,
                        payload,
                        attempt,
                        fencing_token: token,
                        idempotency_key,
                    });
                    state.states.insert(id.clone(), TimerState::Scheduled);
                }
            }
        }
        Ok(())
    }

    fn expire_workers(&self) {
        let mut state = self.state.lock().unwrap();
        let now = Instant::now();
        let now_utc = Utc::now();
        let lost: Vec<String> = state
            .workers
            .iter()
            .filter_map(|(id, worker)| {
                if worker.lease_until < now {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();
        for worker_id in lost {
            state.workers.remove(&worker_id);
            state.history.push(serde_json::json!({
                "type": "WORKER_LOST",
                "worker_id": worker_id,
                "ts": Utc::now(),
            }));
        }

        let expired_runs: Vec<RunId> = state
            .active_runs
            .iter()
            .filter_map(|(run_id, active)| {
                if active.lease_deadline <= now_utc
                    || !state.workers.contains_key(&active.worker_id)
                {
                    Some(run_id.clone())
                } else {
                    None
                }
            })
            .collect();

        for run_id in expired_runs {
            let Some(active) = state.active_runs.remove(&run_id) else {
                continue;
            };
            let Some(spec) = state.timers.get(&active.timer_id).cloned() else {
                continue;
            };
            state.history.push(serde_json::json!({
                "type": "RUN_LEASE_EXPIRED",
                "timer": active.timer_id.as_str(),
                "run_id": active.run_id.0,
                "worker_id": active.worker_id,
                "fencing_token": active.fencing_token,
                "ts": Utc::now(),
            }));
            if active.attempt >= spec.retry.max_attempts {
                state
                    .states
                    .insert(active.timer_id.clone(), TimerState::Dead);
                state.history.push(serde_json::json!({
                    "type": "RUN_DEAD",
                    "timer": active.timer_id.as_str(),
                    "run_id": active.run_id.0,
                    "ts": Utc::now(),
                }));
                continue;
            }
            let TimerTarget::WorkerTask { task, payload } = spec.target else {
                continue;
            };
            let token = *state.fencing.entry(active.timer_id.clone()).or_insert(0) + 1;
            state.fencing.insert(active.timer_id.clone(), token);
            let idempotency_key = format!(
                "{}:{}",
                active.timer_id.as_str(),
                state
                    .next_runs
                    .get(&active.timer_id)
                    .map(DateTime::<Utc>::to_rfc3339)
                    .unwrap_or_else(|| "lease-expired".to_string())
            );
            state.pending.push_back(WorkerRun {
                timer: active.timer_id.as_str().to_string(),
                run_id: active.run_id.0,
                task,
                payload,
                attempt: active.attempt + 1,
                fencing_token: token,
                idempotency_key,
            });
            state.states.insert(active.timer_id, TimerState::Retrying);
        }
    }

    fn claim_pending_for(
        &self,
        worker_id: &str,
        tasks: &[String],
    ) -> Result<Option<WorkerRun>, KronError> {
        let mut state = self.state.lock().unwrap();
        let worker_tasks = state
            .workers
            .get(worker_id)
            .map(|worker| worker.tasks.clone())
            .unwrap_or_else(|| tasks.to_vec());
        let Some(pos) = state.pending.iter().position(|run| {
            tasks.iter().any(|task| task == &run.task)
                && worker_tasks.iter().any(|task| task == &run.task)
        }) else {
            return Ok(None);
        };
        let run = state
            .pending
            .get(pos)
            .cloned()
            .expect("pending position exists");
        let timer_id = TimerId::new(&run.timer);
        let run_id = RunId(run.run_id.clone());
        self.append_command(RaftCommand::ClaimRun {
            timer_id: timer_id.clone(),
            run_id: run_id.clone(),
            worker_id: worker_id.to_string(),
            fencing_token: run.fencing_token,
            attempt: run.attempt,
            lease_deadline: Utc::now() + chrono::Duration::seconds(30),
        })?;
        let run = state.pending.remove(pos).expect("pending position exists");
        state.active_runs.insert(
            run_id.clone(),
            ActiveRun {
                timer_id: timer_id.clone(),
                run_id,
                worker_id: worker_id.to_string(),
                fencing_token: run.fencing_token,
                attempt: run.attempt,
                lease_deadline: Utc::now() + chrono::Duration::seconds(30),
            },
        );
        state.states.insert(timer_id.clone(), TimerState::Running);
        state.history.push(serde_json::json!({
            "type": "RUN_CLAIMED",
            "timer": timer_id.as_str(),
            "run_id": run.run_id,
            "worker_id": worker_id,
            "fencing_token": run.fencing_token,
            "ts": Utc::now(),
        }));
        Ok(Some(run))
    }

    pub fn apply_replicated_command(&self, command: RaftCommand) -> Result<(), KronError> {
        self.append_command(command.clone())?;
        let mut state = self.state.lock().unwrap();
        apply_command(&mut state, &command)?;
        if matches!(
            command,
            RaftCommand::AddNode { .. } | RaftCommand::RemoveNode { .. }
        ) {
            self.write_config(&state.config)?;
        }
        Ok(())
    }

    fn append_replicated_command(&self, command: RaftCommand) -> Result<(), KronError> {
        self.ensure_leader()?;
        let peers = {
            let state = match self.state.try_lock() {
                Ok(state) => state,
                Err(_) => {
                    self.append_command(command)?;
                    return Ok(());
                }
            };
            if !state.leader {
                return Err(KronError::IpcUnavailable(format!(
                    "not_leader: leader is {}",
                    state.config.leader_id
                )));
            }
            state.config.peers.clone()
        };
        let total_nodes = peers.len() + 1;
        let majority = total_nodes / 2 + 1;
        let mut acks = 1;
        let token = server_token(&self.data_dir)?;
        for peer in &peers {
            if replicate_command(peer, &token, &command).is_ok() {
                acks += 1;
            }
        }
        if acks < majority {
            return Err(KronError::IpcUnavailable(format!(
                "replication quorum failed: {acks}/{majority} acknowledgements"
            )));
        }
        self.append_command(command)
    }

    fn append_command(&self, command: RaftCommand) -> Result<(), KronError> {
        let entry = CommandEntry {
            v: 1,
            ts: Utc::now(),
            command,
        };
        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');
        let mut file = self.command_log.lock().unwrap();
        file.write_all(line.as_bytes())?;
        file.sync_data()?;
        Ok(())
    }

    fn replay_commands(&self) -> Result<(), KronError> {
        let path = self.data_dir.join("kron.raft.aof");
        let file = File::open(path)?;
        let reader = BufReader::new(file);
        let mut state = self.state.lock().unwrap();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            let entry: CommandEntry = serde_json::from_str(&line)?;
            apply_command(&mut state, &entry.command)?;
        }
        Ok(())
    }

    fn write_config(&self, config: &ClusterConfig) -> Result<(), KronError> {
        std::fs::write(
            self.data_dir.join("kron.cluster.json"),
            serde_json::to_string_pretty(config)? + "\n",
        )?;
        Ok(())
    }
}

pub fn serve_http(engine: Arc<ClusterEngine>, token: String) -> Result<(), KronError> {
    let addr = {
        let state = engine.state.lock().unwrap();
        state.config.http_addr.clone()
    };
    let listener = TcpListener::bind(&addr)?;
    listener.set_nonblocking(true)?;
    std::fs::write(engine.data_dir.join("kron.http"), format!("{addr}\n"))?;
    engine.start_scheduler();
    loop {
        if engine.is_shutting_down() {
            break;
        }
        match listener.accept() {
            Ok((stream, _)) => {
                let worker_engine = Arc::clone(&engine);
                let token = token.clone();
                thread::spawn(move || {
                    let _ = handle_http_stream(stream, worker_engine, &token);
                });
            }
            Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(50));
            }
            Err(err) => return Err(err.into()),
        }
    }
    Ok(())
}

pub fn server_token(data_dir: &Path) -> Result<String, KronError> {
    if let Ok(token) = std::env::var("KRON_CLUSTER_TOKEN") {
        std::fs::create_dir_all(data_dir)?;
        #[cfg(unix)]
        {
            use std::io::Write;
            use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};
            std::fs::set_permissions(data_dir, std::fs::Permissions::from_mode(0o700))?;
            let mut file = std::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .mode(0o600)
                .open(ipc::token_path(data_dir))?;
            file.write_all(format!("{token}\n").as_bytes())?;
            file.sync_all()?;
        }
        #[cfg(not(unix))]
        std::fs::write(ipc::token_path(data_dir), format!("{token}\n"))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(
                ipc::token_path(data_dir),
                std::fs::Permissions::from_mode(0o600),
            )?;
        }
        return Ok(token);
    }
    ipc::read_or_create_token(data_dir)
}

fn handle_http_stream(
    mut stream: TcpStream,
    engine: Arc<ClusterEngine>,
    token: &str,
) -> Result<(), KronError> {
    let mut reader = BufReader::new(stream.try_clone()?);
    let mut head = String::new();
    loop {
        let mut line = String::new();
        let read = reader.read_line(&mut line)?;
        if read == 0 || line == "\r\n" {
            break;
        }
        head.push_str(&line);
    }
    let mut lines = head.lines();
    let first = lines.next().unwrap_or("");
    let parts: Vec<&str> = first.split_whitespace().collect();
    if parts.len() < 2 {
        return write_json(
            &mut stream,
            400,
            serde_json::json!({"error": "bad request"}),
        );
    }
    let method = parts[0];
    let path = parts[1];
    let header_lines: Vec<String> = lines.map(|line| line.to_string()).collect();
    let content_length = header_lines
        .iter()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("content-length") {
                value.trim().parse::<usize>().ok()
            } else {
                None
            }
        })
        .unwrap_or(0);
    if content_length > 1024 * 1024 {
        return write_json(
            &mut stream,
            413,
            serde_json::json!({"error": "request body too large"}),
        );
    }
    let mut body = vec![0; content_length];
    if content_length > 0 {
        reader.read_exact(&mut body)?;
    }
    let body = String::from_utf8_lossy(&body);
    let authorized = header_lines.iter().any(|line| {
        let Some((name, value)) = line.split_once(':') else {
            return false;
        };
        name.eq_ignore_ascii_case("authorization")
            && crate::ipc::secure_eq(
                value.trim().as_bytes(),
                format!("Bearer {token}").as_bytes(),
            )
    });
    if !authorized {
        return write_json(
            &mut stream,
            401,
            serde_json::json!({"error": "unauthorized"}),
        );
    }
    let response = route_http(method, path, &body, engine);
    match response {
        Ok(HttpResponse { status, body }) => write_json(&mut stream, status, body),
        Err(err) => write_json(
            &mut stream,
            400,
            serde_json::json!({"error": err.to_string()}),
        ),
    }
}

struct HttpResponse {
    status: u16,
    body: serde_json::Value,
}

impl HttpResponse {
    fn ok(body: serde_json::Value) -> Self {
        Self { status: 200, body }
    }

    fn not_found() -> Self {
        Self {
            status: 404,
            body: serde_json::json!({"error": "not found"}),
        }
    }
}

fn route_http(
    method: &str,
    path: &str,
    body: &str,
    engine: Arc<ClusterEngine>,
) -> Result<HttpResponse, KronError> {
    let segments: Vec<&str> = path.trim_matches('/').split('/').collect();
    let body = match (method, segments.as_slice()) {
        ("POST", ["v1", "timers"]) => {
            let request: CreateTimerRequest = serde_json::from_str(body)?;
            serde_json::to_value(engine.create_timer(request)?)?
        }
        ("GET", ["v1", "timers"]) => serde_json::to_value(engine.list())?,
        ("GET", ["v1", "timers", name]) => serde_json::to_value(engine.status(name))?,
        ("GET", ["v1", "timers", name, "history"]) => serde_json::to_value(engine.history(name))?,
        ("POST", ["v1", "workers", "register"]) => {
            let request: WorkerRegisterRequest = serde_json::from_str(body)?;
            engine.register_worker(request)?
        }
        ("POST", ["v1", "workers", "heartbeat"]) => {
            let value: serde_json::Value = serde_json::from_str(body)?;
            let worker_id = value
                .get("worker_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            engine.heartbeat_worker(worker_id)?
        }
        ("POST", ["v1", "workers", "poll"]) => {
            let request: WorkerPollRequest = serde_json::from_str(body)?;
            serde_json::to_value(engine.poll_worker(request)?)?
        }
        ("POST", ["v1", "runs", run_id, "succeed"]) => {
            let request: RunCompleteRequest = serde_json::from_str(body)?;
            engine.complete_run(run_id, request)?
        }
        ("POST", ["v1", "runs", run_id, "fail"]) => {
            let request: RunFailRequest = serde_json::from_str(body)?;
            engine.fail_run(run_id, request)?
        }
        ("GET", ["v1", "cluster", "status"]) => engine.cluster_status(),
        ("POST", ["v1", "cluster", "join"]) => {
            let request: JoinRequest = serde_json::from_str(body)?;
            engine.join(request)?
        }
        ("POST", ["v1", "cluster", "leave"]) => {
            let request: LeaveRequest = serde_json::from_str(body)?;
            engine.leave(request)?
        }
        ("POST", ["v1", "raft", "apply"]) => {
            let command: RaftCommand = serde_json::from_str(body)?;
            engine.apply_replicated_command(command)?;
            serde_json::json!({"applied": true})
        }
        ("POST", ["v1", "runtime", "shutdown"]) => {
            engine.shutdown();
            serde_json::json!({"shutdown": "requested"})
        }
        _ => return Ok(HttpResponse::not_found()),
    };
    Ok(HttpResponse::ok(body))
}

fn write_json(
    stream: &mut TcpStream,
    status: u16,
    value: serde_json::Value,
) -> Result<(), KronError> {
    let body = serde_json::to_string(&value)?;
    let reason = match status {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        _ => "OK",
    };
    write!(
        stream,
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )?;
    stream.flush()?;
    Ok(())
}

fn parse_request_schedule(request: &CreateTimerRequest) -> Result<Schedule, KronError> {
    let selected = [
        request.cron.is_some(),
        request.every.is_some(),
        request.after.is_some(),
        request.at.is_some(),
    ]
    .into_iter()
    .filter(|v| *v)
    .count();
    if selected != 1 {
        return Err(KronError::InvalidCron(
            "expected exactly one of cron, every, after, at".to_string(),
        ));
    }
    if let Some(expr) = &request.cron {
        return Ok(Schedule::Cron { expr: expr.clone() });
    }
    if let Some(every) = &request.every {
        return Ok(Schedule::Every {
            seconds: crate::schedule::parse_duration_str(every)?,
        });
    }
    if let Some(after) = &request.after {
        return Ok(Schedule::After {
            seconds: crate::schedule::parse_duration_str(after)?,
            registered_at: Utc::now(),
        });
    }
    let at = DateTime::parse_from_rfc3339(request.at.as_ref().expect("selected at"))
        .map_err(|err| KronError::InvalidCron(format!("invalid at datetime: {err}")))?
        .with_timezone(&Utc);
    Ok(Schedule::At { at })
}

fn apply_create_timer(
    state: &mut ClusterState,
    spec: DistributedTimerSpec,
) -> Result<(), KronError> {
    let next = spec.schedule.next_run_after(Utc::now(), &spec.timezone)?;
    let id = spec.id.clone();
    state.timers.insert(id.clone(), spec);
    state.states.insert(id.clone(), TimerState::Scheduled);
    if let Some(next) = next {
        state.next_runs.insert(id.clone(), next);
    }
    state.history.push(serde_json::json!({
        "type": "TIMER_CREATED",
        "timer": id.as_str(),
        "ts": Utc::now(),
    }));
    Ok(())
}

fn apply_command(state: &mut ClusterState, command: &RaftCommand) -> Result<(), KronError> {
    match command {
        RaftCommand::CreateTimer {
            spec,
            next_run_at: _,
        }
        | RaftCommand::UpdateTimer {
            spec,
            next_run_at: _,
        } => {
            apply_create_timer(state, spec.clone())?;
        }
        RaftCommand::CancelTimer { timer_id } => {
            state.states.insert(timer_id.clone(), TimerState::Cancelled);
            state.next_runs.remove(timer_id);
        }
        RaftCommand::PauseTimer { timer_id } => {
            state.states.insert(timer_id.clone(), TimerState::Paused);
            state.next_runs.remove(timer_id);
        }
        RaftCommand::ResumeTimer {
            timer_id,
            next_run_at: _,
        } => {
            state.states.insert(timer_id.clone(), TimerState::Scheduled);
            reschedule_timer(state, timer_id);
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
                .any(|run| run.timer_id == *timer_id)
            {
                return Ok(());
            }
            state.active_runs.insert(
                run_id.clone(),
                ActiveRun {
                    timer_id: timer_id.clone(),
                    run_id: run_id.clone(),
                    worker_id: worker_id.clone(),
                    fencing_token: *fencing_token,
                    attempt: *attempt,
                    lease_deadline: *lease_deadline,
                },
            );
            state.states.insert(timer_id.clone(), TimerState::Running);
            state.fencing.insert(timer_id.clone(), *fencing_token);
            state.history.push(serde_json::json!({
                "type": "RUN_CLAIMED",
                "timer": timer_id.as_str(),
                "run_id": run_id.0,
                "worker_id": worker_id,
                "fencing_token": fencing_token,
                "ts": Utc::now(),
            }));
        }
        RaftCommand::CompleteRun {
            run_id,
            worker_id,
            fencing_token,
            completed_at: _,
        } => {
            let valid = state
                .active_runs
                .get(run_id)
                .map(|active| {
                    active.worker_id == *worker_id && active.fencing_token == *fencing_token
                })
                .unwrap_or(false);
            if valid {
                let active = state
                    .active_runs
                    .remove(run_id)
                    .expect("active run checked");
                state
                    .states
                    .insert(active.timer_id.clone(), TimerState::Scheduled);
                state.history.push(serde_json::json!({
                    "type": "RUN_SUCCEEDED",
                    "timer": active.timer_id.as_str(),
                    "run_id": run_id.0,
                    "worker_id": worker_id,
                    "fencing_token": active.fencing_token,
                    "ts": Utc::now(),
                }));
                reschedule_timer(state, &active.timer_id);
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
                .get(run_id)
                .map(|active| {
                    active.worker_id == *worker_id && active.fencing_token == *fencing_token
                })
                .unwrap_or(false);
            if valid {
                let active = state
                    .active_runs
                    .remove(run_id)
                    .expect("active run checked");
                state
                    .states
                    .insert(active.timer_id.clone(), TimerState::Dead);
                state.history.push(serde_json::json!({
                    "type": "RUN_FAILED",
                    "timer": active.timer_id.as_str(),
                    "run_id": run_id.0,
                    "worker_id": worker_id,
                    "error": error,
                    "fencing_token": active.fencing_token,
                    "ts": Utc::now(),
                }));
            }
        }
        RaftCommand::ExpireRunLease { .. } => {}
        RaftCommand::HeartbeatWorker {
            worker_id,
            lease_until: _,
        } => {
            if let Some(worker) = state.workers.get_mut(worker_id) {
                worker.lease_until = Instant::now() + Duration::from_secs(30);
            }
        }
        RaftCommand::RegisterWorker {
            worker_id,
            tasks,
            lease_until: _,
        } => {
            state.workers.insert(
                worker_id.clone(),
                WorkerInfo {
                    tasks: tasks.clone(),
                    lease_until: Instant::now() + Duration::from_secs(30),
                },
            );
            state.history.push(serde_json::json!({
                "type": "WORKER_REGISTERED",
                "worker_id": worker_id,
                "ts": Utc::now(),
            }));
        }
        RaftCommand::UnregisterWorker { worker_id } => {
            state.workers.remove(worker_id);
        }
        RaftCommand::AddNode { node } => {
            if node.node_id != state.config.node_id
                && !state
                    .config
                    .peers
                    .iter()
                    .any(|peer| peer.node_id == node.node_id)
            {
                state.config.peers.push(node.clone());
            }
        }
        RaftCommand::RemoveNode { node_id } => {
            state.config.peers.retain(|peer| peer.node_id != *node_id);
        }
    }
    Ok(())
}

fn replicate_command(peer: &NodeInfo, token: &str, command: &RaftCommand) -> Result<(), KronError> {
    let body = serde_json::to_string(command)?;
    let mut stream = TcpStream::connect(&peer.http_addr)?;
    write!(
        stream,
        "POST /v1/raft/apply HTTP/1.1\r\nHost: {}\r\nAuthorization: Bearer {}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        peer.http_addr,
        token,
        body.len(),
        body
    )?;
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    if response.starts_with("HTTP/1.1 200") {
        Ok(())
    } else {
        Err(KronError::IpcUnavailable(format!(
            "peer {} rejected replication",
            peer.node_id
        )))
    }
}

fn reschedule_timer(state: &mut ClusterState, timer_id: &TimerId) {
    if let Some(spec) = state.timers.get(timer_id) {
        if let Ok(Some(next)) = spec.schedule.next_run_after(Utc::now(), &spec.timezone) {
            state.next_runs.insert(timer_id.clone(), next);
        } else {
            state.next_runs.remove(timer_id);
        }
    }
}

fn summary_for(state: &ClusterState, id: &TimerId) -> Option<DistributedSummary> {
    let spec = state.timers.get(id)?;
    Some(DistributedSummary {
        id: id.as_str().to_string(),
        state: state
            .states
            .get(id)
            .cloned()
            .unwrap_or(TimerState::Scheduled),
        target: spec.target.clone(),
        next_run_at: state.next_runs.get(id).copied(),
        last_status: state
            .history
            .iter()
            .rev()
            .find(|entry| entry.get("timer").and_then(|v| v.as_str()) == Some(id.as_str()))
            .and_then(|entry| {
                entry
                    .get("type")
                    .and_then(|v| v.as_str())
                    .map(str::to_string)
            }),
        fencing_token: *state.fencing.get(id).unwrap_or(&0),
    })
}

fn validate_active_run(
    state: &ClusterState,
    run_id: &RunId,
    worker_id: &str,
    fencing_token: u64,
) -> Result<ActiveRun, KronError> {
    let active = state
        .active_runs
        .get(run_id)
        .cloned()
        .ok_or_else(|| KronError::IpcUnavailable("run not active".to_string()))?;
    if active.worker_id != worker_id {
        return Err(KronError::IpcUnavailable(
            "worker is not run owner".to_string(),
        ));
    }
    if active.fencing_token != fencing_token {
        return Err(KronError::IpcUnavailable("stale fencing token".to_string()));
    }
    Ok(active)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn engine() -> (TempDir, ClusterEngine) {
        let dir = TempDir::new().unwrap();
        let engine = ClusterEngine::open(dir.path(), "n1", "127.0.0.1:0", "127.0.0.1:0").unwrap();
        (dir, engine)
    }

    fn timer_request(name: &str) -> CreateTimerRequest {
        CreateTimerRequest {
            name: name.to_string(),
            cron: None,
            every: Some("1s".to_string()),
            after: None,
            at: None,
            timezone: Some("UTC".to_string()),
            max_attempts: Some(1),
            task: "digest".to_string(),
            payload: Some(serde_json::json!({"hello": "world"})),
        }
    }

    #[test]
    fn raft_command_round_trips() {
        let spec = DistributedTimerSpec {
            id: TimerId::new("roundtrip"),
            schedule: Schedule::Every { seconds: 60 },
            retry: RetryPolicy::no_retry(),
            timezone: "UTC".to_string(),
            target: TimerTarget::WorkerTask {
                task: "digest".to_string(),
                payload: serde_json::json!({}),
            },
            created_at: Utc::now(),
        };
        let next_run_at = spec
            .schedule
            .next_run_after(spec.created_at, &spec.timezone)
            .unwrap();
        let command = RaftCommand::CreateTimer { spec, next_run_at };
        let encoded = serde_json::to_string(&command).unwrap();
        let decoded: RaftCommand = serde_json::from_str(&encoded).unwrap();
        assert!(matches!(decoded, RaftCommand::CreateTimer { .. }));
    }

    #[test]
    fn claim_run_has_single_owner_and_fencing_token() {
        let (_dir, engine) = engine();
        engine.create_timer(timer_request("email")).unwrap();
        {
            let mut state = engine.state.lock().unwrap();
            state.next_runs.insert(
                TimerId::new("email"),
                Utc::now() - chrono::Duration::seconds(1),
            );
        }
        engine
            .register_worker(WorkerRegisterRequest {
                worker_id: "w1".to_string(),
                tasks: vec!["digest".to_string()],
                lease_seconds: Some(30),
            })
            .unwrap();
        engine.enqueue_due_runs().unwrap();
        let run = engine
            .claim_pending_for("w1", &["digest".to_string()])
            .unwrap()
            .unwrap();

        assert_eq!(run.timer, "email");
        assert_eq!(run.fencing_token, 1);
        assert_eq!(engine.state.lock().unwrap().active_runs.len(), 1);
    }

    #[test]
    fn stale_completion_is_rejected() {
        let (_dir, engine) = engine();
        engine.create_timer(timer_request("payment")).unwrap();
        {
            let mut state = engine.state.lock().unwrap();
            state.next_runs.insert(
                TimerId::new("payment"),
                Utc::now() - chrono::Duration::seconds(1),
            );
        }
        engine
            .register_worker(WorkerRegisterRequest {
                worker_id: "w1".to_string(),
                tasks: vec!["digest".to_string()],
                lease_seconds: Some(30),
            })
            .unwrap();
        engine.enqueue_due_runs().unwrap();
        let run = engine
            .claim_pending_for("w1", &["digest".to_string()])
            .unwrap()
            .unwrap();
        let err = engine
            .complete_run(
                &run.run_id,
                RunCompleteRequest {
                    worker_id: "w1".to_string(),
                    fencing_token: run.fencing_token + 1,
                },
            )
            .unwrap_err();
        assert!(err.to_string().contains("stale fencing token"));
    }

    #[test]
    fn expired_worker_requeues_active_run_with_new_fencing_token() {
        let (_dir, engine) = engine();
        let mut request = timer_request("lease");
        request.max_attempts = Some(2);
        engine.create_timer(request).unwrap();
        {
            let mut state = engine.state.lock().unwrap();
            state.next_runs.insert(
                TimerId::new("lease"),
                Utc::now() - chrono::Duration::seconds(1),
            );
        }
        engine
            .register_worker(WorkerRegisterRequest {
                worker_id: "w1".to_string(),
                tasks: vec!["digest".to_string()],
                lease_seconds: Some(30),
            })
            .unwrap();
        engine.enqueue_due_runs().unwrap();
        let first = engine
            .claim_pending_for("w1", &["digest".to_string()])
            .unwrap()
            .unwrap();

        {
            let mut state = engine.state.lock().unwrap();
            state.workers.remove("w1");
            let active = state
                .active_runs
                .get_mut(&RunId(first.run_id.clone()))
                .unwrap();
            active.lease_deadline = Utc::now() - chrono::Duration::seconds(1);
        }
        engine.expire_workers();

        let state = engine.state.lock().unwrap();
        assert!(state.active_runs.is_empty());
        assert_eq!(state.pending.len(), 1);
        let retry = state.pending.front().unwrap();
        assert_eq!(retry.run_id, first.run_id);
        assert_eq!(retry.attempt, 2);
        assert_eq!(retry.fencing_token, first.fencing_token + 1);
        assert_eq!(
            state.states.get(&TimerId::new("lease")),
            Some(&TimerState::Retrying)
        );
    }

    #[test]
    fn join_and_leave_update_membership() {
        let (_dir, engine) = engine();
        engine
            .join(JoinRequest {
                node_id: "n2".to_string(),
                http_addr: "127.0.0.1:7474".to_string(),
                raft_addr: "127.0.0.1:7475".to_string(),
            })
            .unwrap();
        {
            let state = engine.state.lock().unwrap();
            assert_eq!(state.config.peers.len(), 1);
            assert_eq!(state.config.peers[0].node_id, "n2");
        }
        engine
            .leave(LeaveRequest {
                node_id: "n2".to_string(),
            })
            .unwrap();
        assert!(engine.state.lock().unwrap().config.peers.is_empty());
    }
}
