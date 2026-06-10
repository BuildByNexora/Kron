use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::{Path as AxumPath, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use chrono::Utc;
use fs2::FileExt;
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use openraft::{BasicNode, Config, Raft};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

use crate::cluster::{
    CreateTimerRequest, DistributedSummary, DistributedTimerSpec, JoinRequest, LeaveRequest,
    RaftCommand, RunCompleteRequest, RunFailRequest, TimerTarget, WorkerPollRequest,
    WorkerRegisterRequest, WorkerRun,
};
use crate::error::KronError;
use crate::ipc;
use crate::openraft_adapter::file_store::KronRaftFileStore;
use crate::openraft_adapter::network::KronRaftNetworkFactory;
use crate::openraft_adapter::{KronRaftRequest, KronTypeConfig};
use crate::retry::RetryPolicy;
use crate::schedule::Schedule;
use crate::timer::{RunId, TimerId};

pub type KronRaft = Raft<KronTypeConfig>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum AuthRole {
    Reader,
    Worker,
    Operator,
    Admin,
    Raft,
}

impl AuthRole {
    fn allows(self, required: AuthRole) -> bool {
        matches!(self, AuthRole::Admin)
            || self == required
            || matches!((self, required), (AuthRole::Operator, AuthRole::Reader))
    }

    fn as_str(self) -> &'static str {
        match self {
            AuthRole::Reader => "reader",
            AuthRole::Worker => "worker",
            AuthRole::Operator => "operator",
            AuthRole::Admin => "admin",
            AuthRole::Raft => "raft",
        }
    }
}

#[derive(Debug, Clone)]
struct AuthContext {
    actor: String,
    role: AuthRole,
    tenant_id: Option<String>,
}

#[derive(Debug, Deserialize)]
struct TokenFile {
    tokens: Vec<TokenEntry>,
}

#[derive(Debug, Deserialize)]
struct TokenEntry {
    name: String,
    token: String,
    role: AuthRole,
    #[serde(default)]
    tenant_id: Option<String>,
}

#[derive(Clone)]
pub struct OpenRaftCluster {
    node_id: u64,
    node_name: String,
    http_addr: String,
    raft_addr: String,
    data_dir: PathBuf,
    token: String,
    raft: KronRaft,
    store: KronRaftFileStore,
    shutdown: Arc<std::sync::atomic::AtomicBool>,
    claim_lock: Arc<Mutex<()>>,
    _lock_file: Arc<File>,
}

impl OpenRaftCluster {
    pub async fn open(
        data_dir: impl AsRef<Path>,
        node_name: impl Into<String>,
        http_addr: impl Into<String>,
        raft_addr: impl Into<String>,
        token: String,
        bootstrap_single_node: bool,
    ) -> Result<Self, KronError> {
        let data_dir = data_dir.as_ref().to_path_buf();
        if data_dir.join("kron.raft.aof").exists() {
            return Err(KronError::IpcUnavailable(
                "alpha cluster storage requires manual migration before starting OpenRaft"
                    .to_string(),
            ));
        }
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

        let node_name = node_name.into();
        let node_id = parse_node_id(&node_name);
        let http_addr = http_addr.into();
        let raft_addr = raft_addr.into();
        let store = KronRaftFileStore::open(&data_dir)
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        let config = Arc::new(
            Config {
                cluster_name: "kron".to_string(),
                ..Default::default()
            }
            .validate()
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?,
        );
        let network = KronRaftNetworkFactory::new(token.clone());
        let raft = Raft::new(node_id, config, network, store.clone(), store.clone())
            .await
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        if bootstrap_single_node
            && !raft
                .is_initialized()
                .await
                .map_err(|err| KronError::IpcUnavailable(err.to_string()))?
        {
            let mut members = BTreeMap::new();
            members.insert(node_id, BasicNode::new(&raft_addr));
            raft.initialize(members)
                .await
                .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        }
        let cluster = Self {
            node_id,
            node_name,
            http_addr,
            raft_addr,
            data_dir,
            token,
            raft,
            store,
            shutdown: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            claim_lock: Arc::new(Mutex::new(())),
            _lock_file: Arc::new(lock_file),
        };
        cluster.write_local_metadata()?;
        Ok(cluster)
    }

    pub async fn create_timer(
        &self,
        mut request: CreateTimerRequest,
        tenant_id: Option<String>,
    ) -> Result<DistributedSummary, KronError> {
        self.ensure_leader().await?;
        if let Some(scoped_tenant) = tenant_id {
            if request
                .tenant_id
                .as_deref()
                .is_some_and(|requested| requested != scoped_tenant)
            {
                return Err(KronError::IpcUnavailable(
                    "token tenant does not match requested tenant".to_string(),
                ));
            }
            request.tenant_id = Some(scoped_tenant);
        }
        let schedule = parse_request_schedule(&request)?;
        let id = TimerId::new(request.name);
        let spec = DistributedTimerSpec {
            id: id.clone(),
            tenant_id: request.tenant_id,
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
        self.write_command(RaftCommand::CreateTimer { spec, next_run_at })
            .await?;
        self.status(id.as_str())
            .ok_or_else(|| KronError::IpcUnavailable("timer was not applied".to_string()))
    }

    pub fn list(&self) -> Vec<DistributedSummary> {
        self.store.app_state().timers.into_values().collect()
    }

    pub fn list_for_tenant(&self, tenant_id: Option<&str>) -> Vec<DistributedSummary> {
        self.list()
            .into_iter()
            .filter(|summary| tenant_matches(tenant_id, summary.tenant_id.as_deref()))
            .collect()
    }

    pub fn status(&self, name: &str) -> Option<DistributedSummary> {
        self.store.app_state().timers.remove(name)
    }

    pub fn status_for_tenant(
        &self,
        name: &str,
        tenant_id: Option<&str>,
    ) -> Option<DistributedSummary> {
        self.status(name)
            .filter(|summary| tenant_matches(tenant_id, summary.tenant_id.as_deref()))
    }

    pub fn history(&self, name: &str) -> Vec<serde_json::Value> {
        self.store
            .app_state()
            .history
            .into_iter()
            .filter(|entry| entry.get("timer").and_then(|v| v.as_str()) == Some(name))
            .collect()
    }

    pub fn history_for_tenant(
        &self,
        name: &str,
        tenant_id: Option<&str>,
    ) -> Vec<serde_json::Value> {
        self.history(name)
            .into_iter()
            .filter(|entry| {
                let entry_tenant = entry.get("tenant_id").and_then(|v| v.as_str());
                tenant_matches(tenant_id, entry_tenant)
            })
            .collect()
    }

    pub async fn register_worker(
        &self,
        request: WorkerRegisterRequest,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        self.write_command(RaftCommand::RegisterWorker {
            worker_id: request.worker_id,
            tasks: request.tasks,
            lease_until: Utc::now()
                + chrono::Duration::seconds(
                    request.lease_seconds.unwrap_or_else(worker_lease_seconds) as i64,
                ),
        })
        .await?;
        Ok(serde_json::json!({"registered": true}))
    }

    pub async fn heartbeat_worker(&self, worker_id: &str) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        self.write_command(RaftCommand::HeartbeatWorker {
            worker_id: worker_id.to_string(),
            lease_until: Utc::now() + chrono::Duration::seconds(worker_lease_seconds() as i64),
        })
        .await?;
        Ok(serde_json::json!({"ok": true}))
    }

    pub async fn poll_worker(
        &self,
        request: WorkerPollRequest,
        tenant_id: Option<String>,
    ) -> Result<Option<WorkerRun>, KronError> {
        self.ensure_leader().await?;
        self.heartbeat_worker(&request.worker_id).await?;
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            if let Some(run) = self
                .try_claim(&request.worker_id, &request.tasks, tenant_id.as_deref())
                .await?
            {
                return Ok(Some(run));
            }
            if std::time::Instant::now() >= deadline || self.is_shutting_down() {
                return Ok(None);
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    pub async fn complete_run(
        &self,
        run_id: &str,
        request: RunCompleteRequest,
        tenant_id: Option<String>,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        self.validate_active_run(
            run_id,
            &request.worker_id,
            request.fencing_token,
            tenant_id.as_deref(),
        )?;
        self.write_command(RaftCommand::CompleteRun {
            run_id: RunId(run_id.to_string()),
            worker_id: request.worker_id,
            fencing_token: request.fencing_token,
            completed_at: Utc::now(),
        })
        .await?;
        Ok(serde_json::json!({"ok": true}))
    }

    pub async fn fail_run(
        &self,
        run_id: &str,
        request: RunFailRequest,
        tenant_id: Option<String>,
    ) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        self.validate_active_run(
            run_id,
            &request.worker_id,
            request.fencing_token,
            tenant_id.as_deref(),
        )?;
        self.write_command(RaftCommand::FailRun {
            run_id: RunId(run_id.to_string()),
            worker_id: request.worker_id,
            fencing_token: request.fencing_token,
            error: request.error,
            failed_at: Utc::now(),
        })
        .await?;
        Ok(serde_json::json!({"ok": true}))
    }

    pub async fn join(&self, request: JoinRequest) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        let id = parse_node_id(&request.node_id);
        self.raft
            .add_learner(id, BasicNode::new(request.raft_addr), true)
            .await
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        let metrics = self.raft.metrics().borrow().clone();
        let mut voters: BTreeSet<u64> =
            metrics.membership_config.membership().voter_ids().collect();
        voters.insert(id);
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        Ok(serde_json::json!({"joined": true}))
    }

    pub async fn leave(&self, request: LeaveRequest) -> Result<serde_json::Value, KronError> {
        self.ensure_leader().await?;
        let remove_id = parse_node_id(&request.node_id);
        let metrics = self.raft.metrics().borrow().clone();
        let voters: BTreeSet<u64> = metrics
            .membership_config
            .membership()
            .voter_ids()
            .filter(|id| *id != remove_id)
            .collect();
        self.raft
            .change_membership(voters, false)
            .await
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        Ok(serde_json::json!({"left": true}))
    }

    pub fn cluster_status(&self) -> serde_json::Value {
        let metrics = self.raft.metrics().borrow().clone();
        let app = self.store.app_state();
        serde_json::json!({
            "node_id": self.node_name,
            "raft_node_id": self.node_id,
            "leader_id": metrics.current_leader,
            "role": format!("{:?}", metrics.state),
            "timers": app.timers.len(),
            "workers": 0,
            "raft": "openraft",
            "last_log_index": metrics.last_log_index,
            "last_applied": metrics.last_applied,
            "membership": metrics.membership_config,
        })
    }

    pub fn shutdown(&self) {
        self.shutdown
            .store(true, std::sync::atomic::Ordering::SeqCst);
    }

    pub fn is_shutting_down(&self) -> bool {
        self.shutdown.load(std::sync::atomic::Ordering::SeqCst)
    }

    pub async fn serve(self: Arc<Self>) -> Result<(), KronError> {
        let public = public_router(Arc::clone(&self));
        let raft = raft_router(Arc::clone(&self));
        let http_listener = TcpListener::bind(parse_addr(&self.http_addr)?).await?;
        let raft_listener = TcpListener::bind(parse_addr(&self.raft_addr)?).await?;
        write_token_file(&self.data_dir, &self.token)?;
        std::fs::write(
            self.data_dir.join("kron.http"),
            format!("{}\n", self.http_addr),
        )?;
        let lease_cluster = Arc::clone(&self);
        let lease_task = tokio::spawn(async move {
            while !lease_cluster.is_shutting_down() {
                let _ = lease_cluster.expire_active_leases().await;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
        let public_task = tokio::spawn(async move { axum::serve(http_listener, public).await });
        let raft_task = tokio::spawn(async move { axum::serve(raft_listener, raft).await });
        loop {
            if self.is_shutting_down() {
                lease_task.abort();
                public_task.abort();
                raft_task.abort();
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Ok(())
    }

    async fn expire_active_leases(&self) -> Result<(), KronError> {
        if self.raft.current_leader().await != Some(self.node_id) {
            return Ok(());
        }
        let app = self.store.app_state();
        let now = Utc::now();
        for active in app.active_runs.values() {
            if active.lease_deadline > now {
                continue;
            }
            let replacement = app.specs.get(active.timer_id.as_str()).and_then(|spec| {
                if active.attempt >= spec.retry.max_attempts {
                    return None;
                }
                let TimerTarget::WorkerTask { task, payload } = spec.target.clone() else {
                    return None;
                };
                let fencing_token = app
                    .timers
                    .get(active.timer_id.as_str())
                    .map(|summary| summary.fencing_token + 1)
                    .unwrap_or(active.fencing_token + 1);
                Some(WorkerRun {
                    timer: active.timer_id.as_str().to_string(),
                    tenant_id: spec.tenant_id.clone(),
                    run_id: active.run_id.0.clone(),
                    task,
                    payload,
                    attempt: active.attempt + 1,
                    fencing_token,
                    idempotency_key: format!(
                        "{}:lease-expired:{}",
                        active.timer_id.as_str(),
                        active.run_id.0
                    ),
                })
            });
            self.write_command(RaftCommand::ExpireRunLease {
                run_id: active.run_id.clone(),
                replacement,
                expired_at: now,
            })
            .await?;
        }
        Ok(())
    }

    async fn ensure_leader(&self) -> Result<(), KronError> {
        match self.raft.ensure_linearizable().await {
            Ok(_) => Ok(()),
            Err(_) => Err(KronError::IpcUnavailable(
                self.not_leader_response().to_string(),
            )),
        }
    }

    async fn write_command(&self, command: RaftCommand) -> Result<(), KronError> {
        self.raft
            .client_write(KronRaftRequest { command })
            .await
            .map_err(|err| KronError::IpcUnavailable(err.to_string()))?;
        Ok(())
    }

    async fn try_claim(
        &self,
        worker_id: &str,
        tasks: &[String],
        tenant_id: Option<&str>,
    ) -> Result<Option<WorkerRun>, KronError> {
        let claim = {
            let _guard = self.claim_lock.lock().unwrap();
            let app = self.store.app_state();
            let now = Utc::now();
            let active_timer_ids: std::collections::BTreeSet<String> = app
                .active_runs
                .values()
                .map(|run| run.timer_id.as_str().to_string())
                .collect();
            app.timers.values().find_map(|summary| {
                if !tenant_matches(tenant_id, summary.tenant_id.as_deref()) {
                    return None;
                }
                if active_timer_ids.contains(&summary.id) {
                    return None;
                }
                let next = summary.next_run_at?;
                if next > now {
                    return None;
                }
                let TimerTarget::WorkerTask { task, payload } = summary.target.clone() else {
                    return None;
                };
                if !tasks.iter().any(|candidate| candidate == &task) {
                    return None;
                }
                Some(WorkerRun {
                    timer: summary.id.clone(),
                    tenant_id: summary.tenant_id.clone(),
                    run_id: RunId::new().0,
                    task,
                    payload,
                    attempt: 1,
                    fencing_token: summary.fencing_token + 1,
                    idempotency_key: format!("{}:{}", summary.id, next.to_rfc3339()),
                })
            })
        };
        if let Some(run) = claim {
            self.write_command(RaftCommand::ClaimRun {
                timer_id: TimerId::new(&run.timer),
                run_id: RunId(run.run_id.clone()),
                worker_id: worker_id.to_string(),
                fencing_token: run.fencing_token,
                attempt: run.attempt,
                lease_deadline: Utc::now()
                    + chrono::Duration::seconds(worker_lease_seconds() as i64),
            })
            .await?;
            let app = self.store.app_state();
            let still_owner = app
                .active_runs
                .get(&run.run_id)
                .map(|active| {
                    active.worker_id == worker_id && active.fencing_token == run.fencing_token
                })
                .unwrap_or(false);
            return Ok(still_owner.then_some(run));
        }
        Ok(None)
    }

    fn validate_active_run(
        &self,
        run_id: &str,
        worker_id: &str,
        fencing_token: u64,
        tenant_id: Option<&str>,
    ) -> Result<(), KronError> {
        let app = self.store.app_state();
        let active = app
            .active_runs
            .get(run_id)
            .ok_or_else(|| KronError::IpcUnavailable("run not active".to_string()))?;
        if active.worker_id != worker_id {
            return Err(KronError::IpcUnavailable(
                "worker is not run owner".to_string(),
            ));
        }
        if active.fencing_token != fencing_token {
            return Err(KronError::IpcUnavailable("stale fencing token".to_string()));
        }
        let run_tenant = app
            .specs
            .get(active.timer_id.as_str())
            .and_then(|spec| spec.tenant_id.as_deref());
        if !tenant_matches(tenant_id, run_tenant) {
            return Err(KronError::IpcUnavailable(
                "token tenant does not match run tenant".to_string(),
            ));
        }
        Ok(())
    }

    fn not_leader_response(&self) -> serde_json::Value {
        let leader_id = self.raft.metrics().borrow().current_leader;
        let leader_http = if leader_id == Some(self.node_id) {
            Some(self.http_addr.clone())
        } else {
            None
        };
        serde_json::json!({
            "error": "not_leader",
            "leader_id": leader_id,
            "leader_http": leader_http,
        })
    }

    fn write_local_metadata(&self) -> Result<(), KronError> {
        let meta = serde_json::json!({
            "node_id": self.node_name,
            "raft_node_id": self.node_id,
            "http_addr": self.http_addr,
            "raft_addr": self.raft_addr,
        });
        std::fs::write(
            self.data_dir.join("kron.cluster.json"),
            serde_json::to_string_pretty(&meta)? + "\n",
        )?;
        Ok(())
    }
}

fn public_router(cluster: Arc<OpenRaftCluster>) -> Router {
    Router::new()
        .route("/v1/timers", post(create_timer).get(list_timers))
        .route("/v1/timers/:name", get(status_timer))
        .route("/v1/timers/:name/history", get(history_timer))
        .route("/v1/workers/register", post(register_worker))
        .route("/v1/workers/heartbeat", post(heartbeat_worker))
        .route("/v1/workers/poll", post(poll_worker))
        .route("/v1/runs/:run_id/succeed", post(succeed_run))
        .route("/v1/runs/:run_id/fail", post(fail_run))
        .route("/v1/cluster/status", get(cluster_status))
        .route("/v1/cluster/join", post(join_cluster))
        .route("/v1/cluster/leave", post(leave_cluster))
        .route("/v1/runtime/shutdown", post(shutdown_runtime))
        .with_state(cluster)
}

fn raft_router(cluster: Arc<OpenRaftCluster>) -> Router {
    Router::new()
        .route("/__kron/raft/append_entries", post(raft_append_entries))
        .route("/__kron/raft/vote", post(raft_vote))
        .route("/__kron/raft/install_snapshot", post(raft_install_snapshot))
        .with_state(cluster)
}

async fn create_timer(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<CreateTimerRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Operator,
        "timer.create",
        move |auth| async move { cluster.create_timer(request, auth.tenant_id.clone()).await },
    )
    .await
}

async fn list_timers(State(cluster): State<Arc<OpenRaftCluster>>, headers: HeaderMap) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Reader,
        "timer.list",
        move |auth| async move { Ok(cluster.list_for_tenant(auth.tenant_id.as_deref())) },
    )
    .await
}

async fn status_timer(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Reader,
        "timer.status",
        move |auth| async move { Ok(cluster.status_for_tenant(&name, auth.tenant_id.as_deref())) },
    )
    .await
}

async fn history_timer(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    AxumPath(name): AxumPath<String>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Reader,
        "timer.history",
        move |auth| async move { Ok(cluster.history_for_tenant(&name, auth.tenant_id.as_deref())) },
    )
    .await
}

async fn register_worker(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<WorkerRegisterRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Worker,
        "worker.register",
        move |_auth| async move { cluster.register_worker(request).await },
    )
    .await
}

async fn heartbeat_worker(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(value): Json<serde_json::Value>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Worker,
        "worker.heartbeat",
        move |_auth| async move {
            let worker_id = value
                .get("worker_id")
                .and_then(|v| v.as_str())
                .unwrap_or("");
            cluster.heartbeat_worker(worker_id).await
        },
    )
    .await
}

async fn poll_worker(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<WorkerPollRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Worker,
        "worker.poll",
        move |auth| async move { cluster.poll_worker(request, auth.tenant_id.clone()).await },
    )
    .await
}

async fn succeed_run(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    AxumPath(run_id): AxumPath<String>,
    Json(request): Json<RunCompleteRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Worker,
        "run.succeed",
        move |auth| async move {
            cluster
                .complete_run(&run_id, request, auth.tenant_id.clone())
                .await
        },
    )
    .await
}

async fn fail_run(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    AxumPath(run_id): AxumPath<String>,
    Json(request): Json<RunFailRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Worker,
        "run.fail",
        move |auth| async move {
            cluster
                .fail_run(&run_id, request, auth.tenant_id.clone())
                .await
        },
    )
    .await
}

async fn cluster_status(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Reader,
        "cluster.status",
        move |_auth| async move { Ok(cluster.cluster_status()) },
    )
    .await
}

async fn join_cluster(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<JoinRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Admin,
        "cluster.join",
        move |_auth| async move { cluster.join(request).await },
    )
    .await
}

async fn leave_cluster(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<LeaveRequest>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Admin,
        "cluster.leave",
        move |_auth| async move { cluster.leave(request).await },
    )
    .await
}

async fn shutdown_runtime(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Admin,
        "runtime.shutdown",
        move |_auth| async move {
            cluster.shutdown();
            Ok(serde_json::json!({"shutdown": "requested"}))
        },
    )
    .await
}

async fn raft_append_entries(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<AppendEntriesRequest<KronTypeConfig>>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Raft,
        "raft.append_entries",
        move |_auth| async move {
            cluster
                .raft
                .append_entries(request)
                .await
                .map_err(|err| KronError::IpcUnavailable(err.to_string()))
        },
    )
    .await
}

async fn raft_vote(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<VoteRequest<u64>>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Raft,
        "raft.vote",
        move |_auth| async move {
            cluster
                .raft
                .vote(request)
                .await
                .map_err(|err| KronError::IpcUnavailable(err.to_string()))
        },
    )
    .await
}

async fn raft_install_snapshot(
    State(cluster): State<Arc<OpenRaftCluster>>,
    headers: HeaderMap,
    Json(request): Json<InstallSnapshotRequest<KronTypeConfig>>,
) -> Response {
    authed(
        Arc::clone(&cluster),
        &headers,
        AuthRole::Raft,
        "raft.install_snapshot",
        move |_auth| async move {
            cluster
                .raft
                .install_snapshot(request)
                .await
                .map_err(|err| KronError::IpcUnavailable(err.to_string()))
        },
    )
    .await
}

async fn authed<T, F, Fut>(
    cluster: Arc<OpenRaftCluster>,
    headers: &HeaderMap,
    required_role: AuthRole,
    action: &'static str,
    f: F,
) -> Response
where
    T: serde::Serialize,
    F: FnOnce(AuthContext) -> Fut,
    Fut: std::future::Future<Output = Result<T, KronError>>,
{
    let auth = match authenticate(&cluster, headers) {
        Ok(auth) if auth.role.allows(required_role) => auth,
        Ok(auth) => {
            audit(
                &cluster,
                Some(&auth),
                action,
                "forbidden",
                StatusCode::FORBIDDEN.as_u16(),
                Some(required_role.as_str()),
            );
            return (
                StatusCode::FORBIDDEN,
                Json(serde_json::json!({"error": "forbidden", "required_role": required_role.as_str()})),
            )
                .into_response();
        }
        Err(reason) => {
            audit(
                &cluster,
                None,
                action,
                "unauthorized",
                StatusCode::UNAUTHORIZED.as_u16(),
                Some(&reason),
            );
            return (
                StatusCode::UNAUTHORIZED,
                Json(serde_json::json!({"error": "unauthorized"})),
            )
                .into_response();
        }
    };
    match f(auth.clone()).await {
        Ok(value) => {
            audit(
                &cluster,
                Some(&auth),
                action,
                "ok",
                StatusCode::OK.as_u16(),
                None,
            );
            (StatusCode::OK, Json(serde_json::to_value(value).unwrap())).into_response()
        }
        Err(err) => {
            let text = err.to_string();
            let value = if let Some(json_start) = text.find('{') {
                serde_json::from_str(&text[json_start..])
                    .unwrap_or_else(|_| serde_json::json!({"error": text}))
            } else {
                serde_json::json!({"error": text})
            };
            let status = if value.get("error").and_then(|v| v.as_str()) == Some("not_leader") {
                StatusCode::CONFLICT
            } else {
                StatusCode::BAD_REQUEST
            };
            audit(
                &cluster,
                Some(&auth),
                action,
                "error",
                status.as_u16(),
                value.get("error").and_then(|v| v.as_str()),
            );
            (status, Json(value)).into_response()
        }
    }
}

fn authenticate(cluster: &OpenRaftCluster, headers: &HeaderMap) -> Result<AuthContext, String> {
    let bearer = headers
        .get("authorization")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.strip_prefix("Bearer "))
        .ok_or_else(|| "missing bearer token".to_string())?;

    let tokens_path = cluster.data_dir.join("kron.tokens.json");
    if tokens_path.exists() {
        let content = std::fs::read_to_string(&tokens_path)
            .map_err(|err| format!("unable to read token file: {err}"))?;
        let token_file: TokenFile =
            serde_json::from_str(&content).map_err(|err| format!("invalid token file: {err}"))?;
        return token_file
            .tokens
            .into_iter()
            .find(|entry| ipc::secure_eq(entry.token.as_bytes(), bearer.as_bytes()))
            .map(|entry| AuthContext {
                actor: entry.name,
                role: entry.role,
                tenant_id: entry.tenant_id,
            })
            .ok_or_else(|| "unknown token".to_string());
    }

    if ipc::secure_eq(cluster.token.as_bytes(), bearer.as_bytes()) {
        return Ok(AuthContext {
            actor: "legacy-admin-token".to_string(),
            role: AuthRole::Admin,
            tenant_id: None,
        });
    }
    Err("unknown token".to_string())
}

fn audit(
    cluster: &OpenRaftCluster,
    auth: Option<&AuthContext>,
    action: &str,
    outcome: &str,
    status: u16,
    reason: Option<&str>,
) {
    let event = serde_json::json!({
        "ts": Utc::now(),
        "node_id": cluster.node_name,
        "action": action,
        "outcome": outcome,
        "status": status,
        "actor": auth.map(|ctx| ctx.actor.as_str()).unwrap_or("anonymous"),
        "role": auth.map(|ctx| ctx.role.as_str()).unwrap_or("none"),
        "tenant_id": auth.and_then(|ctx| ctx.tenant_id.as_deref()),
        "reason": reason,
    });
    let path = cluster.data_dir.join("kron.audit.jsonl");
    use std::io::Write;
    #[cfg(unix)]
    let file = {
        use std::os::unix::fs::OpenOptionsExt;
        OpenOptions::new()
            .create(true)
            .append(true)
            .mode(0o600)
            .open(&path)
    };
    #[cfg(not(unix))]
    let file = OpenOptions::new().create(true).append(true).open(&path);
    if let Ok(mut file) = file {
        let _ = writeln!(file, "{event}");
        let _ = file.sync_data();
    }
}

fn tenant_matches(request_tenant: Option<&str>, resource_tenant: Option<&str>) -> bool {
    match request_tenant {
        None | Some("*") => true,
        Some(tenant) => resource_tenant == Some(tenant),
    }
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
    let at = chrono::DateTime::parse_from_rfc3339(request.at.as_ref().expect("selected at"))
        .map_err(|err| KronError::InvalidCron(format!("invalid at datetime: {err}")))?
        .with_timezone(&Utc);
    Ok(Schedule::At { at })
}

fn worker_lease_seconds() -> u64 {
    std::env::var("KRON_WORKER_LEASE_SECONDS")
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|seconds| *seconds > 0)
        .unwrap_or(30)
}

pub fn server_token(data_dir: &Path) -> Result<String, KronError> {
    if let Ok(token) = std::env::var("KRON_CLUSTER_TOKEN") {
        return Ok(token);
    }
    if ipc::token_path(data_dir).exists() {
        return ipc::read_token(data_dir);
    }
    ipc::generate_secret_token()
}

fn write_token_file(data_dir: &Path, token: &str) -> Result<(), KronError> {
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
    Ok(())
}

fn parse_node_id(value: &str) -> u64 {
    let digits: String = value.chars().filter(|ch| ch.is_ascii_digit()).collect();
    digits.parse().unwrap_or_else(|_| {
        let mut hash = 1469598103934665603u64;
        for byte in value.as_bytes() {
            hash ^= *byte as u64;
            hash = hash.wrapping_mul(1099511628211);
        }
        hash
    })
}

fn parse_addr(value: &str) -> Result<SocketAddr, KronError> {
    value
        .parse()
        .map_err(|err| KronError::IpcUnavailable(format!("invalid socket address {value}: {err}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auth_roles_allow_only_expected_actions() {
        assert!(AuthRole::Admin.allows(AuthRole::Raft));
        assert!(AuthRole::Admin.allows(AuthRole::Operator));
        assert!(AuthRole::Operator.allows(AuthRole::Reader));
        assert!(AuthRole::Operator.allows(AuthRole::Operator));
        assert!(!AuthRole::Operator.allows(AuthRole::Worker));
        assert!(AuthRole::Worker.allows(AuthRole::Worker));
        assert!(!AuthRole::Worker.allows(AuthRole::Reader));
        assert!(AuthRole::Reader.allows(AuthRole::Reader));
        assert!(!AuthRole::Reader.allows(AuthRole::Operator));
        assert!(AuthRole::Raft.allows(AuthRole::Raft));
        assert!(!AuthRole::Raft.allows(AuthRole::Reader));
    }

    #[test]
    fn tenant_match_restricts_scoped_tokens() {
        assert!(tenant_matches(None, Some("tenant-a")));
        assert!(tenant_matches(Some("*"), Some("tenant-a")));
        assert!(tenant_matches(Some("tenant-a"), Some("tenant-a")));
        assert!(!tenant_matches(Some("tenant-a"), Some("tenant-b")));
        assert!(!tenant_matches(Some("tenant-a"), None));
    }
}
