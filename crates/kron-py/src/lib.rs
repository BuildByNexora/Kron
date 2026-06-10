#![allow(clippy::useless_conversion)]

use std::collections::HashMap;
use std::env;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::path::PathBuf;
use std::sync::mpsc;
use std::sync::{Arc, Mutex, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::Duration as StdDuration;

use chrono::{DateTime, Utc};
use kron_core::engine::TimerFn;
use kron_core::error::KronError;
use kron_core::ipc;
use kron_core::retry::RetryPolicy;
use kron_core::schedule::{parse_duration_str, Schedule};
use kron_core::timer::{RunId, TimerId, TimerState, TimerSummary};
use kron_core::Engine;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyModule, PyTuple};

static GLOBAL: OnceLock<Mutex<GlobalState>> = OnceLock::new();
const DEFAULT_HTTP_TIMEOUT: StdDuration = StdDuration::from_secs(30);

fn global() -> &'static Mutex<GlobalState> {
    GLOBAL.get_or_init(|| Mutex::new(GlobalState::default()))
}

#[derive(Default)]
struct GlobalState {
    runtime: Option<PyKronRuntime>,
    pending: Vec<PendingRegistration>,
}

struct PyKronRuntime {
    engine: Arc<Engine>,
    stop_tx: mpsc::Sender<()>,
    join: Option<JoinHandle<()>>,
    ipc_join: Option<JoinHandle<()>>,
}

struct PendingRegistration {
    name: String,
    schedule: Schedule,
    callable: Py<PyAny>,
    function_name: String,
    retry: RetryPolicy,
    timezone: String,
}

struct PyTimerFn {
    callable: Mutex<Py<PyAny>>,
    function_name: String,
}

impl PyTimerFn {
    fn new(callable: Py<PyAny>, function_name: String) -> Self {
        Self {
            callable: Mutex::new(callable),
            function_name,
        }
    }
}

impl TimerFn for PyTimerFn {
    fn call(&self, timer_id: &TimerId, run_id: &RunId) -> Result<(), String> {
        Python::with_gil(|py| {
            let callable = self
                .callable
                .lock()
                .map_err(|_| "python callable lock poisoned".to_string())?;
            let callable = callable.bind(py);
            if callable_accepts_context(py, callable).unwrap_or(false) {
                let context = PyDict::new_bound(py);
                context
                    .set_item("timer_id", timer_id.as_str())
                    .map_err(|err| err.to_string())?;
                context
                    .set_item("run_id", run_id.0.as_str())
                    .map_err(|err| err.to_string())?;
                callable
                    .call1((context,))
                    .map(|_| ())
                    .map_err(|err| err.to_string())
            } else {
                callable.call0().map(|_| ()).map_err(|err| err.to_string())
            }
        })
    }

    fn name(&self) -> String {
        self.function_name.clone()
    }
}

fn callable_accepts_context(py: Python<'_>, callable: &Bound<'_, PyAny>) -> PyResult<bool> {
    let inspect = py.import_bound("inspect")?;
    let signature = inspect.call_method1("signature", (callable,))?;
    let params = signature.getattr("parameters")?;
    let builtins = py.import_bound("builtins")?;
    let values = builtins
        .call_method1("list", (params.call_method0("values")?,))?
        .downcast_into::<PyList>()?;
    for param in values.iter() {
        let kind = param.getattr("kind")?;
        let kind_name: String = kind.getattr("name")?.extract()?;
        if matches!(
            kind_name.as_str(),
            "POSITIONAL_ONLY" | "POSITIONAL_OR_KEYWORD" | "VAR_POSITIONAL"
        ) {
            return Ok(true);
        }
    }
    Ok(false)
}

#[pyfunction(signature = (name, **kwargs))]
fn schedule(_py: Python<'_>, name: String, kwargs: Option<&Bound<'_, PyDict>>) -> PyResult<()> {
    let kwargs = kwargs.ok_or_else(|| PyValueError::new_err("schedule() requires keyword args"))?;
    let callable = kwargs
        .get_item("fn")?
        .ok_or_else(|| PyValueError::new_err("schedule() requires fn=<callable>"))?;
    if !callable.is_callable() {
        return Err(PyValueError::new_err("fn must be callable"));
    }

    let function_name = callable
        .getattr("__name__")
        .and_then(|name| name.extract::<String>())
        .unwrap_or_else(|_| {
            callable
                .repr()
                .map(|value| value.to_string())
                .unwrap_or_else(|_| "<callable>".to_string())
        });
    let callable = callable.unbind();

    let timezone = get_optional_string(kwargs, "timezone")?.unwrap_or_else(|| "UTC".to_string());
    let max_attempts = kwargs
        .get_item("max_attempts")?
        .map(|value| value.extract::<u32>())
        .transpose()?
        .unwrap_or(3);
    let retry = RetryPolicy {
        max_attempts,
        ..Default::default()
    };
    let schedule = parse_schedule(kwargs)?;

    let registration = PendingRegistration {
        name,
        schedule,
        callable,
        function_name,
        retry,
        timezone,
    };

    let mut state = global()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
    if let Some(runtime) = &state.runtime {
        apply_registration(&runtime.engine, registration)?;
    } else {
        state.pending.push(registration);
    }
    Ok(())
}

#[pyfunction(signature = (data_dir=None))]
fn start(py: Python<'_>, data_dir: Option<String>) -> PyResult<()> {
    let mut state = global()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
    if state.runtime.is_some() {
        return Err(map_kron_error(KronError::AlreadyStarted));
    }

    let data_dir = resolve_data_dir(py, data_dir)?;
    let engine = py
        .allow_threads(|| Engine::open(&data_dir))
        .map_err(map_kron_error)?;
    let engine = Arc::new(engine);

    let pending = std::mem::take(&mut state.pending);
    for registration in pending {
        apply_registration(&engine, registration)?;
    }

    let engine_for_thread = Arc::clone(&engine);
    let ipc_join = ipc::start_server(Arc::clone(&engine)).map_err(map_kron_error)?;
    let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel(1);
    let (stop_tx, stop_rx) = mpsc::channel();
    let join = thread::spawn(move || {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(2)
            .build()
            .expect("failed to build kron tokio runtime");
        let start_result = runtime.block_on(async { engine_for_thread.start() });
        let should_keep_running = start_result.is_ok();
        let _ = ready_tx.send(start_result.map_err(|err| err.to_string()));
        if !should_keep_running {
            return;
        }
        while stop_rx.recv_timeout(StdDuration::from_millis(100)).is_err() {}
    });

    match ready_rx.recv() {
        Ok(Ok(())) => {
            state.runtime = Some(PyKronRuntime {
                engine,
                stop_tx,
                join: Some(join),
                ipc_join: Some(ipc_join),
            });
            Ok(())
        }
        Ok(Err(err)) => {
            let _ = join.join();
            Err(PyRuntimeError::new_err(err))
        }
        Err(_) => {
            let _ = join.join();
            Err(PyRuntimeError::new_err(
                "kron runtime thread failed to start",
            ))
        }
    }
}

#[pyfunction(signature = (data_dir=None))]
fn astart(py: Python<'_>, data_dir: Option<String>) -> PyResult<PyObject> {
    let kron = py.import_bound("kron")?;
    let start_fn = kron.getattr("start")?;
    let args = match data_dir {
        Some(data_dir) => PyTuple::new_bound(py, [data_dir.into_py(py)]),
        None => PyTuple::empty_bound(py),
    };
    let coroutine = call_in_async_thread(py, start_fn, args)?;
    Ok(coroutine.unbind().into())
}

#[pyfunction(signature = (timeout=5.0))]
fn shutdown(py: Python<'_>, timeout: f64) -> PyResult<()> {
    let runtime = {
        let mut state = global()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
        state.runtime.take()
    };

    let Some(mut runtime) = runtime else {
        return Ok(());
    };

    let timeout_duration = seconds_to_duration(timeout)?;
    let shutdown_result = py.allow_threads(|| {
        let tokio_runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
        tokio_runtime
            .block_on(runtime.engine.shutdown(timeout_duration))
            .map_err(map_kron_error)
    });
    if let Err(err) = shutdown_result {
        let mut state = global()
            .lock()
            .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
        state.runtime = Some(runtime);
        return Err(err);
    }

    let _ = runtime.stop_tx.send(());
    if let Some(join) = runtime.join.take() {
        join.join()
            .map_err(|_| PyRuntimeError::new_err("kron runtime thread panicked"))?;
    }
    let socket = ipc::socket_path(runtime.engine.data_dir());
    let _ = std::fs::remove_file(socket);
    let _ = std::fs::remove_file(ipc::port_path(runtime.engine.data_dir()));
    if let Some(join) = runtime.ipc_join.take() {
        let _ = join.join();
    }
    Ok(())
}

#[pyfunction(signature = (timeout=5.0))]
fn ashutdown(py: Python<'_>, timeout: f64) -> PyResult<PyObject> {
    let kron = py.import_bound("kron")?;
    let shutdown_fn = kron.getattr("shutdown")?;
    let args = PyTuple::new_bound(py, [timeout.into_py(py)]);
    let coroutine = call_in_async_thread(py, shutdown_fn, args)?;
    Ok(coroutine.unbind().into())
}

#[pyfunction]
fn status(py: Python<'_>, name: String) -> PyResult<Option<PyObject>> {
    let state = global()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
    let Some(runtime) = &state.runtime else {
        return Ok(None);
    };
    runtime
        .engine
        .status(&name)
        .map(|summary| summary_to_dict(py, summary))
        .transpose()
}

#[pyfunction]
fn astatus(py: Python<'_>, name: String) -> PyResult<PyObject> {
    let kron = py.import_bound("kron")?;
    let status_fn = kron.getattr("status")?;
    let args = PyTuple::new_bound(py, [name.into_py(py)]);
    let coroutine = call_in_async_thread(py, status_fn, args)?;
    Ok(coroutine.unbind().into())
}

#[pyfunction]
fn list(py: Python<'_>) -> PyResult<Vec<PyObject>> {
    let state = global()
        .lock()
        .map_err(|_| PyRuntimeError::new_err("kron global state lock poisoned"))?;
    let Some(runtime) = &state.runtime else {
        return Ok(Vec::new());
    };
    runtime
        .engine
        .list()
        .into_iter()
        .map(|summary| summary_to_dict(py, summary))
        .collect()
}

#[pyfunction]
fn alist(py: Python<'_>) -> PyResult<PyObject> {
    let kron = py.import_bound("kron")?;
    let list_fn = kron.getattr("list")?;
    let coroutine = call_in_async_thread(py, list_fn, PyTuple::empty_bound(py))?;
    Ok(coroutine.unbind().into())
}

fn call_in_async_thread<'py>(
    py: Python<'py>,
    callable: Bound<'py, PyAny>,
    args: Bound<'py, PyTuple>,
) -> PyResult<Bound<'py, PyAny>> {
    let asyncio = py.import_bound("asyncio")?;
    if let Ok(to_thread) = asyncio.getattr("to_thread") {
        let mut values = Vec::with_capacity(args.len() + 1);
        values.push(callable.clone().into_any().unbind());
        values.extend(args.iter().map(|arg| arg.unbind()));
        let call_args = PyTuple::new_bound(py, values);
        return to_thread.call1(call_args);
    }

    let loop_ = asyncio.call_method0("get_running_loop")?;
    let functools = py.import_bound("functools")?;
    let partial = functools.getattr("partial")?.call1({
        let mut values = Vec::with_capacity(args.len() + 1);
        values.push(callable.into_any().unbind());
        values.extend(args.iter().map(|arg| arg.unbind()));
        let call_args = PyTuple::new_bound(py, values);
        call_args
    })?;
    loop_.call_method1("run_in_executor", (py.None(), partial))
}

#[pyclass]
struct Client {
    server: String,
    token: String,
}

#[pymethods]
impl Client {
    #[new]
    fn new(server: String, token: String) -> Self {
        Self { server, token }
    }

    #[pyo3(signature = (name, *, task, payload=None, cron=None, every=None, after=None, at=None, timezone=None, max_attempts=3))]
    #[allow(clippy::too_many_arguments)]
    fn schedule(
        &self,
        py: Python<'_>,
        name: String,
        task: String,
        payload: Option<PyObject>,
        cron: Option<String>,
        every: Option<String>,
        after: Option<String>,
        at: Option<String>,
        timezone: Option<String>,
        max_attempts: u32,
    ) -> PyResult<PyObject> {
        let payload = match payload {
            Some(value) => py_to_json(py, value)?,
            None => serde_json::Value::Null,
        };
        let body = serde_json::json!({
            "name": name,
            "task": task,
            "payload": payload,
            "cron": cron,
            "every": every,
            "after": after,
            "at": at,
            "timezone": timezone.unwrap_or_else(|| "UTC".to_string()),
            "max_attempts": max_attempts,
        });
        json_to_py(
            py,
            http_request(&self.server, &self.token, "POST", "/v1/timers", body)?,
        )
    }

    fn list(&self, py: Python<'_>) -> PyResult<PyObject> {
        json_to_py(
            py,
            http_request(
                &self.server,
                &self.token,
                "GET",
                "/v1/timers",
                serde_json::Value::Null,
            )?,
        )
    }

    fn status(&self, py: Python<'_>, name: String) -> PyResult<PyObject> {
        json_to_py(
            py,
            http_request(
                &self.server,
                &self.token,
                "GET",
                &format!("/v1/timers/{name}"),
                serde_json::Value::Null,
            )?,
        )
    }

    fn history(&self, py: Python<'_>, name: String) -> PyResult<PyObject> {
        json_to_py(
            py,
            http_request(
                &self.server,
                &self.token,
                "GET",
                &format!("/v1/timers/{name}/history"),
                serde_json::Value::Null,
            )?,
        )
    }
}

#[pyclass]
struct Worker {
    server: String,
    token: String,
    worker_id: String,
    tasks: Arc<Mutex<HashMap<String, Py<PyAny>>>>,
}

#[pymethods]
impl Worker {
    #[new]
    #[pyo3(signature = (server, token, worker_id=None))]
    fn new(server: String, token: String, worker_id: Option<String>) -> Self {
        Self {
            server,
            token,
            worker_id: worker_id.unwrap_or_else(|| {
                format!(
                    "worker_{}",
                    chrono::Utc::now().timestamp_nanos_opt().unwrap_or(0)
                )
            }),
            tasks: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn task(&self, py: Python<'_>, name: String) -> PyResult<PyObject> {
        let decorator = PyTaskDecorator {
            worker_tasks: Arc::clone(&self.tasks),
            name,
        };
        Py::new(py, decorator).map(|obj| obj.into_py(py))
    }

    fn register(&self) -> PyResult<()> {
        let tasks = self.task_names()?;
        let body = serde_json::json!({
            "worker_id": self.worker_id,
            "tasks": tasks,
            "lease_seconds": 30,
        });
        http_request(
            &self.server,
            &self.token,
            "POST",
            "/v1/workers/register",
            body,
        )?;
        Ok(())
    }

    #[pyo3(signature = (timeout=20.0))]
    fn run_once(&self, py: Python<'_>, timeout: f64) -> PyResult<bool> {
        let timeout = seconds_to_duration(timeout)?;
        self.register()?;
        let tasks = self.task_names()?;
        let body = serde_json::json!({
            "worker_id": self.worker_id,
            "tasks": tasks,
        });
        let response = py.allow_threads(|| {
            http_request_with_timeout(
                &self.server,
                &self.token,
                "POST",
                "/v1/workers/poll",
                body,
                timeout,
            )
        })?;
        if response.is_null() {
            return Ok(false);
        }
        self.execute_run(py, response)?;
        Ok(true)
    }

    fn run(&self, py: Python<'_>) -> PyResult<()> {
        loop {
            self.run_once(py, 20.0)?;
        }
    }
}

impl Worker {
    fn task_names(&self) -> PyResult<Vec<String>> {
        Ok(self
            .tasks
            .lock()
            .map_err(|_| PyRuntimeError::new_err("worker task lock poisoned"))?
            .keys()
            .cloned()
            .collect())
    }

    fn execute_run(&self, py: Python<'_>, run: serde_json::Value) -> PyResult<()> {
        let task = run
            .get("task")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let run_id = run
            .get("run_id")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        let fencing_token = run
            .get("fencing_token")
            .and_then(|v| v.as_u64())
            .unwrap_or(0);
        let payload = json_to_py(
            py,
            run.get("payload")
                .cloned()
                .unwrap_or(serde_json::Value::Null),
        )?;
        let callable = {
            let tasks = self
                .tasks
                .lock()
                .map_err(|_| PyRuntimeError::new_err("worker task lock poisoned"))?;
            tasks
                .get(&task)
                .map(|callable| callable.clone_ref(py))
                .ok_or_else(|| PyRuntimeError::new_err(format!("task {task} is not registered")))?
        };
        let result = callable.bind(py).call1((payload,));
        match result {
            Ok(_) => {
                let body = serde_json::json!({
                    "worker_id": self.worker_id,
                    "fencing_token": fencing_token,
                });
                http_request(
                    &self.server,
                    &self.token,
                    "POST",
                    &format!("/v1/runs/{run_id}/succeed"),
                    body,
                )?;
            }
            Err(err) => {
                let error_text = err.to_string();
                let body = serde_json::json!({
                    "worker_id": self.worker_id,
                    "fencing_token": fencing_token,
                    "error": error_text,
                });
                http_request(
                    &self.server,
                    &self.token,
                    "POST",
                    &format!("/v1/runs/{run_id}/fail"),
                    body,
                )?;
            }
        }
        Ok(())
    }
}

#[pyclass]
struct PyTaskDecorator {
    worker_tasks: Arc<Mutex<HashMap<String, Py<PyAny>>>>,
    name: String,
}

#[pymethods]
impl PyTaskDecorator {
    fn __call__(&mut self, callable: Py<PyAny>) -> PyResult<Py<PyAny>> {
        Python::with_gil(|py| {
            self.worker_tasks
                .lock()
                .map_err(|_| PyRuntimeError::new_err("worker task lock poisoned"))?
                .insert(self.name.clone(), callable.clone_ref(py));
            Ok(callable)
        })
    }
}

#[pymodule]
fn kron(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(schedule, m)?)?;
    m.add_function(wrap_pyfunction!(start, m)?)?;
    m.add_function(wrap_pyfunction!(astart, m)?)?;
    m.add_function(wrap_pyfunction!(shutdown, m)?)?;
    m.add_function(wrap_pyfunction!(ashutdown, m)?)?;
    m.add_function(wrap_pyfunction!(status, m)?)?;
    m.add_function(wrap_pyfunction!(astatus, m)?)?;
    m.add_function(wrap_pyfunction!(list, m)?)?;
    m.add_function(wrap_pyfunction!(alist, m)?)?;
    m.add_class::<Client>()?;
    m.add_class::<Worker>()?;
    Ok(())
}

fn normalize_server(server: &str) -> String {
    server
        .trim()
        .trim_start_matches("http://")
        .trim_end_matches('/')
        .to_string()
}

fn http_request(
    server: &str,
    token: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
) -> PyResult<serde_json::Value> {
    http_request_with_timeout(server, token, method, path, body, DEFAULT_HTTP_TIMEOUT)
}

fn http_request_with_timeout(
    server: &str,
    token: &str,
    method: &str,
    path: &str,
    body: serde_json::Value,
    timeout: StdDuration,
) -> PyResult<serde_json::Value> {
    let server = normalize_server(server);
    let body = if body.is_null() {
        String::new()
    } else {
        serde_json::to_string(&body).map_err(|err| PyRuntimeError::new_err(err.to_string()))?
    };
    let addr = server
        .to_socket_addrs()
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?
        .next()
        .ok_or_else(|| PyRuntimeError::new_err(format!("could not resolve server {server}")))?;
    let mut stream = TcpStream::connect_timeout(&addr, timeout)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    write!(
        stream,
        "{method} {path} HTTP/1.1\r\nHost: {server}\r\nAuthorization: Bearer {token}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    )
    .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    let (head, body) = response
        .split_once("\r\n\r\n")
        .ok_or_else(|| PyRuntimeError::new_err("invalid HTTP response"))?;
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| PyRuntimeError::new_err("invalid HTTP status line"))?;
    let parsed: serde_json::Value =
        serde_json::from_str(body).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    if !(200..300).contains(&status) {
        let message = parsed
            .get("error")
            .or_else(|| parsed.get("message"))
            .and_then(|value| value.as_str())
            .unwrap_or("HTTP request failed");
        return Err(PyRuntimeError::new_err(format!(
            "HTTP {status} from {server}: {message}"
        )));
    }
    Ok(parsed)
}

fn py_to_json(py: Python<'_>, value: PyObject) -> PyResult<serde_json::Value> {
    let json = py.import_bound("json")?;
    let encoded: String = json.call_method1("dumps", (value,))?.extract()?;
    serde_json::from_str(&encoded).map_err(|err| PyValueError::new_err(err.to_string()))
}

fn json_to_py(py: Python<'_>, value: serde_json::Value) -> PyResult<PyObject> {
    let json = py.import_bound("json")?;
    let encoded =
        serde_json::to_string(&value).map_err(|err| PyRuntimeError::new_err(err.to_string()))?;
    Ok(json.call_method1("loads", (encoded,))?.unbind().into())
}

fn apply_registration(engine: &Arc<Engine>, registration: PendingRegistration) -> PyResult<()> {
    engine
        .schedule(
            registration.name,
            registration.schedule,
            Arc::new(PyTimerFn::new(
                registration.callable,
                registration.function_name,
            )),
            Some(registration.retry),
            Some(registration.timezone),
        )
        .map_err(map_kron_error)
}

fn parse_schedule(kwargs: &Bound<'_, PyDict>) -> PyResult<Schedule> {
    let cron = get_optional_string(kwargs, "cron")?;
    let every = get_optional_string(kwargs, "every")?;
    let after = get_optional_string(kwargs, "after")?;
    let at = kwargs.get_item("at")?;

    let selected = [
        cron.is_some(),
        every.is_some(),
        after.is_some(),
        at.is_some(),
    ]
    .into_iter()
    .filter(|selected| *selected)
    .count();
    if selected != 1 {
        return Err(PyValueError::new_err(
            "schedule() requires exactly one of cron, every, after, at",
        ));
    }

    if let Some(expr) = cron {
        return Ok(Schedule::Cron { expr });
    }
    if let Some(value) = every {
        let seconds = parse_duration_str(&value).map_err(map_kron_error)?;
        return Ok(Schedule::Every { seconds });
    }
    if let Some(value) = after {
        let seconds = parse_duration_str(&value).map_err(map_kron_error)?;
        return Ok(Schedule::After {
            seconds,
            registered_at: Utc::now(),
        });
    }

    let at = at.expect("checked selected at above");
    if let Ok(dt) = at.extract::<DateTime<Utc>>() {
        return Ok(Schedule::At { at: dt });
    }
    if let Ok(value) = at.extract::<String>() {
        let parsed = DateTime::parse_from_rfc3339(&value)
            .map_err(|err| PyValueError::new_err(format!("invalid at datetime: {err}")))?
            .with_timezone(&Utc);
        return Ok(Schedule::At { at: parsed });
    }

    let type_name = at.get_type().name()?.to_string();
    Err(PyValueError::new_err(format!(
        "at must be timezone-aware datetime or RFC3339 string, got {type_name}"
    )))
}

fn get_optional_string(kwargs: &Bound<'_, PyDict>, key: &str) -> PyResult<Option<String>> {
    kwargs
        .get_item(key)?
        .map(|value| value.extract::<String>())
        .transpose()
}

fn resolve_data_dir(py: Python<'_>, explicit: Option<String>) -> PyResult<PathBuf> {
    if let Some(path) = explicit {
        return Ok(PathBuf::from(path));
    }
    if let Ok(path) = env::var("KRON_HOME") {
        return Ok(PathBuf::from(path));
    }

    let sys = py.import_bound("sys")?;
    if let Ok(argv) = sys.getattr("argv") {
        if let Ok(argv) = argv.extract::<Vec<String>>() {
            if let Some(entry) = argv.first() {
                let entry = PathBuf::from(entry);
                if let Some(parent) = entry.parent() {
                    if !parent.as_os_str().is_empty() {
                        return Ok(parent.join(".kron"));
                    }
                }
            }
        }
    }

    if let Ok(home) = env::var("HOME") {
        return Ok(PathBuf::from(home).join(".kron"));
    }
    Ok(PathBuf::from(".kron"))
}

fn summary_to_dict(py: Python<'_>, summary: TimerSummary) -> PyResult<PyObject> {
    let dict = PyDict::new_bound(py);
    dict.set_item("id", summary.id.as_str())?;
    dict.set_item("state", state_to_str(&summary.state))?;
    dict.set_item("fn_name", summary.fn_name)?;
    dict.set_item("last_run_at", summary.last_run_at.map(|dt| dt.to_rfc3339()))?;
    dict.set_item("last_duration_ms", summary.last_duration_ms)?;
    dict.set_item("last_status", summary.last_status)?;
    dict.set_item("next_run_at", summary.next_run_at.map(|dt| dt.to_rfc3339()))?;
    dict.set_item("retries_last_7d", summary.retries_last_7d)?;
    Ok(dict.unbind().into())
}

fn state_to_str(state: &TimerState) -> &'static str {
    match state {
        TimerState::Scheduled => "scheduled",
        TimerState::Running => "running",
        TimerState::Retrying => "retrying",
        TimerState::Dead => "dead",
        TimerState::Orphaned => "orphaned",
        TimerState::Paused => "paused",
        TimerState::Cancelled => "cancelled",
    }
}

fn seconds_to_duration(seconds: f64) -> PyResult<StdDuration> {
    if !seconds.is_finite() || seconds < 0.0 {
        return Err(PyValueError::new_err(
            "timeout must be a non-negative number",
        ));
    }
    Ok(StdDuration::from_secs_f64(seconds))
}

fn map_kron_error(error: KronError) -> PyErr {
    match error {
        KronError::InvalidCron(_) | KronError::InvalidTimezone(_) => {
            PyValueError::new_err(error.to_string())
        }
        KronError::AlreadyStarted
        | KronError::AlreadyStopped
        | KronError::ShutdownTimeout { .. }
        | KronError::DataDirLocked { .. }
        | KronError::TimerNotFound(_)
        | KronError::LogIo(_)
        | KronError::LogSerde(_)
        | KronError::CorruptLog { .. }
        | KronError::InvalidSnapshot(_)
        | KronError::IpcUnavailable(_) => {
            let mut message = error.to_string();
            if let KronError::DataDirLocked { path } = &error {
                if let Some(parent) = std::path::Path::new(path).parent() {
                    message.push_str(&format!(
                        "; another writer may be active; socket: {}",
                        ipc::socket_path(parent).display()
                    ));
                }
            }
            PyRuntimeError::new_err(message)
        }
    }
}
