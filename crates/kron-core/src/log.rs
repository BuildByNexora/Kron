use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::KronError;
use crate::event::{Event, LogEntry};

/// Append-only log backed by a newline-delimited JSON file.
/// Each line is a `LogEntry` serialized with serde_json.
///
/// Design decisions:
/// - One line per entry (NDJSON): simple, grep-friendly, streamable.
/// - No compression in v0: observability trumps disk space.
/// - fsync on every write: durability over throughput (scheduler, not OLTP).
pub struct AppendOnlyLog {
    path: PathBuf,
    file: File,
}

impl AppendOnlyLog {
    /// Open or create the log at the given path.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, KronError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        #[cfg(unix)]
        let file = {
            use std::os::unix::fs::OpenOptionsExt;
            OpenOptions::new()
                .create(true)
                .append(true)
                .mode(0o600)
                .open(&path)?
        };
        #[cfg(not(unix))]
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        Ok(Self { path, file })
    }

    /// Append a single event to the log and fsync.
    pub fn append(&mut self, event: Event) -> Result<(), KronError> {
        let entry = LogEntry::new(event);
        let mut line = serde_json::to_string(&entry)?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn len(&self) -> Result<u64, KronError> {
        Ok(self.file.metadata()?.len())
    }

    pub fn is_empty(&self) -> Result<bool, KronError> {
        Ok(self.len()? == 0)
    }

    pub fn replay_from(&self, offset: u64) -> Result<Vec<LogEntry>, KronError> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(offset))?;
        replay_reader(BufReader::new(file), &self.path)
    }

    /// Replay the entire log, returning entries in order.
    /// A corrupt final line is treated as a crash-truncated tail and ignored.
    /// Corruption before the final line is fatal because silently skipping it
    /// could derive a false state.
    pub fn replay(&self) -> Result<Vec<LogEntry>, KronError> {
        let file = File::open(&self.path)?;
        replay_reader(BufReader::new(file), &self.path)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

fn replay_reader(reader: BufReader<File>, path: &Path) -> Result<Vec<LogEntry>, KronError> {
    let lines = reader.lines().collect::<Result<Vec<_>, _>>()?;
    let last_non_empty = lines.iter().rposition(|line| !line.trim().is_empty());

    let mut entries = Vec::new();
    for (line_no, line) in lines.iter().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        match serde_json::from_str::<LogEntry>(trimmed) {
            Ok(entry) => entries.push(entry),
            Err(e) => {
                if Some(line_no) == last_non_empty && e.is_eof() {
                    eprintln!(
                        "[kron] WARNING: ignoring truncated log tail at line {} in {:?}: {}",
                        line_no + 1,
                        path,
                        e
                    );
                    break;
                }
                return Err(KronError::CorruptLog {
                    path: path.display().to_string(),
                    line: line_no + 1,
                    source: e,
                });
            }
        }
    }

    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use std::fs::OpenOptions;
    use tempfile::TempDir;

    use crate::retry::RetryPolicy;
    use crate::schedule::Schedule;
    use crate::timer::{TimerId, TimerSpec};

    fn spec(id: &str) -> TimerSpec {
        TimerSpec {
            id: TimerId::new(id),
            schedule: Schedule::Every { seconds: 60 },
            retry: RetryPolicy::no_retry(),
            timezone: "UTC".to_string(),
            created_at: Utc::now(),
        }
    }

    #[test]
    fn append_and_replay_round_trips_events() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kron.aof");
        let mut log = AppendOnlyLog::open(&path).unwrap();
        log.append(Event::TimerCreated { spec: spec("a") }).unwrap();
        log.append(Event::TimerCreated { spec: spec("b") }).unwrap();

        let replayed = log.replay().unwrap();
        assert_eq!(replayed.len(), 2);
    }

    #[test]
    fn replay_ignores_truncated_final_line() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kron.aof");
        let mut log = AppendOnlyLog::open(&path).unwrap();
        log.append(Event::TimerCreated { spec: spec("a") }).unwrap();
        drop(log);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        write!(file, "{{\"type\":\"not complete\"").unwrap();
        drop(file);

        let log = AppendOnlyLog::open(&path).unwrap();
        let replayed = log.replay().unwrap();
        assert_eq!(replayed.len(), 1);
    }

    #[test]
    fn replay_rejects_corrupt_middle_line() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("kron.aof");
        let mut log = AppendOnlyLog::open(&path).unwrap();
        log.append(Event::TimerCreated { spec: spec("a") }).unwrap();
        drop(log);

        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        writeln!(file, "not json").unwrap();
        drop(file);

        let mut log = AppendOnlyLog::open(&path).unwrap();
        log.append(Event::TimerCreated { spec: spec("b") }).unwrap();

        let err = log.replay().unwrap_err();
        assert!(matches!(err, KronError::CorruptLog { line: 2, .. }));
    }
}
