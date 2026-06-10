use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::error::KronError;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuditRecord {
    pub seq: u64,
    pub ts: DateTime<Utc>,
    pub node_id: String,
    pub action: String,
    pub outcome: String,
    pub status: u16,
    pub actor: String,
    pub role: String,
    pub tenant_id: Option<String>,
    pub reason: Option<String>,
    pub prev_hash: String,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditState {
    pub seq: u64,
    pub hash: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditVerification {
    pub records: u64,
}

pub fn audit_path(data_dir: &Path) -> PathBuf {
    data_dir.join("kron.audit.jsonl")
}

pub fn compute_hash(record: &AuditRecord) -> String {
    let canonical = serde_json::json!({
        "prev_hash": record.prev_hash,
        "seq": record.seq,
        "ts": record.ts.to_rfc3339(),
        "node_id": record.node_id,
        "actor": record.actor,
        "role": record.role,
        "tenant_id": record.tenant_id,
        "action": record.action,
        "outcome": record.outcome,
        "status": record.status,
        "reason": record.reason,
    });
    let mut hasher = Sha256::new();
    hasher.update(
        serde_json::to_string(&canonical)
            .expect("audit hash canonical json is serializable")
            .as_bytes(),
    );
    format!("{:x}", hasher.finalize())
}

pub fn read_last_state(data_dir: &Path) -> Result<AuditState, KronError> {
    let path = audit_path(data_dir);
    if !path.exists() {
        return Ok(AuditState {
            seq: 0,
            hash: String::new(),
        });
    }
    verify(data_dir).map_err(KronError::IpcUnavailable)?;
    read_last_state_unverified(data_dir)
}

pub fn read_last_state_unverified(data_dir: &Path) -> Result<AuditState, KronError> {
    let path = audit_path(data_dir);
    if !path.exists() {
        return Ok(AuditState {
            seq: 0,
            hash: String::new(),
        });
    }
    let file = File::open(path)?;
    let reader = BufReader::new(file);
    let lines = reader.lines().collect::<Result<Vec<_>, _>>()?;
    let last_non_empty = lines.iter().rposition(|line| !line.trim().is_empty());
    let mut state = AuditState {
        seq: 0,
        hash: String::new(),
    };
    for (line_no, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: AuditRecord = match serde_json::from_str(line) {
            Ok(record) => record,
            Err(err) if Some(line_no) == last_non_empty && err.is_eof() => break,
            Err(err) => return Err(err.into()),
        };
        state.seq = record.seq;
        state.hash = record.hash;
    }
    Ok(state)
}

pub fn verify(data_dir: &Path) -> Result<AuditVerification, String> {
    let path = audit_path(data_dir);
    if !path.exists() {
        return Ok(AuditVerification { records: 0 });
    }
    let file = File::open(path).map_err(|err| err.to_string())?;
    let reader = BufReader::new(file);
    let lines = reader
        .lines()
        .collect::<Result<Vec<_>, _>>()
        .map_err(|err| err.to_string())?;
    let last_non_empty = lines.iter().rposition(|line| !line.trim().is_empty());
    let mut expected_prev = String::new();
    let mut expected_seq = 1u64;
    let mut records = 0u64;
    for (line_no, line) in lines.iter().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let record: AuditRecord = match serde_json::from_str(line) {
            Ok(record) => record,
            Err(err) if Some(line_no) == last_non_empty && err.is_eof() => break,
            Err(err) => {
                return Err(format!(
                    "TAMPERED: line={} invalid json: {}",
                    line_no + 1,
                    err
                ));
            }
        };
        if record.seq != expected_seq {
            return Err(format!(
                "TAMPERED: record seq={} sequence mismatch, expected {}",
                record.seq, expected_seq
            ));
        }
        if record.prev_hash != expected_prev {
            return Err(format!(
                "TAMPERED: record seq={} prev_hash mismatch",
                record.seq
            ));
        }
        let computed = compute_hash(&record);
        if record.hash != computed {
            return Err(format!("TAMPERED: record seq={} hash mismatch", record.seq));
        }
        expected_prev = record.hash;
        expected_seq += 1;
        records += 1;
    }
    Ok(AuditVerification { records })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn record(seq: u64, prev_hash: String, action: &str) -> AuditRecord {
        let mut record = AuditRecord {
            seq,
            ts: DateTime::parse_from_rfc3339("2026-06-10T10:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            node_id: "n1".to_string(),
            action: action.to_string(),
            outcome: "ok".to_string(),
            status: 200,
            actor: "worker".to_string(),
            role: "worker".to_string(),
            tenant_id: Some("tenant-a".to_string()),
            reason: None,
            prev_hash,
            hash: String::new(),
        };
        record.hash = compute_hash(&record);
        record
    }

    #[test]
    fn verifies_hash_chain() {
        let dir = TempDir::new().unwrap();
        let first = record(1, String::new(), "worker.poll");
        let second = record(2, first.hash.clone(), "run.succeed");
        std::fs::write(
            audit_path(dir.path()),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).unwrap(),
                serde_json::to_string(&second).unwrap()
            ),
        )
        .unwrap();
        let result = verify(dir.path()).unwrap();
        assert_eq!(result.records, 2);
        let state = read_last_state(dir.path()).unwrap();
        assert_eq!(state.seq, 2);
        assert_eq!(state.hash, second.hash);
    }

    #[test]
    fn rejects_tampered_hash_chain() {
        let dir = TempDir::new().unwrap();
        let mut first = record(1, String::new(), "worker.poll");
        first.actor = "attacker".to_string();
        std::fs::write(
            audit_path(dir.path()),
            format!("{}\n", serde_json::to_string(&first).unwrap()),
        )
        .unwrap();
        let err = verify(dir.path()).unwrap_err();
        assert!(err.contains("hash mismatch"));
    }

    #[test]
    fn rejects_missing_sequence_number() {
        let dir = TempDir::new().unwrap();
        let first = record(1, String::new(), "worker.poll");
        let second = record(3, first.hash.clone(), "run.succeed");
        std::fs::write(
            audit_path(dir.path()),
            format!(
                "{}\n{}\n",
                serde_json::to_string(&first).unwrap(),
                serde_json::to_string(&second).unwrap()
            ),
        )
        .unwrap();
        let err = verify(dir.path()).unwrap_err();
        assert!(err.contains("sequence mismatch"));
    }

    #[test]
    fn ignores_truncated_final_audit_tail() {
        let dir = TempDir::new().unwrap();
        let first = record(1, String::new(), "worker.poll");
        std::fs::write(
            audit_path(dir.path()),
            format!("{}\n{{\"seq\":", serde_json::to_string(&first).unwrap()),
        )
        .unwrap();
        let result = verify(dir.path()).unwrap();
        assert_eq!(result.records, 1);
        let state = read_last_state(dir.path()).unwrap();
        assert_eq!(state.seq, 1);
        assert_eq!(state.hash, first.hash);
    }
}
