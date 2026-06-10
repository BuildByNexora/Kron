use std::io::Cursor;

use openraft::impls::{BasicNode, OneshotResponder, TokioRuntime};
use openraft::{declare_raft_types, Entry};
use serde::{Deserialize, Serialize};

use crate::cluster::RaftCommand;

pub mod file_store;
pub mod network;
pub mod runtime;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KronRaftRequest {
    pub command: RaftCommand,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KronRaftResponse {
    pub applied: bool,
}

declare_raft_types!(
    pub KronTypeConfig:
        D = KronRaftRequest,
        R = KronRaftResponse,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<KronTypeConfig>,
        SnapshotData = Cursor<Vec<u8>>,
        Responder = OneshotResponder<KronTypeConfig>,
        AsyncRuntime = TokioRuntime
);
