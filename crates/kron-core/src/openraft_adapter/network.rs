use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use crate::openraft_adapter::KronTypeConfig;

#[derive(Clone)]
pub struct KronRaftNetworkFactory {
    token: String,
    client: reqwest::Client,
}

impl KronRaftNetworkFactory {
    pub fn new(token: String) -> Self {
        Self {
            token,
            client: reqwest::Client::new(),
        }
    }
}

pub struct KronRaftNetwork {
    target: u64,
    addr: String,
    token: String,
    client: reqwest::Client,
}

impl RaftNetworkFactory<KronTypeConfig> for KronRaftNetworkFactory {
    type Network = KronRaftNetwork;

    async fn new_client(&mut self, target: u64, node: &openraft::BasicNode) -> Self::Network {
        KronRaftNetwork {
            target,
            addr: node.addr.clone(),
            token: self.token.clone(),
            client: self.client.clone(),
        }
    }
}

impl RaftNetwork<KronTypeConfig> for KronRaftNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<KronTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>>
    {
        self.post("/__kron/raft/append_entries", &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<KronTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, openraft::BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        self.post("/__kron/raft/install_snapshot", &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, openraft::BasicNode, RaftError<u64>>> {
        self.post("/__kron/raft/vote", &rpc).await
    }
}

impl KronRaftNetwork {
    async fn post<T, R, E>(
        &self,
        path: &str,
        body: &T,
    ) -> Result<R, RPCError<u64, openraft::BasicNode, E>>
    where
        T: serde::Serialize + ?Sized,
        R: serde::de::DeserializeOwned,
        E: std::error::Error + 'static,
    {
        let url = format!("http://{}{}", self.addr, path);
        let response = self
            .client
            .post(url)
            .bearer_auth(&self.token)
            .json(body)
            .send()
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))?;
        if !response.status().is_success() {
            let err = std::io::Error::new(
                std::io::ErrorKind::ConnectionRefused,
                format!("raft peer {} returned {}", self.target, response.status()),
            );
            return Err(RPCError::Unreachable(Unreachable::new(&err)));
        }
        response
            .json::<R>()
            .await
            .map_err(|err| RPCError::Unreachable(Unreachable::new(&err)))
    }
}
