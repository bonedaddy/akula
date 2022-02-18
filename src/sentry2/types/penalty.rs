use crate::models::H256;
use ethereum_interfaces::sentry as grpc_sentry;

pub type PeerId = H256;

#[derive(Debug, Clone)]
pub enum PenaltyKind {
    BadBlock,
    DuplicateHeader,
    WrongChildBlockHeight,
    WrongChildDifficulty,
    InvalidSeal,
    TooFarFuture,
    TooFarPast,
}

impl Default for PenaltyKind {
    fn default() -> Self {
        PenaltyKind::BadBlock
    }
}

#[derive(Debug, Clone)]
pub struct Penalty {
    pub peer_id: PeerId,
    pub kind: PenaltyKind,
}

impl Penalty {
    pub const fn new(peer_id: PeerId, kind: PenaltyKind) -> Self {
        Self { peer_id, kind }
    }
}

impl const From<Penalty> for grpc_sentry::PenalizePeerRequest {
    fn from(penalty: Penalty) -> Self {
        grpc_sentry::PenalizePeerRequest {
            peer_id: Some(penalty.peer_id.into()),
            penalty: 0,
        }
    }
}
