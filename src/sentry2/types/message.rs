use super::{header::BlockHeaders, GetBlockBodies, HeaderRequest, PeerId};
use crate::{
    models::{BlockBody, H256},
    sentry2::{
        types::{GetBlockHeaders, GetBlockHeadersParams, NewBlock, NewBlockHashes},
        CHUNK_SIZE,
    },
};
use arrayvec::ArrayVec;
use ethereum_interfaces::sentry as grpc_sentry;
use rlp_derive::{RlpDecodable, RlpDecodableWrapper, RlpEncodable, RlpEncodableWrapper};

#[derive(Debug, Clone, PartialEq)]
pub struct InboundMessage {
    pub msg: Message,
    pub peer_id: PeerId,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum::EnumIter)]
pub enum MessageId {
    Status = 0,
    NewBlockHashes = 1,
    NewBlock = 2,
    Transactions = 3,
    NewPooledTransactionHashes = 4,
    GetBlockHeaders = 5,
    GetBlockBodies = 6,
    GetNodeData = 7,
    GetReceipts = 8,
    GetPooledTransactions = 9,
    BlockHeaders = 10,
    BlockBodies = 11,
    NodeData = 12,
    Receipts = 13,
    PooledTransactions = 14,
}

#[derive(Debug)]
pub struct InvalidMessageId;

impl std::error::Error for InvalidMessageId {}

impl std::fmt::Display for InvalidMessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Invalid message id")
    }
}

impl const TryFrom<ethereum_interfaces::sentry::MessageId> for MessageId {
    type Error = InvalidMessageId;

    #[inline(always)]
    fn try_from(msg: ethereum_interfaces::sentry::MessageId) -> Result<Self, Self::Error> {
        match msg {
            grpc_sentry::MessageId::Status66 => Ok(MessageId::Status),
            grpc_sentry::MessageId::NewBlockHashes66 => Ok(MessageId::NewBlockHashes),
            grpc_sentry::MessageId::Transactions66 => Ok(MessageId::Transactions),
            grpc_sentry::MessageId::GetBlockHeaders66 => Ok(MessageId::GetBlockHeaders),
            grpc_sentry::MessageId::BlockHeaders66 => Ok(MessageId::BlockHeaders),
            grpc_sentry::MessageId::GetBlockBodies66 => Ok(MessageId::GetBlockBodies),
            grpc_sentry::MessageId::BlockBodies66 => Ok(MessageId::BlockBodies),
            grpc_sentry::MessageId::NewBlock66 => Ok(MessageId::NewBlock),
            grpc_sentry::MessageId::NewPooledTransactionHashes66 => {
                Ok(MessageId::NewPooledTransactionHashes)
            }
            grpc_sentry::MessageId::GetPooledTransactions66 => Ok(MessageId::GetPooledTransactions),
            grpc_sentry::MessageId::PooledTransactions66 => Ok(MessageId::PooledTransactions),
            grpc_sentry::MessageId::GetNodeData66 => Ok(MessageId::GetNodeData),
            grpc_sentry::MessageId::NodeData66 => Ok(MessageId::NodeData),
            grpc_sentry::MessageId::GetReceipts66 => Ok(MessageId::GetReceipts),
            grpc_sentry::MessageId::Receipts66 => Ok(MessageId::Receipts),
            _ => Err(InvalidMessageId),
        }
    }
}

impl const From<MessageId> for ethereum_interfaces::sentry::MessageId {
    #[inline(always)]
    fn from(id: MessageId) -> Self {
        match id {
            MessageId::Status => ethereum_interfaces::sentry::MessageId::Status66,
            MessageId::NewBlockHashes => ethereum_interfaces::sentry::MessageId::NewBlockHashes66,
            MessageId::Transactions => ethereum_interfaces::sentry::MessageId::Transactions66,
            MessageId::GetBlockHeaders => ethereum_interfaces::sentry::MessageId::GetBlockHeaders66,
            MessageId::BlockHeaders => ethereum_interfaces::sentry::MessageId::BlockHeaders66,
            MessageId::GetBlockBodies => ethereum_interfaces::sentry::MessageId::GetBlockBodies66,
            MessageId::BlockBodies => ethereum_interfaces::sentry::MessageId::BlockBodies66,
            MessageId::NewBlock => ethereum_interfaces::sentry::MessageId::NewBlock66,
            MessageId::NewPooledTransactionHashes => {
                ethereum_interfaces::sentry::MessageId::NewPooledTransactionHashes66
            }
            MessageId::GetPooledTransactions => {
                ethereum_interfaces::sentry::MessageId::GetPooledTransactions66
            }
            MessageId::PooledTransactions => {
                ethereum_interfaces::sentry::MessageId::PooledTransactions66
            }
            MessageId::GetNodeData => ethereum_interfaces::sentry::MessageId::GetNodeData66,
            MessageId::NodeData => ethereum_interfaces::sentry::MessageId::NodeData66,
            MessageId::GetReceipts => ethereum_interfaces::sentry::MessageId::GetReceipts66,
            MessageId::Receipts => ethereum_interfaces::sentry::MessageId::Receipts66,
        }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewPooledTransactionHashes(pub Vec<H256>);

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockBodies {
    pub request_id: u64,
    pub bodies: Vec<BlockBody>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    NewBlockHashes(NewBlockHashes),
    GetBlockHeaders(GetBlockHeaders),
    GetBlockBodies(GetBlockBodies),
    BlockBodies(BlockBodies),
    BlockHeaders(BlockHeaders),
    NewBlock(Box<NewBlock>),
    NewPooledTransactionHashes(NewPooledTransactionHashes),
}

impl From<HeaderRequest> for Message {
    #[inline(always)]
    fn from(req: HeaderRequest) -> Self {
        Message::GetBlockHeaders(GetBlockHeaders {
            request_id: fastrand::u64(..),
            params: GetBlockHeadersParams {
                start: req.start,
                limit: req.limit,
                skip: req.skip,
                reverse: if req.reverse { 1 } else { 0 },
            },
        })
    }
}

impl From<Vec<H256>> for Message {
    #[inline(always)]
    fn from(req: Vec<H256>) -> Self {
        Message::GetBlockBodies(GetBlockBodies {
            request_id: fastrand::u64(..),
            hashes: req,
        })
    }
}

impl From<ArrayVec<H256, CHUNK_SIZE>> for Message {
    #[inline(always)]
    fn from(req: ArrayVec<H256, CHUNK_SIZE>) -> Self {
        Message::GetBlockBodies(GetBlockBodies {
            request_id: fastrand::u64(..),
            hashes: req.to_vec(),
        })
    }
}

impl Message {
    #[inline(always)]
    pub const fn id(&self) -> MessageId {
        match self {
            Self::NewBlockHashes(_) => MessageId::NewBlockHashes,
            Self::GetBlockHeaders(_) => MessageId::GetBlockHeaders,
            Self::BlockHeaders(_) => MessageId::BlockHeaders,
            Self::NewBlock(_) => MessageId::NewBlock,
            Self::GetBlockBodies(_) => MessageId::GetBlockBodies,
            Self::BlockBodies(_) => MessageId::BlockBodies,
            Self::NewPooledTransactionHashes(_) => MessageId::NewPooledTransactionHashes,
        }
    }
}

impl rlp::Encodable for Message {
    #[inline(always)]
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        match self {
            Self::NewBlockHashes(v) => rlp::Encodable::rlp_append(v, s),
            Self::GetBlockHeaders(v) => rlp::Encodable::rlp_append(v, s),
            Self::BlockHeaders(v) => rlp::Encodable::rlp_append(v, s),
            Self::NewBlock(v) => rlp::Encodable::rlp_append(v, s),
            Self::NewPooledTransactionHashes(v) => rlp::Encodable::rlp_append(v, s),
            Self::GetBlockBodies(v) => rlp::Encodable::rlp_append(v, s),
            Self::BlockBodies(v) => rlp::Encodable::rlp_append(v, s),
        }
    }
}
