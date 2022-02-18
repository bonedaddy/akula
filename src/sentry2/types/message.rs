use super::{header::BlockHeaders, HeaderRequest, PeerId};
use crate::{
    models::H256,
    sentry2::types::{GetBlockHeaders, GetBlockHeadersParams, NewBlock, NewBlockHashes},
};
use ethereum_interfaces::sentry as grpc_sentry;
use rlp_derive::{RlpDecodableWrapper, RlpEncodableWrapper};

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

impl const From<ethereum_interfaces::sentry::MessageId> for MessageId {
    fn from(msg: ethereum_interfaces::sentry::MessageId) -> Self {
        match msg {
            grpc_sentry::MessageId::Status66 => MessageId::Status,
            grpc_sentry::MessageId::NewBlockHashes66 => MessageId::NewBlockHashes,
            grpc_sentry::MessageId::Transactions66 => MessageId::Transactions,
            grpc_sentry::MessageId::GetBlockHeaders66 => MessageId::GetBlockHeaders,
            grpc_sentry::MessageId::BlockHeaders66 => MessageId::BlockHeaders,
            grpc_sentry::MessageId::GetBlockBodies66 => MessageId::GetBlockBodies,
            grpc_sentry::MessageId::BlockBodies66 => MessageId::BlockBodies,
            grpc_sentry::MessageId::NewBlock66 => MessageId::NewBlock,
            grpc_sentry::MessageId::NewPooledTransactionHashes66 => {
                MessageId::NewPooledTransactionHashes
            }
            grpc_sentry::MessageId::GetPooledTransactions66 => MessageId::GetPooledTransactions,
            grpc_sentry::MessageId::PooledTransactions66 => MessageId::PooledTransactions,
            grpc_sentry::MessageId::GetNodeData66 => MessageId::GetNodeData,
            grpc_sentry::MessageId::NodeData66 => MessageId::NodeData,
            grpc_sentry::MessageId::GetReceipts66 => MessageId::GetReceipts,
            grpc_sentry::MessageId::Receipts66 => MessageId::Receipts,
            _ => unreachable!(),
        }
    }
}

impl const From<MessageId> for ethereum_interfaces::sentry::MessageId {
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

#[derive(Debug, Clone, PartialEq)]
pub enum Message {
    NewBlockHashes(NewBlockHashes),
    GetBlockHeaders(GetBlockHeaders),
    BlockHeaders(BlockHeaders),
    NewBlock(Box<NewBlock>),
    NewPooledTransactionHashes(NewPooledTransactionHashes),
}

impl From<HeaderRequest> for Message {
    fn from(req: HeaderRequest) -> Self {
        Message::GetBlockHeaders(GetBlockHeaders {
            request_id: fastrand::u32(..) as u64,
            params: GetBlockHeadersParams {
                start: req.start,
                limit: req.limit,
                skip: req.skip,
                reverse: if req.reverse { 1 } else { 0 },
            },
        })
    }
}

impl Message {
    pub const fn id(&self) -> MessageId {
        match self {
            Self::NewBlockHashes(_) => MessageId::NewBlockHashes,
            Self::GetBlockHeaders(_) => MessageId::GetBlockHeaders,
            Self::BlockHeaders(_) => MessageId::BlockHeaders,
            Self::NewBlock(_) => MessageId::NewBlock,
            Self::NewPooledTransactionHashes(_) => MessageId::NewPooledTransactionHashes,
        }
    }
}

impl rlp::Encodable for Message {
    fn rlp_append(&self, s: &mut rlp::RlpStream) {
        match self {
            Self::NewBlockHashes(v) => rlp::Encodable::rlp_append(v, s),
            Self::GetBlockHeaders(v) => rlp::Encodable::rlp_append(v, s),
            Self::BlockHeaders(v) => rlp::Encodable::rlp_append(v, s),
            Self::NewBlock(v) => rlp::Encodable::rlp_append(v, s),
            Self::NewPooledTransactionHashes(v) => rlp::Encodable::rlp_append(v, s),
        }
    }
}
