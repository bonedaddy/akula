use crate::models::{Block, BlockNumber, H256};
use rlp_derive::*;
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BlockId {
    Hash(H256),
    Number(BlockNumber),
}

impl<T: Into<BlockNumber>> From<T> for BlockId {
    fn from(number: T) -> Self {
        BlockId::Number(number.into())
    }
}

impl From<H256> for BlockId {
    fn from(hash: H256) -> Self {
        BlockId::Hash(hash)
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: BlockNumber,
}

#[derive(Debug, Clone, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewBlockHashes(pub Vec<BlockHashAndNumber>);

impl NewBlockHashes {
    fn new(block_hashes: Vec<(H256, BlockNumber)>) -> Self {
        Self(
            block_hashes
                .into_iter()
                .map(|(hash, number)| BlockHashAndNumber { hash, number })
                .collect(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct NewBlock {
    pub block: Block,
    pub total_difficulty: u128,
}

impl NewBlock {
    pub fn new(block: Block, total_difficulty: u128) -> Self {
        Self {
            block,
            total_difficulty,
        }
    }
}
pub struct BodyRequest {}
