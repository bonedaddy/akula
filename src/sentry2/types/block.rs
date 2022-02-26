use crate::models::{Block, BlockNumber, H256};
use rlp_derive::*;

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockBodies {
    pub request_id: u64,
    pub hashes: Vec<H256>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum BlockId {
    Hash(H256),
    Number(BlockNumber),
}

impl const From<BlockNumber> for BlockId {
    #[inline(always)]
    fn from(number: BlockNumber) -> Self {
        BlockId::Number(number)
    }
}

impl const From<H256> for BlockId {
    #[inline(always)]
    fn from(hash: H256) -> Self {
        BlockId::Hash(hash)
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHashAndNumber {
    pub hash: H256,
    pub number: BlockNumber,
}

impl BlockHashAndNumber {
    #[inline(always)]
    pub const fn new(hash: H256, number: BlockNumber) -> Self {
        Self { hash, number }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodableWrapper, RlpDecodableWrapper)]
pub struct NewBlockHashes(pub Vec<BlockHashAndNumber>);

impl NewBlockHashes {
    #[inline(always)]
    fn new(block_hashes: Vec<(H256, BlockNumber)>) -> Self {
        Self(
            block_hashes
                .into_iter()
                .map(|(hash, number)| BlockHashAndNumber::new(hash, number))
                .collect::<Vec<_>>(),
        )
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct NewBlock {
    pub block: Block,
    pub total_difficulty: u128,
}

impl NewBlock {
    #[inline(always)]
    pub const fn new(block: Block, total_difficulty: u128) -> Self {
        Self {
            block,
            total_difficulty,
        }
    }
}
