use crate::{
    models::{BlockHeader, BlockNumber, H256},
    sentry2::types::*,
};
use rlp_derive::*;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct HeaderRequest {
    pub start: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: bool,
}

impl HeaderRequest {
    #[inline]
    pub const fn new(start: BlockId, limit: u64, skip: u64, reverse: bool) -> Self {
        Self {
            start,
            limit,
            skip,
            reverse,
        }
    }
}

impl const Default for HeaderRequest {
    #[inline(always)]
    fn default() -> Self {
        HeaderRequest {
            start: BlockId::Number(BlockNumber(0)),
            limit: 1024,
            skip: 0,
            reverse: false,
        }
    }
}

pub struct Announce {
    pub hash: H256,
    pub number: BlockNumber,
}

impl Announce {
    #[inline(always)]
    pub const fn new(hash: H256, number: BlockNumber) -> Self {
        Self { hash, number }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeaders {
    pub request_id: u64,
    pub params: GetBlockHeadersParams,
}

impl GetBlockHeaders {
    #[inline(always)]
    pub const fn new(request_id: u64, params: GetBlockHeadersParams) -> Self {
        Self { request_id, params }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct GetBlockHeadersParams {
    pub start: BlockId,
    pub limit: u64,
    pub skip: u64,
    pub reverse: u8,
}

impl GetBlockHeadersParams {
    #[inline(always)]
    pub const fn new(start: BlockId, limit: u64, skip: u64, reverse: u8) -> Self {
        Self {
            start,
            limit,
            skip,
            reverse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, RlpEncodable, RlpDecodable)]
pub struct BlockHeaders {
    pub request_id: u64,
    pub headers: Vec<BlockHeader>,
}

impl BlockHeaders {
    #[inline(always)]
    pub const fn new(request_id: u64, headers: Vec<BlockHeader>) -> Self {
        Self {
            request_id,
            headers,
        }
    }
}
