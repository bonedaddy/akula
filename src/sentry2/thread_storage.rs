use super::types::HeaderRequest;
use crate::models::{BlockNumber, H256};
use lockfree::prelude::{Map, Set};
use std::sync::{
    atomic::{AtomicBool, AtomicU64},
    Arc,
};

pub type HashMap<K, V> = Arc<Map<K, V>>;
pub type HashSet<K> = Arc<Set<K>>;

#[derive(Default)]
pub struct Storage {
    pub chain_height: Arc<AtomicU64>,
    pub seen_announces: HashMap<H256, u64>,
    pub parents_table: HashMap<H256, H256>,
    pub valid_chain: HashSet<H256>,
    pub pending_requests: HashMap<BlockNumber, HeaderRequest>,
    pub done: Arc<AtomicBool>,
}

impl Clone for Storage {
    fn clone(&self) -> Self {
        Self {
            chain_height: self.chain_height.clone(),
            seen_announces: self.seen_announces.clone(),
            parents_table: self.parents_table.clone(),
            valid_chain: self.valid_chain.clone(),
            pending_requests: self.pending_requests.clone(),
            done: self.done.clone(),
        }
    }
}

impl Storage {
    pub fn new() -> Self {
        Self {
            chain_height: Arc::new(AtomicU64::new(0)),
            seen_announces: Arc::new(Map::new()),
            parents_table: Arc::new(Map::new()),
            valid_chain: Arc::new(Set::new()),
            pending_requests: Arc::new(Map::new()),
            done: Arc::new(AtomicBool::new(false)),
        }
    }
}
