use crate::{
    models::{BlockHeader, H256},
    sentry2::Coordinator,
};
use hashbrown::{HashMap, HashSet};
use rayon::slice::ParallelSliceMut;
use std::{
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

const CHUNK_SIZE: usize = 512;
const INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownloader<'a> {
    pub coordinator: Arc<Coordinator>,
    pub bad_headers: HashSet<H256>,
    pub chain_tip: AtomicU64,
    _phantom: PhantomData<&'a ()>,
}

impl<'a> HeaderDownloader<'a> {
    pub fn new(
        coordinator: Arc<Coordinator>,
        bad_headers: HashSet<H256>,
        chain_tip: AtomicU64,
        _phantom: PhantomData<&'a ()>,
    ) -> Self {
        Self {
            coordinator,
            bad_headers,
            chain_tip,
            _phantom,
        }
    }

    pub fn build_segments(&self, headers: Vec<BlockHeader>) {
        headers.par_sort_unstable_by(|a, b| b.number.0.cmp(&a.number.0));
        let mut segments: Vec<ChainSegment> = Vec::new();
        // a table mapped by hash to the index of the chain segment.
        let mut segments_table: HashMap<H256, usize> = HashMap::new();
        // a table mapped by parent hash to their childrens.
        let mut child_table: HashMap<H256, Vec<BlockHeader>> = HashMap::new();
        // a table mapped by hash.
        let mut dedup_table: HashSet<H256> = HashSet::new();

        headers.into_iter().enumerate().for_each(|(i, header)| {
            if self.bad_headers.contains(&header.hash) || dedup_table.contains(&header.hash) {
                // FIXME: fix me.
                return;
            }

            dedup_table.insert(header.hash);
            let childs = child_table.get(&header.hash());

            childs.into_iter().map(|child| {
                if !child_parent_verifier(&child, &header) {
                    // FIXME: Write me please.
                    unreachable!();
                };
            });

            if childs.len() == 1 {
                // Single child, extract segment_id.
                let segment_id = segments_table.get(&childs[0].hash()).copied().unwrap();
            } else {
                // No children or more than one child, create a new segment.
                let segment_id = segments.len();
            }
        });
    }
}

pub fn child_parent_verifier(child: &BlockHeader, parent: &BlockHeader) -> bool {
    // FIXME: write me.
    false
}

pub struct SegmentBuilder {
    // chain segments by latest block hash
    pub anchors: HashMap<H256, ChainSegment>,
}

impl SegmentBuilder {
    pub fn find_anchor(&self, hash: &H256) -> Option<ChainSegment> {
        self.anchors.get(hash).cloned()
    }
}

pub struct ChainSegment {
    pub headers: Vec<BlockHeader>,
}

impl ChainSegment {
    pub fn build(mut headers: Vec<BlockHeader>) {}
}
