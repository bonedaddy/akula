use crate::{
    kv::mdbx::MdbxEnvironment,
    models::{BlockHeader, BlockNumber, H256},
    sentry2::{types::*, Coordinator, CoordinatorStream, SentryCoordinator},
};
use ethereum_interfaces::sentry as grpc_sentry;
use hashbrown::{HashMap, HashSet};
use hashlink::LinkedHashSet;
use mdbx::EnvironmentKind;
use rayon::slice::ParallelSliceMut;
use std::{
    collections::BinaryHeap,
    marker::PhantomData,
    sync::{atomic::AtomicU64, Arc},
    time::Duration,
};

const CHUNK_SIZE: usize = 512;
const INTERVAL: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct Anchor<'a> {
    pub links: Vec<&'a Box<Link<'a>>>,
    pub parent_hash: H256,
    pub block_height: BlockNumber,
    pub timestamp: u64,
    _phantom: PhantomData<&'a ()>,
}

impl PartialEq for Anchor<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.parent_hash == other.parent_hash
    }
}

impl PartialOrd for Anchor<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.timestamp == other.timestamp {
            Some(self.block_height.cmp(&other.block_height))
        } else {
            Some(self.timestamp.cmp(&other.timestamp))
        }
    }
}

/// Link is a chain of headers that are linked together.
/// For a given link, parent link can be found by doing lookup in the table
/// HeaderDownloader.links.get(&link.header.parent_hash).
/// Links encapsule block headers.
/// Links can be either persisted or not. Persisted links encapsule headers
/// that have already been saved to the database, but these links are still present to allow potential reorgs.

#[derive(Debug, Clone)]
pub struct Link<'a> {
    hash: H256,
    height: BlockNumber,
    next: Option<&'a Vec<Box<Link<'a>>>>,
    header: BlockHeader,
    preverified: bool,
    /// Whether the link comes from the database record.
    persisted: bool,
    _phantom: PhantomData<&'a ()>,
}

impl PartialEq for Link<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.hash == other.hash
    }
}

impl PartialOrd for Link<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        if self.persisted {
            Some(other.height.cmp(&self.height))
        } else {
            Some(self.height.cmp(&other.height))
        }
    }
}
pub struct LinkQueue<'a>(BinaryHeap<Box<Link<'a>>>);

pub struct AnchorQueue<'a>(BinaryHeap<Box<Anchor<'a>>>);

pub struct HeaderDownloader<'a> {
    pub coordinator: Arc<Coordinator>,
    pub bad_headers: HashSet<H256>,
    pub chain_tip: AtomicU64,
    pub anchors: HashMap<H256, Anchor<'a>>,
    pub links: HashMap<H256, Box<Link<'a>>>,
    pub seen_announces: LinkedHashSet<H256>,

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
            anchors: todo!(),
            links: todo!(),
            seen_announces: todo!(),
            _phantom,
        }
    }

    pub async fn runtime<E: EnvironmentKind>(
        &'_ mut self,
        db: &'_ MdbxEnvironment<E>,
    ) -> anyhow::Result<()> {
        let msg_ids = vec![
            grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32,
            grpc_sentry::MessageId::from(MessageId::NewBlock) as i32,
            grpc_sentry::MessageId::from(MessageId::NewBlockHashes) as i32,
        ];
        let stream = self.coordinator.recv(msg_ids).await?;

        Ok(())
    }

    pub async fn step(&'_ mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub async fn request_more(&'_ mut self) -> anyhow::Result<()> {
        Ok(())
    }

    pub fn build_segments(&self, mut headers: Vec<BlockHeader>) -> Option<Vec<ChainSegment>> {
        headers.par_sort_unstable_by(|a, b| b.number.0.cmp(&a.number.0));
        let mut segments_table: HashMap<H256, ChainSegment> = HashMap::new();
        // a table mapped by parent hash to their childrens.
        let mut childs_table: HashMap<H256, Vec<BlockHeader>> = HashMap::new();
        // a table mapped by hash.
        let mut dedup_table: HashSet<H256> = HashSet::new();

        for header in headers.into_iter() {
            let header_hash = header.hash();
            if self.bad_headers.contains(&header_hash) || dedup_table.contains(&header_hash) {
                return None;
            }

            dedup_table.insert(header_hash);
            let childs = childs_table
                .get(&header.hash())
                .map_or(Vec::new(), |v| v.clone());

            for child in childs.iter() {
                if !child_parent_valid(&child, &header) {
                    return None;
                }
            }

            let segment = if let 1 = childs.len() {
                segments_table.get(&header.hash()).cloned().unwrap()
            } else {
                segments_table.insert(header.hash(), ChainSegment { headers: childs });
                segments_table.get(&header.hash()).cloned().unwrap()
            };

            segments_table.insert(header.parent_hash, segment);
            childs_table
                .get_mut(&header.parent_hash)
                .get_or_insert(&mut Vec::new())
                .push(header);
        }
        Some(segments_table.values().cloned().collect())
    }

    pub fn find_anchors(&self, segment: &ChainSegment) -> Option<usize> {
        segment
            .headers
            .iter()
            .enumerate()
            .map(|(i, header)| {
                if self.anchors.get(&header.hash()).is_some() {
                    Some(i)
                } else {
                    None
                }
            })
            .find_map(|v| v)
    }

    pub fn find_link(&self, segment: &ChainSegment, start: usize) -> anyhow::Result<(bool, usize)> {
        if self
            .links
            .contains_key(&segment.headers.get(start).unwrap().hash())
        {
            // Duplicate.
            Ok((false, 0))
        } else {
            // Walk the segment from children towards parents
            // Check if the header can be attached to any links
            match segment
                .headers
                .clone()
                .iter()
                .skip(start)
                .enumerate()
                .find_map(|(i, header)| {
                    if self.links.contains_key(&header.parent_hash) {
                        Some(i)
                    } else {
                        None
                    }
                }) {
                Some(v) => Ok((true, v)),
                None => Ok((false, segment.headers.len())),
            }
        }
    }

    pub fn mark_preverified(&'a mut self, mut link: &mut Link<'a>) {
        if link.persisted {
            return;
        }
        link.preverified = true;
        link = self.links.get_mut(&link.hash).unwrap();
    }

    pub fn extend_up(&self, _: &ChainSegment, _: usize, _: usize) -> anyhow::Result<()> {
        todo!()
    }

    pub fn extend_down(&self, _: &ChainSegment, _: usize, _: usize) -> anyhow::Result<()> {
        todo!()
    }
}

fn child_parent_valid(child: &BlockHeader, parent: &BlockHeader) -> bool {
    child.number.0 == parent.number.0 + 1
}

#[derive(Debug, Clone)]
pub struct ChainSegment {
    pub headers: Vec<BlockHeader>,
}

impl ChainSegment {
    pub fn new(headers: Vec<BlockHeader>) -> Self {
        Self { headers }
    }
}

impl From<BlockHeader> for ChainSegment {
    fn from(header: BlockHeader) -> Self {
        ChainSegment::new(vec![header])
    }
}
