use crate::{
    kv::{
        mdbx::{MdbxEnvironment, MdbxTransaction},
        tables,
    },
    models::{BlockHeader, BlockNumber, H256},
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{HeaderRequest, Message, Status},
        Coordinator, SentryCoordinator, SentryPool,
    },
};
use futures_util::{select, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::HashMap;
use hashlink::{LinkedHashMap, LinkedHashSet};
use mdbx::{EnvironmentKind, RO, RW};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use std::{
    collections::VecDeque,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tracing::{info, instrument};

use super::CoordinatorStream;

const BATCH_SIZE: usize = 3 << 15;
const CHUNK_SIZE: usize = 1 << 10;

pub struct HeaderDownloader {
    /// Sentry connector.
    pub sentry: Arc<Coordinator>,
    /// Our current height.
    pub height: BlockNumber,
    /// The set of newly seen hashes.
    pub seen_announces: LinkedHashSet<H256>,
    /// Mapping from the parent hash to the set of children hashes.
    pub childs_table: LinkedHashMap<H256, LinkedHashSet<H256>>,
    /// Mapping from the block hash to it's number and optionally total difficulty.
    pub blocks_table: HashMap<H256, (BlockNumber, Option<u128>)>,
    /// The hash known to belong to the canonical chain(defaults to our latest checkpoint or
    /// genesis).
    pub canonical_marker: H256,

    pub found_tip: bool,
}

impl HeaderDownloader {
    pub fn new<T: Into<SentryPool>, E: EnvironmentKind>(
        conn: T,
        txn: MdbxTransaction<'_, RO, E>,
        chain_config: ChainConfig,
    ) -> anyhow::Result<Self> {
        let block = txn
            .cursor(tables::CanonicalHeader)?
            .last()?
            .unwrap_or((0.into(), chain_config.genesis_block_hash()));
        let td = txn
            .get(tables::HeadersTotalDifficulty, block)?
            .unwrap_or_else(|| chain_config.chain_spec().genesis.seal.difficulty())
            .to_be_bytes()
            .into();
        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config,
                Status::new(block.0 .0, block.1, td),
            )),
            height: block.0,
            seen_announces: LinkedHashSet::new(),
            childs_table: LinkedHashMap::new(),
            blocks_table: HashMap::new(),
            canonical_marker: block.1,
            found_tip: false,
        })
    }

    pub async fn runtime<E: EnvironmentKind>(
        &mut self,
        db: &'_ MdbxEnvironment<E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        if !self.found_tip {
            self.evaluate_chain_tip(&mut stream).await?;
            debug_assert!(self.found_tip);
        }

        while db.begin()?.cursor(tables::CanonicalHeader)?.last().map_or(
            BlockNumber(0),
            |v| match v {
                Some(b) => b.0,
                _ => BlockNumber(0),
            },
        ) < self.height
        {
            self.step(db, Some(&mut stream)).await?;
        }

        Ok(())
    }

    /// Runs next step on the finite time.
    #[instrument(name = "HeaderDownloader::step", skip(self, db, stream))]
    pub async fn step<E: EnvironmentKind>(
        &mut self,
        db: &'_ MdbxEnvironment<E>,
        stream: Option<&mut CoordinatorStream>,
    ) -> anyhow::Result<()> {
        let mut sentry_stream = self.sentry.recv().await?;
        let stream = match stream {
            Some(stream) => {
                drop(sentry_stream);
                stream
            }
            None => &mut sentry_stream,
        };

        if !self.found_tip {
            self.evaluate_chain_tip(stream).await?;
            debug_assert!(self.found_tip);
        }

        let (mut block_number, mut hash) = db
            .begin()?
            .cursor(tables::CanonicalHeader)?
            .last()?
            .unwrap_or((BlockNumber(0), self.sentry.genesis_hash));
        info!("Progress: {:?} of {:?}", block_number, self.height);

        let batch_size = if self.height - block_number <= BlockNumber(BATCH_SIZE as u64) {
            self.height - block_number
        } else {
            BlockNumber(BATCH_SIZE as u64)
        };
        let mut headers = Vec::<BlockHeader>::with_capacity(batch_size.0 as usize);
        while headers.len() < batch_size.0 as usize {
            if !headers.is_empty() {
                let last = headers.last().unwrap();
                (block_number, hash) = (last.number, last.hash());
            }

            headers.extend(
                self.collect_headers(stream, block_number, hash, block_number + batch_size)
                    .await?,
            );
        }

        debug_assert!(headers.len() == batch_size.0 as usize);
        self.flush(db.begin_mutable()?, headers)?;
        info!(
            "Successfully committed batch of {} headers",
            batch_size.0 as usize
        );
        Ok(())
    }

    async fn collect_headers(
        &self,
        stream: &mut CoordinatorStream,
        start: BlockNumber,
        hash: H256,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let mut requests = (start..end)
            .step_by(CHUNK_SIZE)
            .map(|i| {
                (
                    i,
                    HeaderRequest {
                        start: i.into(),
                        limit: CHUNK_SIZE as u64,
                        ..Default::default()
                    },
                )
            })
            .collect::<LinkedHashMap<_, _>>();
        requests.insert(
            start,
            HeaderRequest {
                start: hash.into(),
                limit: CHUNK_SIZE as u64,
                ..Default::default()
            },
        );

        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        let mut headers = Vec::with_capacity(BATCH_SIZE);
        while !requests.is_empty() {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = requests.clone().into_iter().map(|(_,req)| {
                        let sentry = self.sentry.clone();
                        tokio::spawn(async move {
                            let _ = sentry.send_header_request(req).await;
                        })
                    }).collect::<FuturesUnordered<_>>().map(|_| ()).collect::<()>();
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() { continue; }
                    match msg.unwrap().msg {
                        Message::BlockHeaders(v) => {
                            if v.headers.len() == CHUNK_SIZE && requests.contains_key(&v.headers[0].number)
                            && v.headers.last().unwrap().number+1 == v.headers[0].number + (CHUNK_SIZE as u64) {
                                debug_assert!(requests.remove(&v.headers[0].number).is_some());
                                headers.extend(v.headers.into_iter());
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }
        Self::verify_chunks(&mut headers);
        Ok(headers)
    }

    fn verify_chunks(headers: &mut Vec<BlockHeader>) {
        headers.par_sort_unstable_by_key(|v| v.number);

        let valid_till = AtomicUsize::new(0);
        headers
            .par_iter()
            .enumerate()
            .skip(1)
            .for_each(|(i, header)| {
                if header.number != headers[i - 1].number + 1
                    || header.parent_hash != headers[i - 1].hash()
                    || header.timestamp < headers[i - 1].timestamp
                {
                    let mut value = valid_till.load(std::sync::atomic::Ordering::SeqCst);
                    while i < value {
                        // there's window in between, because value can be changed since we read it,
                        // so we need to make sure that we're changing
                        // the same value as we read and if not, reload it and try again if condition meets
                        if valid_till.compare_exchange(
                            value,
                            i,
                            std::sync::atomic::Ordering::SeqCst,
                            std::sync::atomic::Ordering::SeqCst,
                        ) == Ok(value)
                        {
                            break;
                        } else {
                            value = valid_till.load(std::sync::atomic::Ordering::SeqCst);
                        }
                    }
                }
            });

        let value = valid_till.load(std::sync::atomic::Ordering::SeqCst) as usize;
        if value != 0 {
            headers.truncate(value - 1);
        }
    }

    /// Flushes step progress.
    fn flush<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
        headers: Vec<BlockHeader>,
    ) -> anyhow::Result<()> {
        let mut cursor_header_num = txn.cursor(tables::HeaderNumber)?;
        let mut cursor_header = txn.cursor(tables::Header)?;
        let mut cursor_canonical = txn.cursor(tables::CanonicalHeader)?;
        let mut cursor_td = txn.cursor(tables::HeadersTotalDifficulty)?;
        let mut td = txn
            .get(
                tables::HeadersTotalDifficulty,
                cursor_canonical
                    .last()
                    .map_or((0.into(), self.sentry.genesis_hash), |v| match v {
                        Some((b, h)) => (b, h),
                        None => (0.into(), self.sentry.genesis_hash),
                    }),
            )?
            .unwrap_or_default();

        headers.into_iter().for_each(|header| {
            let hash = header.hash();
            let number = header.number;
            td += header.difficulty;

            cursor_header_num.put(hash, number).unwrap();
            cursor_header.put((number, hash), header).unwrap();
            cursor_canonical.put(number, hash).unwrap();
            cursor_td.put((number, hash), td).unwrap();
        });

        txn.commit()?;
        Ok(())
    }

    async fn evaluate_chain_tip(&mut self, stream: &mut CoordinatorStream) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        while !self.found_tip {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = self.sentry.ping().await;
                    if self.try_find_tip().await? { break; }
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() { continue; }
                    match msg.unwrap().msg {
                        Message::NewBlockHashes(v) => {
                            if !v.0.is_empty() && !self.seen_announces.contains(&v.0.last().unwrap().hash)
                            && v.0.last().unwrap().number >= self.height {
                                let block = v.0.last().unwrap();
                                self.seen_announces.insert(block.hash);
                                self.sentry.send_header_request(HeaderRequest {
                                    start: block.hash.into(),
                                    limit: 1,
                                    ..Default::default()
                                }).await?;
                            }
                        },
                        Message::BlockHeaders(v) =>  {
                            if v.headers.len() == 1 && v.headers[0].number > self.sentry.last_ping() {
                                let header = &v.headers[0];
                                let hash = header.hash();

                                self.childs_table.entry(header.parent_hash).or_insert_with(LinkedHashSet::new).insert(hash);
                                self.blocks_table.insert(hash, (header.number, None));
                                self.sentry.send_header_request(HeaderRequest {
                                    start: hash.into(),
                                    limit: 1,
                                    skip: 1,
                                    ..Default::default()
                                }).await?;

                                if self.height < header.number { self.height = header.number; }
                            }
                        },
                        Message::NewBlock(v) => {
                            let (hash, number, parent_hash)
                                = (v.block.header.hash(), v.block.header.number, v.block.header.parent_hash);

                            self.childs_table.entry(parent_hash).or_insert_with(LinkedHashSet::new).insert(hash);
                            self.blocks_table.insert(hash, (number, Some(v.total_difficulty)));
                            self.sentry.send_header_request(HeaderRequest {
                                start: hash.into(),
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            }).await?;
                            if self.height < number { self.height = number; }
                        }
                        _ => continue,
                    }
                }
            }
        }

        Ok(())
    }

    /// Finds chain tip if it's possible at the given moment.
    async fn try_find_tip(&mut self) -> anyhow::Result<bool> {
        let possible_tips = self
            .childs_table
            .values()
            .flat_map(|childs| childs.clone())
            .collect::<LinkedHashSet<_>>();

        let mut longest_path = LinkedHashSet::new();

        for tip in possible_tips {
            let mut path = LinkedHashSet::new();
            path.insert(tip);
            let mut queue = VecDeque::new();
            queue.push_back(tip);

            while let Some(hash) = queue.pop_front() {
                for (parent, childs) in self.childs_table.iter() {
                    if childs.contains(&hash) {
                        path.insert(*parent);
                        queue.push_back(*parent);
                    }
                }
            }
            if path.len() >= longest_path.len() {
                longest_path = path;
            }
        }

        let _ = self
            .childs_table
            .clone()
            .into_iter()
            .flat_map(|(k, mut v)| {
                v.insert(k);
                v
            })
            .map(|v| {
                let sentry = self.sentry.clone();
                tokio::spawn(async move {
                    let _ = sentry
                        .send_header_request(HeaderRequest {
                            start: v.into(),
                            limit: 1,
                            skip: 1,
                            ..Default::default()
                        })
                        .await;
                })
            })
            .collect::<FuturesUnordered<_>>()
            .map(|_| ())
            .collect::<()>();

        if longest_path.len() >= 3 {
            info!("Successfully found canonical chain: {:#?}", longest_path);
            self.canonical_marker = longest_path.pop_back().unwrap();
            self.found_tip = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
