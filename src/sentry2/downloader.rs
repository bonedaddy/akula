use crate::{
    kv::{
        mdbx::MdbxTransaction,
        tables::{self, VariableVec},
    },
    models::{BlockHeader, BlockNumber, H256},
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{HeaderRequest, Message, Status},
        Coordinator, CoordinatorStream, SentryCoordinator, SentryPool, BATCH_SIZE, CHUNK_SIZE,
    },
};
use futures_util::{select, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::HashMap;
use hashlink::{LinkedHashMap, LinkedHashSet, LruCache};
use mdbx::{EnvironmentKind, RO, RW};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use std::{
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};
use tracing::info;

pub struct HeaderDownloader {
    /// Sentry connector.
    pub sentry: Arc<Coordinator>,
    /// Our current height.
    pub height: BlockNumber,
    /// The hash known to belong to the canonical chain(defaults to our latest checkpoint or
    /// genesis).
    pub canonical_marker: H256,
    /// Whethere we found chain tip or not.
    pub found_tip: bool,
    /// The set of newly seen hashes.
    pub seen_announces: LruCache<H256, ()>,
    /// Mapping from the child hash to the parent hash.
    pub parents_table: HashMap<H256, H256>,
    /// Mapping from the block hash to it's number and optionally total difficulty.
    pub blocks_table: HashMap<H256, (BlockNumber, Option<u128>)>,
}

impl HeaderDownloader {
    #[inline]
    pub fn new<T, C, E>(
        conn: T,
        chain_config: C,
        txn: MdbxTransaction<'_, RO, E>,
    ) -> anyhow::Result<Self>
    where
        T: Into<SentryPool>,
        C: Into<ChainConfig>,
        E: EnvironmentKind,
    {
        let chain_config = chain_config.into();

        let (block_number, hash) = txn
            .cursor(tables::CanonicalHeader)?
            .last()?
            .unwrap_or((0.into(), chain_config.genesis_block_hash()));
        let (chain_height, chain_hash) = txn
            .get(tables::LastHeader, VariableVec::default())?
            .unwrap();

        let (found_tip, (height, canonical_marker)) = if chain_height - block_number > BATCH_SIZE {
            (true, (chain_height, chain_hash))
        } else {
            (false, (block_number, hash))
        };
        let td = txn
            .get(tables::HeadersTotalDifficulty, (block_number, hash))?
            .unwrap_or_else(|| chain_config.chain_spec().genesis.seal.difficulty())
            .to_be_bytes()
            .into();

        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config,
                Status::new(block_number.0, hash, td),
            )),
            height,
            canonical_marker,
            found_tip,
            seen_announces: LruCache::new(CHUNK_SIZE),
            blocks_table: Default::default(),
            parents_table: Default::default(),
        })
    }

    #[inline]
    pub async fn step<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        if !self.found_tip {
            self.evaluate_chain_tip(&mut stream).await?;
            assert!(self.found_tip);

            txn.set(
                tables::LastHeader,
                VariableVec::default(),
                (self.height, self.canonical_marker),
            )?;
        }

        let (mut block_number, mut hash) = txn
            .cursor(tables::CanonicalHeader)?
            .last()?
            .unwrap_or((BlockNumber(0), self.sentry.genesis_hash));
        info!("Progress: {:?} of {:?}", block_number, self.height);

        let batch_size = if self.height - block_number <= BlockNumber(BATCH_SIZE as u64) {
            self.height - block_number
        } else {
            BlockNumber(BATCH_SIZE as u64)
        };

        let mut headers = Vec::<BlockHeader>::with_capacity(batch_size.into());
        while headers.len() < batch_size.into() {
            if !headers.is_empty() {
                let last = headers.last().unwrap();
                (block_number, hash) = (last.number, last.hash());
            };
            headers.extend(
                self.collect_headers(&mut stream, block_number, hash, block_number + batch_size)
                    .await?,
            );
        }

        assert!(headers.len() == batch_size.0 as usize);
        self.flush(txn, headers)?;
        Ok(())
    }

    #[inline]
    async fn collect_headers(
        &self,
        stream: &mut CoordinatorStream,
        start: BlockNumber,
        hash: H256,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let mut requests = Self::prepare_requests(start, hash, end);

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
                            if !v.headers.is_empty() && requests.contains_key(&v.headers[0].number)
                            && (v.headers.len() == CHUNK_SIZE
                            || v.headers.len() == requests.get(&v.headers[0].number).unwrap().limit as usize)
                            && (v.headers.last().unwrap().number + 1u8 == v.headers[0].number + CHUNK_SIZE
                                || v.headers.last().unwrap().number + 1u8
                                    == v.headers[0].number + requests.get(&v.headers[0].number).unwrap().limit)
                             {
                                assert!(requests.remove(&v.headers[0].number).is_some());
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

    #[inline]
    /// Verifies given block headers and partially invalidates them if necessary.
    fn verify_chunks(headers: &mut Vec<BlockHeader>) {
        headers.par_sort_unstable_by_key(|v| v.number);

        let valid_till = AtomicUsize::new(0);
        headers
            .par_iter()
            .enumerate()
            .skip(1)
            .for_each(|(i, header)| {
                if header.number != headers[i - 1].number + 1u8
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

    #[inline]
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
        let mut td = cursor_td.last()?.map(|v| v.1).unwrap();

        for header in headers {
            if header.number.0 == 0 {
                continue;
            }

            let hash = header.hash();
            let number = header.number;
            td += header.difficulty;

            cursor_header_num.put(hash, number)?;
            cursor_header.put((number, hash), header)?;
            cursor_canonical.put(number, hash)?;
            cursor_td.put((number, hash), td)?;
        }

        txn.commit()?;
        Ok(())
    }

    #[inline]
    async fn evaluate_chain_tip(&mut self, stream: &mut CoordinatorStream) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        self.found_tip = false;
        while !self.found_tip {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = self.sentry.ping().await;
                    if self.try_find_tip().await? { break; } else {
                        let _ =  self.parents_table.clone().into_iter().flat_map(|(k, v)| {
                            (1..3).map(|skip| {
                                let s = self.sentry.clone();
                                tokio::task::spawn(async move {
                                    let _ = s.send_header_request(HeaderRequest {
                                        start: k.into(),
                                        limit: 1,
                                        skip,
                                        ..Default::default()
                                    }).await;
                                    let _ = s.send_header_request(HeaderRequest {
                                        start: v.into(),
                                        limit: 1,
                                        skip,
                                        ..Default::default()
                                    }).await;
                                })
                            }).collect::<Vec<_>>()
                        }).collect::<FuturesUnordered<_>>().map(|_| ()).collect::<()>();
                    };
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() { continue; }
                    match msg.unwrap().msg {
                        Message::NewBlockHashes(v) => {
                            if !v.0.is_empty() && !self.seen_announces.contains_key(&v.0.last().unwrap().hash)
                            && v.0.last().unwrap().number >= self.height {
                                let block = v.0.last().unwrap();
                                self.seen_announces.insert(block.hash, ());
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

                                self.parents_table.insert(hash, header.parent_hash);
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

                            self.parents_table.insert(hash, parent_hash);
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

    #[inline]
    fn prepare_requests(
        start: BlockNumber,
        hash: H256,
        end: BlockNumber,
    ) -> LinkedHashMap<BlockNumber, HeaderRequest> {
        let mut requests = (start..end)
            .step_by(CHUNK_SIZE as usize)
            .map(|i| {
                (
                    i,
                    HeaderRequest {
                        start: i.into(),
                        limit: if i + CHUNK_SIZE < end {
                            CHUNK_SIZE as u64
                        } else {
                            (end - i).0
                        },
                        ..Default::default()
                    },
                )
            })
            .collect::<LinkedHashMap<_, _>>();
        requests.insert(
            start,
            HeaderRequest {
                start: hash.into(),
                limit: if start + (CHUNK_SIZE as u64) < end {
                    CHUNK_SIZE as u64
                } else {
                    (end - start).0
                },
                ..Default::default()
            },
        );
        requests
    }

    #[inline]
    /// Finds chain tip if it's possible at the given moment.
    async fn try_find_tip(&mut self) -> anyhow::Result<bool> {
        let possible_tips = self
            .parents_table
            .keys()
            .cloned()
            .collect::<LinkedHashSet<_>>();

        let mut longest_path = LinkedHashSet::new();
        for tip in &possible_tips {
            let mut path = LinkedHashSet::from_iter(vec![*tip]);

            let mut current = *tip;
            while let Some(v) = self.parents_table.get(&current) {
                current = *v;
                path.insert(current);
            }

            if path.len() >= longest_path.len() {
                longest_path = path;
            }
        }

        let _ = possible_tips
            .into_iter()
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
            let last = longest_path.pop_back().unwrap();
            self.parents_table.remove(&last);
            info!(
                "Successfully found canonical chain: {:#?} {:?}",
                longest_path, &last
            );
            self.canonical_marker = longest_path.pop_front().unwrap();
            self.found_tip = true;
            Ok(true)
        } else {
            Ok(false)
        }
    }
}
