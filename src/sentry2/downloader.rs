use crate::{
    kv::{mdbx::MdbxTransaction, tables},
    models::{BlockHeader, BlockNumber, H256},
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{HeaderRequest, Message},
        Coordinator, CoordinatorStream, SentryCoordinator, SentryPool,
    },
};
use futures_util::{select, FutureExt, StreamExt};
use hashbrown::{HashMap, HashSet};
use hashlink::LinkedHashSet;
use itertools::Itertools;
use mdbx::{EnvironmentKind, RW};
use rayon::slice::ParallelSliceMut;
use std::{marker::PhantomData, sync::Arc, time::Duration, vec::Drain};
use tracing::info;

const BATCH_SIZE: usize = 89856;

pub struct HeaderDownloader<'a> {
    /// Sentry connector.
    pub sentry: Arc<Coordinator>,
    /// Our current height.
    pub height: BlockNumber,
    /// The set of newly seen hashes.
    pub seen_announces: LinkedHashSet<H256>,
    /// Mapping from the parent hash to the set of children hashes.
    pub childs_table: HashMap<H256, HashSet<H256>>,
    /// Mapping from the block hash to it's number and optionally total difficulty.
    pub blocks_table: HashMap<H256, (BlockNumber, Option<u128>)>,
    /// The hash known to belong to the canonical chain(defaults to our latest checkpoint or
    /// genesis).
    pub canonical_marker: H256,
    /// prepared requests for preverified hashes.
    //    pub prepared_requests: LinkedHashSet<H256>,
    pub found_tip: bool,

    _phantom: PhantomData<&'a ()>,
}

impl<'a> HeaderDownloader<'a> {
    pub fn new<T: Into<SentryPool>>(
        conn: T,
        chain_config: ChainConfig,
        height: BlockNumber,
    ) -> Self {
        Self {
            sentry: Arc::new(Coordinator::new(conn, chain_config.clone(), 0)),
            height,
            seen_announces: LinkedHashSet::new(),
            childs_table: HashMap::new(),
            blocks_table: HashMap::new(),
            canonical_marker: H256::default(),
            found_tip: false,
            _phantom: PhantomData,
        }
    }

    pub async fn evaluate_chain_tip(&'_ mut self) -> anyhow::Result<()> {
        Ok(())
    }

    /// Runtime of the downloader.
    pub async fn runtime(&'_ mut self) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let mut ticker = tokio::time::interval(Duration::from_secs(15));

        loop {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = self.sentry.ping().await;
                    info!("Ping sent {:#?}", self.childs_table);
                    if self.try_find_tip().await? {
                            break
                    };
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() {
                        continue;
                    }
                    match msg.unwrap().msg {
                        Message::NewBlockHashes(v) => {
                            if !v.0.is_empty()
                                && !self.seen_announces.contains(&v.0.last().unwrap().hash)
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
                        Message::BlockHeaders(value) => {
                            if !value.headers.is_empty()
                                && value.headers.last().unwrap().number > self.sentry.last_ping()
                                && value.headers.len()%192 == 0 {

                                info!("{:#?}", value.headers.len());

                            } else if value.headers.len() == 1
                                && value.headers.last().unwrap().number > self.sentry.last_ping()
                                {

                                let header = value.headers.last().unwrap();
                                let hash = header.hash();

                                self.childs_table.entry(header.parent_hash).or_insert_with(HashSet::new).insert(hash);
                                self.blocks_table.insert(hash, (header.number, None));
                                self.sentry.send_header_request(HeaderRequest {
                                    start: hash.into(),
                                    limit: 1,
                                    skip: 1,
                                    ..Default::default()
                                }).await?;
                                if self.height < header.number {
                                    self.height = header.number;
                                }
                            }
                        },
                        Message::NewBlock(value) => {
                            info!("New block: {:#?}", value.block.header.number);

                            let (hash, number, parent_hash)
                                = (value.block.header.hash(), value.block.header.number, value.block.header.parent_hash);

                            self.childs_table.entry(parent_hash).or_insert_with(HashSet::new).insert(hash);
                            self.blocks_table.insert(hash, (number, Some(value.total_difficulty)));
                            self.sentry.send_header_request(HeaderRequest {
                                start: hash.into(),
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            }).await?;
                            self.height = number;
                        },
                        _ => continue,
                    }
                }
            }
        }

        // self.prepare_requests(stream).await?;

        Ok(())
    }

    pub async fn download_canonical<E: EnvironmentKind>(
        &'_ mut self,
        _txn: &'_ MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let pending = Vec::<BlockHeader>::with_capacity(100000);
        let mut ticker = tokio::time::interval(Duration::from_secs(15));

        Ok(())
    }

    /// Runs next step on the finite time.
    pub async fn step<E: EnvironmentKind>(
        &'_ mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        info!("Starting downloader step");
        txn.commit()?;
        Ok(())
    }

    /// Flushes step progress.
    pub async fn flush<E: EnvironmentKind>(
        &'_ mut self,
        txn: &'_ MdbxTransaction<'_, RW, E>,
        headers: Drain<'_, BlockHeader>,
    ) -> anyhow::Result<()> {
        let mut cursor_header_num = txn.cursor(tables::HeaderNumber)?;
        let mut cursor_header = txn.cursor(tables::Header)?;
        let mut cursor_canonical = txn.cursor(tables::CanonicalHeader)?;
        let mut cursor_td = txn.cursor(tables::HeadersTotalDifficulty)?;
        let mut headers = headers.collect_vec();
        let mut td = 0u128.into();
        headers.par_sort_unstable_by_key(|header| header.number);
        headers.into_iter().for_each(|header| {
            let hash = header.hash();
            let number = header.number;
            td += header.difficulty;

            cursor_header_num.put(hash, number).unwrap();
            cursor_header.put((number, hash), header).unwrap();
            cursor_canonical.put(number, hash).unwrap();
            cursor_td.put((number, hash), td).unwrap();
        });

        Ok(())
    }

    /// Finds chain tip if it's possible at the given moment.
    pub async fn try_find_tip(&'_ mut self) -> anyhow::Result<bool> {
        let mut canonical = Vec::new();
        for mut parent in self.childs_table.keys().into_iter().cloned() {
            let mut chain = vec![parent];
            while let Some(childrens) = self.childs_table.get(&parent) {
                if childrens.len() == 1 {
                    parent = *childrens.iter().next().unwrap();
                    chain.push(parent);
                } else {
                    for child in childrens {
                        match self.childs_table.get(child) {
                            Some(v) => {
                                if v.len() == 1 {
                                    parent = *child;
                                    chain.push(parent);
                                }
                            }
                            None => {
                                let _ = self
                                    .sentry
                                    .send_header_request(HeaderRequest {
                                        start: (*child).into(),
                                        limit: 1,
                                        skip: 1,
                                        ..Default::default()
                                    })
                                    .await;
                            }
                        }
                    }
                }
            }
            if chain.len() > canonical.len() {
                canonical = chain;
            }
        }
        if canonical.len() >= 4 {
            info!("Successfully found canonical chain: {:#?}", canonical);
            self.canonical_marker = *canonical.last().unwrap();
            self.found_tip = true;
            return Ok(true);
        } else if !canonical.is_empty() {
            self.sentry
                .send_header_request(HeaderRequest {
                    start: (*canonical.last().unwrap()).into(),
                    limit: 1,
                    skip: 1,
                    ..Default::default()
                })
                .await?;
        }

        Ok(false)
    }

    #[allow(unreachable_code)]
    pub async fn prepare_requests(
        &'_ mut self,
        mut stream: CoordinatorStream,
    ) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        let (height, td) = self.blocks_table.get(&self.canonical_marker).unwrap();
        if td.is_some() {
            let _ = self
                .sentry
                .update_head(*height, self.canonical_marker, td.unwrap().into())
                .await;
        };
        let mut hashes = HashSet::new();
        hashes.insert(self.canonical_marker);
        let mut pending_request = HeaderRequest::new(self.canonical_marker.into(), 576, 575, true);
        loop {
            select! {
                _ = ticker.tick().fuse() => {
                    self.sentry.send_header_request(pending_request.clone()).await?;
                    info!("Hashes {:#?}", hashes);
                }
                msg = stream.next().fuse() => {
                    match msg.unwrap().msg {
                        Message::BlockHeaders(value) => {
                            info!("Headers {:#?}", &value);
                            if value.headers.len() == 1 && value.headers.last().unwrap().number > self.sentry.last_ping() {
                                let header = value.headers.last().unwrap();
                                let hash = header.hash();
                                if hashes.insert(hash) {
                                    continue;
                                }
                                pending_request = HeaderRequest::new(hash.into(), 576, 575, true);
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }

        Ok(())
    }
}
