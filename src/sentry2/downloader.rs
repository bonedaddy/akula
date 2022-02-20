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
use hashbrown::{HashMap, HashSet};
use hashlink::LinkedHashSet;
use itertools::Itertools;
use mdbx::{EnvironmentKind, RO, RW};
use rayon::slice::ParallelSliceMut;
use std::{sync::Arc, time::Duration, vec::Drain};
use tracing::info;

const BATCH_SIZE: usize = 89856;
pub struct HeaderDownloader {
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

    pub found_tip: bool,
}

impl HeaderDownloader {
    pub fn new<T: Into<SentryPool>, E: EnvironmentKind>(
        conn: T,
        txn: MdbxTransaction<'_, RO, E>,
        chain_config: ChainConfig,
    ) -> anyhow::Result<Self> {
        let block = txn
            .cursor(tables::HeaderNumber)?
            .last()?
            .unwrap_or_else(|| (chain_config.genesis_block_hash(), BlockNumber(0)));
        let td = txn
            .get(tables::HeadersTotalDifficulty, (block.1, block.0))?
            .unwrap_or_else(|| chain_config.chain_spec().genesis.seal.difficulty())
            .to_be_bytes()
            .into();
        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config,
                Status::new(block.1 .0, block.0, td),
            )),
            height: block.1,
            seen_announces: LinkedHashSet::new(),
            childs_table: HashMap::new(),
            blocks_table: HashMap::new(),
            canonical_marker: block.0,
            found_tip: false,
        })
    }

    /// Runtime of the downloader.
    #[allow(unreachable_code)]
    pub async fn runtime<E: EnvironmentKind>(
        &mut self,
        db: &'_ MdbxEnvironment<E>,
    ) -> anyhow::Result<()> {
        self.evaluate_chain_tip().await?;
        let (block_number, hash) = db
            .begin()?
            .cursor(tables::HeaderNumber)?
            .last()?
            .map_or((BlockNumber(0), self.sentry.genesis_hash), |(h, v)| (v, h));
        let can_deal_in_one_step = ((if self.height.0 >= BATCH_SIZE as u64 {
            self.height - BATCH_SIZE as u64
        } else {
            self.height
        }) - block_number.0)
            <= BlockNumber(BATCH_SIZE as u64);
        info!(
            "Starting header downloader with height: {}, can_deal_in_one_step: {}",
            self.height, can_deal_in_one_step
        );

        let mut stream = self.sentry.recv().await?;
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        let requests = if !can_deal_in_one_step {
            let mut reqs = LinkedHashSet::new();
            let req = HeaderRequest {
                start: hash.into(),
                limit: 1024,
                skip: 0,
                reverse: false,
            };
            reqs.insert(req);

            (0..87).for_each(|i| {
                reqs.insert(HeaderRequest {
                    start: (block_number + i * 1024).into(),
                    limit: 1024,
                    skip: 0,
                    reverse: false,
                });
            });
            reqs
        } else {
            todo!()
        };
        while !requests.is_empty() {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = requests.iter().take(8).cloned().map(|req| {
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
                            info!("Received {} headers", v.headers.len());
                        }
                        _ => continue,
                    }
                }
            }
        }

        Ok(())
    }

    /// Runs next step on the finite time.
    pub async fn step<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        info!("Starting downloader step");
        txn.commit()?;
        Ok(())
    }

    /// Flushes step progress.
    pub async fn flush<E: EnvironmentKind>(
        &mut self,
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

    pub async fn evaluate_chain_tip(&mut self) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let mut ticker = tokio::time::interval(Duration::from_secs(15));

        while !self.found_tip {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = self.sentry.ping().await;
                    info!("Ping sent {:#?}", self.childs_table);

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

                                self.childs_table.entry(header.parent_hash).or_insert_with(HashSet::new).insert(hash);
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

                            self.childs_table.entry(parent_hash).or_insert_with(HashSet::new).insert(hash);
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
    pub async fn try_find_tip(&mut self) -> anyhow::Result<bool> {
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
                    break;
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
    pub async fn prepare_requests(&mut self) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        let mut stream = self.sentry.recv().await?;

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

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn it_works() {
        use std::{path::PathBuf, sync::Arc};

        use crate::{
            models::BlockNumber,
            sentry::chain_config::ChainsConfig,
            sentry2::{downloader::HeaderDownloader, SentryClient},
        };
        use anyhow::Context;
        use tracing::{info, Level};

        tracing_subscriber::fmt().with_max_level(Level::INFO).init();

        let chain_config = ChainsConfig::default().get("mainnet").unwrap();
        let sentry = SentryClient::connect("http://localhost:8000")
            .await
            .unwrap();
        std::fs::create_dir_all(PathBuf::from("./db")).unwrap();
        std::fs::create_dir_all(PathBuf::from("./db/temp")).unwrap();
        let db = crate::kv::new_database(PathBuf::from("./db").as_path()).unwrap();
        let txn = db.begin_mutable().unwrap();
        let etl_temp_dir = Arc::new(
            tempfile::tempdir_in("./db/temp")
                .context("failed to create ETL temp dir")
                .unwrap(),
        );

        crate::genesis::initialize_genesis(&txn, &*etl_temp_dir, chain_config.chain_spec().clone())
            .unwrap();
        txn.commit().unwrap();

        info!("DB initialized");
        let mut hd = HeaderDownloader::new(sentry, db.begin().unwrap(), chain_config).unwrap();
        hd.runtime(&db).await;
    }
}
