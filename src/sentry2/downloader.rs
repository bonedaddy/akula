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
use futures_util::StreamExt;
use hashbrown::HashMap;
use hashlink::LinkedHashSet;
use lockfree::prelude::{Map, Set};
use mdbx::{EnvironmentKind, RO, RW};
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator},
    slice::ParallelSliceMut,
};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tracing::{info, instrument};

use super::{thread_storage::Storage, CoordinatorStream};

const BATCH_SIZE: usize = 3 << 15;
const CHUNK_SIZE: usize = 1 << 10;

pub struct HeaderDownloader {
    /// Sentry connector.
    pub sentry: Arc<Coordinator>,
    /// Our current height.
    pub height: BlockNumber,
    /// The set of newly seen hashes.
    pub seen_announces: LinkedHashSet<H256>,
    /// Mapping from the child hash to the parent hash.
    pub parents_table: HashMap<H256, H256>,
    /// Mapping from the block hash to it's number and optionally total difficulty.
    pub blocks_table: HashMap<H256, (BlockNumber, Option<u128>)>,
    /// The hash known to belong to the canonical chain(defaults to our latest checkpoint or
    /// genesis).
    pub canonical_marker: H256,

    pub found_tip: bool,
}

fn timenow() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
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
            parents_table: HashMap::new(),
            blocks_table: HashMap::new(),
            canonical_marker: block.1,
            found_tip: false,
        })
    }

    /// Runs next step on the finite time.
    #[allow(unreachable_code)]
    #[instrument(name = "HeaderDownloader::step", skip(self))]
    pub async fn step<E: EnvironmentKind>(
        &mut self,
        db: &'_ MdbxEnvironment<E>,
    ) -> anyhow::Result<()> {
        //let mut stream = self.sentry.recv().await?;

        // let (block_number, mut hash) = db
        //     .begin()?
        //     .cursor(tables::CanonicalHeader)?
        //     .last()?
        //     .unwrap_or((BlockNumber(0), self.sentry.genesis_hash));
        // info!("Progress: {:?} of {:?}", block_number, self.height);

        // let batch_size = if self.height - block_number <= BlockNumber(BATCH_SIZE as u64) {
        //     self.height - block_number
        // } else {
        //     BlockNumber(BATCH_SIZE as u64)
        // };
        // let mut headers = Vec::<BlockHeader>::with_capacity(batch_size.0 as usize);
        // while headers.len() < batch_size.0 as usize {
        //     headers.extend(
        //         self.collect_headers(
        //             &mut stream,
        //             0.into(),
        //             self.sentry.genesis_hash,
        //             BlockNumber(BATCH_SIZE as u64),
        //         )
        //         .await?,
        //     );
        // }

        Ok(())
    }

    async fn collect_headers(
        &self,
        stream: &mut CoordinatorStream,
        start: BlockNumber,
        hash: H256,
        end: BlockNumber,
    ) -> anyhow::Result<Vec<BlockHeader>> {
        let mut thread_storage = Storage::new();
        thread_storage.pending_requests = Arc::new(Self::prepare_requests(start, hash, end));

        let sender = self.sentry.clone();
        let storage = thread_storage.clone();

        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(15));

            #[allow(irrefutable_let_patterns)]
            while let _ = ticker.tick().await {
                if storage.done.load(Ordering::SeqCst) {
                    break;
                }
                for req in storage.pending_requests.iter().map(|v| v.1.clone()) {
                    dbg!(&req);
                    #[allow(unused_results)]
                    let _ = sender.send_header_request(req).await;
                }

                let current_timestamp = timenow();
                for task in storage
                    .seen_announces
                    .iter()
                    .map(|v| *v)
                    .into_iter()
                    .map(|(hash, timestamp)| {
                        if timestamp + 100 < current_timestamp {
                            storage.parents_table.remove(&hash);
                            storage.seen_announces.remove(&hash);
                        }
                        let s = sender.clone();
                        tokio::spawn(async move {
                            for i in 1..3 {
                                let _ = s
                                    .send_header_request(HeaderRequest {
                                        start: hash.into(),
                                        limit: 1,
                                        skip: i,
                                        ..Default::default()
                                    })
                                    .await;
                            }
                        })
                    })
                    .collect::<Vec<_>>()
                {
                    task.await.unwrap();
                }
                storage
                    .valid_chain
                    .iter()
                    .map(|v| *v)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .for_each(|hash| {
                        storage.valid_chain.remove(&hash);
                    });

                for hash in Self::try_find_tip(
                    storage
                        .parents_table
                        .iter()
                        .map(|v| (v.0, v.1))
                        .collect::<HashMap<_, _>>(),
                ) {
                    storage.valid_chain.insert(hash).unwrap();
                    for i in 1..3 {
                        sender
                            .send_header_request(HeaderRequest {
                                start: hash.into(),
                                limit: 1,
                                skip: i,
                                ..Default::default()
                            })
                            .await
                            .unwrap();
                    }
                }
            }
        });
        let batch = (end - start).0 as usize;

        let storage = thread_storage;
        let mut headers = Vec::<BlockHeader>::with_capacity(batch);

        while let Some(msg) = stream.next().await {
            match msg.msg {
                Message::BlockHeaders(v) => {
                    if !(v.headers.is_empty())
                        && storage.pending_requests.get(&v.headers[0].number).is_some()
                        && v.headers[0].number
                            + storage
                                .pending_requests
                                .get(&v.headers[0].number)
                                .map_or(1024, |v| v.1.limit)
                            == v.headers.last().unwrap().number + 1
                    {
                        storage.pending_requests.remove(&v.headers[0].number);
                        headers.extend(v.headers);
                        if headers.len() >= batch {
                            storage.done.store(true, Ordering::SeqCst);
                            break;
                        }
                    } else if v.headers.len() == 1
                        && v.headers[0].number.0 >= storage.chain_height.load(Ordering::SeqCst)
                        && v.headers[0].number > self.sentry.last_ping()
                    {
                        let header = &v.headers[0];
                        let hash = header.hash();
                        storage
                            .parents_table
                            .insert(v.headers[0].hash(), v.headers[0].parent_hash);
                        storage.seen_announces.insert(hash, timenow());
                        let _ = self
                            .sentry
                            .send_header_request(HeaderRequest {
                                start: (hash).into(),
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            })
                            .await;

                        let mut value = storage.chain_height.load(Ordering::SeqCst);
                        while value < header.number.0 {
                            if storage.chain_height.compare_exchange(
                                value,
                                header.number.0,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) == Ok(value)
                            {
                                break;
                            }
                            value = storage.chain_height.load(Ordering::SeqCst);
                        }
                    }
                }
                Message::NewBlockHashes(value)
                    if !value.0.is_empty()
                        && storage.seen_announces.get(&value.0[0].hash).is_none()
                        && value.0[0].number.0 >= storage.chain_height.load(Ordering::SeqCst) =>
                {
                    value.0.into_iter().for_each(|h| {
                        let _ = storage.seen_announces.insert(h.hash, timenow());
                    });
                }
                Message::NewBlock(v) => {
                    if v.block.header.number > self.sentry.last_ping()
                        && storage.parents_table.get(&v.block.header.hash()).is_none()
                    {
                        //info!("Got a new block: {:#?}", &v);
                        let hash = v.block.header.hash();
                        storage.seen_announces.insert(hash, timenow());
                        storage
                            .parents_table
                            .insert(v.block.header.hash(), v.block.header.parent_hash);
                        let _ = self
                            .sentry
                            .send_header_request(HeaderRequest {
                                start: (hash).into(),
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            })
                            .await;

                        let mut value = storage.chain_height.load(Ordering::SeqCst);
                        while v.block.header.number.0 < value {
                            if storage.chain_height.compare_exchange(
                                value,
                                v.block.header.number.0,
                                Ordering::SeqCst,
                                Ordering::SeqCst,
                            ) == Ok(value)
                            {
                                break;
                            }
                            value = storage.chain_height.load(Ordering::SeqCst);
                        }
                    }
                }
                _ => continue,
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
    /// Finds chain tip if it's possible at the given moment.
    fn try_find_tip(parents_table: HashMap<H256, H256>) -> Set<H256> {
        let possible_tips = parents_table
            .iter()
            .map(|v| v.0)
            .collect::<LinkedHashSet<_>>();
        let mut longest_path = LinkedHashSet::new();
        for tip in possible_tips {
            let mut path = LinkedHashSet::new();
            let mut current = *tip;
            while let Some(v) = parents_table.get(&current) {
                current = *v;
                path.insert(current);
            }

            if path.len() > longest_path.len() {
                longest_path = path;
            }
        }
        info!("longest path: {:?}", &longest_path);
        longest_path.into_iter().collect::<Set<_>>()
    }

    fn prepare_requests(
        start: BlockNumber,
        hash: H256,
        end: BlockNumber,
    ) -> Map<BlockNumber, HeaderRequest> {
        let requests = (start..end)
            .step_by(CHUNK_SIZE)
            .map(|i| {
                (
                    i,
                    HeaderRequest {
                        start: i.into(),
                        limit: match i + (CHUNK_SIZE as u64) < end {
                            true => CHUNK_SIZE as u64,
                            false => (end - i).0,
                        },
                        ..Default::default()
                    },
                )
            })
            .collect::<Map<_, _>>();
        requests.insert(
            start,
            HeaderRequest {
                start: hash.into(),
                limit: match start + (CHUNK_SIZE as u64) < end {
                    true => CHUNK_SIZE as u64,
                    false => (end - start).0,
                },
                ..Default::default()
            },
        );
        requests
    }
}
