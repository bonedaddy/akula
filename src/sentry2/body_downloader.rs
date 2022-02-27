use crate::{
    kv::{mdbx::MdbxTransaction, tables},
    models::{BlockNumber, H256},
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{Message, Status},
        Coordinator, SentryCoordinator, SentryPool, BATCH_SIZE, CHUNK_SIZE,
    },
};
use arrayvec::ArrayVec;
use futures_util::{select, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::HashMap;
use mdbx::{EnvironmentKind, RO, RW};
use std::{sync::Arc, time::Duration};
use tracing::{info, trace};

pub type HashChunk = ArrayVec<H256, CHUNK_SIZE>;

pub struct BodyDownloader {
    pub sentry: Arc<Coordinator>,
}

impl BodyDownloader {
    #[inline]
    pub fn new<T, E>(
        conn: T,
        chain_config: ChainConfig,
        txn: MdbxTransaction<'_, RO, E>,
    ) -> anyhow::Result<Self>
    where
        T: Into<SentryPool>,
        E: EnvironmentKind,
    {
        let block = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap();
        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config,
                Status::new(
                    block.0 .0,
                    block.1,
                    txn.get(tables::HeadersTotalDifficulty, block)?
                        .unwrap()
                        .to_be_bytes()
                        .into(),
                ),
            )),
        })
    }

    #[inline]
    pub async fn step<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let block_number = txn.cursor(tables::BlockBody)?.last()?.unwrap().0 .0;
        trace!("block_number: {:#?}", block_number);
        let last_block_number = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap().0;
        trace!("Last block number: {:#?}", last_block_number);

        let (batch_size, _) = if last_block_number - block_number >= BlockNumber(BATCH_SIZE as u64)
        {
            (
                BlockNumber(BATCH_SIZE as u64),
                (block_number + BlockNumber(BATCH_SIZE as u64)),
            )
        } else {
            (last_block_number - block_number, last_block_number)
        };
        trace!("batch_size: {:#?}", batch_size);

        let mut chunks = txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(block_number))
            .filter_map(Result::ok)
            .map_while(|(number, hash)| -> Option<H256> {
                (number - block_number < batch_size).then(|| hash)
            })
            .collect::<Vec<_>>()
            .chunks(1024)
            .enumerate()
            .map(|(i, hashes)| {
                (
                    BlockNumber((i * CHUNK_SIZE) as u64) + block_number,
                    HashChunk::from_iter(hashes.to_vec()),
                )
            })
            .collect::<HashMap<_, _>>();

        trace!("Chunks: {:#?}", chunks);
        let mut block_bodies = HashMap::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));

        while !chunks.is_empty() {
            select! {
                _ = ticker.tick().fuse() => {
                    info!("Resending {} requests", chunks.len());
                    let _ = chunks.clone().into_values().map(|req| {
                            let s = self.sentry.clone();
                            tokio::spawn(async move { s.send_body_request(req).await })
                        }).collect::<FuturesUnordered<_>>().map(|_| ()).collect::<()>();
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() { continue; }
                    match msg.unwrap().msg {
                        Message::BlockBodies(v) => {
                                if !v.bodies.is_empty() && !block_bodies.contains_key(&v.bodies)
                                    && (v.bodies.len() == CHUNK_SIZE || v.bodies.len() ==  (batch_size.0 as usize % CHUNK_SIZE)) {
                                    let block_number = v.bodies.iter().rev().skip_while(|v| {
                                        v.ommers.is_empty()
                                    }).map(|v| {
                                        BlockNumber(v.ommers.last().unwrap().number.0 - v.ommers.last().unwrap().number.0%1024)
                                    }).next().unwrap();
                                    if !chunks.contains_key(&block_number) {
                                        continue;
                                    }
                                    info!("Received new block bodies, left requests={}, bodies total={}", chunks.len(), block_bodies.len());
                                    chunks.remove(&block_number);
                                    block_bodies.insert(v.bodies, block_number);
                                }
                        },
                        _ => continue,
                    };
                }
            }
        }

        // let mut block_bodies = block_bodies
        //     .into_iter()
        //     .map(|(bodies, marker)| (bodies, marker))
        //     .collect::<Vec<_>>();

        // block_bodies.par_sort_unstable_by_key(|(_, number)| number.0);
        // let start = block_bodies
        //     .iter()
        //     .map(|(_, number)| BlockNumber(number.0))
        //     .next()
        //     .unwrap();
        // let mut hash_cursor = txn.cursor(tables::CanonicalHeader)?.walk(Some(start));
        // let block_bodies = block_bodies
        //     .into_iter()
        //     .flat_map(|(bodies, _)| bodies)
        //     .map(|body| {
        //         hash_cursor
        //             .next()
        //             .unwrap()
        //             .map(|(number, hash)| (number, hash, body))
        //             .unwrap()
        //     })
        //     .collect::<Vec<_>>();

        // for item in block_bodies.iter().take(10) {
        //     info!("{:#?}", item);
        // }

        Ok(())
    }
}
