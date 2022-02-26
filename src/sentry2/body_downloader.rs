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
use hashbrown::{HashMap, HashSet};
use mdbx::{EnvironmentKind, RO, RW};
use std::{sync::Arc, time::Duration};
use tracing::{info, trace};

pub struct BodyDownloader {
    pub sentry: Arc<Coordinator>,
}

pub type HashChunk = ArrayVec<H256, CHUNK_SIZE>;

impl BodyDownloader {
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
        let block = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap();
        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config.into(),
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
        let last_block_number = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap().0;

        let (batch_size, _) = if last_block_number - block_number >= BlockNumber(BATCH_SIZE as u64)
        {
            (
                BlockNumber(BATCH_SIZE as u64),
                (block_number + BlockNumber(BATCH_SIZE as u64)),
            )
        } else {
            (last_block_number - block_number, last_block_number)
        };

        let mut chunks = txn
            .cursor(tables::CanonicalHeader)?
            .walk(Some(block_number))
            .map_while(|v| {
                let (number, hash) = v.unwrap();
                if number - block_number < batch_size {
                    Some(hash)
                } else {
                    None
                }
            })
            .collect::<ArrayVec<_, BATCH_SIZE>>()
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
        let mut block_bodies = HashSet::new();
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
                                if !v.bodies.is_empty() && !block_bodies.contains(&v.bodies)
                                    && (v.bodies.len() == CHUNK_SIZE || v.bodies.len() ==  (batch_size.0 as usize % CHUNK_SIZE)) {
                                    let block_number = v.bodies.iter().filter_map(|v| {
                                        if !v.ommers.is_empty() {
                                            Some(v.ommers.last().unwrap().number - v.ommers.last().unwrap().number.0%1024)
                                        } else {
                                            None
                                        }
                                    }).last().unwrap();
                                    if !chunks.contains_key(&block_number) {
                                        continue;
                                    }
                                    info!("Received new block bodies, left requests={}, bodies total={}", chunks.len(), block_bodies.len());
                                    chunks.remove(&block_number);
                                    block_bodies.insert(v.bodies);
                                }
                        },
                        _ => continue,
                    };
                }
            }
        }

        Ok(())
    }
}
