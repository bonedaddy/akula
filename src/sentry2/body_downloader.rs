use crate::{
    kv::{mdbx::MdbxTransaction, tables},
    models::BlockNumber,
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{Message, Status},
        Coordinator, SentryCoordinator, SentryPool, BATCH_SIZE, CHUNK_SIZE,
    },
};
use futures_util::{select, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::{HashMap, HashSet};
use mdbx::{EnvironmentKind, RO, RW};
use std::{sync::Arc, time::Duration};
use tracing::{info, log::trace};

pub struct BodyDownloader {
    pub sentry: Arc<Coordinator>,
}

impl BodyDownloader {
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

    pub async fn step<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let block_number = txn.cursor(tables::BlockBody)?.last()?.unwrap().0 .0;
        let last_block_number = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap().0;
        let (_batch_size, _last_block_number) =
            if last_block_number - block_number >= BlockNumber(BATCH_SIZE as u64) {
                (
                    BlockNumber(BATCH_SIZE as u64),
                    (block_number + BlockNumber(BATCH_SIZE as u64)),
                )
            } else {
                (last_block_number - block_number, last_block_number)
            };
        let cursor = txn.cursor(tables::CanonicalHeader)?;
        let mut chunks = cursor
            .walk(Some(block_number))
            .map(|v| v.unwrap().1)
            .collect::<Vec<_>>()
            .chunks(CHUNK_SIZE as usize)
            .enumerate()
            .map(|(i, v)| {
                (
                    BlockNumber(i as u64 * CHUNK_SIZE) + block_number,
                    v.to_vec(),
                )
            })
            .collect::<HashMap<BlockNumber, Vec<_>>>();
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
                                if v.bodies.len() == 1024 && !block_bodies.contains(&v.bodies) {
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
