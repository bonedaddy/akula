#![allow(unreachable_code)]
use crate::{
    kv::{mdbx::MdbxTransaction, tables},
    models::BlockNumber,
    sentry::chain_config::ChainConfig,
    sentry2::{
        types::{Message, Status},
        Coordinator, SentryCoordinator, SentryPool,
    },
};
use futures_util::{select, stream::FuturesUnordered, FutureExt, StreamExt};
use hashbrown::{HashMap, HashSet};
use mdbx::{EnvironmentKind, RO, RW};
use std::{ops::ControlFlow, sync::Arc, time::Duration};
use tracing::info;

const BATCH_SIZE: usize = 3 << 15;
const CHUNK_SIZE: usize = 1 << 10;

pub struct BodyDownloader {
    pub sentry: Arc<Coordinator>,
}

impl BodyDownloader {
    pub fn new<T: Into<SentryPool>, E: EnvironmentKind>(
        conn: T,
        txn: MdbxTransaction<'_, RO, E>,
        chain_config: ChainConfig,
    ) -> anyhow::Result<Self> {
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

    pub async fn step<E: EnvironmentKind>(
        &mut self,
        txn: MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut stream = self.sentry.recv().await?;
        let block_number = txn.cursor(tables::BlockBody)?.last()?.unwrap().0 .0;
        let last_block_number = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap().0;
        let (_batch_size, last_block_number) =
            if last_block_number - block_number >= BlockNumber(BATCH_SIZE as u64) {
                (
                    BlockNumber(BATCH_SIZE as u64),
                    (block_number + BlockNumber(BATCH_SIZE as u64)),
                )
            } else {
                (last_block_number - block_number, last_block_number)
            };
        let cursor = txn.cursor(tables::CanonicalHeader)?;
        let requests = cursor
            .walk(Some(block_number))
            .map(|v| {
                if let Ok(v) = v {
                    if v.0 < last_block_number {
                        ControlFlow::Continue((v.0, v.1))
                    } else {
                        ControlFlow::Break(())
                    }
                } else {
                    unreachable!()
                }
            })
            .filter_map(|v| match v {
                ControlFlow::Continue(v) => Some(v),
                ControlFlow::Break(_) => None,
            })
            .collect::<HashMap<_, _>>();

        let mut chunks = requests
            .values()
            .into_iter()
            .copied()
            .collect::<Vec<_>>()
            .chunks(1024)
            .enumerate()
            .map(|(i, v)| (BlockNumber((i as u64) * 1024), v.into()))
            .collect::<HashMap<BlockNumber, Vec<_>>>();
        let mut block_bodies = HashSet::new();
        let mut ticker = tokio::time::interval(Duration::from_secs(30));

        while !requests.is_empty() {
            select! {
            _ = ticker.tick().fuse() => {
                info!("Resending {} requests", chunks.len());
                let _ = chunks.clone().into_iter().map(|req| {
                        let s = self.sentry.clone();
                        tokio::spawn(async move { s.send_body_request(req.1).await })
                    }).collect::<FuturesUnordered<_>>().map(|_| ()).collect::<()>();
            }
            msg = stream.next().fuse() => {
                if msg.is_none() { continue; }
                match msg.unwrap().msg {
                    Message::BlockBodies(v) => {
                            if v.bodies.len() == 1024 && !block_bodies.contains(&v.bodies) {
                                let bodies = v.bodies;
                                let block_number = bodies.clone().into_iter().filter_map(|v| {
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
                                block_bodies.insert(bodies);
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
