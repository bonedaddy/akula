use crate::{
    kv::{
        mdbx::MdbxTransaction,
        tables::{self, TruncateStart},
    },
    models::{BlockBody, BlockNumber, BodyForStorage, MessageWithSignature, TxIndex, H256},
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
use rayon::{
    iter::{IntoParallelIterator, ParallelDrainRange, ParallelIterator},
    prelude::ParallelSliceMut,
};
use std::{sync::Arc, time::Duration};
use tracing::{info, trace};

pub type HashChunk = ArrayVec<H256, CHUNK_SIZE>;

pub type BodyForInsertion = (
    BlockNumber,
    H256,
    Vec<(H256, MessageWithSignature)>,
    BodyForStorage,
);

pub struct BodyDownloader {
    pub sentry: Arc<Coordinator>,
}

impl BodyDownloader {
    #[inline]
    /// Creates a new `BodyDownloader`
    /// using the given connection facility.
    pub fn new<T, E>(
        conn: T,
        chain_config: ChainConfig,
        txn: MdbxTransaction<'_, RO, E>,
    ) -> anyhow::Result<Self>
    where
        T: Into<SentryPool>,
        E: EnvironmentKind,
    {
        let (block_number, hash) = txn.cursor(tables::CanonicalHeader)?.last()?.unwrap();
        Ok(Self {
            sentry: Arc::new(Coordinator::new(
                conn,
                chain_config,
                Status::new(
                    block_number.0,
                    hash,
                    txn.get(tables::HeadersTotalDifficulty, (block_number, hash))?
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
        let block_number = txn
            .cursor(tables::BlockBody)?
            .last()?
            .map(|((block_number, _), _)| block_number + 1)
            .unwrap();
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
        trace!("Batch size: {:#?}", batch_size);

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
                let block_number = BlockNumber((i * CHUNK_SIZE) as u64) + block_number;
                dbg!(block_number);
                (block_number, HashChunk::from_iter(hashes.to_vec()))
            })
            .collect::<HashMap<_, _>>();

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
                                if !v.bodies.is_empty() && (v.bodies.len() == CHUNK_SIZE || v.bodies.len() ==  (batch_size.0 as usize % CHUNK_SIZE)) {
                                    let block_number = v.bodies.iter().rev().skip_while(|v| {
                                        v.ommers.is_empty()
                                    }).map(|v| {
                                        BlockNumber(v.ommers.last().unwrap().number.0 - v.ommers.last().unwrap().number.0%1024)
                                    }).next().unwrap();
                                    if !chunks.contains_key(&block_number) {
                                        continue;
                                    }
                                    info!("Received new block bodies, left requests={}, bodies total={}", chunks.len(), block_bodies.len());
                                    assert!(chunks.remove(&block_number).is_some());
                                    block_bodies.insert(block_number, v.bodies);
                                }
                        },
                        _ => continue,
                    };
                }
            }
        }

        Self::insert_block_bodies(
            Self::prepare_block_bodies(block_bodies.drain(), &txn)?,
            &txn,
        )?;

        txn.commit()?;
        Ok(())
    }

    #[inline(always)]
    pub fn insert_block_bodies<E: EnvironmentKind>(
        block_bodies: Vec<BodyForInsertion>,
        txn: &'_ MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<()> {
        let mut cursor = txn.cursor(tables::BlockBody)?;
        let mut block_tx_cursor = txn.cursor(tables::BlockTransaction)?;
        let mut lookup_cursor = txn.cursor(tables::BlockTransactionLookup)?;
        let mut base_tx_id = cursor
            .last()?
            .map(|((_, _), v)| v.base_tx_id.0 + v.tx_amount)
            .unwrap();

        block_bodies.into_iter().try_for_each(
            |(block_number, hash, transactions, mut body)| -> anyhow::Result<()> {
                body.base_tx_id = TxIndex(base_tx_id);
                cursor.put((block_number, hash), body)?;
                transactions
                    .into_iter()
                    .try_for_each(|(hash, transaction)| -> anyhow::Result<()> {
                        base_tx_id += 1;
                        lookup_cursor.put(hash, TruncateStart(block_number))?;
                        block_tx_cursor.put(TxIndex(base_tx_id), transaction)?;
                        Ok(())
                    })
            },
        )?;

        Ok(())
    }

    #[inline(always)]
    pub fn prepare_block_bodies<E: EnvironmentKind>(
        block_bodies: hashbrown::hash_map::Drain<BlockNumber, Vec<BlockBody>>,
        txn: &'_ MdbxTransaction<'_, RW, E>,
    ) -> anyhow::Result<Vec<BodyForInsertion>> {
        let mut block_bodies = block_bodies.collect::<Vec<_>>();
        block_bodies.par_sort_unstable_by_key(|(number, _)| number.0);
        let start = block_bodies
            .iter()
            .map(|(number, _)| BlockNumber(number.0))
            .next();

        Ok(block_bodies
            .drain(..)
            .flat_map(|(_, v)| v)
            .zip(
                txn.cursor(tables::CanonicalHeader)?
                    .walk(start)
                    .filter_map(Result::ok)
                    .filter_map(Option::Some),
            )
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|(mut body, (number, hash))| {
                let txs = body
                    .transactions
                    .par_drain(..)
                    .map(|tx| (tx.hash(), tx))
                    .collect::<Vec<_>>();
                let tx_amount = txs.len() as u64;
                (
                    number,
                    hash,
                    txs,
                    BodyForStorage {
                        base_tx_id: TxIndex(0),
                        tx_amount,
                        uncles: body.ommers,
                    },
                )
            })
            .collect::<Vec<_>>())
    }
}
