use crate::{
    kv::{mdbx::MdbxEnvironment, tables},
    models::{BlockHeader, BlockNumber, DifficultyBomb, H256},
    sentry::chain_config::ChainsConfig,
    sentry2::{types::*, Coordinator, SentryCoordinator},
};
use ethereum_interfaces::sentry as grpc_sentry;
use futures_util::{stream::FuturesUnordered, FutureExt};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use mdbx::EnvironmentKind;
use rayon::slice::ParallelSliceMut;
use std::{
    ops::{Generator, GeneratorState},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio_stream::StreamExt;
use tracing::info;

use super::CoordinatorStream;

const CHUNK_SIZE: usize = 256;
const BATCH_SIZE: usize = 98304 * 4;
const INTERVAL: Duration = Duration::from_secs(10);

pub type Headers = Vec<BlockHeader>;

pub struct HeaderDownloader<'a> {
    pub coordinator: Arc<Coordinator>,
    pub bad_headers: HashSet<H256>,
    pub preverified: HashSet<H256>,
    pub pending: Vec<BlockHeader>,
    pub height: BlockNumber,
    pub carefully: bool,

    // maybe_tip is block number that could be the tip of the cannonical chain
    // but we can't give such guarantee that it is.
    pub maybe_tip: Option<Box<NewBlock>>,
    _phantom: std::marker::PhantomData<&'a ()>,
}

fn verify_slices(headers: Vec<BlockHeader>) -> anyhow::Result<()> {
    let _ = headers
        .into_iter()
        .tuple_windows()
        .map(|(parent, child)| {
            std::thread::spawn(move || {
                (
                    child.number.0,
                    child.number == parent.number + 1
                        && child.parent_hash == parent.hash()
                        && child.timestamp >= parent.timestamp,
                )
            })
        })
        .filter_map::<_, _>(|task| -> Option<usize> {
            match task.join() {
                // FIXME: Rerequest invalid headers.
                Ok((_, true)) => None,
                _ => unreachable!(),
            }
        })
        .collect::<Vec<_>>();
    Ok(())
}

struct RequestGenerator {
    pub height: BlockNumber,
    pub maybe_tip: BlockNumber,
    state: usize,
}

impl RequestGenerator {
    fn new(height: BlockNumber, maybe_tip: Option<BlockNumber>) -> Self {
        Self {
            height,
            maybe_tip: maybe_tip.unwrap_or(BlockNumber(u64::MAX)),
            state: 0,
        }
    }
}

impl Generator for RequestGenerator {
    type Yield = HeaderRequest;
    type Return = ();

    fn resume(self: std::pin::Pin<&mut Self>, _: ()) -> GeneratorState<Self::Yield, Self::Return> {
        let this = self.get_mut();
        if this.state <= CHUNK_SIZE {
            this.state += 1;

            if this.height + 192 > this.maybe_tip {
                this.height = this.maybe_tip;
                GeneratorState::Yielded(HeaderRequest {
                    number: this.height,
                    limit: this.maybe_tip.0 - this.height.0,
                    ..Default::default()
                })
            } else {
                this.height.0 += 192;
                GeneratorState::Yielded(HeaderRequest {
                    number: this.height,
                    ..Default::default()
                })
            }
        } else {
            GeneratorState::Complete(())
        }
    }
}

impl<'a> HeaderDownloader<'a> {
    pub fn new(coordinator: Arc<Coordinator>, height: BlockNumber) -> Self {
        Self {
            coordinator,
            height,
            bad_headers: HashSet::new(),
            preverified: HashSet::new(),
            pending: Vec::new(),
            maybe_tip: None,
            carefully: false,
            _phantom: std::marker::PhantomData,
        }
    }

    #[allow(unreachable_code)]
    pub async fn runtime<E: EnvironmentKind>(
        &mut self,
        db: &'_ MdbxEnvironment<E>,
    ) -> anyhow::Result<()> {
        //    pub async fn runtime(&mut self) -> anyhow::Result<()> {
        let mut stream = self
            .coordinator
            .recv(vec![
                grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32,
                grpc_sentry::MessageId::from(MessageId::NewBlock) as i32,
            ])
            .await
            .unwrap();

        loop {
            let txn = db.begin_mutable()?;
            let headers = self.request_more(&mut stream).await?;
            let new_head = headers.len() as u64;
            headers.into_iter().for_each(|header| {
                let number = header.number;
                let hash = header.hash();
                txn.set(tables::Header, (header.number, hash), header)
                    .unwrap();
                txn.set(tables::HeaderNumber, hash, number).unwrap();
                txn.set(tables::CanonicalHeader, number, hash).unwrap();
            });
            txn.commit()?;
            self.height.0 += new_head;
        }
        Ok(())
    }

    pub async fn request_more(
        &mut self,
        stream: &mut CoordinatorStream,
    ) -> anyhow::Result<Headers> {
        let mut tick = tokio::time::interval(INTERVAL);
        let mut headers = Vec::new();
        let mut gen = RequestGenerator::new(
            self.height,
            self.maybe_tip.clone().map(|tip| tip.block.header.number),
        );
        let mut pending_requests = (0..CHUNK_SIZE)
            .filter_map(
                |_| match unsafe { Pin::new_unchecked(&mut gen).resume(()) } {
                    GeneratorState::Yielded(v) => {
                        if v.limit < 192 {
                            self.carefully = true;
                        };
                        Some((v.number, v))
                    }
                    _ => None,
                },
            )
            .collect::<HashMap<_, _>>();

        while !pending_requests.is_empty() {
            futures_util::select! {
                _ = tick.tick().fuse() => {
                    info!("{:#?}", self.height.0 + 192 * ((CHUNK_SIZE - pending_requests.len()) as u64));
                    let _ = pending_requests
                        .values()
                        .cloned()
                        .map(|req| {
                            let coordinator = self.coordinator.clone();
                            tokio::spawn(async move {
                                let _ = coordinator.send_header_request(req).await;
                            })
                        })
                        .collect::<FuturesUnordered<_>>();
                }
                msg = stream.next().fuse() => {
                    let value = match msg.unwrap().msg {
                        Message::BlockHeaders(value) => {
                            // FIXME: Length one is an edge case, that should be handled properly.
                            if value.headers.len() <= 1
                                || !(pending_requests.contains_key(&value.headers[0].number))
                                || value.headers.len() != pending_requests.get(&value.headers[0].number).unwrap().limit as usize
                                || (value.headers[0].number.0 + (pending_requests.get(&value.headers[0].number).unwrap().limit-1) as u64)
                                    != value.headers[value.headers.len() - 1].number.0 {
                                continue;
                            }
                            value
                        }
                        Message::NewBlock(value) => {
                            self.maybe_tip = Some(value);
                            continue
                        }
                        _ => unreachable!(),
                    };

                    if self.carefully {
                        match  verify_slices(value.headers.clone()) {
                            Ok(_) => {
                                pending_requests.remove(&value.headers[0].number);
                                headers.push((value.headers[0].number.0 as usize, value.headers));
                            }
                            _ => continue,
                        };
                    } else {
                        pending_requests.remove(&value.headers[0].number);
                        headers.push((value.headers[0].number.0 as usize, value.headers));
                    }
                }

            }
        }
        headers.par_sort_unstable_by_key(|(i, _)| *i);
        let headers = headers.into_iter().flat_map(|(_, h)| h).collect::<Vec<_>>();
        verify_slices(headers.clone())?;
        Ok(headers)
    }
}

fn verify_params() -> anyhow::Result<(
    Option<BlockNumber>,
    Option<BlockNumber>,
    Option<DifficultyBomb>,
)> {
    match &ChainsConfig::default()
        .get("mainnet")?
        .chain_spec()
        .consensus
        .seal_verification
    {
        crate::models::SealVerificationParams::Ethash {
            homestead_formula,
            byzantium_formula,
            difficulty_bomb,
            ..
        } => Ok((
            *homestead_formula,
            *byzantium_formula,
            difficulty_bomb.clone(),
        )),
        _ => unreachable!(),
    }
}
