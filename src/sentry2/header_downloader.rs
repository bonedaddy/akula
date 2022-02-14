use crate::{
    downloader::PreverifiedHashesConfig,
    models::{BlockHeader, BlockNumber, H256},
    sentry2::{types::*, Coordinator, SentryCoordinator},
};
use futures_util::{stream::FuturesUnordered, FutureExt, StreamExt, TryStreamExt};
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use std::{
    collections::VecDeque,
    ops::{Generator, GeneratorState},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use super::broadcast_stream;

const CHUNK_SIZE: usize = 256;

const BATCH_SIZE: usize = 98304 * 4;

const INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownloader {
    coordinator: Arc<Coordinator>,
    preverified: HashSet<H256>,

    requests: Vec<HashMap<H256, HeaderRequest>>,
    pending: Vec<BlockHeader>,
}

impl HeaderDownloader {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        let chunks =
            Self::prepare_requests(PreverifiedHashesConfig::new("mainnet").unwrap().hashes);

        Self {
            coordinator,
            requests: chunks,
            preverified: HashSet::new(),
            pending: Vec::new(),
        }
    }

    pub async fn spin(&mut self) -> anyhow::Result<()> {
        let mut stream = self.coordinator.recv_headers().await?;
        let chunks = self.requests.clone().into_iter();

        info!("Starting header downloader");
        for mut chunk in chunks {
            let mut timer = tokio::time::interval(INTERVAL);
            while chunk.len() > 0 {
                futures_util::select! {
                    msg = stream.next().fuse() => {
                        let msg = msg.unwrap();
                        let msg = match msg.msg {
                            Message::BlockHeaders(value) => value,
                            _ => continue,
                        };
                        if msg.headers.len() != 192 {
                            continue;
                        }
                        let first_header_hash = msg.headers[0].clone().hash();
                        info!("Received headers first header: {:#?}", msg.headers[0]);
                        if chunk.contains_key(&first_header_hash) {
                            self.pending.extend(msg.headers);
                            chunk.remove(&first_header_hash);
                        }
                    }
                    _ = timer.tick().fuse() => {
                        info!("Sending chunk of size {}", chunk.len());
                        let c = self.coordinator.clone();
                        let mut tasks = Vec::new();
                        chunk.
                            clone()
                            .into_iter()
                            .for_each(|(_, v)| {
                                let c = c.clone();
                                tasks.push(async move { c.send_header_request(v).await });
                            });
                        for task in tasks {
                            let _ = task.await;
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn prepare_requests(hashes: Vec<H256>) -> Vec<HashMap<H256, HeaderRequest>> {
        let requests = hashes
            .into_iter()
            .enumerate()
            .map(|(i, hash)| {
                (
                    hash,
                    HeaderRequest::new(Some(hash), BlockNumber(i as u64 * 192), 192, 0, false),
                )
            })
            .collect::<Vec<_>>()
            .chunks(CHUNK_SIZE)
            .into_iter()
            .map(|chunk| chunk.iter().cloned().map(|(k, v)| (k, v)).collect())
            .collect::<Vec<HashMap<_, _>>>();

        requests
    }
}
