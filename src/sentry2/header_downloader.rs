use crate::{
    downloader::PreverifiedHashesConfig,
    models::{BlockHeader, BlockNumber, H256},
    sentry2::{types::*, Coordinator, SentryCoordinator},
};
use futures_util::FutureExt;
use hashbrown::{HashMap, HashSet};
use std::{sync::Arc, time::Duration};
use tokio_stream::StreamExt;

const CHUNK_SIZE: usize = 256;
const BATCH_SIZE: usize = 98304 * 4;
const INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownloader {
    pub coordinator: Arc<Coordinator>,
    pub bad_headers: HashSet<H256>,
    pub preverified: HashSet<H256>,
    pub requests: Vec<HashMap<H256, HeaderRequest>>,
    pub pending: Vec<BlockHeader>,
}

impl HeaderDownloader {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        let chunks =
            Self::prepare_requests(PreverifiedHashesConfig::new("mainnet").unwrap().hashes);

        Self {
            coordinator,
            bad_headers: HashSet::new(),
            preverified: HashSet::new(),
            requests: chunks,
            pending: Vec::new(),
        }
    }

    pub async fn spin(&mut self) -> anyhow::Result<()> {
        let mut stream = self.coordinator.recv_headers().await.unwrap();
        let chunks = self.requests.clone().into_iter();

        for mut chunk in chunks {
            let mut timer = tokio::time::interval(INTERVAL);

            while chunk.len() > 0 {
                futures_util::select! {
                    msg = stream.next().fuse() => {
                        let msg = match msg.unwrap().msg {
                            Message::BlockHeaders(value) => if value.headers.len() == 192
                                && chunk.contains_key(&value.headers[0].clone().hash()) {
                                value
                            } else {
                                continue
                            },
                            _ => continue,
                        };
                        chunk.remove(&msg.headers[0].clone().hash());
                        self.pending.extend(msg.headers);
                    }
                    _ = timer.tick().fuse() => {
                        let c = self.coordinator.clone();
                        let mut tasks = Vec::new();
                        chunk
                            .clone()
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
