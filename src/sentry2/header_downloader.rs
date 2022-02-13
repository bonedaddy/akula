use crate::{
    downloader::PreverifiedHashesConfig,
    models::{BlockHeader, BlockNumber, H256},
    sentry2::{types::*, Coordinator, SentryCoordinator},
};
use anyhow::anyhow;
use futures_util::FutureExt;
use hashbrown::HashSet;
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    slice::ParallelSlice,
};
use std::{sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc, Semaphore};
use tracing::{info, warn};

use super::broadcast_stream;

const CHUNK_SIZE: usize = 32;

const BATCH_SIZE: usize = 98304 * 4;

const INTERVAL: Duration = Duration::from_secs(5);

pub struct HeaderDownloader {
    coordinator: Arc<Coordinator>,
    preverified: HashSet<H256>,
    requests: Vec<Vec<HeaderRequest>>,
    pending: Vec<BlockHeader>,
}

fn dummy_verify(headers: &mut Vec<BlockHeader>) -> anyhow::Result<()> {
    headers.dedup_by(|a, b| (a.number == b.number) || (a.hash() == b.hash()));
    let mut last_number = 0;
    let mut last_timestamp = 0;
    for header in headers.iter() {
        if header.number.0 <= last_number || header.timestamp <= last_timestamp {
            return Err(anyhow!("invalid header: {:#?}", header));
        };
        last_number = header.number.0;
        last_timestamp = header.timestamp;
    }
    Ok(())
}

async fn worker(
    c: Arc<Coordinator>,
    req: HeaderRequest,
    mut rx: broadcast::Receiver<InboundMessage>,
    tx: mpsc::Sender<Vec<BlockHeader>>,
) {
    let mut timer = tokio::time::interval(INTERVAL);

    loop {
        futures_util::select! {
            _ = timer.tick().fuse() => {
                let _ = c.send_header_request(req.clone()).await;
            }
            msg = rx.recv().fuse() => {
                let msg = msg.unwrap();
                let peer_id = msg.peer_id;
                match msg.msg {
                    Message::BlockHeaders(v) => {
                        if v.headers.len() != 192
                        {
                            warn!("Penalizing peer for invalid payload {:#?}", peer_id);
                            let penalty = Penalty::new(peer_id, PenaltyKind::default());
                            c.penalize_peer(penalty).await.unwrap();
                            continue;
                        }

                        // FIXME: Replace with another concurrency primitive, because rn each worker receives all messages.
                        if v.headers.first().unwrap().hash() == req.hash.unwrap() {
                            tx.send(v.headers).await.unwrap();
                            return;
                        }
                    }
                    _ => unreachable!(),
                }

            }
        }
    }
}

impl HeaderDownloader {
    pub fn new(coordinator: Arc<Coordinator>) -> Self {
        // FIXME: Replace with normal initialization.
        let hashes = PreverifiedHashesConfig::new("mainnet").unwrap().hashes;
        let requests = Self::prepare_requests(hashes.clone());
        let preverified = hashes
            .into_iter()
            .map(|h| h.into())
            .collect::<HashSet<H256>>();

        Self {
            coordinator,
            preverified,
            requests,
            pending: Vec::new(),
        }
    }

    pub async fn spin(&mut self) -> anyhow::Result<()> {
        let (msg_tx, _) = broadcast::channel::<InboundMessage>(CHUNK_SIZE * 4);
        let c = self.coordinator.clone();
        tokio::spawn(broadcast_stream::<InboundMessage, _>(
            c.recv_headers().await?,
            msg_tx.clone(),
        ));

        let (headers_tx, mut headers_rx) = mpsc::channel(CHUNK_SIZE * 16);
        let sema = Arc::new(Semaphore::new(CHUNK_SIZE));

        for chunk in self.requests.clone().into_iter() {
            let mut join_handler = Vec::new();

            for request in chunk {
                let permit = sema.clone().acquire_owned().await?;
                let c = self.coordinator.clone();
                let msg_rx = msg_tx.subscribe();
                let headers_tx = headers_tx.clone();
                let request = request.clone();

                join_handler.push(tokio::spawn(async move {
                    worker(c, request, msg_rx, headers_tx).await;
                    drop(permit);
                }))
            }

            for task in join_handler {
                task.await.unwrap();
            }

            while let Ok(h) = headers_rx.try_recv() {
                self.pending.extend(h);
            }
            if self.pending.len() >= BATCH_SIZE {
                let mut headers = self.pending.drain(..BATCH_SIZE).collect::<Vec<_>>();
                let _ = dummy_verify(&mut headers).unwrap();
                info!("Successfully verified {} headers", BATCH_SIZE);
            }
            info!("Downloading BlockHeaders {}", self.pending.len());
        }

        Ok(())
    }

    fn prepare_requests(hashes: Vec<H256>) -> Vec<Vec<HeaderRequest>> {
        let chunks = hashes
            .into_par_iter()
            .enumerate()
            .map(|(i, hash)| {
                HeaderRequest::new(Some(hash), BlockNumber(i as u64 * 192), 192, 0, false)
            })
            .collect::<Vec<_>>()
            .par_chunks(32)
            .map(|chunk| chunk.into())
            .collect::<Vec<Vec<_>>>();
        chunks
    }
}
