use crate::{
    downloader::PreverifiedHashesConfig,
    models::{BlockHeader, BlockNumber, H256},
    sentry2::{types::*, Coordinator, SentryCoordinator},
};
use futures_util::FutureExt;
use hashbrown::HashSet;
use std::{collections::VecDeque, sync::Arc, time::Duration};
use tokio::sync::{broadcast, mpsc};
use tracing::info;

use super::broadcast_stream;

const CHUNK_SIZE: usize = 32;

const BATCH_SIZE: usize = 98304 * 4;

const INTERVAL: Duration = Duration::from_secs(10);

pub struct HeaderDownloader {
    coordinator: Arc<Coordinator>,
    preverified: HashSet<H256>,
    requests: Vec<HeaderRequest>,
    pending: Vec<BlockHeader>,
}

async fn new_worker(
    coordinator: Arc<Coordinator>,
    rx: async_channel::Receiver<HeaderRequest>,
    tx: mpsc::Sender<Vec<BlockHeader>>,
    mut msg_rx: broadcast::Receiver<InboundMessage>,
) {
    let mut timer = tokio::time::interval(INTERVAL);
    let mut req: Option<HeaderRequest>;
    match rx.recv().await {
        Ok(v) => req = Some(v),
        _ => unreachable!(),
    }
    coordinator
        .send_header_request(req.clone().unwrap())
        .await
        .unwrap();

    loop {
        futures_util::select! {
             _ = timer.tick().fuse() => {
                coordinator.send_header_request(req.clone().unwrap()).await.unwrap();
            }
            msg = msg_rx.recv().fuse() => {
                let msg = match msg {
                    Ok(v) => v,
                    _ => continue,
                };
                match msg.msg {
                    Message::BlockHeaders(value) => {
                        let headers = value.headers;
                        if headers.len() != 192 {
                            continue;
                        }
                        if headers[0].clone().number.0 == req.clone().unwrap().number.0 {
                            tx.send(headers).await.unwrap();
                            match rx.recv().await {
                                Ok(v) => req = Some(v),
                                _ => return,
                            }
                        }
                        coordinator.send_header_request(req.clone().unwrap()).await.unwrap();
                    }
                    _ => unreachable!(),
                }
            }
        }
    }
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
        let mut queue = VecDeque::new();
        self.requests
            .clone()
            .into_iter()
            .for_each(|request| queue.push_back(request));

        let (tx, rx) = async_channel::bounded::<HeaderRequest>(CHUNK_SIZE * 4);
        let (msg_tx, _) = broadcast::channel::<InboundMessage>(CHUNK_SIZE * 4);
        let (tx_headers, mut rx_headers) = mpsc::channel(CHUNK_SIZE * 2);

        tokio::spawn(broadcast_stream(
            self.coordinator.clone().recv_headers().await?,
            msg_tx.clone(),
        ));
        let pool = (0..CHUNK_SIZE)
            .map(|_| {
                let c = self.coordinator.clone();
                let rx = rx.clone();
                let msg_rx = msg_tx.subscribe();
                tokio::spawn(new_worker(c, rx, tx_headers.clone(), msg_rx))
            })
            .collect::<Vec<_>>();
        tokio::spawn(async move {
            for _ in 0..CHUNK_SIZE * 2 {
                tx.send(queue.pop_front().unwrap()).await.unwrap();
            }
            info!("Header downloader started");

            let mut headers = Vec::new();
            while let Some(value) = rx_headers.recv().await {
                headers.extend(value);
                if queue.is_empty() {
                    return;
                }
                tx.send(queue.pop_front().unwrap()).await.unwrap();
                info!("{} of {} headers", headers.len(), 13824000);
            }
        });

        for w in pool {
            w.await.unwrap();
        }
        Ok(())
    }

    fn prepare_requests(hashes: Vec<H256>) -> Vec<HeaderRequest> {
        let requests = hashes
            .into_iter()
            .enumerate()
            .map(|(i, hash)| {
                HeaderRequest::new(Some(hash), BlockNumber(i as u64 * 192), 192, 0, false)
            })
            .collect::<Vec<_>>();
        requests
    }
}
