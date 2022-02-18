use crate::{
    models::{Block, BlockNumber, H256, U256},
    sentry::chain_config::ChainConfig,
    sentry2::types::*,
};
use async_trait::async_trait;
use auto_impl::auto_impl;
use ethereum_interfaces::sentry as grpc_sentry;
use futures_core::Stream;
use futures_util::{FutureExt, StreamExt};
use std::{
    pin::Pin,
    sync::{atomic::AtomicU64, Arc},
};
use tokio::sync::broadcast;

pub struct SentryPool(Vec<SentryClient>);

impl From<SentryClient> for SentryPool {
    fn from(sentry: SentryClient) -> Self {
        Self(vec![sentry])
    }
}

impl const From<Vec<SentryClient>> for SentryPool {
    fn from(sentry: Vec<SentryClient>) -> Self {
        Self(sentry)
    }
}

pub type SentryClient = grpc_sentry::sentry_client::SentryClient<tonic::transport::Channel>;

#[derive(Clone)]
pub struct Coordinator {
    pub sentries: Vec<SentryClient>,
    //pub hd: Arc<HeaderDownloader>,
    pub genesis_hash: H256,
    network_id: u64,
    hard_forks: Vec<u64>,
    status: Arc<AtomicStatus>,
    pub ping_counter: Arc<AtomicU64>,
}

impl Coordinator {
    pub fn new<T: Into<SentryPool>>(
        sentry: T,
        chain_config: ChainConfig,
        block_height: u64,
    ) -> Self {
        let genesis_hash = chain_config.genesis_block_hash();
        let network_id = chain_config.network_id().0;
        let hard_forks = chain_config
            .chain_spec()
            .gather_forks()
            .into_iter()
            .map(|v| v.0)
            .collect::<Vec<_>>();
        let total_difficulty: H256 = chain_config
            .chain_spec()
            .genesis
            .seal
            .difficulty()
            .to_be_bytes()
            .into();
        let status = Status::new(block_height, genesis_hash, total_difficulty);
        Self {
            sentries: sentry.into().0,
            status: Arc::new(AtomicStatus::new(status)),
            genesis_hash,
            network_id,
            hard_forks,
            ping_counter: Arc::new(AtomicU64::new(1)),
        }
    }

    pub fn last_ping(&self) -> BlockNumber {
        BlockNumber(self.ping_counter.load(std::sync::atomic::Ordering::Relaxed))
    }
}

pub type SentryInboundStream = futures_util::stream::Map<
    tonic::Streaming<grpc_sentry::InboundMessage>,
    fn(Result<grpc_sentry::InboundMessage, tonic::Status>) -> Option<InboundMessage>,
>;

#[async_trait]
impl SentryCoordinator for Coordinator {
    fn update_status(&self, status: Status) -> anyhow::Result<()> {
        self.status.store(status);
        Ok(())
    }
    #[inline(always)]
    async fn set_status(&self) -> anyhow::Result<()> {
        let status = self.status.load();
        let status_data = grpc_sentry::StatusData {
            network_id: self.network_id,
            total_difficulty: Some(status.total_difficulty.into()),
            best_hash: Some(status.hash.into()),
            fork_data: Some(grpc_sentry::Forks {
                genesis: Some(self.genesis_hash.into()),
                forks: self.hard_forks.clone(),
            }),
            max_block: status.height,
        };
        let mut futs = Vec::new();
        self.sentries.iter().for_each(|sentry| {
            let mut sentry = sentry.clone();
            let status_data = status_data.clone();
            futs.push(async move {
                let _ = sentry.hand_shake(tonic::Request::new(())).await;
                let _ = sentry.set_status(status_data).await;
            });
        });

        futures_util::future::join_all(futs).await;

        Ok(())
    }
    async fn send_body_request(&self, req: BodyRequest) -> anyhow::Result<()> {
        let transform = move |_req: BodyRequest| -> anyhow::Result<Message> {
            Err(anyhow::anyhow!("Not implemented"))
        };
        let msg = transform(req).unwrap();
        let predicate =
            move || -> anyhow::Result<PeerFilter> { Err(anyhow::anyhow!("Not implemented")) };
        self.send_message(msg, predicate().unwrap()).await?;
        Ok(())
    }
    #[inline(always)]
    async fn send_header_request(&self, req: HeaderRequest) -> anyhow::Result<()> {
        self.set_status().await?;
        self.send_message(req.into(), PeerFilter::Random(50))
            .await?;

        Ok(())
    }
    async fn recv(&self) -> anyhow::Result<CoordinatorStream> {
        self.set_status().await?;

        Ok(futures_util::stream::select_all(
            futures_util::future::join_all(
                self.sentries
                    .iter()
                    .map(|s| {
                        recv_sentry(
                            s,
                            vec![
                                grpc_sentry::MessageId::from(MessageId::NewBlockHashes) as i32,
                                grpc_sentry::MessageId::from(MessageId::NewBlock) as i32,
                                grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32,
                            ],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await,
        ))
    }

    async fn recv_headers(&self) -> anyhow::Result<CoordinatorStream> {
        self.set_status().await?;

        Ok(futures_util::stream::select_all(
            futures_util::future::join_all(
                self.sentries
                    .iter()
                    .map(|s| {
                        recv_sentry(
                            s,
                            vec![grpc_sentry::MessageId::from(MessageId::BlockHeaders) as i32],
                        )
                    })
                    .collect::<Vec<_>>(),
            )
            .await,
        ))
    }

    async fn broadcast_block(&self, _block: Block, _total_difficulty: u128) -> anyhow::Result<()> {
        let _fut = async move || {};
        Ok(())
    }
    async fn propagate_new_block_hashes(
        &self,
        _block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()> {
        Ok(())
    }

    async fn propagate_transactions(&self, _transactions: Vec<H256>) -> anyhow::Result<()> {
        Ok(())
    }

    async fn update_head(
        &self,
        height: BlockNumber,
        hash: H256,
        total_difficultyy: U256,
    ) -> anyhow::Result<()> {
        let td = H256::from_slice(&total_difficultyy.to_be_bytes());
        let status = Status::new(height.0, hash, td);
        self.status.store(status);
        self.set_status().await?;

        Ok(())
    }

    async fn penalize_peer(&self, penalty: Penalty) -> anyhow::Result<()> {
        let mut tasks = Vec::new();

        self.sentries.iter().for_each(|s| {
            let sentry = s.clone();
            tasks.push(tokio::spawn(async move {
                sentry
                    .clone()
                    .penalize_peer(grpc_sentry::PenalizePeerRequest {
                        peer_id: Some(penalty.peer_id.into()),
                        penalty: 0,
                    })
                    .await
                    .unwrap();
            }));
        });

        for task in tasks {
            let _ = task.await;
        }

        Ok(())
    }

    #[inline(always)]
    async fn ping(&self) -> anyhow::Result<()> {
        let _ = self
            .send_header_request(HeaderRequest {
                start: BlockNumber(
                    self.ping_counter
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed),
                )
                .into(),
                limit: 1,
                ..Default::default()
            })
            .await;
        Ok(())
    }

    #[inline(always)]
    async fn send_message(&self, msg: Message, predicate: PeerFilter) -> anyhow::Result<()> {
        let data = grpc_sentry::OutboundMessageData {
            id: grpc_sentry::MessageId::from(msg.id()) as i32,
            data: rlp::encode(&msg).into(),
        };

        let proxy_message = async move |mut s: SentryClient,
                                        filter: PeerFilter,
                                        req: grpc_sentry::OutboundMessageData|
                    -> anyhow::Result<()> {
            s.hand_shake(tonic::Request::new(())).await?;
            match filter {
                PeerFilter::All => s.send_message_to_all(req).boxed(),
                PeerFilter::PeerId(peer_id) => s
                    .send_message_by_id(grpc_sentry::SendMessageByIdRequest {
                        data: Some(req),
                        peer_id: Some(peer_id.into()),
                    })
                    .boxed(),
                PeerFilter::MinBlock(min_block) => s
                    .send_message_by_min_block(grpc_sentry::SendMessageByMinBlockRequest {
                        data: Some(req),
                        min_block,
                    })
                    .boxed(),
                PeerFilter::Random(max_peers) => s
                    .send_message_to_random_peers(grpc_sentry::SendMessageToRandomPeersRequest {
                        data: Some(req),
                        max_peers,
                    })
                    .boxed(),
            }
            .await?;
            Ok(())
        };
        let sentries = self.sentries.clone();

        if sentries.len() > 1 {
            let rand_sentry = sentries
                .get(fastrand::usize(0..sentries.len() - 1))
                .unwrap();
            proxy_message(rand_sentry.clone(), predicate, data).await?;
        } else {
            proxy_message(sentries[0].clone(), predicate, data).await?;
        }

        Ok(())
    }

    async fn peer_count(&self) -> anyhow::Result<u64> {
        let peer_count: u64 = futures_util::future::join_all(
            self.sentries
                .iter()
                .map(
                    async move |s| -> anyhow::Result<grpc_sentry::PeerCountReply> {
                        let mut s = s.clone();
                        s.hand_shake(tonic::Request::new(())).await?;
                        Ok(s.peer_count(grpc_sentry::PeerCountRequest {})
                            .await?
                            .into_inner())
                    },
                )
                .collect::<Vec<_>>(),
        )
        .await
        .into_iter()
        .map(move |r| if let Ok(r) = r { r.count } else { 0 })
        .sum();

        Ok(peer_count)
    }
}

async fn recv_sentry(s: &SentryClient, ids: Vec<i32>) -> SingleSentryStream {
    let mut s = s.clone();
    s.hand_shake(tonic::Request::new(())).await.unwrap();

    SingleSentryStream(
        s.messages(grpc_sentry::MessagesRequest { ids })
            .await
            .unwrap()
            .into_inner(),
    )
}
pub struct SingleSentryStream(tonic::codec::Streaming<grpc_sentry::InboundMessage>);

pub type CoordinatorStream = futures_util::stream::SelectAll<SingleSentryStream>;

pub async fn broadcast_stream<T, S>(mut stream: S, tx: broadcast::Sender<T>)
where
    S: Stream<Item = T> + Unpin,
{
    while let Some(msg) = stream.next().await {
        let _ = tx.send(msg);
    }
}

impl tokio_stream::Stream for SingleSentryStream {
    type Item = InboundMessage;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match Pin::new(&mut this.0).poll_next(cx) {
            std::task::Poll::Ready(Some(Ok(value))) => {
                let msg = match decode_rlp_message(
                    match MessageId::try_from(grpc_sentry::MessageId::from_i32(value.id).unwrap()) {
                        Ok(id) => id,
                        _ => return std::task::Poll::Pending,
                    },
                    &value.data,
                ) {
                    Ok(v) => v,
                    _ => return std::task::Poll::Pending,
                };
                std::task::Poll::Ready(Some(InboundMessage {
                    msg,
                    peer_id: value.peer_id.unwrap_or_default().into(),
                }))
            }
            _ => std::task::Poll::Pending,
        }
    }
}

#[async_trait]
#[auto_impl(&, Box, Arc)]
pub trait SentryCoordinator: Send + Sync {
    fn update_status(&self, status: Status) -> anyhow::Result<()>;
    async fn set_status(&self) -> anyhow::Result<()>;
    async fn send_body_request(&self, req: BodyRequest) -> anyhow::Result<()>;
    async fn send_header_request(&self, req: HeaderRequest) -> anyhow::Result<()>;
    async fn recv(&self) -> anyhow::Result<CoordinatorStream>;
    async fn recv_headers(&self) -> anyhow::Result<CoordinatorStream>;
    async fn broadcast_block(&self, block: Block, total_difficulty: u128) -> anyhow::Result<()>;
    async fn propagate_new_block_hashes(
        &self,
        block_hashes: Vec<(H256, BlockNumber)>,
    ) -> anyhow::Result<()>;
    async fn propagate_transactions(&self, transactions: Vec<H256>) -> anyhow::Result<()>;
    async fn update_head(
        &self,
        height: BlockNumber,
        hash: H256,
        total_difficulty: U256,
    ) -> anyhow::Result<()>;
    async fn penalize_peer(&self, penalty: Penalty) -> anyhow::Result<()>;
    async fn ping(&self) -> anyhow::Result<()>;
    async fn send_message(&self, message: Message, predicate: PeerFilter) -> anyhow::Result<()>;
    async fn peer_count(&self) -> anyhow::Result<u64>;
}
