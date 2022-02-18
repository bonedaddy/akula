#![allow(unreachable_code)]
use crate::{
    models::{BlockNumber, H256},
    sentry2::{
        types::{HeaderRequest, Message, MessageId},
        Coordinator, CoordinatorStream, SentryCoordinator,
    },
};
use futures_util::{select, FutureExt, StreamExt};
use hashbrown::{HashMap, HashSet};
use hashlink::LinkedHashSet;
use std::{marker::PhantomData, sync::Arc, time::Duration};
use tracing::info;

pub struct HeaderDownloader<'a> {
    /// Sentry connector.
    pub sentry: Arc<Coordinator>,
    /// Our current height.
    pub height: BlockNumber,
    /// The set of newly seen hashes.
    pub seen_announces: LinkedHashSet<H256>,
    /// Mapping from the parent hash to the set of children hashes.
    pub childs_table: HashMap<H256, HashSet<H256>>,
    /// Mapping from the block hash to it's number and optionally total difficulty.
    pub blocks_table: HashMap<H256, (BlockNumber, Option<u128>)>,
    /// The vector of hashes known to belong to the canonical chain.
    pub preverified: Vec<H256>,
    /// The hash known to belong to the canonical chain(with exception for genesis and preverified hashes), defaults to 0 because we haven't yet discovered canonical chain.
    pub canonical_marker: H256,

    _phantom: PhantomData<&'a ()>,
}

impl<'a> HeaderDownloader<'a> {
    pub fn new(sentry: Arc<Coordinator>, height: BlockNumber) -> Self {
        Self {
            sentry,
            height,
            seen_announces: LinkedHashSet::new(),
            childs_table: HashMap::new(),
            blocks_table: HashMap::new(),
            preverified: Vec::new(),
            canonical_marker: H256::default(),
            _phantom: PhantomData,
        }
    }

    pub async fn runtime(
        &'_ mut self, // pub async fn runtime<E: EnvironmentKind>(
                      //     &'_ mut self,
                      //     db: &'_ MdbxEnvironment<E>
    ) -> anyhow::Result<()> {
        //let _ = db.begin_mutable()?;
        let mut stream = self.sentry.recv().await?;
        let mut ticker = tokio::time::interval(Duration::from_secs(60));

        loop {
            select! {
                _ = ticker.tick().fuse() => {
                    let _ = self.sentry.ping().await;
                    info!("Ping sent {:#?}", self.childs_table);

                    let mut canonical = Vec::new();
                    self.childs_table.into_iter().for_each(|(parent, childs)| {
                        let mut chain = vec![parent];
                        if childs.len() > 1 {
                            for c in childs.into_iter() {
                                if self.childs_table.get(&c).unwrap().len() == 1 {
                                    chain.push(c);
                                }
                            }
                        } else {
                             chain.push(childs.into_iter().next().unwrap())
                        }
                        let mut hash = *chain.last().unwrap();
                        while let Some(child) = self.childs_table.get(&hash) {
                            if child.len() > 1 {
                                return;
                            }
                            chain.push(*child.into_iter().next().unwrap());
                            hash = *child;
                        }
                        if chain.len() > canonical.len() {
                            canonical = chain;
                        }
                    });
                    if canonical.len() >= 4 {
                        info!("Successfully found canonical chain: {:#?}", canonical);
                        self.canonical_marker = *canonical.last().unwrap();
                        break;
                    } else if !canonical.is_empty() {
                        let hash = canonical.last().unwrap().clone();
                        self.sentry.send_header_request(HeaderRequest{
                            hash: Some(hash),
                            number: self.blocks_table.get(&hash).unwrap().0,
                            limit: 1,
                            skip: 1,
                            ..Default::default()
                        }).await?;
                    }
                }
                msg = stream.next().fuse() => {
                    if msg.is_none() {
                        continue;
                    }

                    match msg.unwrap().msg {
                        Message::NewBlockHashes(value) => {
                            if !value.0.is_empty()
                                && !self.seen_announces.contains(&value.0.last().unwrap().hash)
                                && value.0.last().unwrap().number >= self.height {

                                let block = &value.0.last().unwrap();
                                self.seen_announces.insert(block.hash);
                                self.sentry.send_header_request(HeaderRequest {
                                    hash: Some(block.hash),
                                    number: block.number,
                                    limit: 1,
                                    ..Default::default()
                                }).await?;
                            }
                        },
                        Message::BlockHeaders(value) => {
                            if !value.headers.is_empty()
                                && value.headers.last().unwrap().number > self.sentry.last_ping() {

                                let header = value.headers.last().unwrap();
                                let hash = header.hash();
                                self.childs_table.entry(header.parent_hash).or_insert_with(HashSet::new).insert(hash);
                                self.blocks_table.insert(hash, (header.number, None));
                                self.sentry.send_header_request(HeaderRequest {
                                    hash: Some(hash),
                                    number: header.number,
                                    limit: 1,
                                    skip: 1,
                                    ..Default::default()
                                }).await?;
                                if self.height < header.number {
                                    self.height = header.number;
                                }
                            }
                        },
                        Message::NewBlock(value) => {
                            info!("New block: {:#?}", value.block.header.number);

                            let (
                                hash,
                                number,
                                parent_hash,
                            ) = (value.block.header.hash(), value.block.header.number, value.block.header.parent_hash);

                            self.childs_table.entry(parent_hash).or_insert_with(HashSet::new).insert(hash);
                            self.blocks_table.insert(hash, (number, Some(value.total_difficulty)));
                            self.sentry.send_header_request(HeaderRequest {
                                hash: Some(hash),
                                number,
                                limit: 1,
                                skip: 1,
                                ..Default::default()
                            }).await?;
                            self.height = number;
                        },
                        _ => continue,
                    }
                }
            }
        }

        self.prepare_requests(stream).await?;

        Ok(())
    }

    async fn prepare_requests(&'_ mut self, mut stream: CoordinatorStream) -> anyhow::Result<()> {
        let mut ticker = tokio::time::interval(Duration::from_secs(15));
        let (height, td) = self.blocks_table.get(&self.canonical_marker).unwrap();
        if td.is_some() {
            let _ = self
                .sentry
                .update_head(*height, self.canonical_marker, td.unwrap().into())
                .await;
        };
        let mut hashes = HashSet::new();
        hashes.insert(self.canonical_marker);
        let mut pending_request =
            HeaderRequest::new(Some(self.canonical_marker), *height, 576, 575, true);
        loop {
            select! {
                _ = ticker.tick().fuse() => {
                    self.sentry.send_header_request(pending_request.clone()).await?;
                    info!("Hashes {:#?}", hashes);
                }
                msg = stream.next().fuse() => {
                    match msg.unwrap().msg {
                        Message::BlockHeaders(value) => {
                            info!("Headers {:#?}", &value);
                            if value.headers.len() == 1 && value.headers.last().unwrap().number > self.sentry.last_ping() {
                                let header = value.headers.last().unwrap();
                                let hash = header.hash();
                                if hashes.insert(hash) {
                                    continue;
                                }
                                pending_request = HeaderRequest::new(Some(hash), header.number, 576, 575, true);
                            }
                        }
                        _ => continue,
                    }
                }
            }
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use tracing::Level;

    use crate::{sentry::chain_config::ChainsConfig, sentry2::SentryClient};

    #[allow(unused_imports)]
    use super::*;
    #[tokio::test(flavor = "multi_thread")]
    async fn it_works() {
        // tracing_subscriber::fmt()
        //     .with_max_level(Level::DEBUG)
        //     .init();

        // let chain_config = ChainsConfig::default().get("mainnet").unwrap();
        // let sentry = SentryClient::connect("http://localhost:8000")
        //     .await
        //     .unwrap();

        // let mut coordinator = Arc::new(Coordinator::new(vec![sentry], chain_config, 0));
        // let mut hd = HeaderDownloader::new(coordinator, BlockNumber(0));
        // hd.runtime().await;
    }
}
