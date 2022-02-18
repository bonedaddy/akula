use akula::{
    models::BlockNumber,
    sentry::chain_config::ChainsConfig,
    sentry2::{downloader::HeaderDownloader, SentryClient},
};
use tracing::Level;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let chain_config = ChainsConfig::default().get("mainnet").unwrap();
    let sentry = SentryClient::connect("http://localhost:8000")
        .await
        .unwrap();
    let mut hd = HeaderDownloader::new(sentry, chain_config, BlockNumber(0));
    hd.runtime().await.unwrap();
}

