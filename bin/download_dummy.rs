use akula::{
    sentry::chain_config::ChainsConfig,
    sentry2::{header_downloader::HeaderDownloader, Coordinator, SentryClient},
};
use std::sync::Arc;
use tracing_subscriber::{prelude::*, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info,rpc=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();

    let chain_config = ChainsConfig::default().get("mainnet")?;
    let coordinator = Coordinator::new(
        vec![SentryClient::connect("http://localhost:8000").await?],
        chain_config,
        0,
    );
    let mut hd = HeaderDownloader::new(Arc::new(coordinator));
    hd.spin().await?;
    Ok(())
}
