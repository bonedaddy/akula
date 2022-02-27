use akula::{
    sentry::chain_config::ChainConfig,
    sentry2::{body_downloader::BodyDownloader, SentryClient},
};
use anyhow::Context;
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tonic::transport::Channel;
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
struct Args {
    /// Akula data directory
    #[clap(long = "datadir")]
    data_dir: PathBuf,

    /// Chain
    #[clap(long = "chain")]
    chain: String,

    /// Sentry address
    #[clap(long = "addr")]
    addr: String,

    /// Header downloader-only mode.
    #[clap(long = "header-only")]
    header_only: bool,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(
            std::env::var(EnvFilter::DEFAULT_ENV)
                .unwrap_or_default()
                .is_empty()
                .then(|| EnvFilter::new("akula=trace"))
                .unwrap_or_else(EnvFilter::from_default_env),
        )
        .init();

    let chain_config: ChainConfig = args.chain.try_into()?;
    let sentry =
        SentryClient::connect(Channel::builder(args.addr.parse()?).http2_adaptive_window(true))
            .await?;
    std::fs::create_dir_all(args.data_dir.as_path())?;
    std::fs::create_dir_all(&args.data_dir.join("etl-temp"))?;
    let db = akula::kv::new_database(args.data_dir.as_path())?;
    let txn = db.begin_mutable()?;
    akula::genesis::initialize_genesis(
        &txn,
        &*Arc::new(
            tempfile::tempdir_in(&args.data_dir.join("etl-temp"))
                .context("failed to create ETL temp dir")?,
        ),
        chain_config.chain_spec().clone(),
    )?;
    txn.commit()?;

    let mut bd = BodyDownloader::new(sentry, chain_config, db.begin()?)?;
    bd.step(db.begin_mutable()?).await?;

    Ok(())
}
