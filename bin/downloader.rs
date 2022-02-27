use akula::{
    sentry::chain_config::ChainConfig,
    sentry2::{body_downloader::BodyDownloader, downloader::HeaderDownloader, SentryClient},
};
use anyhow::Context;
use clap::Parser;
use std::{path::PathBuf, sync::Arc};
use tonic::transport::Channel;
use tracing_subscriber::EnvFilter;

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

const MAX_WINDOW_SIZE: Option<u32> = Some((1 << 31) - 1);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let chain_config: ChainConfig = args.chain.try_into()?;
    let sentry = SentryClient::connect(
        Channel::builder(args.addr.parse()?)
            .initial_connection_window_size(MAX_WINDOW_SIZE)
            .initial_stream_window_size(MAX_WINDOW_SIZE),
    )
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

    let mut hd = HeaderDownloader::new(sentry.clone(), chain_config.clone(), db.begin()?)?;
    hd.step(db.begin_mutable()?).await?;
    let mut bd = BodyDownloader::new(sentry, chain_config, db.begin()?)?;
    bd.step(db.begin_mutable()?).await?;

    Ok(())
}
