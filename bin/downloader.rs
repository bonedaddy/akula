use std::{path::PathBuf, sync::Arc};

use akula::{
    sentry::chain_config::ChainsConfig,
    sentry2::{body_downloader::BodyDownloader, downloader::HeaderDownloader, SentryClient},
};
use anyhow::Context;
use clap::Parser;
use tracing::info;
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let chain_config = match args.chain.as_str() {
        "mainnet" | "ethereum" => ChainsConfig::default().get("mainnet")?,
        "ropsten" => ChainsConfig::default().get("ropsten")?,
        _ => panic!("Unsupported chain"),
    };

    let sentry = SentryClient::connect(args.addr).await?;
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

    info!("DB initialized");
    // let mut hd = HeaderDownloader::new(sentry.clone(), db.begin()?, chain_config.clone())?;
    // hd.step(db.begin_mutable()?).await?;

    let mut bd = BodyDownloader::new(sentry, db.begin()?, chain_config)?;
    bd.step(db.begin_mutable()?).await?;

    Ok(())
}
