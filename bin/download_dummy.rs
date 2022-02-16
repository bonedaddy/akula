use akula::{
    binutil::AkulaDataDir,
    sentry::chain_config::ChainsConfig,
    sentry2::{header_downloader::HeaderDownloader, Coordinator, SentryClient},
};
use anyhow::Context;
use clap::Parser;
use std::sync::Arc;
use tracing::{span, Level};
use tracing_subscriber::{prelude::*, EnvFilter};

#[derive(Parser)]
#[clap(name = "Akula", about = "Next-generation Ethereum implementation.")]
pub struct Opt {
    /// Path to Akula database directory.
    #[clap(long = "datadir", help = "Database directory path", default_value_t)]
    pub data_dir: AkulaDataDir,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let env_filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("akula=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer().with_target(false))
        .with(env_filter)
        .init();
    let chain_config = ChainsConfig::default().get("mainnet")?;
    let opt = Opt::parse();
    std::fs::create_dir_all(&opt.data_dir.0)?;
    let akula_chain_data_dir = opt.data_dir.chain_data_dir();
    let etl_temp_path = opt.data_dir.etl_temp_dir();
    let _ = std::fs::remove_dir_all(&etl_temp_path);
    std::fs::create_dir_all(&etl_temp_path)?;
    let etl_temp_dir =
        Arc::new(tempfile::tempdir_in(&etl_temp_path).context("failed to create ETL temp dir")?);
    let db = akula::kv::new_database(&akula_chain_data_dir)?;
    {
        let span = span!(Level::INFO, "", " Genesis initialization ");
        let _g = span.enter();
        let txn = db.begin_mutable()?;
        if akula::genesis::initialize_genesis(
            &txn,
            &*etl_temp_dir,
            chain_config.chain_spec().clone(),
        )? {
            txn.commit()?;
        }
    }

    let coordinator = Arc::new(Coordinator::new(
        vec![SentryClient::connect("http://127.0.0.1:8001").await?],
        chain_config,
        0,
    ));
    let mut hd = HeaderDownloader::new(coordinator);
    hd.runtime(&db).await?;

    Ok(())
}
