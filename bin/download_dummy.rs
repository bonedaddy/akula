use std::{path::PathBuf, sync::Arc};

use akula::{
    models::BlockNumber,
    sentry::chain_config::ChainsConfig,
    sentry2::{downloader::HeaderDownloader, SentryClient},
};
use anyhow::Context;
use tracing::{info, Level};

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(Level::DEBUG)
        .init();

    let chain_config = ChainsConfig::default().get("mainnet").unwrap();
    let sentry = SentryClient::connect("http://localhost:8000")
        .await
        .unwrap();
    std::fs::create_dir_all(PathBuf::from("./db")).unwrap();
    std::fs::create_dir_all(PathBuf::from("./db/temp")).unwrap();
    let db = akula::kv::new_database(PathBuf::from("./db").as_path()).unwrap();
    let txn = db.begin_mutable().unwrap();
    let etl_temp_dir = Arc::new(
        tempfile::tempdir_in("./db/temp")
            .context("failed to create ETL temp dir")
            .unwrap(),
    );

    akula::genesis::initialize_genesis(&txn, &*etl_temp_dir, chain_config.chain_spec().clone())
        .unwrap();
    txn.commit().unwrap();

    info!("DB initialized");
    let mut hd = HeaderDownloader::new(sentry, chain_config, BlockNumber(0));
    hd.runtime(&db).await.unwrap();
}
