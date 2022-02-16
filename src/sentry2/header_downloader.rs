use std::time::Duration;

use crate::models::BlockHeader;

const CHUNK_SIZE: usize = 512;
const INTERVAL: Duration = Duration::from_secs(10);

pub type Headers = Vec<BlockHeader>;
