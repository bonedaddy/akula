pub mod body_downloader;
mod coordinator;
pub mod downloader;
pub mod types;

pub use coordinator::*;

pub const CHUNK_SIZE: usize = 1 << 10;
pub const BATCH_SIZE: usize = 3 << 15;
