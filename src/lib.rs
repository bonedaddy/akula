#![feature(
    bool_to_option,
    generator_trait,
    generators,
    let_else,
    map_first_last,
    never_type,
    step_trait,
    type_alias_impl_trait,
    integer_atomics,
    async_closure,
    const_try,
    inline_const,
    const_option,
    adt_const_params,
    let_chains,
    associated_type_bounds,
    const_trait_impl,
    const_impl_trait,
    allow_internal_unstable,
    const_fn_trait_bound,
    const_precise_live_drops,
    const_result,
    const_convert,
    entry_insert,
    const_mut_refs,
    derive_default_enum,
    control_flow_enum,
    const_num_from_num,
    array_chunks,
    slice_as_chunks
)]
#![recursion_limit = "256"]
#![allow(
    dead_code,
    incomplete_features,
    clippy::mutable_key_type,
    clippy::type_complexity,
    clippy::unused_io_amount
)]

pub mod accessors;
#[doc(hidden)]
pub mod binutil;
mod bitmapdb;
pub mod chain;
pub mod consensus;
pub mod crypto;
pub mod downloader;
pub mod etl;
pub mod execution;
pub mod kv;
pub mod models;
pub mod res;
pub mod sentry;
pub mod sentry2;
pub mod stagedsync;
pub mod stages;
mod state;
pub mod trie;
pub(crate) mod util;

pub use stagedsync::stages::StageId;
pub use state::*;
pub use util::*;
