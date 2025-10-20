use std::path::PathBuf;
use std::sync::Arc;

use crate::DatabasePath;
use clap::Args;
use firewood::v2::api;
use firewood_storage::{CacheReadStrategy, FileBacked, NodeStore};
use nonzero_ext::nonzero;

#[derive(Debug, Args)]
pub struct Options {
    #[command(flatten)]
    pub database: DatabasePath,
}

pub(super) fn run(opts: &Options) -> Result<(), api::Error> {
    let db_path = PathBuf::from(&opts.database.dbpath);
    let node_cache_size = nonzero!(1usize);
    let free_list_cache_size = nonzero!(1usize);

    let fb = FileBacked::new(
        db_path,
        node_cache_size,
        free_list_cache_size,
        false,
        false,                         // don't create if missing
        CacheReadStrategy::WritesOnly, // we scan the database once - no need to cache anything
    )?;
    let storage = Arc::new(fb);
    let mut nodestore = NodeStore::open(storage)?;
    nodestore.clear_freelist()?;

    Ok(())
}
