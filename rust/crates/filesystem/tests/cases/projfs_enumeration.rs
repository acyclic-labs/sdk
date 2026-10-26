//! Concurrent ProjFS enumeration against the compiled filesystem library.

#![cfg(windows)]

use acyclic_fs::{
    CancellationToken, Fs, IdempotencyKey, LocalOptions, NativeMaterializationWork, WorkBudget,
};
use bytes::Bytes;
use std::collections::HashSet;
use std::sync::{Arc, Barrier};

fn setup_work() -> Result<NativeMaterializationWork, Box<dyn std::error::Error>> {
    acyclic_fs::initialize_native_mount_services()?;
    Ok(
        NativeMaterializationWork::new(WorkBudget::UNBOUNDED, &CancellationToken::new())
            .map_err(|(error, _)| error)?,
    )
}

async fn local_workspace(
    root: &std::path::Path,
    name: &str,
    work: &NativeMaterializationWork,
) -> Result<
    acyclic_fs::Workspace<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    Box<dyn std::error::Error>,
> {
    Ok(Fs::local(LocalOptions::new(root.join("state")))
        .await?
        .create_workspace(name, WorkBudget::UNBOUNDED, work.cancellation())
        .await
        .map_err(|failure| failure.error)?
        .value)
}

#[path = "../support/native_access.rs"]
mod native_access;
use native_access::manual_mount_options;

include!("../../src/native_mount/projfs/tests/enumeration.rs");
