use super::*;
use crate::model::{Lifecycle, VolumeConfig};
use crate::{CancellationToken, Fs, WorkBudget};
use acyclic_stream::OwnedBytes;
use std::error::Error;
#[cfg(all(feature = "native-mount", not(target_arch = "wasm32")))]
use std::path::Path;
use std::sync::Arc;

macro_rules! value {
    ($result:expr) => {
        $result
            .map(|receipt| receipt.value)
            .map_err(|failure| failure.error)
    };
}

macro_rules! unbounded {
    ($receiver:expr, $method:ident($($arg:expr),* $(,)?)) => {
        value!($receiver.$method($($arg,)* WorkBudget::UNBOUNDED, &CancellationToken::new()).await)
    };
}

fn allow<E>(
    _: WorkBudget,
) -> std::future::Ready<crate::MeasuredResult<crate::OperationReceipt<()>, E>> {
    std::future::ready(Ok(crate::OperationReceipt {
        value: (),
        work: crate::WorkCounters::UNCHARGED,
    }))
}

async fn fork_workspace<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    source: &Workspace<A, O>,
    name: &str,
    options: ForkOptions<A, O>,
) -> Result<Workspace<A, O>, WorkspaceError> {
    value!(
        source
            .fork(
                name,
                options,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                allow
            )
            .await
    )
}

async fn commit<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    transaction: &mut Transaction<A, O>,
) -> Result<TransactionCommit<A, O>, WorkspaceError> {
    value!(
        transaction
            .commit::<WorkspaceError, _, _>(
                None,
                crate::PublicationPermit::Unrestricted,
                WorkBudget::UNBOUNDED,
                &CancellationToken::new(),
                allow
            )
            .await
    )
}

mod change_sets;
mod fork_selection;
mod generation;
mod identity;
mod joins;
mod materialization;
mod merge_drivers;
mod operation_identity;
mod publication;
mod publication_guards;
mod reads;
mod rebase;
mod restore;
mod sibling_directories;
mod sibling_files;
mod transactions;

fn selected_transaction_budget() -> WorkBudget {
    WorkBudget {
        authority_records_read: 1_000_000,
        authority_records_appended: 1_000_000,
        authority_bytes_read: 1_000_000,
        authority_bytes_written: 1_000_000,
        object_probes: 1_000_000,
        backend_read_operations: 1_000_000,
        backend_write_operations: 1_000_000,
        durability_operations: 1_000_000,
        page_reads: 1_000_000,
        page_writes: 1_000_000,
        object_bytes_read: 1_000_000,
        object_bytes_written: 1_000_000,
        bytes_hashed: 1_000_000,
        bytes_copied: 1_000_000,
        bytes_encoded: 1_000_000,
        source_bytes_read: 1_000_000,
        source_path_components: 1_000_000,
        source_entries_visited: 1_000_000,
        output_bytes: 1_000_000,
        items_examined: 1_000_000,
        items_returned: 1_000_000,
        allocation_operations: 1_000_000,
        peak_allocation_bytes: 64 * 1024 * 1024,
        materializations: 1_000_000,
    }
}

#[derive(Debug, thiserror::Error)]
enum OrdinaryOwnerError {
    #[error(transparent)]
    Sdk(WorkspaceError),
    #[error("ordinary owner denied publication")]
    Denied {
        task: [u8; 16],
        revision: u64,
        expired: bool,
    },
}

impl From<WorkspaceError> for OrdinaryOwnerError {
    fn from(error: WorkspaceError) -> Self {
        Self::Sdk(error)
    }
}

impl From<FsError> for OrdinaryOwnerError {
    fn from(error: FsError) -> Self {
        Self::Sdk(error.into())
    }
}

async fn generation_changes<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    from: &Generation<A, O>,
    to: &Generation<A, O>,
    maximum: u32,
) -> Result<ChangeSet<A, O>, WorkspaceError> {
    from.diff_to(
        to,
        maximum,
        WorkBudget::UNBOUNDED,
        &CancellationToken::new(),
    )
    .await
    .map_err(|failure| failure.error)
}
