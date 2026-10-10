//! Finite per-attempt accounting over the Filesystem's measured engine APIs.
use super::*;
use acyclic_fs::{CancellationToken, OperationFailure, OperationReceipt, WorkBudget, WorkCounters, WorkspaceError};
use std::sync::Mutex;

/// The pinned Filesystem creation contract interprets maximum_work as the
/// allowance for each physical non-byte counter, and maximum_bytes as the
/// allowance for each physical byte counter. Counters aggregate across the
/// complete attempt, including work receipts on failed boundaries.
pub(crate) struct PreparationIo {
    budget: WorkBudget,
    work: Mutex<WorkCounters>,
    pub(crate) cancellation: CancellationToken,
}

impl PreparationIo {
    pub(crate) fn new(request: &crate::resource_preparation::ResourcePreparationRequest) -> Result<Self> {
        request.validate()?;
        let units = request.maximum_work;
        let bytes = request.maximum_bytes;
        let budget = WorkBudget {
            authority_records_read: units, authority_records_appended: units,
            authority_bytes_read: bytes, authority_bytes_written: bytes,
            object_probes: units, backend_read_operations: units, backend_write_operations: units,
            durability_operations: units, page_reads: units, page_writes: units,
            object_bytes_read: bytes, object_bytes_written: bytes,
            bytes_hashed: bytes, bytes_copied: bytes, bytes_encoded: bytes,
            source_bytes_read: bytes, source_path_components: units, source_entries_visited: units,
            output_bytes: bytes, items_examined: units, items_returned: units,
            allocation_operations: units, peak_allocation_bytes: bytes, materializations: units,
        };
        Ok(Self { budget, work: Mutex::new(WorkCounters::default()), cancellation: CancellationToken::new() })
    }

    pub(crate) fn remaining(&self) -> Result<WorkBudget> {
        self.work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?
            .remaining(self.budget).map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(crate) fn charge(&self, next: WorkCounters) -> Result<()> {
        let mut work = self.work.lock().map_err(|_| Error::Storage("creation IO accounting is poisoned".into()))?;
        // Retain failed-boundary work even when it exceeds the admitted limit.
        *work = work.checked_add(next).map_err(|error| Error::Invalid(error.to_string()))?;
        work.verify(self.budget).map_err(|error| Error::Invalid(error.to_string()))
    }

    pub(crate) fn receive<T>(&self, result: std::result::Result<OperationReceipt<T>, OperationFailure<WorkspaceError>>) -> Result<T> {
        match result {
            Ok(receipt) => { self.charge(receipt.work)?; Ok(receipt.value) }
            Err(failure) => { self.charge(*failure.work)?; Err(map_error(failure.error)) }
        }
    }

    pub(crate) fn encode<T: Serialize>(&self, value: &T, maximum_bytes: u64) -> Result<Vec<u8>> {
        let bytes = encode_record(value, maximum_bytes.min(self.remaining()?.bytes_encoded))?;
        self.charge(WorkCounters { bytes_encoded: bytes.len() as u64, ..WorkCounters::default() })?;
        Ok(bytes)
    }
}

pub(super) async fn open<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: &PreparationIo,
) -> Result<acyclic_fs::Workspace<A, O>> {
    reference.validate()?;
    host.validate_provider(reference.as_resource().provider())?;
    let name = std::str::from_utf8(reference.as_resource().key())
        .map_err(|_| Error::Invalid("workspace reference key must be UTF-8".into()))?;
    io.receive(host.filesystem.open_workspace_measured(name, io.remaining()?, &io.cancellation).await)
}

pub(super) async fn generation<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, workspace: &acyclic_fs::Workspace<A, O>,
    reference: &crate::resources::GenerationRef, io: &PreparationIo,
) -> Result<acyclic_fs::Generation<A, O>> {
    reference.validate()?;
    host.validate_provider(reference.as_resource().provider())?;
    let bytes: [u8; 32] = reference.as_resource().key().try_into()
        .map_err(|_| Error::Invalid("generation reference key must be 32 bytes".into()))?;
    io.receive(workspace.generation_measured(
        acyclic_fs::GenerationId::new(acyclic_fs::Digest::from_bytes(bytes)),
        io.remaining()?, &io.cancellation,
    ).await)
}

pub(super) async fn read<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, path: &str, maximum_bytes: u64,
    io: Option<&PreparationIo>,
) -> Result<bytes::Bytes> {
    let Some(io) = io else { return host.read(reference, None, path, maximum_bytes).await; };
    let workspace = open(host, reference, io).await?;
    io.receive(workspace.read_measured(path, maximum_bytes, io.remaining()?, &io.cancellation).await)
}

pub(super) async fn resolve<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    let Some(io) = io else { return host.resolve(reference).await; };
    let workspace = open(host, reference, io).await?;
    let generation = io.receive(workspace.head_measured(io.remaining()?, &io.cancellation).await)?;
    Ok(super::super::WorkspaceObservation {
        workspace: reference.clone(), generation: host.generation_ref(&generation)?,
    })
}

pub(super) async fn create<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, name: &str, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<()> {
    let Some(io) = io else {
        host.filesystem.create_workspace(name).await.map_err(map_error)?;
        return Ok(());
    };
    let operation = acyclic_fs::OperationId::from_bytes(
        super::super::filesystem_key(key).into_bytes(),
    );
    let lifecycle = if host.filesystem.capabilities().durable {
        acyclic_fs::Lifecycle::Durable
    } else {
        acyclic_fs::Lifecycle::Ephemeral
    };
    io.receive(host.filesystem.create_workspace_with_operation_measured(
        name, acyclic_fs::VolumeConfig::portable(lifecycle),
        operation, io.remaining()?, &io.cancellation,
    ).await)?;
    Ok(())
}

pub(super) async fn apply<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, reference: &WorkspaceRef,
    expected: Option<&crate::resources::GenerationRef>, mutations: &[WorkspaceMutation],
    key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<crate::resources::GenerationRef> {
    let Some(io) = io else { return host.apply(reference, expected, mutations, key).await; };
    let workspace = open(host, reference, io).await?;
    let generation = match expected {
        Some(reference) => generation(host, &workspace, reference, io).await?,
        None => io.receive(workspace.head_measured(io.remaining()?, &io.cancellation).await)?,
    };
    let mut transaction = io.receive(workspace.begin_pinned_transaction_measured(
        &generation, super::super::filesystem_key(key), io.remaining()?, &io.cancellation,
    ).await)?;
    for mutation in mutations {
        let WorkspaceMutation::PutFile { path, bytes } = mutation else {
            return Err(Error::Invalid("creation journal only publishes exact files".into()));
        };
        let result = transaction.put_measured(
            path, bytes::Bytes::copy_from_slice(bytes), io.remaining()?, &io.cancellation,
        ).await;
        match result {
            Ok(work) => io.charge(work)?,
            Err(failure) => { io.charge(*failure.work)?; return Err(map_error(failure.error)); }
        }
    }
    match io.receive(transaction.commit_with_permit_measured(
        acyclic_fs::PublicationPermit::Unrestricted, io.remaining()?, &io.cancellation,
    ).await)? {
        acyclic_fs::TransactionCommit::Committed(generation)
        | acyclic_fs::TransactionCommit::AlreadyCommitted(generation) => host.generation_ref(&generation),
        acyclic_fs::TransactionCommit::Conflict { .. } => Err(Error::Conflict("workspace generation changed".into())),
        acyclic_fs::TransactionCommit::Fenced => Err(Error::Conflict("workspace writer was fenced".into())),
        acyclic_fs::TransactionCommit::IdempotencyConflict => Err(Error::Conflict("filesystem idempotency key belongs to another mutation".into())),
    }
}

pub(super) async fn retain<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, generation: &acyclic_fs::Generation<A, O>, io: &PreparationIo,
) -> Result<()> {
    let reference = host.generation_ref(generation)?;
    let identity = format!("harness-generation-{}", hex::encode(reference.as_resource().key()));
    io.receive(generation.pin_measured(identity, io.remaining()?, &io.cancellation).await)?;
    Ok(())
}

pub(super) async fn fork<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    host: &FilesystemHost<A, O>, source: &VolumeRef,
    source_generation: &crate::resources::GenerationRef, child: &VolumeRef,
    paths: Option<Vec<String>>, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    let Some(io) = io else { return host.fork_volume_pinned(source, source_generation, child, paths, key).await; };
    source.validate()?;
    child.validate()?;
    host.validate_provider(source.provider())?;
    host.validate_provider(child.provider())?;
    if source == child { return Err(Error::Invalid("volume fork requires a distinct destination".into())); }
    let source_ref = workspace_ref(host.provider.clone(), &source.storage_name()?)?;
    let workspace = open(host, &source_ref, io).await?;
    let source = generation(host, &workspace, source_generation, io).await?;
    retain(host, &source, io).await?;
    let child_name = child.storage_name()?;
    let key = super::super::filesystem_key(key);
    let options = acyclic_fs::ForkOptions::from_generation(source, key);
    let options = match paths { Some(paths) => options.inherit_paths(paths), None => options };
    let forked = io.receive(workspace.fork_measured(&child_name, options, io.remaining()?, &io.cancellation).await)?;
    let initial = io.receive(forked.operation_generation_measured(key, io.remaining()?, &io.cancellation).await)?
        .ok_or_else(|| Error::Storage("fork creation has no durable generation".into()))?;
    retain(host, &initial, io).await?;
    Ok(super::super::WorkspaceObservation {
        workspace: workspace_ref(host.provider.clone(), &child_name)?,
        generation: host.generation_ref(&initial)?,
    })
}
