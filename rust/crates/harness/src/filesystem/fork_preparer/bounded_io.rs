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
    host: &FilesystemHost<A, O>, source: &VolumeRef, grant: &ContentGrant,
    source_generation: &crate::resources::GenerationRef, child: &VolumeRef,
    paths: Option<Vec<String>>, key: &IdempotencyKey, io: Option<&PreparationIo>,
) -> Result<super::super::WorkspaceObservation> {
    grant.require(source, VolumeOperation::Read)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::conversation::{VolumeClass, VolumeOwner};
    use crate::core::{AggregateKind, AuthorityIssuer};
    use acyclic_fs::Fs;

    #[tokio::test]
    async fn exact_read_gate_precedes_backend_and_pinned_fork_never_writes_back() -> Result<()> {
        let provider = crate::resources::ProviderRef::new("source-read-control", "filesystem", "2")?;
        let host = FilesystemHost::new(Fs::memory(), provider.clone())?;
        let project = |name: &str| VolumeRef::new(provider.clone(), name, VolumeClass::Project,
            VolumeOwner::Project("source-read-control".into()));
        let source = project("selected")?;
        let caller = project("caller")?;
        let child = project("child")?;
        let denied_child = project("denied-child")?;
        let initial = host.create_volume(&source).await?;
        let caller_head = host.create_volume(&caller).await?;
        let selected_generation = host.apply(&initial.workspace, Some(&initial.generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"selected".to_vec() }],
            &IdempotencyKey::new("selected-generation")?).await?;
        let wrong_generation = host.apply(&caller_head.workspace, Some(&caller_head.generation),
            &[WorkspaceMutation::PutFile { path: "/other.txt".into(), bytes: b"other-source".to_vec() }],
            &IdempotencyKey::new("other-source-generation")?).await?;
        let issuer = AuthorityIssuer::new("source-read-control", [41; 32], Authority {
            kind: AggregateKind::Conversation, id: "caller".into(),
        });
        let scope = issuer.root("exact-project-read", crate::Capabilities::new([
            source.capability(VolumeOperation::Read)?, caller.capability(VolumeOperation::Read)?,
        ]));
        let selected_read = ContentGrant::verify(&issuer.verifier(), &scope, &source, VolumeOperation::Read)?;
        let caller_read = ContentGrant::verify(&issuer.verifier(), &scope, &caller, VolumeOperation::Read)?;
        assert!(matches!(fork(&host, &source, &caller_read, &selected_generation, &denied_child,
            None, &IdempotencyKey::new("denied-source-read")?, None).await, Err(Error::Unauthorized(_))));
        let denied_ref = workspace_ref(provider.clone(), &denied_child.storage_name()?)?;
        assert!(host.resolve(&denied_ref).await.is_err());
        assert!(fork(&host, &source, &selected_read, &wrong_generation, &denied_child,
            None, &IdempotencyKey::new("wrong-generation")?, None).await.is_err());
        assert!(host.resolve(&denied_ref).await.is_err());
        let later_generation = host.apply(&initial.workspace, Some(&selected_generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"later".to_vec() }],
            &IdempotencyKey::new("later-generation")?).await?;
        let forked = fork(&host, &source, &selected_read, &selected_generation, &child,
            None, &IdempotencyKey::new("pinned-selected-fork")?, None).await?;
        assert_eq!(host.read(&forked.workspace, Some(&forked.generation), "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"selected"));
        host.apply(&forked.workspace, Some(&forked.generation),
            &[WorkspaceMutation::PutFile { path: "/value.txt".into(), bytes: b"child".to_vec() }],
            &IdempotencyKey::new("isolated-child-edit")?).await?;
        assert_eq!(host.resolve(&initial.workspace).await?.generation, later_generation);
        assert_eq!(host.read(&initial.workspace, None, "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"later"));
        assert_eq!(host.read(&initial.workspace, Some(&selected_generation), "/value.txt", 32).await?,
            bytes::Bytes::from_static(b"selected"));
        Ok(())
    }
}
