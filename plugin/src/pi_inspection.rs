//! Passive selected filesystem pages; execution and source authority are never adopted.

use acyclic_fs::demand::DemandSource as _;
use acyclic_harness::runtime::TaskStateProvider;
use acyclic_harness::store::native_work_io;
use acyclic_harness::{
    Error, Result, TaskId,
    conversation::{MAX_PRIVATE_DIRECTORY_PAGE, VolumeRef, validate_content_path},
    durable_host::TaskJournalOwner,
    filesystem::{FilesystemTaskRuntime, TaskSourceInspection},
    runtime::TaskContext,
};
use acyclic_stream::LocalStream;
use serde::{Deserialize, Serialize};

/// Independently authenticates both selections under the ORIGINAL root's retained admission tree.
pub(crate) async fn authorize_selected_source(
    runtime: &FilesystemTaskRuntime<
        LocalStream,
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
    >,
    original_root: TaskId,
    caller: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    source: TaskId,
    maximum_depth: u32,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    if caller.durable_task_id() != Some(owner.task_id()) {
        return Err(selected_unauthorized(
            "selected graph changed its ORIGINAL caller context",
        ));
    }
    if maximum_depth == 0 {
        return Err(selected_invalid("selected ancestry bound is zero"));
    }
    for selected in [owner.task_id(), source] {
        let mut task = selected;
        let mut authenticated = false;
        for _ in 0..maximum_depth {
            let admission_custody = runtime
                .task_host()
                .observe_admission_with_native_work(task, native_work)
                .await?;
            let admission = &admission_custody.0;
            if admission.task_id() != task {
                return Err(selected_unauthorized(
                    "selected admission substituted its task identity",
                ));
            }
            if task == original_root {
                if admission.parent.is_some() {
                    return Err(selected_unauthorized(
                        "selected original root is itself a child",
                    ));
                }
                authenticated = true;
                break;
            }
            task = admission.parent.ok_or_else(|| {
                selected_unauthorized("selected task belongs to another original root")
            })?;
        }
        if !authenticated {
            return Err(selected_unauthorized(
                "selected task ancestry exceeds its authenticated bound",
            ));
        }
    }
    owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await
}

fn selected_unauthorized(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Unauthorized,
        reason,
    }
}

fn selected_invalid(reason: &'static str) -> Error {
    Error::NativeBoundary {
        code: acyclic_harness::wire::ErrorCode::Invalid,
        reason,
    }
}

fn validate_page(directory: &str, maximum_entries: u32) -> Result<()> {
    if !directory.is_empty() {
        validate_content_path(directory)?;
    }
    if maximum_entries == 0 || maximum_entries as usize > MAX_PRIVATE_DIRECTORY_PAGE {
        return Err(selected_invalid(
            "selected inspection page bound is invalid",
        ));
    }
    Ok(())
}

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum PiDifferenceKind {
    Added,
    Removed,
    Modified,
    KindChanged,
}

#[derive(Serialize)]
pub(crate) struct PiDifference {
    pub(crate) name: String,
    pub(crate) kind: PiDifferenceKind,
}

/// Identity retained by the original ROOT creator, not captured anew from a selected child or a tool-supplied path.
pub(crate) struct PiOriginalWritebackRoot {
    pub(crate) task: TaskId,
    pub(crate) volume: VolumeRef,
    pub(crate) workspace: acyclic_fs::WorkspaceId,
    pub(crate) identity: acyclic_fs::NativeRootIdentity,
    pub(crate) path: std::path::PathBuf,
    pub(crate) operation_directory: std::path::PathBuf,
    pub(crate) _custody: [acyclic_fs::NativeAllocation; 3],
}

/// Constructed only by the actual selected frozen exactification/restore engines below.
pub(crate) struct PiWritebackGenerations<'a> {
    from: acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    candidate:
        acyclic_fs::Generation<acyclic_fs::LocalAuthorityBackend, acyclic_fs::LocalObjectBackend>,
    pub(crate) original_snapshot: acyclic_fs::LazySnapshotRef,
    pub(crate) imported: acyclic_fs::GenerationId,
    operation: acyclic_harness::OperationId,
    paths: &'a [String],
}

/// Retains the existing root provider while the SDK exactifier is detached.
pub(crate) struct PiWritebackPublicationGuard {
    provider: std::sync::Arc<crate::pi_root_provider::PiRootProvider>,
    owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
    allocation: crate::pi_root_allocation::PiRootAllocation,
    key: String,
    write_capability: String,
    identity: acyclic_fs::NativeRootIdentity,
    _custody: [acyclic_fs::NativeAllocation; 4],
}

impl PiWritebackPublicationGuard {
    pub(crate) async fn new(
        protected: std::sync::Arc<crate::pi_owner::PiOwnerConfig>,
        resources: &crate::ServiceResources,
        filesystem: std::sync::Arc<crate::pi_native_host::PiNativeFilesystem>,
        stream: std::sync::Arc<LocalStream>,
        context: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
        request: (
            crate::pi_root_inputs::PiRootRequest,
            acyclic_fs::NativeAllocation,
        ),
        key: &str,
        identity: acyclic_fs::NativeRootIdentity,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        if request.0.binding.task_id != protected.policy().root_task
            || owner.task_id() != request.0.binding.task_id
            || context.durable_task_id() != Some(owner.task_id())
        {
            return Err(selected_unauthorized(
                "writeback retained another original Root creator",
            ));
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        let volume = request
            .0
            .binding
            .project_volumes
            .get(key)
            .ok_or_else(|| selected_unauthorized("writeback lost its original project"))?;
        let mut io = native_work_io(work)?;
        let write_capability = volume.capability(
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut io),
        )?;
        let preparation = crate::pi_root_provider::load_original_preparation_with_native_work(
            &protected,
            &filesystem,
            &request.0.binding,
            &request.0.root,
            context,
            work,
        )
        .await?;
        let provider = crate::pi_root_provider::root_provider(
            protected,
            resources,
            filesystem,
            stream,
            preparation,
            None,
            work.clone(),
        )?;
        drop(request);
        let attempt = acyclic_harness::EffectAttemptId::from_bytes(
            provider.effect_id_with_native_work(work)?.into_bytes(),
        );
        let ((reference, allocation), allocation_custody) = provider
            .observe_allocation_with_native_work(attempt, work)
            .await?
            .ok_or_else(|| {
                selected_unauthorized("writeback has no original created Root allocation")
            })?;
        drop(reference);
        let key = work.display(key)?;
        let slot = work.claim(
            std::mem::size_of::<Self>() as u64,
            1,
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..acyclic_fs::WorkCounters::UNCHARGED
        })?;
        Ok(std::sync::Arc::new(Self {
            provider,
            owner,
            allocation,
            key: key.0,
            write_capability,
            identity,
            _custody: [allocation_custody, key.1, io.into_custody(), slot],
        }))
    }

    pub(crate) async fn verify(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        self.owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await?;
        let preparation = &self.provider.preparation;
        let volume = preparation
            .request
            .binding
            .project_volumes
            .get(&self.key)
            .ok_or_else(|| selected_unauthorized("writeback lost its retained original project"))?;
        if preparation.context.durable_task_id() != Some(self.owner.task_id())
            || !preparation
                .context
                .scope()
                .grants()
                .contains(&self.write_capability)
        {
            return Err(selected_unauthorized(
                "writeback original context lost its project WRITE grant",
            ));
        }
        self.owner.require_volume_grant(
            volume,
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut native_work_io(work)?),
        )?;
        let identity = self
            .provider
            .verify_created_source(&self.owner, &self.key, &self.allocation, work)
            .await?;
        if identity != self.identity {
            return Err(selected_unauthorized(
                "writeback changed its held original native Root identity",
            ));
        }
        self.owner.require_volume_grant(
            volume,
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut native_work_io(work)?),
        )?;
        self.owner
            .verify(false, &mut Some(&mut native_work_io(work)?))
            .await
    }
}

pub(crate) async fn build_writeback_generations<'a>(
    root: &PiOriginalWritebackRoot,
    native_context: &std::sync::Arc<acyclic_harness::runtime::TaskContext>,
    owner: &std::sync::Arc<TaskJournalOwner<LocalStream>>,
    verifier: &acyclic_harness::core::AuthorityVerifier,
    scope: &acyclic_harness::core::Scope,
    input_owner: &crate::pi_owner::PiOwnerConfig,
    filesystem: &acyclic_harness::filesystem::FilesystemHost<
        acyclic_fs::LocalAuthorityBackend,
        acyclic_fs::LocalObjectBackend,
    >,
    actual_root: &crate::pi_root_provider::PiRootHandle,
    original: acyclic_fs::LazySnapshotRef,
    imported: &acyclic_fs::Generation<
        acyclic_fs::LocalAuthorityBackend,
        acyclic_fs::LocalObjectBackend,
    >,
    paths: &'a [String],
    operation: acyclic_harness::OperationId,
    permit: acyclic_fs::PublicationPermit,
    maximum_paths: u32,
    cancellation: &acyclic_fs::CancellationToken,
    native_work: &acyclic_fs::NativeMaterializationWork,
    publication_guard: std::sync::Arc<PiWritebackPublicationGuard>,
) -> Result<PiWritebackGenerations<'a>> {
    let context = native_context.as_ref();

    authenticate_root(root, context, owner, verifier, scope, native_work).await?;
    let mut grant_io = native_work_io(native_work)?;
    let _read_grant = acyclic_harness::conversation::ContentGrant::verify(
        verifier,
        scope,
        &root.volume,
        acyclic_harness::conversation::VolumeOperation::Read,
        context.scope().limits().file_bytes,
        &mut Some(&mut grant_io),
    )?;
    let storage_name = {
        let mut io = native_work_io(native_work)?;
        let value = root.volume.storage_name(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    if original.workspace_id != root.workspace
        || imported.workspace_id() != root.workspace
        || actual_root.workspace.workspace().id() != root.workspace
        || input_owner.policy().root_task != root.task
        || actual_root.workspace.workspace().name().as_str() != storage_name.0
        || actual_root.physical.source.inner().root_identity() != root.identity
        || paths.is_empty()
        || maximum_paths == 0
        || paths.len() > maximum_paths as usize
    {
        return Err(selected_unauthorized(
            "writeback baseline is not the ORIGINAL creator's frozen native root",
        ));
    }
    let retained = (paths.len() as u64)
        .checked_mul((std::mem::size_of::<String>() + std::mem::size_of::<&str>()) as u64)
        .ok_or_else(|| selected_invalid("selected writeback container size overflow"))?;
    let custody = native_work.claim(
        retained,
        u64::from((retained) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    native_work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 2,
        ..acyclic_fs::WorkCounters::default()
    })?;
    let mut absolute_paths = (
        Vec::with_capacity(paths.len()),
        paths.iter().map(String::as_str).collect::<Vec<_>>(),
        custody,
    );
    absolute_paths.1.sort_unstable();
    if absolute_paths.1.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(selected_invalid("writeback repeats a selected path"));
    }
    for path in paths {
        validate_content_path(path)?;
        let mut absolute = native_work.display(&format_args!("/{path}"))?;
        absolute_paths.2.absorb(&mut absolute.1)?;
        absolute_paths.0.push(absolute.0);
    }
    let contract = (
        root.task,
        &root.volume,
        root.workspace,
        root.identity.to_bytes(),
        &root.path,
        &root.operation_directory,
        original,
        imported.id(),
        paths,
        operation,
    );
    let staged = crate::pi_root_provider::stage_pi_input(
        input_owner,
        filesystem,
        operation,
        "writeback-plan",
        &contract,
        native_work,
    )
    .await?;
    drop(staged);
    let destination = native_work.display(&format_args!("pi-writeback-{operation}-baseline"))?;
    let (from, candidate) = actual_root
        .workspace
        .exactify_snapshot_selected_at_generation_with_native_work_guarded(
            original,
            imported,
            &absolute_paths.0,
            &destination.0,
            acyclic_fs::IdempotencyKey::from_bytes(
                phase_operation(operation, b"pi-writeback-baseline").into_bytes(),
            ),
            acyclic_fs::IdempotencyKey::from_bytes(
                phase_operation(operation, b"pi-writeback-candidate").into_bytes(),
            ),
            permit,
            native_work,
            cancellation,
            move |original: &acyclic_fs::NativeMaterializationWork| {
                let publication_guard = std::sync::Arc::clone(&publication_guard);
                let original = original.clone();
                async move { publication_guard.verify(&original).await }
            },
        )
        .await?;
    authenticate_root(root, context, owner, verifier, scope, native_work).await?;
    Ok(PiWritebackGenerations {
        from,
        candidate,
        original_snapshot: original,
        imported: imported.id(),
        operation,
        paths,
    })
}

fn phase_operation(
    operation: acyclic_harness::OperationId,
    phase: &[u8],
) -> acyclic_harness::OperationId {
    use sha2::Digest as _;
    let mut hash = sha2::Sha256::new();
    hash.update(phase);
    hash.update(operation.into_bytes());
    let digest = hash.finalize();
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&digest[..16]);
    acyclic_harness::OperationId::from_bytes(bytes)
}

/// Owns the actual issued caller/destination contexts, not copied scopes.
struct PiSelectedPublicationGuard {
    caller: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
    caller_owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
    destination: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
    destination_owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
    volume: VolumeRef,
    write_capability: String,
    mount_guard: Option<std::sync::Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>>,
    _custody: [acyclic_fs::NativeAllocation; 2],
}

impl PiSelectedPublicationGuard {
    fn new(
        caller: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        caller_owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
        destination: std::sync::Arc<acyclic_harness::runtime::TaskContext>,
        destination_owner: std::sync::Arc<TaskJournalOwner<LocalStream>>,
        mount_guard: Option<std::sync::Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>>,
        volume: &VolumeRef,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<std::sync::Arc<Self>> {
        let mut io = native_work_io(work)?;
        let volume = volume.copy(&mut Some(&mut io))?;
        let capability = volume.capability(
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut io),
        )?;
        let slot = work.claim(
            std::mem::size_of::<Self>() as u64,
            u64::from((std::mem::size_of::<Self>() as u64) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        work.charge(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..acyclic_fs::WorkCounters::default()
        })?;
        Ok(std::sync::Arc::new(Self {
            caller,
            caller_owner,
            destination,
            destination_owner,
            volume,
            write_capability: capability,
            mount_guard,
            _custody: [io.into_custody(), slot],
        }))
    }

    async fn verify(&self, work: &acyclic_fs::NativeMaterializationWork) -> Result<()> {
        if let Some(guard) = &self.mount_guard {
            let original_volume = work.clone();
            guard
                .check(&original_volume)
                .map_err(|cause| {
                    Error::from(acyclic_fs::native_mount::MountLifecycleError::Native(cause))
                })?
                .await
                .map_err(|cause| {
                    Error::from(acyclic_fs::native_mount::MountLifecycleError::Native(cause))
                })?;
        }
        for (index, (context, owner)) in [
            (&self.caller, &self.caller_owner),
            (&self.destination, &self.destination_owner),
        ]
        .into_iter()
        .enumerate()
        {
            if index == 1
                && std::sync::Arc::ptr_eq(&self.caller, &self.destination)
                && std::sync::Arc::ptr_eq(&self.caller_owner, &self.destination_owner)
            {
                continue;
            }
            owner
                .verify(false, &mut Some(&mut native_work_io(work)?))
                .await?;
            if context.durable_task_id() != Some(owner.task_id())
                || !context
                    .scope()
                    .grants()
                    .contains(&self.write_capability)
            {
                return Err(selected_unauthorized(
                    "selected publication lost its original caller or destination WRITE grant",
                ));
            }
            owner.require_volume_grant(
                &self.volume,
                acyclic_harness::conversation::VolumeOperation::Write,
                &mut Some(&mut native_work_io(work)?),
            )?;
            owner
                .verify(false, &mut Some(&mut native_work_io(work)?))
                .await?;
        }
        Ok(())
    }
}

/// Private prepared custody; clients receive only the digest and bounded plan.
pub(crate) struct PiPreparedWriteback<'a> {
    root: &'a PiOriginalWritebackRoot,
    pub(crate) generations: &'a PiWritebackGenerations<'a>,
    options: &'a acyclic_fs::MaterializeOptions,
    excluded_names: &'a [&'a str],
    pub(crate) journal: acyclic_fs::MaterializationJournal,
    pub(crate) approval_digest: String,
    pub(crate) canonical_digest_input: String,
    _codec_custody: acyclic_fs::NativeAllocation,
}

pub(crate) async fn prepare_writeback<'a>(
    root: &'a PiOriginalWritebackRoot,
    context: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    verifier: &acyclic_harness::core::AuthorityVerifier,
    scope: &acyclic_harness::core::Scope,
    generations: &'a PiWritebackGenerations<'a>,
    state: &acyclic_fs::LocalCoreStateStore,
    maximum_paths: u32,
    options: &'a acyclic_fs::MaterializeOptions,
    excluded_names: &'a [&'a str],
    cancellation: &acyclic_fs::CancellationToken,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<PiPreparedWriteback<'a>> {
    authenticate_root(root, context, owner, verifier, scope, native_work).await?;
    let from = &generations.from;
    let candidate = &generations.candidate;
    let paths = generations.paths;
    if paths.is_empty()
        || maximum_paths == 0
        || paths.len() > maximum_paths as usize
        || generations.original_snapshot.workspace_id != root.workspace
        || from.workspace_id() != candidate.workspace_id()
    {
        return Err(selected_unauthorized(
            "writeback candidate is not the original selected root workspace",
        ));
    }
    let operation = acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes());
    let guard_work = native_work.clone();
    let guard_cause = std::sync::Mutex::new(None);
    let prepared = acyclic_fs::materializer::publish_native_transition_inner(
        from,
        candidate,
        state,
        acyclic_fs::NativeWorkspacePublication {
            root: &root.path,
            source_root_identity: root.identity,
            operation_directory: &root.operation_directory,
            operation_id: operation,
            from: from.id(),
            to: candidate.id(),
            excluded_names,
            options,
            budget: native_work.budget(),
            cancellation,
        },
        native_work.clone(),
        true,
        &mut || async {
            authenticate_root(root, context, owner, verifier, scope, &guard_work)
                .await
                .map_err(|error| native_guard_cause(error, &guard_cause))
        },
    )
    .await;
    let journal = match prepared {
        Ok(journal) => journal,
        Err(source) => {
            return Err(guard_cause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| Error::NativeWorkspacePublication {
                    source,
                    custody: None,
                }));
        }
    };
    if journal.root_identity != Some(root.identity.to_bytes())
        || journal.phase != acyclic_fs::MaterializationPhase::Prepared
    {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "writeback preparation has another original root or is not ready for approval",
        });
    }
    require_selected_edits(&journal.plan.edits, paths)?;
    authenticate_root(root, context, owner, verifier, scope, native_work).await?;
    let digest_value = writeback_digest_value(
        root,
        operation,
        from.id(),
        candidate.id(),
        paths,
        generations.original_snapshot,
        generations.imported,
        &journal,
    );
    let mut digest = crate::pi_root_provider::digest_pi_value(
        &digest_value,
        native_work.budget().bytes_encoded,
        native_work,
    )?;
    let mut encoded = crate::pi_root_provider::canonical_pi_value(
        &digest_value,
        crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
        native_work,
    )?;
    digest.1.absorb(&mut encoded.1)?;
    let canonical_digest_input = String::from_utf8(encoded.0).map_err(|error| {
        drop(error);
        selected_invalid("canonical SDK approval codec returned non-UTF8")
    })?;
    Ok(PiPreparedWriteback {
        root,
        generations,
        options,
        excluded_names,
        journal,
        approval_digest: digest.0,
        canonical_digest_input,
        _codec_custody: digest.1,
    })
}

/// Publishes only the exact user-confirmed preparation.
pub(crate) async fn publish_writeback(
    prepared: &PiPreparedWriteback<'_>,
    context: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    verifier: &acyclic_harness::core::AuthorityVerifier,
    scope: &acyclic_harness::core::Scope,
    state: &acyclic_fs::LocalCoreStateStore,
    confirmed_digest: &str,
    confirmed_by_user: bool,
    cancellation: &acyclic_fs::CancellationToken,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::MaterializationJournal> {
    let root = prepared.root;
    let generations = prepared.generations;
    let operation = acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes());
    authenticate_root(root, context, owner, verifier, scope, native_work).await?;
    let from = &generations.from;
    let candidate = &generations.candidate;
    let digest = crate::pi_root_provider::digest_pi_value(
        &writeback_digest_value(
            root,
            operation,
            from.id(),
            candidate.id(),
            generations.paths,
            generations.original_snapshot,
            generations.imported,
            &prepared.journal,
        ),
        native_work.budget().bytes_encoded,
        native_work,
    )?;
    if !confirmed_by_user
        || confirmed_digest != prepared.approval_digest
        || generations.original_snapshot.workspace_id != root.workspace
        || digest.0 != prepared.approval_digest
    {
        return Err(selected_unauthorized(
            "writeback lacks exact original-root user approval",
        ));
    }
    drop(digest);
    let work = native_work.clone();
    let guard_work = work.clone();
    let guard_cause = std::sync::Mutex::new(None);
    let receipt = acyclic_fs::publish_native_generation_transition(
        from,
        candidate,
        state,
        acyclic_fs::NativeWorkspacePublication {
            root: &root.path,
            source_root_identity: root.identity,
            operation_directory: &root.operation_directory,
            operation_id: operation,
            from: from.id(),
            to: candidate.id(),
            excluded_names: prepared.excluded_names,
            options: prepared.options,
            budget: native_work.budget(),
            cancellation,
        },
        work,
        || async {
            authenticate_root(root, context, owner, verifier, scope, &guard_work)
                .await
                .map_err(|error| native_guard_cause(error, &guard_cause))?;
            if !confirmed_by_user || confirmed_digest != prepared.approval_digest {
                return Err(native_guard_cause(
                    selected_unauthorized("writeback original approval changed"),
                    &guard_cause,
                ));
            }
            Ok(())
        },
    )
    .await;
    let value = match receipt {
        Ok(receipt) => receipt.value,
        Err((source, _producer_work)) => {
            return Err(guard_cause
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .take()
                .unwrap_or_else(|| match source {
                    acyclic_fs::NativeWorkspacePublicationError::Native(cause) => {
                        Error::NativeWork {
                            cause,
                            custody: None,
                        }
                    }
                    source => Error::NativeWorkspacePublication {
                        source,
                        custody: None,
                    },
                }));
        }
    };
    Ok(value)
}

/// A replay can acknowledge only the SDK's authenticated completed original journal, with the same exact user digest.
pub(crate) fn validate_applied_writeback(
    root: &PiOriginalWritebackRoot,
    generations: &PiWritebackGenerations<'_>,
    journal: &acyclic_fs::MaterializationJournal,
    confirmed_digest: &str,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    if journal.phase != acyclic_fs::MaterializationPhase::Applied
        || journal.root_identity != Some(root.identity.to_bytes())
        || journal.plan.operation_id
            != acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes())
        || !matches!(journal.plan.intent, acyclic_fs::MaterializationIntent::GenerationTransition { from, to }
            if from == generations.from.id() && to == generations.candidate.id())
        || generations.original_snapshot.workspace_id != root.workspace
    {
        return Err(selected_unauthorized(
            "completed writeback differs from its original root and generations",
        ));
    }
    require_selected_edits(&journal.plan.edits, &generations.paths)?;
    let digest = crate::pi_root_provider::digest_pi_value(
        &writeback_digest_value(
            root,
            acyclic_fs::OperationId::from_bytes(generations.operation.into_bytes()),
            generations.from.id(),
            generations.candidate.id(),
            &generations.paths,
            generations.original_snapshot,
            generations.imported,
            journal,
        ),
        native_work.budget().bytes_encoded,
        native_work,
    )?;
    if digest.0 != confirmed_digest {
        return Err(selected_unauthorized(
            "completed writeback changed its exact user approval",
        ));
    }
    Ok(())
}

fn native_guard_cause(
    error: Error,
    cause: &std::sync::Mutex<Option<Error>>,
) -> acyclic_fs::NativeWorkspacePublicationError {
    let mut retained = cause
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    if retained.is_none() {
        *retained = Some(error);
    }
    acyclic_fs::NativeTreeMaterializationError::Io(std::io::Error::from(
        std::io::ErrorKind::PermissionDenied,
    ))
    .into()
}

async fn authenticate_root(
    root: &PiOriginalWritebackRoot,
    context: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    verifier: &acyclic_harness::core::AuthorityVerifier,
    scope: &acyclic_harness::core::Scope,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<()> {
    owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    let capability = {
        let mut io = native_work_io(native_work)?;
        let value = root.volume.capability(
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut io),
        )?;
        (value, io.into_custody())
    };
    if context.durable_task_id() != Some(root.task)
        || owner.task_id() != root.task
        || !root.path.is_absolute()
        || !root.operation_directory.is_absolute()
        || !context.scope().grants().contains(&capability.0)
    {
        return Err(selected_unauthorized(
            "writeback does not own the ORIGINAL root task and destination",
        ));
    }
    let mut grant_io = native_work_io(native_work)?;
    let _grant = acyclic_harness::conversation::ContentGrant::verify(
        verifier,
        scope,
        &root.volume,
        acyclic_harness::conversation::VolumeOperation::Write,
        context.scope().limits().file_bytes,
        &mut Some(&mut grant_io),
    )?;
    owner.require_volume_grant(
        &root.volume,
        acyclic_harness::conversation::VolumeOperation::Write,
        &mut Some(&mut native_work_io(native_work)?),
    )?;
    Ok(())
}

fn writeback_digest_value<'a>(
    root: &'a PiOriginalWritebackRoot,
    operation: acyclic_fs::OperationId,
    from: acyclic_fs::GenerationId,
    to: acyclic_fs::GenerationId,
    paths: &'a [String],
    original_snapshot: acyclic_fs::LazySnapshotRef,
    imported: acyclic_fs::GenerationId,
    journal: &'a acyclic_fs::MaterializationJournal,
) -> impl Serialize + 'a {
    (
        root.task,
        &root.volume,
        root.workspace,
        root.identity.to_bytes(),
        &root.path,
        &root.operation_directory,
        operation,
        from,
        to,
        paths,
        original_snapshot,
        imported,
        &journal.plan,
        journal.root_identity,
        journal.preparation_identity,
        &journal.preimages,
    )
}

fn require_selected_edits(
    edits: &[acyclic_fs::MaterializationEdit],
    paths: &[String],
) -> Result<()> {
    let selected = |path: &str| {
        paths.iter().any(|prefix| {
            path == prefix
                || path
                    .strip_prefix(prefix)
                    .is_some_and(|suffix| suffix.starts_with('/'))
        })
    };
    for edit in edits {
        let allowed = match edit {
            acyclic_fs::MaterializationEdit::Install { path, .. }
            | acyclic_fs::MaterializationEdit::Remove { path }
            | acyclic_fs::MaterializationEdit::SetMetadata { path, .. } => selected(path),
            acyclic_fs::MaterializationEdit::Rename { from, to } => selected(from) && selected(to),
            _ => false,
        };
        if !allowed {
            return Err(selected_unauthorized(
                "native candidate changes an unselected writeback path",
            ));
        }
    }
    Ok(())
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeInspectionCursor<V = VolumeRef> {
    pub(crate) source: TaskId,
    pub(crate) volume: V,
    pub(crate) snapshot: acyclic_fs::LazySnapshotRef,
    pub(crate) directory: String,
    pub(crate) continuation: Vec<u8>,
}

#[derive(Serialize)]
pub(crate) struct PiNativeInspectionEntry {
    pub(crate) name: String,
    pub(crate) kind: acyclic_fs::demand::SourceNodeKind,
}

#[derive(Serialize)]
pub(crate) struct PiNativeInspectionPage<'a> {
    pub(crate) source: TaskId,
    pub(crate) volume: &'a VolumeRef,
    pub(crate) snapshot: acyclic_fs::LazySnapshotRef,
    pub(crate) directory: String,
    pub(crate) entries: Vec<PiNativeInspectionEntry>,
    pub(crate) cursor: Option<PiNativeInspectionCursor<&'a VolumeRef>>,
    #[serde(skip)]
    _custody: acyclic_fs::NativeAllocation,
}

/// Original source capability plus frozen logical identity.
pub(crate) struct PiNativeSelectedSource<'a> {
    workspace: &'a crate::LocalLazyWorkspace,
    source: TaskId,
    reader: TaskId,
    volume: &'a VolumeRef,
    snapshot: acyclic_fs::LazySnapshotRef,
    inspection: &'a TaskSourceInspection<
        LocalStream,
        crate::LocalAuthorityBackend,
        crate::LocalObjectBackend,
    >,
}

impl<'a> PiNativeSelectedSource<'a> {
    pub(crate) fn snapshot(&self) -> acyclic_fs::LazySnapshotRef {
        self.snapshot
    }

    /// The inspection is constructed by the shared runtime from the opaque ORIGINAL-caller ProjectSourceReadHandle.
    pub(crate) fn select(
        inspection: &'a TaskSourceInspection<
            LocalStream,
            crate::LocalAuthorityBackend,
            crate::LocalObjectBackend,
        >,
        source: TaskId,
        reader: TaskId,
        volume: &'a VolumeRef,
        workspace: &'a crate::LocalLazyWorkspace,
        snapshot: acyclic_fs::LazySnapshotRef,
        native_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Self> {
        let storage_name = {
            let mut io = native_work_io(native_work)?;
            let value = volume.storage_name(&mut Some(&mut io))?;
            (value, io.into_custody())
        };
        if workspace.workspace().name().as_str() != storage_name.0
            || snapshot.workspace_id != workspace.workspace().id()
        {
            return Err(selected_unauthorized(
                "native selected snapshot differs from its original allocation",
            ));
        }
        Ok(Self {
            workspace,
            source,
            reader,
            volume,
            snapshot,
            inspection,
        })
    }
    pub(crate) fn volume(&self) -> &VolumeRef {
        self.volume
    }

    fn require_reader(&self, context: &TaskContext, path: &str) -> Result<()> {
        if context.durable_task_id() != Some(self.reader) {
            return Err(selected_unauthorized(
                "native selected reader belongs to another original caller",
            ));
        }
        if !path.is_empty() {
            validate_content_path(path)?;
        }
        Ok(())
    }

    pub(crate) async fn inspect<'b>(
        &'b self,
        context: &TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        directory: &str,
        cursor: Option<&PiNativeInspectionCursor>,
        maximum_entries: u32,
        cancellation: &acyclic_fs::CancellationToken,
        native_work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PiNativeInspectionPage<'b>> {
        owner
            .verify(false, &mut Some(&mut native_work_io(native_work)?))
            .await?;
        if context.durable_task_id() != Some(owner.task_id()) {
            return Err(selected_unauthorized(
                "native inspection changed its ORIGINAL caller context",
            ));
        }
        self.require_reader(context, directory)?;
        validate_page(directory, maximum_entries)?;
        let continuation = if let Some(cursor) = cursor {
            if cursor.source != self.source
                || &cursor.volume != self.volume
                || cursor.snapshot != self.snapshot
                || cursor.directory != directory
            {
                return Err(selected_invalid(
                    "native selected cursor belongs to another snapshot or directory",
                ));
            }
            if cursor.continuation.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES {
                return Err(selected_invalid(
                    "native selected cursor exceeds its transport bound",
                ));
            }
            Some(
                acyclic_fs::LazyDirectoryCursor::decode_with_native_work(
                    &cursor.continuation,
                    self.workspace.workspace().profile(),
                    self.workspace.workspace().limits(),
                    native_work,
                    cancellation,
                )
                .map_err(|(source, custody)| Error::NativeSource { source, custody })?,
            )
        } else {
            None
        };
        let absolute = native_work.display(&format_args!("/{directory}"))?;
        let page = self
            .inspection
            .list_native_directory_with_native_work(
                self.workspace,
                self.snapshot,
                &absolute.0,
                continuation.map(|(cursor, _)| cursor),
                maximum_entries,
                native_work,
            )
            .await?;
        let retained = (page.0.entries.len() as u64)
            .checked_mul(std::mem::size_of::<PiNativeInspectionEntry>() as u64)
            .and_then(|bytes| bytes.checked_add(directory.len() as u64))
            .ok_or_else(|| selected_invalid("native inspection payload size overflow"))?;
        let custody = native_work.claim(
            retained,
            u64::from((retained) != 0),
            acyclic_fs::WorkCounters::UNCHARGED,
        )?;
        native_work.charge(acyclic_fs::WorkCounters {
            bytes_copied: directory.len() as u64,
            allocation_operations: u64::from(!page.0.entries.is_empty())
                + u64::from(!directory.is_empty()),
            ..acyclic_fs::WorkCounters::default()
        })?;
        let mut output = (
            Vec::with_capacity(page.0.entries.len()),
            directory.to_owned(),
            custody,
        );
        for entry in &page.0.entries {
            let mut name = entry
                .name
                .unicode_text_with_native_work(native_work, cancellation)?;
            output.2.absorb(&mut name.1)?;
            output.0.push(PiNativeInspectionEntry {
                name: name.0,
                kind: entry.kind,
            });
        }
        let cursor = if let Some(continuation) = &page.0.next {
            let mut continuation = continuation
                .encode_with_native_work(native_work, cancellation)
                .map_err(|(source, custody)| Error::NativeSource { source, custody })?;
            let mut directory = native_work.display(&format_args!("{directory}"))?;
            output.2.absorb(&mut continuation.1)?;
            output.2.absorb(&mut directory.1)?;
            Some(PiNativeInspectionCursor {
                source: self.source,
                volume: self.volume,
                snapshot: self.snapshot,
                directory: directory.0,
                continuation: continuation.0,
            })
        } else {
            None
        };
        drop(page);
        owner
            .verify(false, &mut Some(&mut native_work_io(native_work)?))
            .await?;
        Ok(PiNativeInspectionPage {
            source: self.source,
            volume: self.volume,
            snapshot: self.snapshot,
            directory: output.1,
            entries: output.0,
            cursor,
            _custody: output.2,
        })
    }

    /// The canonical sealed reader retains its original SDK byte capacity through the caller's encoding/publication; this is not post-hoc adoption.
    pub(crate) async fn read_with_native_work(
        &self,
        context: &TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        path: &str,
        maximum_bytes: u64,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.require_reader(context, path)?;
        if context.durable_task_id() != Some(owner.task_id()) || owner.task_id() != self.reader {
            return Err(selected_unauthorized(
                "native selected read changed its ORIGINAL caller owner",
            ));
        }
        let absolute = work.display(&format_args!("/{path}"))?;
        self.inspection
            .read_native_path_with_native_work(
                self.workspace,
                self.snapshot,
                &absolute.0,
                maximum_bytes.min(context.scope().limits().file_bytes),
                work,
            )
            .await
    }
    pub(crate) async fn lookup_with_native_work(
        &self,
        context: &TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        path: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(acyclic_fs::LazyLookup, acyclic_fs::NativeAllocation)> {
        self.require_reader(context, path)?;
        if context.durable_task_id() != Some(owner.task_id()) || owner.task_id() != self.reader {
            return Err(selected_unauthorized(
                "native selected lookup changed its ORIGINAL caller owner",
            ));
        }
        let absolute = work.display(&format_args!("/{path}"))?;
        self.inspection
            .lookup_native_path_with_native_work(self.workspace, self.snapshot, &absolute.0, work)
            .await
    }
    pub(crate) async fn read_link_with_native_work(
        &self,
        context: &TaskContext,
        owner: &TaskJournalOwner<LocalStream>,
        path: &str,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_stream::OwnedBytes> {
        self.require_reader(context, path)?;
        if context.durable_task_id() != Some(owner.task_id()) || owner.task_id() != self.reader {
            return Err(selected_unauthorized(
                "native selected link read changed its ORIGINAL caller owner",
            ));
        }
        let absolute = work.display(&format_args!("/{path}"))?;
        self.inspection
            .read_native_link_with_native_work(self.workspace, self.snapshot, &absolute.0, work)
            .await
    }
}

/// The source's sealed canonical selected plan preserves actual recorded metadata, directories and negative proofs.
pub(crate) async fn import_native_project(
    source: &PiNativeSelectedSource<'_>,
    caller_native: &std::sync::Arc<acyclic_harness::runtime::TaskContext>,
    caller_owner: &std::sync::Arc<TaskJournalOwner<LocalStream>>,
    destination_native: &std::sync::Arc<acyclic_harness::runtime::TaskContext>,
    destination_owner: &std::sync::Arc<TaskJournalOwner<LocalStream>>,
    destination_workspace: &crate::LocalLazyWorkspace,
    mount: Option<&crate::LocalSingleMount>,
    mount_guard: Option<std::sync::Arc<dyn acyclic_fs::native_mount::NativeMountCallbackGuard>>,
    destination_volume: &VolumeRef,
    checkpoint: acyclic_fs::LazySnapshotRef,
    verifier: &acyclic_harness::core::AuthorityVerifier,
    scope: &acyclic_harness::core::Scope,
    paths: &[String],
    operation: acyclic_harness::OperationId,
    permit: acyclic_fs::PublicationPermit,
    maximum_paths: u32,
    maximum_bytes: u64,
    cancellation: &acyclic_fs::CancellationToken,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<acyclic_fs::LazySnapshotRef> {
    let caller = caller_native.as_ref();
    let destination = destination_native.as_ref();

    if mount.is_some() != mount_guard.is_some() {
        return Err(selected_unauthorized(
            "selected mounted publication lacks its original SDK source WRITE guard",
        ));
    }
    caller_owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    destination_owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    if caller.durable_task_id() != Some(caller_owner.task_id())
        || destination.durable_task_id() != Some(destination_owner.task_id())
    {
        return Err(selected_unauthorized(
            "native import changed its ORIGINAL caller or destination context",
        ));
    }
    let capability = {
        let mut io = native_work_io(native_work)?;
        let value = destination_volume.capability(
            acyclic_harness::conversation::VolumeOperation::Write,
            &mut Some(&mut io),
        )?;
        (value, io.into_custody())
    };
    let storage_name = {
        let mut io = native_work_io(native_work)?;
        let value = destination_volume.storage_name(&mut Some(&mut io))?;
        (value, io.into_custody())
    };
    if paths.is_empty()
        || maximum_paths == 0
        || paths.len() > maximum_paths as usize
        || maximum_bytes == 0
        || destination_workspace.workspace().name().as_str() != storage_name.0
        || checkpoint.workspace_id != destination_workspace.workspace().id()
        || !destination.scope().grants().contains(&capability.0)
        || !caller.scope().grants().contains(&capability.0)
    {
        return Err(selected_unauthorized(
            "native import lacks its original destination WRITE grant or selected checkpoint",
        ));
    }
    let mut grant_io = native_work_io(native_work)?;
    let _grant = acyclic_harness::conversation::ContentGrant::verify(
        verifier,
        scope,
        destination_volume,
        acyclic_harness::conversation::VolumeOperation::Write,
        destination.scope().limits().file_bytes,
        &mut Some(&mut grant_io),
    )?;
    let retained = (paths.len() as u64)
        .checked_mul(
            (std::mem::size_of::<acyclic_fs::kernel::NamespacePath>() + std::mem::size_of::<&str>())
                as u64,
        )
        .ok_or_else(|| selected_invalid("selected namespace container size overflow"))?;
    let custody = native_work.claim(
        retained,
        u64::from((retained) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    native_work.charge(acyclic_fs::WorkCounters {
        allocation_operations: 2,
        ..acyclic_fs::WorkCounters::default()
    })?;
    let mut requested = (
        Vec::with_capacity(paths.len()),
        paths.iter().map(String::as_str).collect::<Vec<_>>(),
        custody,
    );
    requested.1.sort_unstable();
    if requested.1.windows(2).any(|pair| pair[0] == pair[1]) {
        return Err(selected_invalid("native import repeats a selected path"));
    }
    for path in paths {
        validate_content_path(
            path,
            &mut Some(&mut acyclic_harness::store::native_work_io(native_work)?),
        )?;
        source.require_reader(caller, path)?;
        caller_owner
            .verify(false, &mut Some(&mut native_work_io(native_work)?))
            .await?;
        let absolute = native_work.display(&format_args!("/{path}"))?;
        let mut parsed = source
            .workspace
            .workspace()
            .namespace_path_with_native_work(&absolute.0, native_work, cancellation)
            .map_err(Error::from)?;
        requested.2.absorb(&mut parsed.1)?;
        requested.0.push(parsed.0);
    }
    let plan = source
        .inspection
        .prepare_native_selected_import_with_native_work(
            source.workspace,
            source.snapshot,
            &requested.0,
            native_work,
        )
        .await?;
    caller_owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    if plan.0.logical_bytes() > maximum_bytes
        || plan.0.maximum_regular_file_bytes() > caller.scope().limits().file_bytes
    {
        return Err(selected_invalid(
            "selected import exceeds its actual selected byte allowance",
        ));
    }
    let publication_guard = PiSelectedPublicationGuard::new(
        std::sync::Arc::clone(caller_native),
        std::sync::Arc::clone(caller_owner),
        std::sync::Arc::clone(destination_native),
        std::sync::Arc::clone(destination_owner),
        mount_guard,
        destination_volume,
        native_work,
    )?;
    let operation = acyclic_fs::OperationId::from_bytes(operation.into_bytes());
    let value = if let Some(mount) = mount {
        mount
            .import_selected_work_guarded(
                checkpoint,
                plan.0,
                plan.1,
                operation,
                permit,
                &native_work,
                cancellation,
                move |original: &acyclic_fs::NativeMaterializationWork| {
                    let publication_guard = std::sync::Arc::clone(&publication_guard);
                    let original = original.clone();
                    async move { publication_guard.verify(&original).await }
                },
            )
            .await?
    } else {
        (async {
            let cancellation = cancellation;
            let accounting = acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                native_work,
                cancellation,
            )?;
            destination_workspace
                .import_selected(
                    checkpoint,
                    plan.0,
                    plan.1,
                    operation,
                    permit,
                    &accounting,
                    move |original: &acyclic_fs::NativeMaterializationWork| {
                        let publication_guard = std::sync::Arc::clone(&publication_guard);
                        let original = original.clone();
                        async move { publication_guard.verify(&original).await }
                    },
                )
                .await
        })
        .await?
    };
    Ok(value)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeSourceKey<V = VolumeRef> {
    source: TaskId,
    volume: V,
    pub(crate) snapshot: acyclic_fs::LazySnapshotRef,
}

impl PiNativeSourceKey<&VolumeRef> {
    fn matches(&self, value: &PiNativeSourceKey) -> bool {
        self.source == value.source
            && self.volume == &value.volume
            && self.snapshot == value.snapshot
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PiNativeDiffCursor<V = VolumeRef> {
    before: PiNativeSourceKey<V>,
    pub(crate) after: PiNativeSourceKey<V>,
    directory: String,
    before_pass: bool,
    continuation: Option<Vec<u8>>,
}

#[derive(Serialize)]
pub(crate) struct PiNativeDiffPage<'a> {
    pub(crate) entries: Vec<PiDifference>,
    pub(crate) directory: String,
    pub(crate) cursor: Option<PiNativeDiffCursor<&'a VolumeRef>>,
    #[serde(skip)]
    _custody: acyclic_fs::NativeAllocation,
}

/// Two bounded native-order passes, so a source's host enumeration order is never assumed to be lexical.
pub(crate) async fn diff_native<'a>(
    before: &'a PiNativeSelectedSource<'_>,
    after: &'a PiNativeSelectedSource<'_>,
    context: &TaskContext,
    owner: &TaskJournalOwner<LocalStream>,
    directory: &str,
    cursor: Option<&PiNativeDiffCursor>,
    maximum_entries: u32,
    cancellation: &acyclic_fs::CancellationToken,
    native_work: &acyclic_fs::NativeMaterializationWork,
) -> Result<PiNativeDiffPage<'a>> {
    owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(selected_unauthorized(
            "native diff changed its ORIGINAL caller context",
        ));
    }
    before.require_reader(context, directory)?;
    after.require_reader(context, directory)?;
    validate_page(directory, maximum_entries)?;
    let before_key = PiNativeSourceKey {
        source: before.source,
        volume: before.volume,
        snapshot: before.snapshot,
    };
    let after_key = PiNativeSourceKey {
        source: after.source,
        volume: after.volume,
        snapshot: after.snapshot,
    };
    let before_pass = if let Some(cursor) = cursor {
        if !before_key.matches(&cursor.before)
            || !after_key.matches(&cursor.after)
            || cursor.directory != directory
        {
            return Err(selected_invalid(
                "native diff cursor belongs to another source, snapshot or directory",
            ));
        }
        cursor.before_pass
    } else {
        false
    };
    let current = if before_pass { before } else { after };
    let other = if before_pass { after } else { before };
    let continuation = if let Some(bytes) = cursor.and_then(|cursor| cursor.continuation.as_deref())
    {
        if bytes.len() > crate::MAXIMUM_CONTROL_MESSAGE_BYTES {
            return Err(selected_invalid(
                "native diff cursor exceeds its transport bound",
            ));
        }
        Some(
            acyclic_fs::LazyDirectoryCursor::decode_with_native_work(
                bytes,
                current.workspace.workspace().profile(),
                current.workspace.workspace().limits(),
                native_work,
                cancellation,
            )
            .map_err(|(source, custody)| Error::NativeSource { source, custody })?,
        )
    } else {
        None
    };
    let absolute = native_work.display(&format_args!("/{directory}"))?;
    let page = match current
        .inspection
        .list_native_directory_with_native_work(
            current.workspace,
            current.snapshot,
            &absolute.0,
            continuation.map(|(cursor, _)| cursor),
            maximum_entries,
            native_work,
        )
        .await
    {
        Ok(page) => Some(page),
        Err(Error::NativeSource { source, custody })
            if matches!(
                source.semantic_error(),
                acyclic_fs::LazyWorkspaceError::NotFound
            ) =>
        {
            drop(source);
            drop(custody);
            None
        }
        Err(source) => return Err(source),
    };
    let count = page.as_ref().map_or(0, |(page, _)| page.entries.len());
    let retained = (count as u64)
        .checked_mul(std::mem::size_of::<PiDifference>() as u64)
        .and_then(|bytes| bytes.checked_add(directory.len() as u64))
        .ok_or_else(|| selected_invalid("native diff payload size overflow"))?;
    let custody = native_work.claim(
        retained,
        u64::from((retained) != 0),
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    native_work.charge(acyclic_fs::WorkCounters {
        bytes_copied: directory.len() as u64,
        allocation_operations: u64::from(count != 0) + u64::from(!directory.is_empty()),
        ..acyclic_fs::WorkCounters::default()
    })?;
    let mut output = (Vec::with_capacity(count), directory.to_owned(), custody);
    for entry in page
        .as_ref()
        .into_iter()
        .flat_map(|(page, _)| page.entries.iter())
    {
        let mut name = entry
            .name
            .unicode_text_with_native_work(native_work, cancellation)?;
        let path = if directory.is_empty() {
            native_work.display(&format_args!("{}", name.0))?
        } else {
            native_work.display(&format_args!("{directory}/{}", name.0))?
        };
        let counterpart = match other
            .lookup_with_native_work(context, owner, &path.0, native_work)
            .await
        {
            Ok(lookup) => Some(lookup),
            Err(Error::NotFound(_)) => None,
            Err(Error::NativeSource { source, custody })
                if matches!(
                    source.semantic_error(),
                    acyclic_fs::LazyWorkspaceError::NotFound
                ) =>
            {
                drop(source);
                drop(custody);
                None
            }
            Err(cause) => return Err(cause),
        };
        if before_pass {
            if counterpart.is_none() {
                output.2.absorb(&mut name.1)?;
                output.0.push(PiDifference {
                    name: name.0,
                    kind: PiDifferenceKind::Removed,
                });
            }
            continue;
        }
        let Some(counterpart) = counterpart else {
            output.2.absorb(&mut name.1)?;
            output.0.push(PiDifference {
                name: name.0,
                kind: PiDifferenceKind::Added,
            });
            continue;
        };
        let current_lookup = current
            .lookup_with_native_work(context, owner, &path.0, native_work)
            .await?;
        let old_stat = counterpart.0.stat();
        let new_stat = current_lookup.0.stat();
        let kind = if old_stat.kind != new_stat.kind {
            Some(PiDifferenceKind::KindChanged)
        } else if old_stat.logical_bytes != new_stat.logical_bytes
            || counterpart.0.metadata() != current_lookup.0.metadata()
            || native_extra_facts_differ(&counterpart.0, &current_lookup.0)
        {
            Some(PiDifferenceKind::Modified)
        } else {
            match new_stat.kind {
                acyclic_fs::demand::SourceNodeKind::RegularFile => {
                    let old = before
                        .read_with_native_work(
                            context,
                            owner,
                            &path.0,
                            context.scope().limits().file_bytes,
                            native_work,
                        )
                        .await?;
                    let new = after
                        .read_with_native_work(
                            context,
                            owner,
                            &path.0,
                            context.scope().limits().file_bytes,
                            native_work,
                        )
                        .await?;
                    (old.as_slice() != new.as_slice()).then_some(PiDifferenceKind::Modified)
                }
                acyclic_fs::demand::SourceNodeKind::SymbolicLink => {
                    let old = before
                        .read_link_with_native_work(context, owner, &path.0, native_work)
                        .await?;
                    let new = after
                        .read_link_with_native_work(context, owner, &path.0, native_work)
                        .await?;
                    (old.as_slice() != new.as_slice()).then_some(PiDifferenceKind::Modified)
                }
                _ => None,
            }
        };
        if let Some(kind) = kind {
            output.2.absorb(&mut name.1)?;
            output.0.push(PiDifference { name: name.0, kind });
        }
        owner
            .verify(false, &mut Some(&mut native_work_io(native_work)?))
            .await?;
    }
    let next = if let Some(next) = page.as_ref().and_then(|(page, _)| page.next.as_ref()) {
        let mut continuation = next
            .encode_with_native_work(native_work, cancellation)
            .map_err(|(source, custody)| Error::NativeSource { source, custody })?;
        let mut directory = native_work.display(&format_args!("{directory}"))?;
        output.2.absorb(&mut continuation.1)?;
        output.2.absorb(&mut directory.1)?;
        Some(PiNativeDiffCursor {
            before: before_key,
            after: after_key,
            directory: directory.0,
            before_pass,
            continuation: Some(continuation.0),
        })
    } else if !before_pass {
        let mut directory = native_work.display(&format_args!("{directory}"))?;
        output.2.absorb(&mut directory.1)?;
        Some(PiNativeDiffCursor {
            before: before_key,
            after: after_key,
            directory: directory.0,
            before_pass: true,
            continuation: None,
        })
    } else {
        None
    };
    drop(page);
    owner
        .verify(false, &mut Some(&mut native_work_io(native_work)?))
        .await?;
    Ok(PiNativeDiffPage {
        entries: output.0,
        directory: output.1,
        cursor: next,
        _custody: output.2,
    })
}

fn native_extra_facts_differ(
    before: &acyclic_fs::LazyLookup,
    after: &acyclic_fs::LazyLookup,
) -> bool {
    let links = |lookup: &acyclic_fs::LazyLookup| match lookup {
        acyclic_fs::LazyLookup::Authored { stat, .. } => Some(stat.link_count),
        acyclic_fs::LazyLookup::Shadow {
            source_link_count, ..
        } => Some(*source_link_count),
        acyclic_fs::LazyLookup::Source(node) => node.link_count,
    };
    if links(before) != links(after) {
        return true;
    }
    match (before, after) {
        (acyclic_fs::LazyLookup::Source(before), acyclic_fs::LazyLookup::Source(after)) => {
            before.device != after.device
        }
        _ => false,
    }
}
