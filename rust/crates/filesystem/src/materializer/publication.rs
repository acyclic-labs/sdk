use super::*;
use std::sync::Arc;

/// Authenticated generation publication through the existing journal.
pub struct NativeWorkspacePublication<'a> {
    /// Physical checkout root.
    pub root: &'a Path,
    /// Owner-witnessed checkout allocation.
    pub source_root_identity: crate::NativeRootIdentity,
    /// Service-owned recovery directory.
    pub operation_directory: &'a Path,
    /// Retry identity.
    pub operation_id: OperationId,
    /// Source generation.
    pub from: GenerationId,
    /// Target generation.
    pub to: GenerationId,
    /// Root names preserved by publication.
    pub excluded_names: &'a [&'a str],
    /// Staging policy.
    pub options: &'a crate::MaterializeOptions,
    /// Shared finite allowance.
    pub budget: crate::WorkBudget,
    /// Cancellation boundary.
    pub cancellation: &'a crate::CancellationToken,
}

/// Stages and captures before checkout edits.
pub async fn prepare_native_generation_transition<
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
) -> Result<
    crate::OperationReceipt<MaterializationJournal>,
    (NativeWorkspacePublicationError, crate::WorkCounters),
> {
    let work = NativeMaterializationWork::new(publication.budget, publication.cancellation)
        .map_err(|(cause, work)| (NativeWorkspacePublicationError::Native(cause), work))?;
    let result = async {
        let mut guard = || std::future::ready(Ok::<_, NativeWorkspacePublicationError>(()));
        let (materializer, journal) =
            prepare_generation(from, to, state, publication, &work, &mut guard).await?;
        match journal.phase {
            MaterializationPhase::Prepared => Ok(journal),
            MaterializationPhase::Preparing => {
                let operation = journal.plan.operation_id;
                let mut guard = || verify_generation_source(&materializer, operation, from);
                materializer.capture_preimages(journal, &mut guard).await
            }
            _ => Err(NativeWorkspacePublicationError::MismatchedJournal),
        }
    }
    .await;
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: work.receipt(),
        })
        .map_err(|cause| (cause, work.receipt()))
}

/// Reads and validates original progress without checkout effects.
pub async fn load_native_materialization_journal(
    state: &crate::LocalCoreStateStore,
    operation: OperationId,
    budget: crate::WorkBudget,
    cancellation: &crate::CancellationToken,
) -> Result<
    crate::OperationReceipt<Option<MaterializationJournal>>,
    (NativeWorkspacePublicationError, crate::WorkCounters),
> {
    let work = NativeMaterializationWork::new(budget, cancellation)
        .map_err(|(cause, work)| (NativeWorkspacePublicationError::Native(cause), work))?;
    let result = async {
        let journal = state
            .materialization_with_work(work.clone())
            .load(operation)
            .await?;
        if let Some(journal) = &journal {
            if !valid_journal(journal, operation) {
                return Err(NativeWorkspacePublicationError::MismatchedJournal);
            }
            check_native_plan(&journal.plan, &work)?;
        }
        Ok(journal)
    }
    .await;
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: work.receipt(),
        })
        .map_err(|cause| (cause, work.receipt()))
}

/// Publishes with authorization before every effect.
pub async fn publish_native_generation_transition<A, O, F, Fut>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    mut before_effect: F,
) -> Result<
    crate::OperationReceipt<MaterializationJournal>,
    (NativeWorkspacePublicationError, crate::WorkCounters),
>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    let result = async {
        let (materializer, journal) =
            prepare_generation(from, to, state, publication, &work, &mut before_effect).await?;
        let operation = journal.plan.operation_id;
        let mut guard = || {
            let authorized = before_effect();
            let materializer = &materializer;
            async move {
                authorized.await?;
                verify_generation_source(materializer, operation, from).await
            }
        };
        materializer
            .replay(journal, MaterializationRecovery::Complete, &mut guard)
            .await
    }
    .await;
    result
        .map(|value| crate::OperationReceipt {
            value,
            work: work.receipt(),
        })
        .map_err(|cause| (cause, work.receipt()))
}

async fn verify_generation_source<A: crate::AsyncAuthorityStore, O: crate::AsyncObjectStore>(
    materializer: &JournaledMaterializer<
        crate::core_state::MeasuredMaterializationStore<'_>,
        Arc<NativeTreeMaterializationBackend>,
    >,
    operation: OperationId,
    generation: &crate::Generation<A, O>,
) -> Result<(), NativeWorkspacePublicationError> {
    let journal = materializer
        .load(operation)
        .await?
        .ok_or(NativeWorkspacePublicationError::MismatchedJournal)?;
    let initial = match journal.phase {
        MaterializationPhase::Preparing | MaterializationPhase::Prepared => true,
        MaterializationPhase::Applying if journal.applied == 0 => {
            match (journal.plan.edits.first(), journal.preimages.first()) {
                (Some(edit), Some(preimage)) => matches!(
                    materializer.backend.observe(edit, preimage).await?,
                    MaterializationObservation::Preimage | MaterializationObservation::Unverified
                ),
                _ => false,
            }
        }
        _ => false,
    };
    if initial {
        let work = materializer.backend.work();
        for edit in &journal.plan.edits {
            if !preflight(
                &materializer.backend.held_root,
                edit_path(edit),
                generation,
                !matches!(edit, MaterializationEdit::SetMetadata { .. }),
                false,
                &work,
            )
            .await?
            {
                return Err(NativeTreeMaterializationError::ExternalMutation.into());
            }
        }
    }
    Ok(())
}

async fn prepare_generation<'a, A, O, F, Fut>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    state: &'a crate::LocalCoreStateStore,
    request: NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<
    (
        JournaledMaterializer<
            crate::core_state::MeasuredMaterializationStore<'a>,
            Arc<NativeTreeMaterializationBackend>,
        >,
        MaterializationJournal,
    ),
    NativeWorkspacePublicationError,
>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    before_effect().await?;
    if work.budget() != request.budget || from.id() != request.from || to.id() != request.to {
        return Err(NativeWorkspacePublicationError::MismatchedJournal);
    }
    let mut root = work.path(request.root, None)?;
    let mut operation = work.path(request.operation_directory, None)?;
    root.1.absorb(&mut operation.1)?;
    let allocation = root.1;
    let root = root.0;
    let operation = operation.0;
    let actual = validate_native_preparation_location(&root, &operation, work)?;
    if actual != request.source_root_identity {
        return Err(NativeTreeMaterializationError::AllocationChanged {
            expected: request.source_root_identity,
            actual,
        }
        .into());
    }
    let store = state.materialization_with_work(work.clone());
    let mut journal = if let Some(journal) = store.load(request.operation_id).await? {
        if !valid_journal(&journal, request.operation_id)
            || journal.root_identity != Some(actual.to_bytes())
            || journal.plan.intent
                != (MaterializationIntent::GenerationTransition {
                    from: request.from,
                    to: request.to,
                })
        {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        check_native_plan(&journal.plan, work)?;
        work.set_original_work(journal.work);
        journal
    } else {
        let (plan, custody) = generation_plan(from, to, &request, work).await?;
        let parent = crate::native_host::HostRoot::open(
            operation
                .parent()
                .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
            &mut &*work,
        )?;
        let journal = MaterializationJournal {
            version: JOURNAL_VERSION,
            revision: 0,
            plan,
            root_identity: Some(actual.to_bytes()),
            preparation_identity: None,
            preparation_progress: Some(MaterializationPreparationProgress {
                parent: parent.identity().to_bytes(),
                operation: None,
                target: None,
                backup: None,
                pending: None,
            }),
            preimages: Vec::new(),
            phase: MaterializationPhase::Preparing,
            applied: 0,
            restored: 0,
            work: work.journal_work()?,
            custody: Some(custody),
        };
        let (stored, journal) =
            compare_next_journal::<_, NativeTreeMaterializationError>(&store, journal).await?;
        if !stored {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        journal
    };
    if journal.preparation_progress.is_some() {
        journal = NativeTreeMaterializationBackend::prepare_allocations(
            &store,
            journal,
            &root,
            &operation,
            work,
            before_effect,
        )
        .await?;
    }
    let backend = NativeTreeMaterializationBackend::new(root, operation, allocation, work.clone())?;
    let materializer = JournaledMaterializer::new(store, backend);
    materializer.verify_allocation(&journal).await?;
    if journal.phase == MaterializationPhase::Preparing && journal.preimages.is_empty() {
        verify_generation_source(&materializer, request.operation_id, from).await?;
        let (reset, identity) = materializer.reset_stage(journal, "", before_effect).await?;
        journal = reset;
        let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
        let mut selections = Vec::new();
        for edit in &journal.plan.edits {
            if let MaterializationEdit::Install { path, .. } = edit {
                let (absolute, mut allocation) = native_absolute_string(path, work)?;
                storage.absorb(&mut allocation)?;
                native_vec_push(&mut selections, absolute, &mut storage)?;
            }
        }
        if !selections.is_empty() {
            let (destination, _allocation) = materializer.backend.staging_path("")?;
            let options = crate::MaterializeOptions {
                destination,
                expected_root_identity: Some(identity),
                maximum_directory_entries: request.options.maximum_directory_entries,
                maximum_extent_spans: request.options.maximum_extent_spans,
                transfer_bytes: request.options.transfer_bytes,
            };
            journal = materializer
                .stage_generation(to, &selections, &options, journal, before_effect)
                .await?;
        }
    }
    Ok((materializer, journal))
}

/// Publication failure with the original admitted cause.
#[derive(Debug, Error)]
pub enum NativeWorkspacePublicationError {
    /// Authenticated workspace access failed.
    #[error(transparent)]
    Workspace(#[from] crate::WorkspaceError),
    /// Host publication failed.
    #[error(transparent)]
    Native(#[from] NativeTreeMaterializationError),
    /// Selected staging failed.
    #[error(transparent)]
    NativeMaterialize(#[from] crate::MaterializeError),
    /// Durable access failed.
    #[error(transparent)]
    State(#[from] crate::LocalCoreStateStoreError),
    /// Journal protocol failed.
    #[error(transparent)]
    Materialization(
        #[from]
        MaterializationError<crate::LocalCoreStateStoreError, NativeTreeMaterializationError>,
    ),
    /// Retry intent changed.
    #[error("publication journal differs from requested generations")]
    MismatchedJournal,
    /// Host IO failed.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// Stage completion was not acknowledged.
    #[error(transparent)]
    NativeStageUncertain(#[from] NativeStageUncertainty<NativeWorkspacePublicationError>),
}
