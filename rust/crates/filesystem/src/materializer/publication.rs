use super::*;
use std::sync::Arc;

fn finish_publication<T>(
    result: Result<T, NativeWorkspacePublicationError>,
    work: &NativeMaterializationWork,
) -> crate::MeasuredResult<crate::OperationReceipt<T>, NativeWorkspacePublicationError> {
    crate::performance::finish_measured(result, work.receipt(), Into::into)
}

// Keep construction scratch for each publication phase out of the caller's
// active poll frame. The same operation retains custody of the boxed future.
pub(super) async fn publication_step<T, E, F>(
    work: &NativeMaterializationWork,
    create: impl FnOnce() -> F,
) -> Result<T, NativeWorkspacePublicationError>
where
    F: Future<Output = Result<T, E>>,
    NativeWorkspacePublicationError: From<E>,
{
    crate::heap_future::native_future(work, create)?
        .await
        .map_err(NativeWorkspacePublicationError::from)
}

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
    work: NativeMaterializationWork,
) -> crate::MeasuredResult<
    crate::OperationReceipt<MaterializationJournal>,
    NativeWorkspacePublicationError,
> {
    let result = async {
        let mut guard = || std::future::ready(Ok::<_, NativeWorkspacePublicationError>(()));
        let (materializer, journal) =
            prepare_generation(from, to, state, publication, &work, &mut guard).await?;
        let Some(materializer) = materializer else {
            return Ok(journal);
        };
        match journal.phase {
            MaterializationPhase::Prepared => Ok(journal),
            MaterializationPhase::Preparing => {
                materializer.capture_preimages(journal, &mut guard).await
            }
            _ => Err(NativeWorkspacePublicationError::MismatchedJournal),
        }
    }
    .await;
    finish_publication(result, &work)
}

/// Reads and validates original progress without checkout effects.
pub async fn load_native_materialization_journal(
    state: &crate::LocalCoreStateStore,
    operation: OperationId,
    work: NativeMaterializationWork,
) -> crate::MeasuredResult<
    crate::OperationReceipt<Option<MaterializationJournal>>,
    NativeWorkspacePublicationError,
> {
    let result = async {
        let journal = state.materialization(work.clone()).load(operation).await?;
        if let Some(journal) = &journal {
            if !valid_journal(journal, operation) {
                return Err(NativeWorkspacePublicationError::MismatchedJournal);
            }
            check_native_plan(&journal.plan, &work)?;
        }
        Ok(journal)
    }
    .await;
    finish_publication(result, &work)
}

/// Publishes with authorization before every effect.
pub async fn publish_native_generation_transition<A, O, F, Fut>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    publication: NativeWorkspacePublication<'_>,
    work: NativeMaterializationWork,
    mut before_effect: F,
) -> crate::MeasuredResult<
    crate::OperationReceipt<MaterializationJournal>,
    NativeWorkspacePublicationError,
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
        let Some(materializer) = materializer else {
            return Ok(journal);
        };
        materializer
            .replay(
                journal,
                MaterializationRecovery::Complete,
                &mut before_effect,
            )
            .await
    }
    .await;
    finish_publication(result, &work)
}

async fn prepare_generation<A, O, F, Fut>(
    from: &crate::Generation<A, O>,
    to: &crate::Generation<A, O>,
    state: &crate::LocalCoreStateStore,
    request: NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<
    (
        Option<
            JournaledMaterializer<
                crate::LocalCoreStateStore,
                Arc<NativeTreeMaterializationBackend>,
            >,
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
    // Admitted before the first open: planning, preparation and the backend
    // all hold descriptors under this one admission.
    let admission = NativeTreeMaterializationBackend::admission().await?;
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
    // The layout proof (one mount, no nesting) buys rename safety, so it runs
    // once on each path that reaches an effect, before that path's first one.
    // A publication without effects reads no host topology.
    let store = state.materialization(work.clone());
    let mut journal = if let Some(journal) = store.load(request.operation_id).await? {
        validate_generation_journal(&journal, &request)?;
        check_native_plan(&journal.plan, work)?;
        work.set_original_work(journal.work);
        if journal.phase == MaterializationPhase::Applied && journal.plan.edits.is_empty() {
            // A retry still answers for the checkout it names now.
            matched_root_identity(work.root_identity(&root)?, &request)?;
            return Ok((None, journal));
        }
        publication_root_identity(&root, &operation, &request, work)?;
        journal
    } else {
        let (plan, custody) =
            publication_step(work, || generation_plan(from, to, &request, work)).await?;
        if plan.edits.is_empty() && from.id() == to.id() {
            // Equal generations stay empty on every retry, independent of
            // later host edits. Changed generations still need a durable
            // acknowledgement even when their files already match the host.
            // Planning held the root and matched its identity to the request.
            return Ok((
                None,
                MaterializationJournal {
                    history_id: OperationId::new(),
                    version: JOURNAL_VERSION,
                    revision: 1,
                    plan,
                    root_identity: Some(request.source_root_identity.to_bytes()),
                    preparation_identity: None,
                    preparation_progress: None,
                    preimages: Vec::new(),
                    phase: MaterializationPhase::Applied,
                    applied: 0,
                    restored: 0,
                    work: work.journal_work()?,
                    custody: Some(custody),
                },
            ));
        }
        let actual = publication_root_identity(&root, &operation, &request, work)?;
        let parent = super::directory_preparation::prepare_publication_parent(
            &operation,
            work,
            before_effect,
        )
        .await?;
        let journal = MaterializationJournal {
            history_id: OperationId::new(),
            version: JOURNAL_VERSION,
            revision: 1,
            plan,
            root_identity: Some(actual.to_bytes()),
            preparation_identity: None,
            preparation_progress: Some(MaterializationPreparationProgress {
                parent: parent.identity().to_bytes(),
                pending: false,
            }),
            preimages: Vec::new(),
            phase: MaterializationPhase::Preparing,
            applied: 0,
            restored: 0,
            work: work.journal_work()?,
            custody: Some(custody),
        };
        let (stored, journal) = store
            .compare_and_swap(request.operation_id, 0, journal)
            .await?;
        if !stored {
            return Err(NativeWorkspacePublicationError::MismatchedJournal);
        }
        journal
    };
    if journal.preparation_progress.is_some() {
        journal = publication_step(work, || {
            NativeTreeMaterializationBackend::prepare_allocations(
                &store,
                journal,
                &root,
                &operation,
                work,
                before_effect,
            )
        })
        .await?;
    }
    let backend = NativeTreeMaterializationBackend::new(
        root,
        operation,
        allocation,
        work.clone(),
        admission,
    )?;
    let materializer = JournaledMaterializer::new(store, backend);
    materializer.verify_allocation(&journal).await?;
    let journal =
        stage_prepared_generation(to, &materializer, journal, &request, work, before_effect)
            .await?;
    Ok((Some(materializer), journal))
}

async fn stage_prepared_generation<A, O, F, Fut>(
    to: &crate::Generation<A, O>,
    materializer: &JournaledMaterializer<
        crate::LocalCoreStateStore,
        Arc<NativeTreeMaterializationBackend>,
    >,
    mut journal: MaterializationJournal,
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
    before_effect: &mut F,
) -> Result<MaterializationJournal, NativeWorkspacePublicationError>
where
    A: crate::AsyncAuthorityStore,
    O: crate::AsyncObjectStore,
    F: FnMut() -> Fut + Send,
    Fut: Future<Output = Result<(), NativeWorkspacePublicationError>> + Send,
{
    if journal.phase == MaterializationPhase::Preparing && journal.preimages.is_empty() {
        before_effect().await?;
        materializer.verify_effect_context(&journal).await?;
        let (identity, completed) = materializer.reset_preparation_target(journal).await?;
        journal = completed;
        let (_storage, selections) = install_selections(&journal, work)?;
        if !selections.is_empty() {
            let (destination, _allocation) = materializer.backend.work.path(
                materializer.backend.operation_directory_path(),
                Some(Path::new("target")),
            )?;
            let options = crate::MaterializeOptions {
                materialized_view: Some(
                    crate::NativeViewBaseline::new(request.source_root_identity, false)
                        .recorded_in(&materializer.store),
                ),
                destination,
                expected_root_identity: Some(identity),
                maximum_directory_entries: request.options.maximum_directory_entries,
                maximum_extent_spans: request.options.maximum_extent_spans,
                transfer_bytes: request.options.transfer_bytes,
            };
            journal = publication_step(work, || {
                materializer.stage_generation(
                    to,
                    &selections,
                    &options,
                    journal,
                    work,
                    before_effect,
                )
            })
            .await?;
        }
    }
    Ok(journal)
}

impl JournaledMaterializer<crate::LocalCoreStateStore, Arc<NativeTreeMaterializationBackend>> {
    /// Resets only this operation's staging under its original journal record.
    pub(crate) async fn reset_preparation_target(
        &self,
        journal: MaterializationJournal,
    ) -> Result<(crate::NativeRootIdentity, MaterializationJournal), NativeWorkspacePublicationError>
    {
        let completion = self.store.prepare_action(journal).await?;
        let (identity, completion) = self.backend.reset_target_directory("", completion).await?;
        Ok((identity, completion.await?))
    }
}

fn publication_root_identity(
    root: &Path,
    operation: &Path,
    request: &NativeWorkspacePublication<'_>,
    work: &NativeMaterializationWork,
) -> Result<crate::NativeRootIdentity, NativeWorkspacePublicationError> {
    matched_root_identity(
        validate_native_preparation_location(root, operation, work)?,
        request,
    )
}

fn matched_root_identity(
    actual: crate::NativeRootIdentity,
    request: &NativeWorkspacePublication<'_>,
) -> Result<crate::NativeRootIdentity, NativeWorkspacePublicationError> {
    if actual != request.source_root_identity {
        return Err(NativeTreeMaterializationError::AllocationChanged {
            expected: request.source_root_identity,
            actual,
        }
        .into());
    }
    Ok(actual)
}

fn validate_generation_journal(
    journal: &MaterializationJournal,
    request: &NativeWorkspacePublication<'_>,
) -> Result<(), NativeWorkspacePublicationError> {
    if !valid_journal(journal, request.operation_id)
        || journal.root_identity != Some(request.source_root_identity.to_bytes())
        || journal.plan.intent.generation_transition() != Some((request.from, request.to))
    {
        return Err(NativeWorkspacePublicationError::MismatchedJournal);
    }
    Ok(())
}

fn install_selections(
    journal: &MaterializationJournal,
    work: &NativeMaterializationWork,
) -> Result<(crate::NativeAllocation, Vec<String>), NativeWorkspacePublicationError> {
    let mut storage = work.claim(0, 0, crate::WorkCounters::UNCHARGED)?;
    let mut selections = Vec::new();
    for edit in &journal.plan.edits {
        if let MaterializationEdit::Install { path, .. } = edit {
            let relative = copy_string(path, &mut storage)?;
            push_buffer(&mut selections, relative, &mut storage)?;
        }
    }
    Ok((storage, selections))
}

/// Publication failure with the original admitted cause.
#[derive(Debug, Error)]
pub enum NativeWorkspacePublicationError {
    /// The record exhausted its unique write revisions; no replacement was published.
    #[error("record revision is exhausted")]
    RevisionExhausted,
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
impl From<crate::WorkObservationError> for NativeWorkspacePublicationError {
    fn from(error: crate::WorkObservationError) -> Self {
        NativeTreeMaterializationError::from(error).into()
    }
}
