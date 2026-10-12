use super::*;
use std::sync::Arc;

/// Accepted native work whose durable completion is uncertain.
#[derive(Debug)]
pub struct NativeStageUncertainty<E> {
    primary: Box<Option<E>>,
    /// Last acknowledged work prefix.
    pub last_durable_work: crate::WorkCounters,
    /// Actual work observed by this invocation.
    pub observed_work: crate::WorkCounters,
    allocation: NativeAllocation,
}
impl<E> NativeStageUncertainty<E> {
    /// Original typed cause.
    pub fn cause(&self) -> &E {
        self.primary
            .as_ref()
            .as_ref()
            .expect("finished uncertainty")
    }
    fn reserve(
        work: &NativeMaterializationWork,
    ) -> Result<(Box<Option<E>>, NativeAllocation), NativeTreeMaterializationError> {
        let allocation = work.claim(
            std::mem::size_of::<Option<E>>() as u64,
            u64::from(std::mem::size_of::<Option<E>>() != 0),
            crate::WorkCounters::UNCHARGED,
        )?;
        Ok((Box::new(None), allocation))
    }
    fn finish(
        slot: (Box<Option<E>>, NativeAllocation),
        cause: E,
        last_durable_work: crate::WorkCounters,
        observed_work: crate::WorkCounters,
    ) -> Self {
        let (mut primary, allocation) = slot;
        *primary = Some(cause);
        Self {
            primary,
            allocation,
            last_durable_work,
            observed_work,
        }
    }
}
impl<E: std::fmt::Display> std::fmt::Display for NativeStageUncertainty<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native stage completion uncertain: {}", self.cause())
    }
}
impl<E: std::error::Error + 'static> std::error::Error for NativeStageUncertainty<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause())
    }
}

impl<S: MaterializationJournalStore>
    JournaledMaterializer<S, Arc<NativeTreeMaterializationBackend>>
{
    /// Stages selected generations through the same prepaid journal.
    #[cfg(feature = "native-mount")]
    pub(super) async fn stage_generation<A, O, E, F, Fut>(
        &self,
        generation: &crate::Generation<A, O>,
        selections: &[String],
        options: &crate::MaterializeOptions,
        journal: MaterializationJournal,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        A: crate::AsyncAuthorityStore,
        O: crate::AsyncObjectStore,
        E: From<crate::MaterializeError>
            + From<crate::WorkspaceError>
            + From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>>
            + Send,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let work = self.backend.work();
        let slot = NativeStageUncertainty::<E>::reserve(&work)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        let state = std::sync::Mutex::new((
            Some(journal),
            None::<S::ActionCompletion<'_>>,
            None::<crate::WorkCounters>,
        ));
        let reader = generation
            .workspace()
            .reader(generation.id(), work, work.cancellation())
            .await
            .map_err(E::from)?;
        let mut control = |phase| {
            use crate::native_mount::NativeMaterializationActionPhase::{Before, Completed};
            let owner = if phase == Before {
                Some(before_effect())
            } else {
                None
            };
            let state = &state;
            async move {
                match phase {
                    Before => {
                        let journal = state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .0
                            .take()
                            .ok_or_else(|| {
                                E::from(MaterializationError::Backend(
                                    NativeTreeMaterializationError::InvalidLayout,
                                ))
                            })?;
                        owner.expect("before action owns guard").await?;
                        self.verify_allocation(&journal).await.map_err(E::from)?;
                        let last = journal.work;
                        let completion = self
                            .store
                            .prepare_action(journal)
                            .await
                            .map_err(|error| E::from(MaterializationError::Store(error)))?;
                        *state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            (None, Some(completion), Some(last));
                    }
                    Completed => {
                        let completion = state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner)
                            .1
                            .take()
                            .ok_or_else(|| {
                                E::from(MaterializationError::Backend(
                                    NativeTreeMaterializationError::InvalidLayout,
                                ))
                            })?;
                        let journal = completion
                            .await
                            .map_err(|error| E::from(MaterializationError::Store(error)))?;
                        *state
                            .lock()
                            .unwrap_or_else(std::sync::PoisonError::into_inner) =
                            (Some(journal), None, None);
                    }
                }
                Ok(())
            }
        };
        let result = crate::native_mount::materialize_checkout_host_paths_with_work_guarded(
            &reader,
            selections,
            options,
            &work,
            &mut control,
        )
        .await;
        drop(control);
        let (journal, completion, last) = state
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        drop(completion);
        match (result, journal, last) {
            (Ok(_), Some(journal), None) => Ok(journal),
            (Err(cause), _, Some(last)) => Err(E::from(NativeStageUncertainty::finish(
                slot,
                cause,
                last,
                work.receipt(),
            ))),
            (Err(cause), _, None) => Err(cause),
            _ => Err(E::from(MaterializationError::Backend(
                NativeTreeMaterializationError::InvalidLayout,
            ))),
        }
    }
    #[cfg(feature = "native-mount")]
    pub(super) async fn reset_stage<E, F, Fut>(
        &self,
        journal: MaterializationJournal,
        relative: &str,
        before_effect: &mut F,
    ) -> Result<(MaterializationJournal, crate::NativeRootIdentity), E>
    where
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>
            + From<NativeStageUncertainty<E>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let work = self.backend.work();
        let slot = NativeStageUncertainty::<E>::reserve(&work)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        before_effect().await?;
        self.verify_allocation(&journal).await.map_err(E::from)?;
        let last = journal.work;
        let completion = self
            .store
            .prepare_action(journal)
            .await
            .map_err(|error| E::from(MaterializationError::Store(error)))?;
        let identity = match self.backend.reset_target_directory(relative).await {
            Ok(identity) => identity,
            Err(error) => {
                return Err(E::from(NativeStageUncertainty::finish(
                    slot,
                    E::from(MaterializationError::Backend(error)),
                    last,
                    work.receipt(),
                )));
            }
        };
        let journal = completion.await.map_err(|error| {
            E::from(NativeStageUncertainty::finish(
                slot,
                E::from(MaterializationError::Store(error)),
                last,
                work.receipt(),
            ))
        })?;
        Ok((journal, identity))
    }
}
impl NativeTreeMaterializationBackend {
    pub(super) async fn prepare_allocations<S, F, Fut, E>(
        store: &S,
        mut journal: MaterializationJournal,
        root: &Path,
        operation: &Path,
        work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> Result<MaterializationJournal, E>
    where
        S: MaterializationJournalStore,
        E: From<MaterializationError<S::Error, NativeTreeMaterializationError>>,
        F: FnMut() -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
    {
        let backend = MaterializationError::<S::Error, NativeTreeMaterializationError>::Backend;
        let incompatible =
            MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal;
        let expected_root =
            crate::NativeRootIdentity::from_bytes(journal.root_identity.ok_or(incompatible)?);
        let mut progress = journal.preparation_progress.ok_or(
            MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal,
        )?;
        if !valid_journal(&journal, journal.plan.operation_id) {
            return Err(MaterializationError::<S::Error, NativeTreeMaterializationError>::IncompatibleJournal.into());
        }
        check_native_plan(&journal.plan, work).map_err(backend)?;
        if progress.pending.is_some() {
            return Err(backend(NativeTreeMaterializationError::IndeterminatePreparation).into());
        }
        for step in [
            MaterializationPreparationAllocation::Operation,
            MaterializationPreparationAllocation::Target,
            MaterializationPreparationAllocation::Backup,
        ] {
            let known = match step {
                MaterializationPreparationAllocation::Operation => progress.operation,
                MaterializationPreparationAllocation::Target => progress.target,
                MaterializationPreparationAllocation::Backup => progress.backup,
            };
            if known.is_some() {
                continue;
            }
            before_effect().await?;
            verify_preparation(root, expected_root, operation, progress, work).map_err(backend)?;
            let current = store
                .load(journal.plan.operation_id)
                .await
                .map_err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Store)?;
            if current.as_ref() != Some(&journal) {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict
                        .into(),
                );
            }
            progress.pending = Some(step);
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(backend)?;
            let (stored, pending) =
                compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = pending;
            if !stored {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended
                        .into(),
                );
            }
            let current = store
                .load(journal.plan.operation_id)
                .await
                .map_err(MaterializationError::<S::Error, NativeTreeMaterializationError>::Store)?;
            if current.as_ref() != Some(&journal) {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::PlanConflict
                        .into(),
                );
            }
            verify_preparation(root, expected_root, operation, progress, work).map_err(backend)?;
            let parent = crate::native_host::HostRoot::open(
                operation
                    .parent()
                    .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
                &mut &*work,
            )
            .map_err(backend)?;
            let parent = parent
                .open_dir_held(Path::new(""), &mut &*work)
                .map_err(backend)?;
            let leaf = Path::new(
                operation
                    .file_name()
                    .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
            );
            let directory = match step {
                MaterializationPreparationAllocation::Operation => {
                    crate::native_host::create_new_directory_held(&parent, leaf, &mut &*work)
                        .map_err(backend)?
                }
                _ => {
                    let operation = recorded_child(
                        &parent,
                        leaf,
                        progress.operation.ok_or_else(|| {
                            backend(NativeTreeMaterializationError::InvalidLayout)
                        })?,
                        work,
                    )
                    .map_err(backend)?;
                    crate::native_host::create_new_directory_held(
                        &operation,
                        if step == MaterializationPreparationAllocation::Target {
                            Path::new("target")
                        } else {
                            Path::new("backup")
                        },
                        &mut &*work,
                    )
                    .map_err(backend)?
                }
            };
            let identity = crate::native_host::directory_identity(&directory, &mut &*work)
                .map_err(backend)?
                .to_bytes();
            match step {
                MaterializationPreparationAllocation::Operation => {
                    progress.operation = Some(identity)
                }
                MaterializationPreparationAllocation::Target => progress.target = Some(identity),
                MaterializationPreparationAllocation::Backup => progress.backup = Some(identity),
            }
            progress.pending = None;
            journal.preparation_progress = Some(progress);
            journal.work = work.journal_work().map_err(backend)?;
            let (stored, completed) =
                compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
            journal = completed;
            if !stored {
                return Err(
                    MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended
                        .into(),
                );
            }
            before_effect().await?;
            verify_preparation(root, expected_root, operation, progress, work).map_err(backend)?;
        }
        journal.preparation_identity = Some(MaterializationPreparationIdentity {
            operation: progress
                .operation
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
            target: progress
                .target
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
            backup: progress
                .backup
                .ok_or_else(|| backend(NativeTreeMaterializationError::InvalidLayout))?,
        });
        journal.preparation_progress = None;
        journal.work = work.journal_work().map_err(backend)?;
        let (stored, journal) =
            compare_next_journal::<S, NativeTreeMaterializationError>(store, journal).await?;
        if !stored {
            return Err(
                MaterializationError::<S::Error, NativeTreeMaterializationError>::Contended.into(),
            );
        }
        Ok(journal)
    }
}
fn recorded_child(
    parent: &cap_std::fs::Dir,
    leaf: &Path,
    expected: [u8; 16],
    work: &NativeMaterializationWork,
) -> Result<cap_std::fs::Dir, NativeTreeMaterializationError> {
    let identity = crate::native_host::directory_identity(parent, &mut &*work)?;
    work.charge(NativeResourceCredit::ReadOperations.delta(1))?;
    let parent = crate::native_host::HostRoot {
        directory: parent.try_clone()?,
        identity,
    };
    let directory = parent.open_dir_held(leaf, &mut &*work)?;
    let actual = crate::native_host::directory_identity(&directory, &mut &*work)?;
    let expected = crate::NativeRootIdentity::from_bytes(expected);
    if actual != expected {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
    }
    Ok(directory)
}
fn verify_preparation(
    root: &Path,
    expected: crate::NativeRootIdentity,
    operation: &Path,
    progress: MaterializationPreparationProgress,
    work: &NativeMaterializationWork,
) -> Result<(), NativeTreeMaterializationError> {
    let actual = work.root_identity(root)?;
    if actual != expected {
        return Err(NativeTreeMaterializationError::AllocationChanged { expected, actual });
    }
    let parent = crate::native_host::HostRoot::open(
        operation
            .parent()
            .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
        &mut &*work,
    )?;
    let expected = crate::NativeRootIdentity::from_bytes(progress.parent);
    if parent.identity() != expected {
        return Err(NativeTreeMaterializationError::AllocationChanged {
            expected,
            actual: parent.identity(),
        });
    }
    if let Some(identity) = progress.operation {
        let parent = parent.open_dir_held(Path::new(""), &mut &*work)?;
        let operation = recorded_child(
            &parent,
            Path::new(
                operation
                    .file_name()
                    .ok_or(NativeTreeMaterializationError::InvalidLayout)?,
            ),
            identity,
            work,
        )?;
        if let Some(identity) = progress.target {
            recorded_child(&operation, Path::new("target"), identity, work)?;
        }
        if let Some(identity) = progress.backup {
            recorded_child(&operation, Path::new("backup"), identity, work)?;
        }
    }
    Ok(())
}
