use super::*;
#[cfg(feature = "native-mount")]
use std::sync::Arc;

/// Accepted staging whose completion has not been acknowledged durably.
#[derive(Debug)]
pub struct NativeStageUncertainty<E> {
    cause: Box<Option<E>>,
    _allocation: NativeAllocation,
}
impl<E> NativeStageUncertainty<E> {
    #[cfg(feature = "native-mount")]
    fn new(work: &NativeMaterializationWork) -> Result<Self, NativeTreeMaterializationError> {
        let bytes = std::mem::size_of::<Option<E>>() as u64;
        let allocation =
            work.claim(bytes, u64::from(bytes != 0), crate::WorkCounters::UNCHARGED)?;
        Ok(Self {
            cause: Box::new(None),
            _allocation: allocation,
        })
    }
    #[cfg(feature = "native-mount")]
    fn fail(mut self, error: E) -> Self {
        *self.cause = Some(error);
        self
    }
    /// Original staging failure.
    pub fn cause(&self) -> &E {
        self.cause.as_ref().as_ref().expect("staging failure")
    }
}

impl<E: std::fmt::Display> std::fmt::Display for NativeStageUncertainty<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "native staging completion uncertain: {}", self.cause())
    }
}
impl<E: std::error::Error + 'static> std::error::Error for NativeStageUncertainty<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.cause())
    }
}

#[cfg(feature = "native-mount")]
impl<S: MaterializationJournalStore>
    JournaledMaterializer<S, Arc<NativeTreeMaterializationBackend>>
{
    /// Stages through this store's fenced journal and the caller's existing work.
    pub async fn stage_generation<A, O, E, F, Fut>(
        &self,
        generation: &crate::Generation<A, O>,
        selections: &[String],
        options: &crate::MaterializeOptions,
        journal: MaterializationJournal,
        work: &NativeMaterializationWork,
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
        let invalid = || {
            E::from(MaterializationError::Backend(
                NativeTreeMaterializationError::InvalidLayout,
            ))
        };
        if !work.shares_ledger(self.backend.work())
            || !journal.is_valid_for(journal.plan.operation_id)
            || journal.phase != MaterializationPhase::Preparing
            || journal.preparation_progress.is_some()
            || !journal.preimages.is_empty()
        {
            return Err(invalid());
        }
        let failure = NativeStageUncertainty::new(work)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        let state = super::stage_state::StageState::<_, S::ActionCompletion<'_>>::new(journal);
        let reader = generation
            .workspace()
            .checkout(
                crate::model::GenerationSelector::Exact(generation.id()),
                crate::model::CheckoutMode::read_only_pinned(),
                work,
                work.cancellation(),
            )
            .await
            .and_then(|checkout| {
                checkout
                    .into_pinned_reader_with_native_work(work, work.cancellation())
                    .map_err(crate::WorkspaceError::from)
            })
            .map_err(E::from)?;
        let mut control = |phase| {
            use crate::native_mount::NativeMaterializationActionPhase::{Before, Completed};
            let authorized = if phase == Before {
                Some(before_effect())
            } else {
                None
            };
            let state = &state;
            let invalid = &invalid;
            async move {
                match phase {
                    Before => {
                        state
                            .before(
                                authorized.expect("effect authorization"),
                                |journal| async {
                                    self.check_plan(&journal.plan).map_err(E::from)?;
                                    self.check_custody(&journal).map_err(E::from)?;
                                    // Acceptance checks both live tails; the backend
                                    // separately checks the physical allocation.
                                    self.verify_allocation(&journal).await.map_err(E::from)?;
                                    self.store.prepare_action(journal).await.map_err(|error| {
                                        E::from(MaterializationError::Store(error))
                                    })
                                },
                                invalid,
                            )
                            .await?;
                    }
                    Completed => {
                        state
                            .completed(
                                |completion| async {
                                    completion.await.map_err(|error| {
                                        E::from(MaterializationError::Store(error))
                                    })
                                },
                                invalid,
                            )
                            .await?;
                    }
                }
                Ok(())
            }
        };
        let result = crate::native_mount::materialize_checkout_host_paths_with_work_guarded(
            &reader,
            selections,
            options,
            work,
            &mut control,
        )
        .await;
        let (journal, completion, last) = state.into_inner();
        drop(completion);
        match (result, journal, last) {
            (Ok(_), Some(journal), false) => Ok(journal),
            (Err(error), _, true) => Err(E::from(failure.fail(error))),
            (Err(error), _, false) => Err(error),
            _ => Err(invalid()),
        }
    }
}
