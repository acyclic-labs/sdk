use super::*;
#[cfg(feature = "native-mount")]
use std::sync::Arc;

/// Accepted staging whose completion has not been acknowledged durably.
#[derive(Debug)]
pub struct NativeStageUncertainty<E> {
    cause: Box<Option<E>>,
    /// Last acknowledged work prefix.
    pub last_durable_work: crate::WorkCounters,
    /// Actual invocation work.
    pub observed_work: crate::WorkCounters,
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
            last_durable_work: crate::WorkCounters::UNCHARGED,
            observed_work: crate::WorkCounters::UNCHARGED,
            _allocation: allocation,
        })
    }
    #[cfg(feature = "native-mount")]
    fn fail(mut self, error: E, last_durable_work: crate::WorkCounters) -> Self {
        *self.cause = Some(error);
        self.last_durable_work = last_durable_work;
        self.observed_work = self._allocation.work().receipt();
        self
    }
    /// Original staging failure.
    pub fn cause(&self) -> &E {
        self.cause.as_ref().as_ref().expect("staging failure")
    }
}

#[cfg(feature = "native-mount")]
impl<S: MaterializationJournalStore>
    JournaledMaterializer<S, Arc<NativeTreeMaterializationBackend>>
{
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
        let failure = NativeStageUncertainty::new(work)
            .map_err(|error| E::from(MaterializationError::Backend(error)))?;
        before_effect().await?;
        self.verify_effect_context(&journal)
            .await
            .map_err(E::from)?;
        let last_durable_work = journal.work;
        let completion = self
            .store
            .prepare_action(journal)
            .await
            .map_err(|error| E::from(MaterializationError::Store(error)))?;
        let result = async {
            let identity = self
                .backend
                .reset_target_directory(relative)
                .await
                .map_err(|error| E::from(MaterializationError::Backend(error)))?;
            let journal = completion
                .await
                .map_err(|error| E::from(MaterializationError::Store(error)))?;
            Ok((journal, identity))
        }
        .await;
        result.map_err(|error| E::from(failure.fail(error, last_durable_work)))
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
    pub fn stage_generation<A, O, E, F, Fut>(
        &self,
        generation: &crate::Generation<A, O>,
        selections: &[String],
        options: &crate::MaterializeOptions,
        journal: MaterializationJournal,
        work: &NativeMaterializationWork,
        before_effect: &mut F,
    ) -> impl Future<Output = Result<MaterializationJournal, E>> + Send
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
        async move {
            if !work.shares_ledger(self.backend.work())
                || !journal.is_valid_for(journal.plan.operation_id)
                || journal.phase != MaterializationPhase::Preparing
                || journal.preparation_progress.is_some()
                || !journal.preimages.is_empty()
            {
                return Err(E::from(MaterializationError::Backend(
                    NativeTreeMaterializationError::InvalidLayout,
                )));
            }
            let failure = NativeStageUncertainty::new(work)
                .map_err(|error| E::from(MaterializationError::Backend(error)))?;
            let state = parking_lot::Mutex::new((
                Some(journal),
                None::<S::ActionCompletion<'_>>,
                None::<crate::WorkCounters>,
            ));
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
                async move {
                    match phase {
                        Before => {
                            authorized.expect("effect authorization").await?;
                            let journal = state.lock().0.take().ok_or_else(|| {
                                E::from(MaterializationError::Backend(
                                    NativeTreeMaterializationError::InvalidLayout,
                                ))
                            })?;
                            self.verify_effect_context(&journal)
                                .await
                                .map_err(E::from)?;
                            let last = journal.work;
                            let completion = self
                                .store
                                .prepare_action(journal)
                                .await
                                .map_err(|error| E::from(MaterializationError::Store(error)))?;
                            *state.lock() = (None, Some(completion), Some(last));
                        }
                        Completed => {
                            let completion = state.lock().1.take().ok_or_else(|| {
                                E::from(MaterializationError::Backend(
                                    NativeTreeMaterializationError::InvalidLayout,
                                ))
                            })?;
                            let journal = completion
                                .await
                                .map_err(|error| E::from(MaterializationError::Store(error)))?;
                            *state.lock() = (Some(journal), None, None);
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
            drop(control);
            let (journal, completion, last) = state.into_inner();
            drop(completion);
            match (result, journal, last) {
                (Ok(_), Some(journal), None) => Ok(journal),
                (Err(error), _, Some(last_durable_work)) => {
                    Err(E::from(failure.fail(error, last_durable_work)))
                }
                (Err(error), _, None) => Err(error),
                _ => Err(E::from(MaterializationError::Backend(
                    NativeTreeMaterializationError::InvalidLayout,
                ))),
            }
        }
    }
}
