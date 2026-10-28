use super::*;

/// A known failure, constructible only while the dispatch capability is held.
pub struct PreDispatchFailure {
    message: String,
    original: Option<Box<Error>>,
}

impl PreDispatchFailure {
    /// The deterministic preparation diagnostic.
    pub fn message(&self) -> &str {
        &self.message
    }

    pub(super) fn into_error(self) -> Error {
        match self.original {
            Some(error) => *error,
            None => Error::Invalid(self.message),
        }
    }
}

struct BeforeDispatch;

pub(super) struct ProviderAttempt {
    _started: (),
}

impl ProviderAttempt {
    pub(super) fn generate<'a>(
        self,
        provider: &'a dyn ModelProvider,
        request: &'a crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
    ) -> acyclic_stream::BoxProviderStream<'a, Result<ModelEvent>> {
        provider.generate(request, dispatch)
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn generate_native<'a>(
        self,
        provider: &'a dyn ModelProvider,
        request: &'a crate::model::PreparedModelRequest,
        dispatch: crate::model::ModelDispatch,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, crate::model::NativeModelStream<'a>>> {
        provider.generate_with_native_work(request, dispatch, work)
    }
}

/// One capability shared by the response loop and every nested summary.
pub(crate) struct DispatchPhase(Option<BeforeDispatch>);

impl DispatchPhase {
    pub(crate) fn can_fail(&self, error: &Error) -> bool {
        self.0.is_some()
            && matches!(
                error.code(),
                crate::wire::ErrorCode::Invalid
                    | crate::wire::ErrorCode::Conflict
                    | crate::wire::ErrorCode::Unauthorized
                    | crate::wire::ErrorCode::Unsupported
                    | crate::wire::ErrorCode::NotFound
            )
            && !matches!(
                error,
                Error::InteractionRejected(_) | Error::Indeterminate(_)
            )
    }

    pub(crate) fn new() -> Self {
        Self(Some(BeforeDispatch))
    }

    // Called before claiming an attempt, including a claim whose acknowledgement
    // is lost. A capability is never restored, even if the provider returns Err.
    pub(super) fn start(&mut self) -> ProviderAttempt {
        self.delegate();
        ProviderAttempt { _started: () }
    }

    // The command can classify construction errors only until the executor
    // takes responsibility for tracking provider attempts.
    pub(crate) fn delegate(&mut self) {
        self.0.take();
    }

    pub(crate) async fn finish<T>(
        self,
        result: Result<ExecutionProgress<T>>,
        journal: &dyn ExecutionJournal,
        operation: OperationId,
        io: Option<&PreparationIo>,
    ) -> Result<ExecutionProgress<T>> {
        let error = match result {
            Err(error) if self.can_fail(&error) => error,
            result => return result,
        };
        // A fresh invocation can be a replay. Only an authenticated empty
        // dispatch history permits conversion; an unreadable history proves nothing.
        let mut replay = ExecutionReplay::new(operation);
        loop {
            match replay.next_page(journal, io).await {
                Ok(Some(page)) => {
                    if page.iter().any(|record| {
                        matches!(
                            record.event,
                            ExecutionEvent::ModelStarted { .. }
                                | ExecutionEvent::ToolStarted { .. }
                        )
                    }) {
                        return Err(error);
                    }
                }
                Ok(None) => break,
                Err(replay_error) => return Err(replay_error),
            }
        }
        let message =
            with_history_io(io, |work| preparation_format(format_args!("{error}"), work))?;
        // Ordinary executor APIs retain their original error. Native progress
        // publishes only the funded diagnostic, without retaining an opaque
        // error's unaccountable allocations or enlarging the success frame.
        let original = io.is_none().then(|| Box::new(error));
        Ok(ExecutionProgress::PreDispatchFailed(PreDispatchFailure {
            message,
            original,
        }))
    }
}

/// Dispatch claims use the provider's pre-admitted durable publication. A
/// barrier constructed after a committed start could strand an unissued effect.
pub(crate) async fn claim<T>(future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
    return acyclic_stream::requiring_durability(future).await;
    #[cfg(not(all(feature = "filesystem-local", not(target_arch = "wasm32"))))]
    future.await
}

pub(crate) async fn run<T>(
    journal: &dyn ExecutionJournal,
    io: Option<&PreparationIo>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    PreparedPhase::new(journal, io)?.run(future).await
}

/// Completion custody acquired before dispatch or a cancellable phase begins.
pub(crate) struct PreparedPhase<'a> {
    barrier: Option<crate::conversation::BoundedContentFuture<'a, ()>>,
}

impl<'a> PreparedPhase<'a> {
    pub(crate) fn new(
        journal: &'a dyn ExecutionJournal,
        io: Option<&'a PreparationIo>,
    ) -> Result<Self> {
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        if journal.supports_deferred_durability() {
            return Ok(Self {
                barrier: Some(journal.durability_barrier(io)?),
            });
        }
        let _ = (journal, io);
        Ok(Self { barrier: None })
    }

    pub(crate) async fn run<T>(
        self,
        future: impl std::future::Future<Output = Result<T>>,
    ) -> Result<T> {
        #[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
        if let Some(barrier) = self.barrier {
            let result = acyclic_stream::deferring_durability(future).await;
            // Includes errors and the winning branch of a cancellation race.
            crate::obs::waiting("journal-durability", "prepared-phase", barrier).await?;
            return result;
        }
        let _ = self.barrier;
        future.await
    }
}
