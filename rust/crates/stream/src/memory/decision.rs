/// A canonical conditional decision, before its visibility transition. The
/// caller retains the state write guard through any external durable boundary.
enum Decision<T, F> {
    Returned(T),
    Accepted { bytes: usize, install: F },
}

impl<T, F: FnOnce(&mut State, bool) -> Result<T, StreamError>> Decision<T, F> {
    fn install(self, core: &MemoryStream, state: &mut State) -> Result<T, StreamError> {
        match self {
            Self::Returned(result) => Ok(result),
            Self::Accepted { bytes, install } => core.publish(bytes, || install(state, false)),
        }
    }

    #[cfg(any(
        feature = "postgres",
        all(feature = "wasm-journal", target_arch = "wasm32")
    ))]
    async fn durable(
        self,
        core: &MemoryStream,
        state: &mut State,
        persist: impl Future<Output = Result<(), StreamError>>,
    ) -> Result<T, StreamError> {
        match self {
            Self::Returned(result) => Ok(result),
            Self::Accepted { bytes, install } => {
                // Failure or cancellation drops the uninstalled plan. There is
                // no visible mutation or notification before this succeeds.
                persist.await?;
                core.publish(bytes, || install(state, true))
            }
        }
    }
}

#[cfg(any(
    feature = "postgres",
    all(feature = "wasm-journal", target_arch = "wasm32")
))]
impl MemoryStream {
    pub(crate) async fn apply_durable(
        &self,
        command: crate::journal::Command,
        deadline: Option<u64>,
        persist: impl Future<Output = Result<(), StreamError>>,
    ) -> Result<crate::journal::Outcome, StreamError> {
        use crate::journal::{Command, Outcome};
        let mut state = self.state.write().await;
        match command {
            Command::Append(request) => self
                .plan_append_locked(&mut state, request)?
                .durable(self, &mut state, persist)
                .await
                .map(|(result, _)| Outcome::Append(result)),
            Command::Fork(request) => self
                .plan_fork_locked(&mut state, request)?
                .durable(self, &mut state, persist)
                .await
                .map(Outcome::Fork),
            Command::Commit(mut request) => {
                let digest = commit_digest(&mut request)?;
                self.plan_commit_locked(&mut state, request, digest, deadline)?
                    .durable(self, &mut state, persist)
                    .await
                    .map(|(result, _)| Outcome::Commit(result))
            }
        }
    }
}
