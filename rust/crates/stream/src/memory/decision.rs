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
            Self::Accepted { bytes, install } => {
                let result = core.publish(bytes, || install(state, false))?;
                core.applied(state);
                Ok(result)
            }
        }
    }

    #[cfg(any(
        feature = "local",
        feature = "postgres",
        all(
            any(feature = "wasm", feature = "wasm-journal"),
            target_arch = "wasm32"
        )
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
                let result = core.publish(bytes, || install(state, true))?;
                core.applied(state);
                Ok(result)
            }
        }
    }
}

#[cfg(any(
    feature = "local",
    feature = "postgres",
    all(
        any(feature = "wasm", feature = "wasm-journal"),
        target_arch = "wasm32"
    )
))]
impl MemoryStream {
    pub(crate) async fn apply_durable(
        &self,
        command: crate::journal::Command,
        deadline: Option<u64>,
        persist: impl Future<Output = Result<(), StreamError>>,
    ) -> Result<crate::journal::Outcome, StreamError> {
        use crate::journal::{Command, Outcome};
        let (command, digest) = match command {
            Command::Commit(mut request) => {
                let digest = self.commit_request_digest(&mut request)?;
                (Command::Commit(request), Some(digest))
            }
            command => (command, None),
        };
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
            Command::Commit(request) => {
                let digest = digest.ok_or(StreamError::InvalidResponse)?;
                self.plan_commit_locked(&mut state, request, digest, deadline)?
                    .durable(self, &mut state, persist)
                    .await
                    .map(|(result, _)| Outcome::Commit(result))
            }
        }
    }
}
