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
    pub(crate) fn canonicalize(
        &self,
        command: crate::journal::Command,
    ) -> Result<crate::journal::CanonicalCommand, StreamError> {
        use crate::journal::{CanonicalCommand, Command};
        let mut command = command.validated()?;
        let digest = match &mut command {
            Command::Commit(request) => Some(self.commit_request_digest(request)?),
            _ => None,
        };
        Ok(CanonicalCommand { command, digest })
    }

    pub(crate) async fn apply_durable(
        &self,
        canonical: &crate::journal::CanonicalCommand,
        deadline: Option<u64>,
        persist: impl Future<Output = Result<(), StreamError>>,
    ) -> Result<crate::replicated::Execution, StreamError> {
        use crate::journal::{Command, Outcome};
        use crate::replicated::Execution;
        let mut state = self.state.write().await;
        match &canonical.command {
            Command::Append(request) => self
                .plan_append_locked(&mut state, request)?
                .durable(self, &mut state, persist)
                .await
                .map(|(result, allocation)| Execution {
                    outcome: Outcome::Append(result),
                    allocation,
                }),
            Command::Fork(request) => self
                .plan_fork_locked(&mut state, request)?
                .durable(self, &mut state, persist)
                .await
                .map(|result| Execution {
                    outcome: Outcome::Fork(result),
                    allocation: None,
                }),
            Command::Commit(request) => {
                let digest = canonical.digest.ok_or(StreamError::InvalidResponse)?;
                self.plan_commit_locked(&mut state, request, digest, deadline)?
                    .durable(self, &mut state, persist)
                    .await
                    .map(|(result, allocation)| Execution {
                        outcome: Outcome::Commit(result),
                        allocation,
                    })
            }
        }
    }
}
