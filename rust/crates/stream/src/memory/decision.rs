/// A canonical conditional decision, before its visibility transition. The
/// caller retains publication ownership through any external durable boundary.
enum Decision<T, F> {
    Returned(T),
    Accepted { bytes: usize, install: F },
}

impl<T, F: FnOnce(&mut State, bool) -> Result<T, StreamError>> Decision<T, F> {
    fn install(
        self,
        core: &MemoryStream,
        state: &mut State,
        durable: bool,
    ) -> Result<T, StreamError> {
        match self {
            Self::Returned(result) => Ok(result),
            Self::Accepted { bytes, install } => {
                let result = core.publish(bytes, || install(state, durable))?;
                core.applied(state);
                Ok(result)
            }
        }
    }

    fn durable(
        self,
        core: &MemoryStream,
        mut state: impl std::ops::DerefMut<Target = State>,
        mut persist: std::pin::Pin<&mut impl Future<Output = Result<(), StreamError>>>,
    ) -> impl Future<Output = Result<T, StreamError>> {
        async move {
            if matches!(&self, Self::Returned(_)) {
                return self.install(core, &mut state, true);
            }
            // An immediate acknowledgement needs no second state acquisition.
            if let std::task::Poll::Ready(result) = futures::poll!(persist.as_mut()) {
                result?;
                return self.install(core, &mut state, true);
            }
            // The publication owner excludes competing state transitions.
            // Readers can keep using the last accepted state during I/O.
            #[cfg(not(loom))]
            drop(state);
            #[cfg(loom)]
            let state = crate::verification::fault_is(
                crate::verification::Fault::HoldStateDuringDurability,
            )
            .then_some(state);
            // Failure or cancellation drops the uninstalled plan. There is
            // no visible mutation or notification before this succeeds.
            persist.await?;
            #[cfg(loom)]
            drop(state);
            let mut state = core.state.write().await;
            self.install(core, &mut state, true)
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
        command: crate::WriteRequest,
    ) -> Result<crate::journal::CanonicalCommand, StreamError> {
        use crate::WriteRequest;
        use crate::journal::CanonicalCommand;
        let mut command = command.validated()?;
        let digest = match &mut command {
            WriteRequest::Commit(request) => Some(self.commit_request_digest(request)?),
            _ => None,
        };
        Ok(CanonicalCommand { command, digest })
    }

    pub(crate) fn apply_durable(
        &self,
        canonical: &crate::journal::CanonicalCommand,
        deadline: Option<u64>,
        mut persist: std::pin::Pin<&mut impl Future<Output = Result<(), StreamError>>>,
    ) -> impl Future<Output = Result<crate::replicated::Execution, StreamError>> {
        async move {
            use crate::replicated::Execution;
            use crate::{WriteOutcome, WriteRequest};
            let _publication = self.publication.lock().await;
            #[cfg(loom)]
            let _publication = (!crate::verification::fault_is(
                crate::verification::Fault::ReleasePendingPublication,
            ))
            .then_some(_publication);
            let mut state = self.state.write().await;
            match &canonical.command {
                WriteRequest::Append(request) => self
                    .plan_append_locked(&mut state, request)?
                    .durable(self, state, persist.as_mut())
                    .await
                    .map(|(result, allocation)| Execution {
                        outcome: WriteOutcome::Append(result),
                        allocation,
                    }),
                WriteRequest::Fork(request) => self
                    .plan_fork_locked(&mut state, request)?
                    .durable(self, state, persist.as_mut())
                    .await
                    .map(|result| Execution {
                        outcome: WriteOutcome::Fork(result),
                        allocation: None,
                    }),
                WriteRequest::Commit(request) => {
                    let digest = canonical.digest.ok_or(StreamError::InvalidResponse)?;
                    self.plan_commit_locked(&mut state, request, digest, deadline)?
                        .durable(self, state, persist.as_mut())
                        .await
                        .map(|(result, allocation)| Execution {
                            outcome: WriteOutcome::Commit(result),
                            allocation,
                        })
                }
            }
        }
    }
}
