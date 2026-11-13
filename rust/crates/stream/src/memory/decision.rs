/// A canonical conditional decision, before its visibility transition. The
/// caller retains publication ownership through any external durable boundary.
enum Decision<T, F> {
    Returned(T),
    Accepted { bytes: usize, install: F },
}

impl<T, F: FnOnce(&mut State, bool, Option<u64>) -> Result<T, StreamError>> Decision<T, F> {
    fn install(
        self,
        core: &MemoryStream,
        state: &mut State,
        durable: bool,
    ) -> Result<T, StreamError> {
        self.install_at(core, state, durable, None)
    }

    fn install_at(
        self,
        core: &MemoryStream,
        state: &mut State,
        durable: bool,
        at_millis: Option<u64>,
    ) -> Result<T, StreamError> {
        match self {
            Self::Returned(result) => Ok(result),
            Self::Accepted { bytes, install } => {
                let result = core.publish(bytes, || install(state, durable, at_millis))?;
                core.applied(state);
                Ok(result)
            }
        }
    }

    /// Persists then installs a durable decision. An ephemeral one installs
    /// at once and never reaches `persist`: no frame, no fsync.
    fn durable(
        self,
        core: &MemoryStream,
        mut state: impl std::ops::DerefMut<Target = State>,
        ephemeral: bool,
        mut persist: std::pin::Pin<&mut impl Future<Output = Result<Option<u64>, StreamError>>>,
    ) -> impl Future<Output = Result<T, StreamError>> {
        async move {
            if ephemeral || matches!(&self, Self::Returned(_)) {
                return self.install(core, &mut state, !ephemeral);
            }
            // An immediate acknowledgement needs no second state acquisition.
            if let std::task::Poll::Ready(result) = futures::poll!(persist.as_mut()) {
                return self.install_at(core, &mut state, true, result?);
            }
            // The publication owner excludes competing state transitions.
            // Readers can keep using the last accepted state during I/O.
            #[cfg(not(all(loom, not(target_arch = "wasm32"))))]
            drop(state);
            #[cfg(all(loom, not(target_arch = "wasm32")))]
            let state = crate::verification::fault_is(
                crate::verification::Fault::HoldStateDuringDurability,
            )
            .then_some(state);
            // Failure or cancellation drops the uninstalled plan. There is
            // no visible mutation or notification before this succeeds.
            let at_millis = persist.await?;
            #[cfg(all(loom, not(target_arch = "wasm32")))]
            drop(state);
            let mut state = core.state.write().await;
            self.install_at(core, &mut state, true, at_millis)
        }
    }
}

impl MemoryStream {
    pub(crate) fn canonicalize(
        &self,
        command: crate::WriteRequest,
    ) -> Result<crate::journal::CanonicalCommand, StreamError> {
        command.canonicalized_with(|request| self.commit_request_digest(request).map(Some))
    }

    pub(crate) fn apply_durable(
        &self,
        canonical: &crate::journal::CanonicalCommand,
        deadline: Option<u64>,
        mut persist: std::pin::Pin<&mut impl Future<Output = Result<Option<u64>, StreamError>>>,
    ) -> impl Future<Output = Result<crate::replicated::Execution, StreamError>> {
        async move {
            use crate::replicated::Execution;
            use crate::{WriteOutcome, WriteRequest};
            let _publication = self.publication.lock().await;
            #[cfg(all(loom, not(target_arch = "wasm32")))]
            let _publication = (!crate::verification::fault_is(
                crate::verification::Fault::ReleasePendingPublication,
            ))
            .then_some(_publication);
            let mut state = self.state.write().await;
            let ephemeral = matches!(
                state.ephemeral.write_scope(&canonical.command)?,
                Scope::Ephemeral(_)
            );
            match &canonical.command {
                WriteRequest::Append(request) => self
                    .plan_append_locked(&mut state, request)?
                    .durable(self, state, ephemeral, persist.as_mut())
                    .await
                    .map(|(result, allocation)| Execution {
                        outcome: WriteOutcome::Append(result),
                        _allocation: allocation,
                        ephemeral,
                    }),
                WriteRequest::Fork(request) => self
                    .plan_fork_locked(&mut state, request)?
                    .durable(self, state, ephemeral, persist.as_mut())
                    .await
                    .map(|result| Execution {
                        outcome: WriteOutcome::Fork(result),
                        _allocation: None,
                        ephemeral,
                    }),
                WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
                    let digest = canonical.digest.ok_or(StreamError::InvalidResponse)?;
                    self.plan_commit_locked(
                        &mut state,
                        request,
                        digest,
                        deadline,
                        matches!(&canonical.command, WriteRequest::CommitAccepted(_)),
                    )?
                    .durable(self, state, ephemeral, persist.as_mut())
                    .await
                    .map(|(result, allocation)| Execution {
                        outcome: result,
                        _allocation: allocation,
                        ephemeral,
                    })
                }
            }
        }
    }
}
