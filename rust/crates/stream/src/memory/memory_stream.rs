/// Deterministic process-local provider with durable-lifetime successful request identities.
#[derive(Clone)]
pub struct MemoryStream {
    // Lock order: LocalStream visibility -> state -> watch internals.
    state: Arc<RwLock<State>>,
    /// The one applied-revision authority of this incarnation; advanced under
    /// the state write guard after each publication.
    pub(crate) revisions: Arc<crate::AppliedRevisions>,
    operation_claims: Arc<crate::OperationClaims>,
    limits: MemoryLimits,
    /// The trusted clock that deadlines are checked against.
    clock: Arc<dyn UnixMillisClock>,
    /// Clock whose sampled commit time is recorded in each new record.
    commit_clock: Arc<dyn UnixMillisClock>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) work: Option<Arc<Work>>,
    #[cfg(loom)]
    pub(crate) retain_pending_read: bool,
}

impl Default for MemoryStream {
    fn default() -> Self {
        Self::new(MemoryLimits::default())
    }
}

impl MemoryStream {
    /// Constructs one bounded independent provider.
    #[must_use]
    pub fn new(limits: MemoryLimits) -> Self {
        Self::new_with_clock(limits, Arc::new(SystemUnixMillisClock))
    }

    /// Constructs a provider with an injected trusted clock.
    #[must_use]
    pub fn new_with_clock(limits: MemoryLimits, clock: Arc<dyn UnixMillisClock>) -> Self {
        Self::new_with_commit_clock(limits, Arc::clone(&clock), clock)
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) fn with_commit_clock(&self, clock: Arc<dyn UnixMillisClock>) -> Self {
        Self {
            commit_clock: clock,
            ..self.clone()
        }
    }

    pub(crate) fn new_with_commit_clock(
        limits: MemoryLimits,
        clock: Arc<dyn UnixMillisClock>,
        commit_clock: Arc<dyn UnixMillisClock>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
            revisions: Arc::new(crate::AppliedRevisions::in_process()),
            operation_claims: Arc::new(crate::OperationClaims::default()),
            limits,
            clock,
            commit_clock,
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            #[cfg(loom)]
            retain_pending_read: false,
        }
    }
    /// Rebinds to an externally owned authority: replicas replaying one shared
    /// journal in one order, or a store whose incarnation outlives a process,
    /// issue and honour the same tokens.
    #[cfg(any(
        test,
        feature = "local",
        all(feature = "postgres", not(target_arch = "wasm32"))
    ))]
    #[must_use]
    pub(crate) fn with_authority(mut self, scope: crate::RevisionScope, incarnation: u128) -> Self {
        self.revisions = Arc::new(crate::AppliedRevisions::new(scope, incarnation, 0));
        self
    }

    /// Publishes this state's applied revision while the write guard is held.
    fn applied(&self, state: &State) {
        self.revisions.advance(state.decision);
    }
    fn commit_request_digest(&self, request: &mut CommitRequest) -> Result<[u8; 32], StreamError> {
        #[cfg(not(target_arch = "wasm32"))]
        return if self.work.is_some() {
            self.digest(request)
        } else {
            commit_digest(request)
        };
        #[cfg(target_arch = "wasm32")]
        commit_digest(request)
    }

    pub(crate) async fn commit_inner(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        let digest = self.commit_request_digest(&mut request)?;
        self.commit_locked(&mut *self.state.write().await, request, digest, deadline)
    }
}

#[cfg(loom)]
impl MemoryStream {
    pub(crate) async fn model_hold_writer(&self) -> impl Send + '_ {
        self.state.write().await
    }
}
