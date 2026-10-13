/// Deterministic process-local provider. It retains each idempotency result and committed
#[derive(Clone)]
pub struct MemoryStream {
    state: Arc<RwLock<State>>,
    limits: MemoryLimits,
    /// The trusted clock that deadlines are checked against.
    clock: Arc<dyn UnixMillisClock>,
    /// Clock whose sampled commit time is recorded in each new record.
    commit_clock: Arc<dyn UnixMillisClock>,
    #[cfg(not(target_arch = "wasm32"))]
    pub(crate) work: Option<Arc<Work>>,
}

impl Default for MemoryStream {
    fn default() -> Self {
        Self::new(MemoryLimits::default())
    }
}

impl MemoryStream {
    pub(crate) async fn next_follow_record(
        &self,
        path: &StreamPath,
        cursor: &mut HistoryCursor,
    ) -> Result<Option<Record>, StreamError> {
        let state = self.state.read().await;
        let current = state.paths.get(path).ok_or(StreamError::NotFound)?;
        cursor.sync(current.history.as_ref(), current.tail);
        Ok(cursor.next_record())
    }

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

    pub(crate) fn new_with_commit_clock(
        limits: MemoryLimits,
        clock: Arc<dyn UnixMillisClock>,
        commit_clock: Arc<dyn UnixMillisClock>,
    ) -> Self {
        Self {
            state: Arc::new(RwLock::new(State::default())),
            limits,
            clock,
            commit_clock,
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
        }
    }
    pub(crate) async fn commit_inner(
        &self,
        mut request: CommitRequest,
        deadline: Option<u64>,
    ) -> Result<(CommitOutcome, Allocation), StreamError> {
        normalize_commit(&mut request)?;
        validate_commit_shape(&request)?;
        #[cfg(not(target_arch = "wasm32"))]
        let digest = if self.work.is_some() {
            self.digest(&mut request)?
        } else {
            commit_digest(&mut request)?
        };
        #[cfg(target_arch = "wasm32")]
        let digest = commit_digest(&mut request)?;
        self.commit_locked(&mut *self.state.write().await, request, digest, deadline)
    }
}
