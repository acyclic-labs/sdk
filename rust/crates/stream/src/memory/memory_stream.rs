/// Deterministic process-local provider with durable-lifetime successful request identities.
#[derive(Clone)]
pub struct MemoryStream {
    // Lock order: LocalStream visibility -> publication -> state -> watch internals.
    // Publication owns ordering across durable I/O; state guards only visible facts.
    publication: Arc<Mutex<()>>,
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
    /// Optional authority-side acceptance rule, decided at commit time.
    /// Shared by every clone of this ledger, so a wrapper installs it once.
    admission: Arc<std::sync::OnceLock<Arc<dyn CommitAdmission>>>,
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

    #[cfg(not(target_arch = "wasm32"))]
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
            publication: Arc::new(Mutex::new(())),
            state: Arc::new(RwLock::new(State::default())),
            revisions: Arc::new(crate::AppliedRevisions::in_process()),
            operation_claims: Arc::new(crate::OperationClaims::default()),
            limits,
            clock,
            commit_clock,
            admission: Arc::default(),
            #[cfg(not(target_arch = "wasm32"))]
            work: None,
            #[cfg(loom)]
            retain_pending_read: false,
        }
    }
    /// Installs this ledger's authority-side time rule. At each new commit's
    /// decision, after every condition holds and before anything is reserved,
    /// that commit's own authority time must have reached the rule's
    /// not-before time for it, else it is refused with [`StreamError::not_before`]
    /// carrying that authority time. Replays never re-run it. A ledger has at
    /// most one rule: installing that same rule again, as every opener of a
    /// shared ledger does, is a no-op; a different rule is `AlreadyExists`.
    pub fn install_commit_admission(
        &self,
        admission: Arc<dyn CommitAdmission>,
    ) -> Result<(), StreamError> {
        self.admission
            .set(admission)
            .or_else(|admission| match self.admission.get() {
                Some(installed) if Arc::ptr_eq(installed, &admission) => Ok(()),
                _ => Err(StreamError::AlreadyExists),
            })
    }

    fn not_before(&self, request: &CommitRequest) -> Option<u64> {
        self.admission
            .get()
            .and_then(|rule| rule.not_before(request))
    }

    /// Decides the installed rule with the commit's authority time in micros.
    fn admit_at(&self, request: &CommitRequest, micros: u64) -> Result<(), StreamError> {
        admit_not_before(self.not_before(request), micros)
    }
    /// Rebinds to an externally owned authority: replicas replaying one shared
    /// journal in one order, or a store whose incarnation outlives a process,
    /// issue and honour the same tokens.
    #[cfg(any(
        test,
        not(target_arch = "wasm32"),
        all(feature = "wasm-journal", target_arch = "wasm32")
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
}

#[cfg(loom)]
impl MemoryStream {
    pub(crate) async fn model_hold_writer(&self) -> impl Send + '_ {
        self.state.write().await
    }
}

/// An authority-side time rule over new commits: the earliest authority time,
/// in Unix milliseconds, at which a commit may be accepted. It reads only the
/// request's own mutations; requests carry no time field for it.
pub trait CommitAdmission: Send + Sync + 'static {
    /// The earliest acceptance time of `request`, or `None` for any time.
    fn not_before(&self, request: &CommitRequest) -> Option<u64>;
}

fn admit_not_before(not_before: Option<u64>, micros: u64) -> Result<(), StreamError> {
    let authority = micros / 1_000;
    match not_before {
        Some(not_before) if authority < not_before => Err(StreamError::not_before(authority)),
        _ => Ok(()),
    }
}
