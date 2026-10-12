use super::*;

impl<A, O, D, S> LazyMountSource<A, O, D, S>
where
    A: AsyncAuthorityStore + Send + Sync + 'static,
    O: AsyncObjectStore + Send + Sync + 'static,
    D: DemandSource + 'static,
    S: LazyWorkspaceStore,
{
/// Composes lazy reads with one ordinary authored checkout adapter.
    pub(super) fn new(
        lazy: Arc<LazyWorkspace<A, O, D, S>>,
        authored: Arc<CheckoutMountSource<A, O>>,
        root: String,
    ) -> Result<Self, super::NativeMountError> {
        // Source names resolve exactly while a folding checkout would resolve
        // authored names by case; one namespace cannot do both.
        if authored.folds_names() {
            return Err(super::NativeMountError::ProfileUnavailable {
                profile: "case-folding",
                platform: "a lazy source projection",
            });
        }
        // A source the host cannot watch, or refuses to, is read afresh.
        let source_watch = lazy
            .watch_source(Arc::new(SourceChangeRecorder {
                lazy: Arc::downgrade(&lazy),
                authored: Arc::downgrade(&authored),
            }))
            .ok()
            .flatten();
        Ok(Self {
            lazy,
            authored,
            root,
            runtime: Arc::new(CallbackRuntime::create()?),
            resolutions: Resolutions::default(),
            source_absent: SourceAbsentDirectories::default(),
            authored_directories: Mutex::new(HashMap::new()),
            unstaged: Arc::default(),
            cursors: CursorTable::new(MAXIMUM_LAZY_DIRECTORY_CURSORS),
            source_view: Arc::new(SourceViewGate::new()),
            removals: Mutex::new(MountedRemovals::default()),
            detached: Arc::new(Mutex::new(BTreeMap::new())),
            open_sources: Arc::new(Mutex::new(BTreeMap::new())),
            source_watch,
            linked: Mutex::default(),
        })
    }

/// Cancels the authored callback adapter.
    pub fn cancel(&self) {
        self.cursors.clear();
        self.authored.cancel();
    }

pub(super) fn wait<T: Send, F>(&self, create: impl FnOnce() -> F + Send) -> Result<T, MountSourceError>
    where
        F: std::future::Future<Output = Result<T, MountSourceError>>,
    {
        self.runtime.wait(create)
    }
}
