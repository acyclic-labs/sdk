use super::*;

impl Projector {
    pub(super) fn new() -> Self {
        Self {
            clock: super::super::clock::Clock::capture(),
            state: Mutex::new(ProjectorState::default()),
            tree: Mutex::new(()),
            progress: tokio::sync::Notify::new(),
            paths: KeyedLocks::default(),
            changed: Condvar::new(),
            worker: Mutex::new(None),
            lifecycle: Mutex::new(ProjectorLifecycle::default()),
            lifecycle_changed: Condvar::new(),
            service: std::sync::OnceLock::new(),
        }
    }

    /// Publish one projector change to both native worker and future waiters.
    /// Future registrations are enabled before observing the protected state.
    pub(super) fn signal_changed(&self) {
        self.changed.notify_all();
        self.progress.notify_waiters();
    }

    /// Signals new work: wakes `settle` waiters and this view's runner,
    /// its own lifecycle thread or the shared instance's projector service.
    pub(super) fn wake(&self) {
        self.signal_changed();
        if let Some(hook) = self.service.get() {
            hook.schedule();
        }
    }

    pub(super) fn written(
        &self,
        path: MountPath,
        facts: EntryFacts,
        owned: Option<Owned>,
        basis: Option<ReadBasis>,
    ) {
        lock_recover(&self.state).written.insert(
            path,
            WrittenEntry {
                owned,
                facts: Some(facts),
                basis,
            },
        );
    }

    pub(super) fn materialize(
        &self,
        projection: Projection<'_>,
        path: MountPath,
    ) -> Result<(), String> {
        let _tree = lock_recover(&self.tree);
        let result = self.materialize_locked(projection, vec![path]);
        // Entries this listing superseded outside a pass are attempted by
        // the next pass, not on a timer.
        let mut state = lock_recover(&self.state);
        if !state.pending.is_empty() {
            state.retry = true;
            self.wake();
        }
        result
    }

    pub(super) fn materialize_locked(
        &self,
        projection: Projection<'_>,
        directories: Vec<MountPath>,
    ) -> Result<(), String> {
        if directories.is_empty() {
            return Ok(());
        }
        let wanted = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MATERIALIZE_WORKERS);
        // Helpers come from one process-wide pool, so concurrent views
        // never hold more than its size in extra threads between them.
        let helpers = MaterializeHelpers::acquire(wanted.saturating_sub(1));
        let queue = MaterializeQueue::new(directories);
        std::thread::scope(|scope| {
            for _ in 0..helpers.count() {
                scope.spawn(|| self.materialize_from(&projection, &queue));
            }
            self.materialize_from(&projection, &queue);
        });
        drop(helpers);
        queue.finish()
    }

    pub(super) fn materialize_from(&self, projection: &Projection<'_>, queue: &MaterializeQueue) {
        while let Some(directory) = queue.next() {
            let written = self.materialize_directory(*projection, directory);
            queue.done(written);
        }
    }

    pub(super) fn materialize_directory(
        &self,
        projection: Projection<'_>,
        directory: MountPath,
    ) -> Result<Vec<MountPath>, String> {
        let known = lock_recover(&self.state)
            .directories
            .get(&directory)
            .map(|materialized| materialized.names.clone())
            .unwrap_or_default();
        let basis = ReadBasis::sample(projection.runtime.source.as_ref());
        let entries = match source_listing(projection, &directory) {
            Ok(entries) => entries,
            Err(MountSourceError::NotFound) => {
                self.remove_tree(projection, &directory);
                return Ok(Vec::new());
            }
            Err(MountSourceError::Stale) => {
                let mut state = lock_recover(&self.state);
                state
                    .directories
                    .entry(directory.clone())
                    .or_insert(Materialized {
                        basis: None,
                        names: HashMap::new(),
                    });
                state.stale.insert(directory);
                self.wake();
                return Ok(Vec::new());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.0.len());
        let mut created = Vec::new();
        let mut written = Vec::with_capacity(entries.0.len());
        let result = (|| {
            for entry in &entries.0 {
                let is_directory = entry.info.IsDirectory;
                let child = directory.child(entry.component.clone());
                names.insert(entry.component.clone(), is_directory);
                let previous = known.get(&entry.component).copied().or_else(|| {
                    lock_recover(&self.state)
                        .pending
                        .contains_key(&child)
                        .then_some(false)
                });
                if previous == Some(is_directory) {
                    continue;
                }
                let Some(mut child) = self.retire_previous_child(projection, child, previous)?
                else {
                    return Ok(Vec::new());
                };
                let placed = match basis {
                    Some(basis) => with_creating_name(
                        &projection.runtime.creating_name,
                        &mut child,
                        (entry.facts.lookup.node.file_id, basis),
                        |path| write_entry(projection, path, entry),
                    ),
                    None => write_entry(projection, &child, entry),
                }?;
                if placed == Placed::Retry {
                    names.remove(entry.component.as_slice());
                    lock_recover(&self.state).stale.insert(directory.clone());
                    self.wake();
                    continue;
                }
                if is_directory {
                    created.push(child);
                } else if placed != Placed::Nothing {
                    written.push((child, entry.facts, placed.owned()));
                }
            }
            let mut state = lock_recover(&self.state);
            for (name, was_directory) in &known {
                if names.contains_key(name) {
                    continue;
                }
                let child = directory.child(name.clone());
                if *was_directory {
                    drop(state);
                    self.remove_tree(projection, &child);
                    state = lock_recover(&self.state);
                } else if !state.pending.contains_key(&child) {
                    let pending = state.written.remove(&child).unwrap_or(WrittenEntry {
                        owned: None,
                        facts: None,
                        basis: None,
                    });
                    state.pending.insert(child, pending);
                }
            }
            for (path, facts, owned) in written.drain(..) {
                state.written.insert(
                    path,
                    WrittenEntry {
                        owned,
                        facts: Some(facts),
                        basis,
                    },
                );
            }
            state
                .directories
                .insert(directory, Materialized { basis, names });
            Ok(created)
        })();
        for (path, facts, owned) in written {
            self.written(path, facts, owned, basis);
        }
        result
    }

    fn retire_previous_child(
        &self,
        projection: Projection<'_>,
        child: MountPath,
        previous: Option<bool>,
    ) -> Result<Option<MountPath>, String> {
        match previous {
            Some(true) => self.remove_tree(projection, &child),
            Some(false) => {
                let since = lock_recover(&self.state).releases;
                let (owned, released) = self.release_recorded(projection, &child);
                if released? == Release::Busy {
                    let mut state = lock_recover(&self.state);
                    let pending = state.written.remove(&child).unwrap_or(WrittenEntry {
                        owned,
                        facts: None,
                        basis: None,
                    });
                    state.relist.insert(child.clone());
                    state.pending.entry(child).or_insert(pending);
                    if state.held(since) {
                        self.wake();
                    }
                    return Ok(None);
                }
            }
            None => {}
        }
        Ok(Some(child))
    }

    pub(super) fn release_recorded(
        &self,
        projection: Projection<'_>,
        path: &MountPath,
    ) -> (Option<Owned>, Result<Release, String>) {
        self.paths.with(path, || {
            let owned = {
                let state = lock_recover(&self.state);
                state
                    .written
                    .get(path)
                    .or_else(|| state.pending.get(path))
                    .and_then(|written| written.owned)
            };
            let released = release(projection, path, owned);
            if released == Ok(Release::Done) {
                let mut state = lock_recover(&self.state);
                state.written.remove(path);
                state.pending.remove(path);
                state.relist.remove(path);
            }
            (owned, released)
        })
    }

    pub(super) fn remove_tree(&self, projection: Projection<'_>, directory: &MountPath) {
        let mut removed = {
            let mut state = lock_recover(&self.state);
            let beneath = state
                .directories
                .keys()
                .filter(|path| projfs_path_suffix(path, directory).is_some())
                .cloned()
                .collect::<Vec<_>>();
            beneath
                .into_iter()
                .filter_map(|path| state.directories.remove(&path).map(|entry| (path, entry)))
                .collect::<Vec<_>>()
        };
        removed.sort_by_key(|(path, _)| std::cmp::Reverse(path.components().len()));
        let since = lock_recover(&self.state).releases;
        let mut held = false;
        for (path, materialized) in removed {
            for (name, is_directory) in materialized.names {
                if !is_directory {
                    let child = path.child(name);
                    let (owned, released) = self.release_recorded(projection, &child);
                    if released == Ok(Release::Busy) {
                        held = true;
                        let mut state = lock_recover(&self.state);
                        let written = state.written.remove(&child).unwrap_or(WrittenEntry {
                            owned,
                            facts: None,
                            basis: None,
                        });
                        state.pending.entry(child).or_insert(written);
                        if state.held(since) {
                            self.wake();
                        }
                    }
                }
            }
            if held {
                lock_recover(&self.state).vacated.insert(path);
            } else {
                remove_vacated(projection, &path);
            }
        }
    }

    pub(super) fn clear_vacated(&self, projection: Projection<'_>) {
        let mut cleared = {
            let mut state = lock_recover(&self.state);
            let state = &mut *state;
            let beneath = |directory: &MountPath, path: &MountPath| {
                projfs_path_suffix(path, directory).is_some()
            };
            let cleared = state
                .vacated
                .iter()
                .filter(|directory| !state.pending.keys().any(|path| beneath(directory, path)))
                .cloned()
                .collect::<Vec<_>>();
            state
                .vacated
                .retain(|directory| !cleared.contains(directory));
            cleared
                .into_iter()
                .filter(|directory| {
                    !state
                        .directories
                        .keys()
                        .any(|path| beneath(directory, path))
                })
                .collect::<Vec<_>>()
        };
        cleared.sort_by_key(|path| std::cmp::Reverse(path.components().len()));
        for directory in cleared {
            remove_vacated(projection, &directory);
        }
    }

    pub(super) fn current_beneath(
        &self,
        source: &dyn MountFilesystem,
        from: &MountPath,
    ) -> Vec<MountPath> {
        lock_recover(&self.state)
            .written
            .iter()
            .filter(|(path, written)| {
                projfs_path_suffix(path, from).is_some()
                    && written.facts.is_some()
                    && !written.superseded(source, path)
            })
            .map(|(path, _)| path.clone())
            .collect()
    }

    pub(super) fn renamed(&self, from: &MountPath, to: &MountPath, carried: Carried) {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        let moved = state
            .directories
            .keys()
            .filter_map(|path| rebased(path, from, to).map(|moved| (path.clone(), moved)))
            .collect::<Vec<_>>();
        for (old, new) in moved {
            if let Some(materialized) = state.directories.remove(&old) {
                state.directories.insert(new, materialized);
            }
        }
        for projector in [&mut state.written, &mut state.pending] {
            let moved = projector
                .keys()
                .filter_map(|path| rebased(path, from, to).map(|moved| (path.clone(), moved)))
                .collect::<Vec<_>>();
            for (old, new) in moved {
                if let Some(written) = projector.remove(&old) {
                    projector.insert(new, written);
                }
            }
        }
        let is_directory = state.directories.contains_key(to);
        if let (Some(parent), Some(name)) = (from.parent(), from.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent)
        {
            materialized.names.remove(name.as_slice());
        }
        if let (Some(parent), Some(name)) = (to.parent(), to.components().last())
            && let Some(materialized) = state.directories.get_mut(&parent)
        {
            materialized.names.insert(name.clone(), is_directory);
        }
        for (path, facts) in carried.files {
            let written = state.written.remove(&path);
            let pending = state.pending.remove(&path);
            state.written.insert(
                path,
                WrittenEntry {
                    owned: written.or(pending).and_then(|written| written.owned),
                    facts: Some(facts),
                    basis: carried.basis,
                },
            );
        }
    }

    pub(super) fn invalidate(&self, path: &MountPath) {
        let mut state = lock_recover(&self.state);
        state.written.remove(path);
        if let (Some(parent), Some(name)) = (path.parent(), path.components().last()) {
            if let Some(materialized) = state.directories.get_mut(&parent) {
                materialized.names.remove(name.as_slice());
            }
            state.stale.insert(parent);
        }
        self.wake();
    }

    pub(super) fn invalidate_all(&self) {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        state.stale.extend(state.directories.keys().cloned());
        let written = std::mem::take(&mut state.written);
        state.pending.extend(written);
        self.wake();
    }

    pub(super) fn attach(self: &Arc<Self>, runtime: Arc<Runtime>) -> Result<(), NativeMountError> {
        let projector = Arc::clone(self);
        let worker = std::thread::Builder::new()
            .name("acyclic-projfs-projector".to_owned())
            .spawn(move || projector.run_lifecycle(runtime))
            .map_err(NativeMountError::Io)?;
        *lock_recover(&self.worker) = Some(worker);
        Ok(())
    }

    pub(super) fn run(&self, runtime: &Runtime, context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT) {
        let mut state = lock_recover(&self.state);
        loop {
            match state.readiness() {
                Readiness::Stopped => return,
                Readiness::Ready => {}
                Readiness::Idle => {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    continue;
                }
            }
            let ran;
            (state, ran) = self.pass(runtime, context, state);
            if !ran {
                // The failure is recorded; the next notification runs again.
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
            }
        }
    }

    /// One pass for the shared projector service; returns what the view
    /// needs next.
    pub(super) fn service_pass(
        &self,
        runtime: &Runtime,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
    ) -> Readiness {
        let state = lock_recover(&self.state);
        let readiness = state.readiness();
        if matches!(readiness, Readiness::Stopped | Readiness::Idle) {
            return readiness;
        }
        let (state, ran) = self.pass(runtime, context, state);
        match state.readiness() {
            Readiness::Stopped => Readiness::Stopped,
            // A pass that could not start recorded its failure; the next
            // notification schedules the view again.
            _ if !ran => Readiness::Idle,
            readiness => readiness,
        }
    }

    /// Relists stale directories and replaces superseded entries once.
    /// Returns false when the pass could not start.
    fn pass<'a>(
        &'a self,
        runtime: &Runtime,
        context: PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT,
        mut state: MutexGuard<'a, ProjectorState>,
    ) -> (MutexGuard<'a, ProjectorState>, bool) {
        let source = runtime.source.as_ref();
        let work = match runtime.native_work(false) {
            Ok(work) => work,
            Err(error) => {
                state.failure = Some(error.to_string());
                state.processed = state.notified;
                state.retry = false;
                self.signal_changed();
                return (state, false);
            }
        };
        let projection = Projection {
            runtime,
            work: &work,
            context,
            starting: false,
        };
        state.retry = false;
        state.relisting = !state.stale.is_empty();
        let through = state.notified;
        let mut listings = state
            .directories
            .iter()
            .filter(|(path, directory)| {
                directory
                    .basis
                    .is_none_or(|basis| !basis.still_describes(source, path, None))
            })
            .map(|(path, _)| path.clone())
            .collect::<Vec<_>>();
        listings.extend(state.stale.drain());
        listings.sort_by_key(|path| path.components().len());
        listings.dedup();
        let superseded = state
            .written
            .iter()
            .filter(|(path, written)| written.superseded(source, path))
            .map(|(path, written)| (path.clone(), *written))
            .collect::<Vec<_>>();
        let mut unconfirmed = Vec::new();
        for (path, written) in superseded {
            state.written.remove(&path);
            if written.unconfirmed(source, &path) {
                unconfirmed.push((path, written));
            } else {
                state.pending.insert(path, written);
            }
        }
        drop(state);
        self.confirm(source, unconfirmed);
        let mut failure = None;
        for directory in listings {
            let _tree = lock_recover(&self.tree);
            let known = lock_recover(&self.state)
                .directories
                .contains_key(&directory);
            if known && let Err(error) = self.materialize_locked(projection, vec![directory]) {
                failure.get_or_insert(error);
            }
        }
        self.replace_pending(projection, &mut failure);
        self.clear_vacated(projection);
        state = lock_recover(&self.state);
        state.relisting = false;
        state.processed = state.processed.max(through);
        if let Some(failure) = failure {
            state.failure.get_or_insert(failure);
        }
        self.signal_changed();
        (state, true)
    }

    pub(super) fn replace_pending(&self, projection: Projection<'_>, failure: &mut Option<String>) {
        let mut attempted = lock_recover(&self.state)
            .pending
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        attempted.sort_by_key(|path| std::cmp::Reverse(path.components().len()));
        for path in attempted {
            let outcome = self.paths.with(&path, || {
                let (since, written) = {
                    let state = lock_recover(&self.state);
                    (state.releases, state.pending.get(&path).copied())
                };
                let Some(written) = written else {
                    return Ok(());
                };
                let mut current = written;
                let replaced = replace_written(projection, &path, &mut current);
                let mut state = lock_recover(&self.state);
                match replaced {
                    Ok(Placed::Owned(_) | Placed::Unowned) => {
                        state.record_attempt(&path, &written, Some(current));
                        Ok(())
                    }
                    Ok(Placed::Nothing) => {
                        state.record_attempt(&path, &written, None);
                        Ok(())
                    }
                    Ok(Placed::Retry) => {
                        // Inside a pass: a set retry runs the next pass.
                        state.held(since);
                        Ok(())
                    }
                    Err(error) => {
                        state.record_attempt(&path, &written, None);
                        Err(error)
                    }
                }
            });
            if let Err(error) = outcome {
                failure.get_or_insert(error);
            }
        }
    }

    pub(super) fn confirm(
        &self,
        source: &dyn MountFilesystem,
        unconfirmed: Vec<(MountPath, WrittenEntry)>,
    ) {
        for (path, written) in unconfirmed {
            let basis = ReadBasis::sample(source);
            let current = source.lookup_pinned(&path);
            let mut state = lock_recover(&self.state);
            match current {
                Ok(Some((lookup, pin)))
                    if written
                        .facts
                        .is_some_and(|facts| facts.describe(&EntryFacts { lookup, pin })) =>
                {
                    state
                        .written
                        .entry(path)
                        .or_insert(WrittenEntry { basis, ..written });
                }
                _ => {
                    state.pending.insert(path, written);
                }
            }
        }
    }

    /// A release event: a handle closed or deferred captures drained.
    pub(super) fn retry(&self) {
        let mut state = lock_recover(&self.state);
        state.releases = state.releases.wrapping_add(1);
        if !state.pending.is_empty() {
            state.retry = true;
            self.wake();
        }
    }

    pub(super) fn settle(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        let target = lock_recover(&self.state).notified;
        let cancellation = work.cancellation_wait()?;
        let mut cancellation = std::pin::pin!(cancellation);
        acyclic_native_runtime::wait_native_future(
            async {
                loop {
                    let changed = self.progress.notified();
                    let mut changed = std::pin::pin!(changed);
                    changed.as_mut().enable();
                    // No projector lock spans ledger admission or cancellation
                    // registration. A change before the check remains retained.
                    work.remaining()?;
                    {
                        let mut state = lock_recover(&self.state);
                        let pending = !state.stopping
                            && (state.processed < target
                                || state.relisting
                                || !state.stale.is_empty()
                                || !state.pending.is_empty()
                                || !state.vacated.is_empty());
                        if !pending {
                            return state
                                .failure
                                .take()
                                .map_or(Ok(()), |failure| Err(NativeMountError::Driver(failure)));
                        }
                    }
                    // Cancellation must complete the task itself: the native
                    // executor retains admitted work until its future settles.
                    let _ = futures::future::select(changed, cancellation.as_mut()).await;
                }
            },
            &work.runtime_meter(),
            std::future::pending(),
        )
        .map_err(|error| NativeMountError::from(work.runtime_error(error)))?
    }
}
