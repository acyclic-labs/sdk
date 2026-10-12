use super::*;

impl Projector {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(ProjectorState::default()),
            tree: Mutex::new(()),
            paths: KeyedLocks::default(),
            changed: Condvar::new(),
            worker: Mutex::new(None),
            lifecycle: Mutex::new(ProjectorLifecycle::default()),
            lifecycle_changed: Condvar::new(),
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
                file_id: facts.lookup.node.file_id,
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
        self.materialize_locked(projection, vec![path])
    }

    pub(super) fn materialize_locked(
        &self,
        projection: Projection<'_>,
        directories: Vec<MountPath>,
    ) -> Result<(), String> {
        if directories.is_empty() {
            return Ok(());
        }
        let workers = std::thread::available_parallelism()
            .map_or(1, usize::from)
            .min(MATERIALIZE_WORKERS);
        let queue = MaterializeQueue::new(directories);
        let shared = SharedProjection(projection);
        std::thread::scope(|scope| {
            for _ in 1..workers {
                scope.spawn(|| self.materialize_from(&shared, &queue));
            }
            self.materialize_from(&shared, &queue);
        });
        queue.finish()
    }

    pub(super) fn materialize_from(
        &self,
        projection: &SharedProjection<'_>,
        queue: &MaterializeQueue,
    ) {
        while let Some(directory) = queue.next() {
            let written = self.materialize_directory(projection.0, directory);
            queue.done(written);
        }
    }

    pub(super) fn materialize_directory(
        &self,
        projection: Projection<'_>,
        directory: MountPath,
    ) -> Result<Vec<MountPath>, String> {
        let basis = ReadBasis::sample(projection.runtime.source.as_ref());
        let entries = match source_listing(projection, &directory) {
            Ok(entries) => entries,
            Err(MountSourceError::NotFound) => return Ok(Vec::new()),
            Err(MountSourceError::Stale) => {
                let mut state = lock_recover(&self.state);
                state.directories.insert(
                    directory.clone(),
                    Materialized {
                        basis: None,
                        names: HashMap::new(),
                    },
                );
                state.stale.insert(directory);
                self.changed.notify_all();
                return Ok(Vec::new());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.0.len());
        let mut found = Vec::new();
        let mut written = Vec::with_capacity(entries.0.len());
        let mut retry = false;
        for entry in &entries.0 {
            let child = directory.child(entry.component.clone());
            let is_directory = entry.info.IsDirectory;
            let placed = write_entry(projection, &child, entry)?;
            if placed == Placed::Retry {
                retry = true;
                continue;
            }
            if is_directory {
                found.push(child);
            } else if placed != Placed::Nothing {
                written.push((child, entry.facts, placed.owned()));
            }
            names.insert(entry.component.clone(), is_directory);
        }
        let mut state = lock_recover(&self.state);
        if retry {
            state.stale.insert(directory.clone());
            self.changed.notify_all();
        }
        for (path, facts, owned) in written {
            state.written.insert(
                path,
                WrittenEntry {
                    file_id: facts.lookup.node.file_id,
                    owned,
                    facts: Some(facts),
                    basis,
                },
            );
        }
        state
            .directories
            .insert(directory, Materialized { basis, names });
        Ok(found)
    }

    pub(super) fn reconcile(
        &self,
        projection: Projection<'_>,
        directory: &MountPath,
    ) -> Result<(), String> {
        let _tree = lock_recover(&self.tree);
        let Some(known) = lock_recover(&self.state)
            .directories
            .get(directory)
            .map(|materialized| materialized.names.clone())
        else {
            return Ok(());
        };
        let basis = ReadBasis::sample(projection.runtime.source.as_ref());
        let entries = match source_listing(projection, directory) {
            Ok(entries) => entries,
            Err(MountSourceError::NotFound) => {
                self.remove_tree(projection, directory);
                return Ok(());
            }
            Err(MountSourceError::Stale) => {
                lock_recover(&self.state).stale.insert(directory.clone());
                return Ok(());
            }
            Err(error) => return Err(format!("listing {directory:?}: {error}")),
        };
        let mut names = HashMap::with_capacity(entries.0.len());
        let mut created = Vec::new();
        for entry in &entries.0 {
            let is_directory = entry.info.IsDirectory;
            let child = directory.child(entry.component.clone());
            names.insert(entry.component.clone(), is_directory);
            let written_before = known.get(&entry.component).copied().or_else(|| {
                lock_recover(&self.state)
                    .pending
                    .contains_key(&child)
                    .then_some(false)
            });
            match written_before.as_ref() {
                Some(&was_directory) if was_directory == is_directory => continue,
                Some(&true) => self.remove_tree(projection, &child),
                Some(&false) => {
                    let (owned, released) = self.release_recorded(projection, &child);
                    if released? == Release::Busy {
                        let mut state = lock_recover(&self.state);
                        let written = state.written.remove(&child).unwrap_or(WrittenEntry {
                            file_id: FileId::from_bytes([0; 16]),
                            owned,
                            facts: None,
                            basis: None,
                        });
                        state.relist.insert(child.clone());
                        state.pending.entry(child).or_insert(written);
                        return Ok(());
                    }
                }
                None => {}
            }
            let placed = write_entry(projection, &child, entry)?;
            if placed == Placed::Retry {
                names.remove(entry.component.as_slice());
                lock_recover(&self.state).stale.insert(directory.clone());
                continue;
            }
            if is_directory {
                created.push(child);
            } else if placed != Placed::Nothing {
                self.written(child, entry.facts, placed.owned(), basis);
            }
        }
        for (name, was_directory) in &known {
            if names.contains_key(name) {
                continue;
            }
            let child = directory.child(name.clone());
            if *was_directory {
                self.remove_tree(projection, &child);
            } else {
                let mut state = lock_recover(&self.state);
                if !state.pending.contains_key(&child) {
                    let placeholder = state.written.remove(&child).unwrap_or(WrittenEntry {
                        file_id: FileId::from_bytes([0; 16]),
                        owned: None,
                        facts: None,
                        basis: None,
                    });
                    state.pending.insert(child, placeholder);
                }
            }
        }
        lock_recover(&self.state)
            .directories
            .insert(directory.clone(), Materialized { basis, names });
        self.materialize_locked(projection, created)
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
                            file_id: FileId::from_bytes([0; 16]),
                            owned,
                            facts: None,
                            basis: None,
                        });
                        state.pending.entry(child).or_insert(written);
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
                    file_id: facts.lookup.node.file_id,
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
        self.changed.notify_all();
    }

    pub(super) fn invalidate_all(&self) {
        let mut state = lock_recover(&self.state);
        let state = &mut *state;
        state.stale.extend(state.directories.keys().cloned());
        let written = std::mem::take(&mut state.written);
        state.pending.extend(written);
        self.changed.notify_all();
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
        let source = runtime.source.as_ref();
        let mut state = lock_recover(&self.state);
        loop {
            loop {
                if state.stopping {
                    return;
                }
                if state.processed < state.notified || state.retry || !state.stale.is_empty() {
                    break;
                }
                if state.pending.is_empty() {
                    state = self
                        .changed
                        .wait(state)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                } else {
                    let (next, waited) = self
                        .changed
                        .wait_timeout(state, HELD_RETRY)
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    state = next;
                    if waited.timed_out() {
                        break;
                    }
                }
            }
            let work = match runtime.native_work(false) {
                Ok(work) => work,
                Err(error) => {
                    state.failure = Some(error.to_string());
                    state.processed = state.notified;
                    state.retry = false;
                    self.changed.notify_all();
                    continue;
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
                if let Err(error) = self.reconcile(projection, &directory) {
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
            self.changed.notify_all();
        }
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
                let Some(written) = lock_recover(&self.state).pending.get(&path).copied() else {
                    return Ok(());
                };
                let replaced = replace_written(projection, &path, &written);
                let mut state = lock_recover(&self.state);
                match replaced {
                    Ok(Replaced::Rewritten(current)) => {
                        state.record_attempt(&path, &written, Some(*current));
                        Ok(())
                    }
                    Ok(Replaced::Released) => {
                        state.record_attempt(&path, &written, None);
                        Ok(())
                    }
                    Ok(Replaced::Busy) => Ok(()),
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

    pub(super) fn retry(&self) {
        let mut state = lock_recover(&self.state);
        if !state.pending.is_empty() {
            state.retry = true;
            self.changed.notify_all();
        }
    }

    pub(super) fn settle(&self) -> Result<(), NativeMountError> {
        let mut state = lock_recover(&self.state);
        let target = state.notified;
        let mut deadline = None;
        while !state.stopping
            && (state.processed < target
                || state.relisting
                || !state.stale.is_empty()
                || !state.pending.is_empty()
                || !state.vacated.is_empty())
        {
            if state.processed < target || state.relisting || !state.stale.is_empty() {
                state = self
                    .changed
                    .wait(state)
                    .unwrap_or_else(std::sync::PoisonError::into_inner);
                continue;
            }
            let deadline =
                *deadline.get_or_insert_with(|| std::time::Instant::now() + HELD_SETTLE_LIMIT);
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                let held = state.pending.keys().next().cloned();
                return Err(NativeMountError::Driver(format!(
                    "a superseded placeholder is held open and cannot be replaced: {held:?}"
                )));
            }
            state = self
                .changed
                .wait_timeout(state, remaining)
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .0;
        }
        state
            .failure
            .take()
            .map_or(Ok(()), |failure| Err(NativeMountError::Driver(failure)))
    }
}
