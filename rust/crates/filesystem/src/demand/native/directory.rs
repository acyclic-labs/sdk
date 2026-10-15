use crate::kernel::{KernelAccounting, KernelOwnedAllocation, LogicalName, Owned};
#[cfg(unix)]
use crate::native_name::host_name_bytes_with_source_work as source_name;
#[cfg(windows)]
use crate::native_name::host_wide_name_bytes_with_source_work as source_name;

struct DirectoryState {
    directory: NamespacePath,
    version: SourceVersion,
    entries: HostStatReader,
    pending: Option<(HostListedEntry, u64)>,
    replay: Option<Arc<Owned<Vec<SourceDirectoryEntry>>>>,
    offset: usize,
    eof: bool,
    ownership: SourcePayloadOwnership,
}
impl DirectoryState {
    fn has_next(&self, end: usize) -> bool {
        !self.eof
            || self
                .replay
                .as_ref()
                .is_some_and(|entries| end < entries.value.len())
    }
    fn read(&mut self, work: &mut CallbackWork) -> Result<(), DemandError> {
        if self.pending.is_none() && !self.eof {
            work.check()?;
            work.admit(WorkCounters {
                source_entries_visited: 1,
                ..WorkCounters::UNCHARGED
            })?;
            match self.entries.next(work)? {
                None => self.eof = true,
                Some((entry, bytes)) => {
                    self.ownership.retain(bytes, work)?;
                    self.pending = Some((entry, bytes));
                    work.record(WorkCounters {
                        source_entries_visited: 1,
                        ..WorkCounters::UNCHARGED
                    })?;
                }
            }
        }
        Ok(())
    }
}

/// A producer position held until the consumer accepts its page.
pub struct SourceDirectoryPublication {
    source: NativeDemandSource,
    reference: SourceReference,
    token: u64,
    state: Option<DirectoryState>,
    restore: bool,
    end: usize,
    original: Option<crate::NativeMaterializationWork>,
    _ownership: SourcePayloadOwnership,
}
impl std::fmt::Debug for SourceDirectoryPublication {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceDirectoryPublication")
            .field("pending", &self.state.is_some())
            .finish()
    }
}
fn publication_cleanup() -> WorkCounters {
    WorkCounters {
        items_examined: 1,
        ..WorkCounters::UNCHARGED
    }
}
impl SourceDirectoryPublication {
    fn finish(&mut self, end: Option<usize>) -> Result<(), DemandError> {
        let result = (|| {
            if self.restore || end.is_some() && self.state.is_some() {
                let mut active = self
                    .source
                    .inner
                    .cursors
                    .lock()
                    .map_err(|_| DemandError::StaleCursor)?;
                if self.source.reference() != self.reference {
                    return Err(DemandError::StaleSource);
                }
                if self.restore && !active.contains_key(&self.token) {
                    return Err(DemandError::StaleCursor);
                }
                let state = self
                    .state
                    .take()
                    .filter(|state| end.is_none_or(|end| state.has_next(end)));
                if let Some(mut state) = state {
                    if let Some(end) = end {
                        state.offset = end;
                    }
                    if !self.restore
                        && active.len() >= 1024
                        && let Some(oldest) = active.keys().next().copied()
                    {
                        active.remove(&oldest);
                    }
                    active.insert(self.token, Some(state));
                } else {
                    active.remove(&self.token);
                }
                self.restore = false;
            }
            Ok(())
        })();
        self.state.take();
        let retired = self.original.take().map_or(Ok(()), |original| {
            original.with_original_leaf(|work| {
                work.complete(publication_cleanup(), publication_cleanup())
            })
        });
        result.and(retired)
    }
    fn check_owner(&self, accounting: &KernelAccounting<'_>) -> Result<(), DemandError> {
        match (&self.original, accounting.native_work()) {
            (Some(owner), Some(work)) if owner.shares_allocation_context(work) => Ok(()),
            (None, None) => Ok(()),
            _ => Err(DemandError::NativeWorkUnavailable),
        }
    }
    /// Commits the producer position after consumer projection succeeds.
    pub fn accept(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        self.check_owner(accounting)?;
        accounting.check().map_err(DemandError::from)?;
        if self.original.is_none() {
            accounting
                .charge(publication_cleanup())
                .map_err(DemandError::from)?;
        }
        self.finish(Some(self.end))
    }
    /// Restores the producer position even if the consumer was cancelled.
    pub fn abort(&mut self, accounting: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        self.check_owner(accounting)?;
        if let Some((_, work, budget, _)) = accounting.standalone_parts() {
            work.charge(&publication_cleanup(), &budget)?;
        }
        self.finish(None)
    }
}
impl Drop for SourceDirectoryPublication {
    fn drop(&mut self) {
        let _ = self.finish(None);
    }
}

impl NativeDemandSource {
    fn open_directory_state(
        &self,
        directory: &NamespacePath,
        version: SourceVersion,
        work: &mut CallbackWork,
    ) -> Result<DirectoryState, DemandError> {
        let (relative, bytes) = self.relative(directory, work)?;
        let result = self.inner.root.read_dir_stats(&relative, work);
        drop(relative);
        work.release(bytes)?;
        let (entries, bytes) = result?;
        let ownership = SourcePayloadOwnership::new(work)?;
        ownership.retain(bytes, work)?;
        let bytes =
            work.claim(directory.retained_bytes() + std::mem::size_of::<DirectoryState>() as u64)?;
        ownership.retain(bytes, work)?;
        Ok(DirectoryState {
            directory: directory.clone(),
            version,
            entries,
            pending: None,
            replay: None,
            offset: 0,
            eof: false,
            ownership,
        })
    }
    fn next_entry(
        &self,
        directory: &DemandPath,
        entry: &HostListedEntry,
        backing: &mut Owned<Vec<SourceDirectoryEntry>>,
        work: &mut CallbackWork,
    ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
        let observed;
        let stat = if let Some(stat) = &entry.stat {
            stat
        } else {
            #[cfg(unix)]
            let result = self.inner.root.stat(&directory.join(&entry.name), work);
            #[cfg(windows)]
            let result = self
                .inner
                .root
                .open_dir_held(directory, work)
                .and_then(|parent| {
                    crate::native_host::stat_windows_leaf(&parent, &entry.name, work)
                });
            observed = match result {
                Ok(stat) => stat,
                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(None);
                }
                Err(error) => return Err(error),
            };
            &observed
        };
        let ((encoding, bytes), allocation) = source_name(
            &entry.name,
            self.inner.profile,
            self.inner.limits.maximum_component_bytes,
            work,
        )?;
        backing.bytes += bytes.capacity() as u64;
        if let Some(allocation) = allocation {
            backing
                .allocation
                .absorb(&mut KernelOwnedAllocation::NativeOriginal(allocation))
                .map_err(DemandError::from)?;
        }
        let name = LogicalName::new(encoding, bytes, self.inner.limits.maximum_component_bytes)
            .map_err(|_| DemandError::InvalidRequest)?;
        Ok(Some(SourceDirectoryEntry {
            name,
            node: Self::node(stat),
        }))
    }
    fn page_entries(
        &self,
        state: &mut DirectoryState,
        maximum: usize,
        work: &mut CallbackWork,
    ) -> Result<SourceDirectoryEntries, DemandError> {
        if let Some(backing) = &state.replay
            && state.offset < backing.value.len()
        {
            return Ok(SourceDirectoryEntries {
                backing: backing.clone(),
                range: state.offset..(state.offset + maximum).min(backing.value.len()),
            });
        }
        state.replay.take();
        state.offset = 0;
        let mut page = account_source(work, |accounting, _| {
            let entries = Owned::allocate(maximum, accounting).map_err(DemandError::from)?;
            SourceDirectoryPage::stateless(entries, None, state.version, accounting)
        })?;
        let backing = Arc::get_mut(&mut page.entries.backing).ok_or(DemandError::InvalidRequest)?;
        let (relative, bytes) = self.relative(&state.directory, work)?;
        let result = (|| {
            for _ in 0..maximum {
                state.read(work)?;
                let Some((entry, _)) = &state.pending else {
                    break;
                };
                let converted = self.next_entry(&relative, entry, backing, work)?;
                let (entry, bytes) = state.pending.take().ok_or(DemandError::StaleCursor)?;
                drop(entry);
                state.ownership.retire(bytes)?;
                if !work.is_native_original() {
                    work.release(bytes)?;
                }
                if let Some(entry) = converted
                    && !self.excluded.excludes_child(&state.directory, &entry.name)
                {
                    backing.value.push(entry);
                }
            }
            state.read(work)
        })();
        drop(relative);
        let released = work.release(bytes);
        page.entries.range.end = backing.value.len();
        state.replay = Some(page.entries.backing.clone());
        result.and(released)?;
        Ok(page.entries)
    }
    fn directory_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum: u32,
        work: &mut CallbackWork,
    ) -> Result<SourceDirectoryPage, DemandError> {
        if maximum == 0 || maximum > 4096 {
            return Err(DemandError::InvalidRequest);
        }
        work.started_io(WorkCounters {
            items_examined: u64::from(maximum) * self.excluded.paths.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        self.check(source, work)?;
        if self.excluded.excludes(directory) {
            return Err(DemandError::Absent);
        }
        self.observe_directory(directory, work)?;
        let stat = self.metadata(directory, work)?.ok_or(DemandError::Absent)?;
        if !stat.is_dir() {
            return Err(DemandError::NotDirectory);
        }
        let observed = version(&stat);
        let restore = cursor.is_some();
        let (token, state) = if let Some(cursor) = cursor {
            if cursor.source != source {
                return Err(DemandError::StaleCursor);
            }
            let mut active = self
                .inner
                .cursors
                .lock()
                .map_err(|_| DemandError::StaleCursor)?;
            let slot = active
                .get_mut(&cursor.token)
                .ok_or(DemandError::StaleCursor)?;
            let state = slot.as_ref().ok_or(DemandError::CursorBusy)?;
            if state.directory != *directory || state.version != observed {
                return Err(DemandError::StaleCursor);
            }
            (cursor.token, slot.take().ok_or(DemandError::CursorBusy)?)
        } else {
            let token = self
                .inner
                .next_cursor
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| DemandError::StaleCursor)?
                + 1;
            (token, self.open_directory_state(directory, observed, work)?)
        };
        let mut lease = SourceDirectoryPublication {
            source: self.clone(),
            reference: source,
            token,
            state: Some(state),
            restore,
            end: 0,
            original: None,
            _ownership: SourcePayloadOwnership::default(),
        };
        let state = lease.state.as_mut().ok_or(DemandError::StaleCursor)?;
        let entries = self.page_entries(state, maximum as usize, work)?;
        let next = state
            .has_next(entries.range.end)
            .then_some(SourceCursor { source, token });
        self.check(source, work)?;
        if self.metadata(directory, work)?.as_ref().map(version) != Some(observed) {
            lease.state.take();
            return Err(DemandError::StaleVersion);
        }
        work.record(WorkCounters {
            items_returned: entries.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let ownership = SourcePayloadOwnership::new(work)?;
        let bytes = work.claim(std::mem::size_of::<SourceDirectoryPublication>() as u64)?;
        ownership.retain(bytes, work)?;
        let original = work.native_source_work();
        if let Some(original) = &original {
            original.with_original_leaf(|work| work.reserve(publication_cleanup()))?;
        }
        lease.end = entries.range.end;
        lease.original = original;
        lease._ownership = ownership;
        Ok(SourceDirectoryPage {
            entries,
            next,
            version: observed,
            publication: Some(Box::new(lease)),
        })
    }
}
