struct DirectoryState {
    directory: NamespacePath,
    version: SourceVersion,
    entries: HostStatReader,
    pending: Option<(HostListedEntry, u64)>,
    replay: Option<Arc<crate::kernel::Owned<Vec<SourceDirectoryEntry>>>>,
    offset: usize,
    eof: bool,
    ownership: SourcePayloadOwnership,
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

impl SourceDirectoryPublication {
    fn discard(&mut self) {
        self.state.take();
        if self.restore {
            if let Ok(mut active) = self.source.inner.cursors.lock() {
                active.remove(&self.token);
            }
            self.restore = false;
        }
    }

    fn commit(&mut self, end: usize) -> Result<(), DemandError> {
        let mut active = self
            .source
            .inner
            .cursors
            .lock()
            .map_err(|_| DemandError::StaleCursor)?;
        if self.source.reference() != self.reference {
            return Err(DemandError::StaleSource);
        }
        if self.restore {
            let Some(slot) = active.get_mut(&self.token) else {
                return Err(DemandError::StaleCursor);
            };
            let mut state = self.state.take().ok_or(DemandError::StaleCursor)?;
            state.offset = end;
            *slot = Some(state);
            self.restore = false;
        } else {
            if active.len() >= 1024
                && let Some(oldest) = active.keys().next().copied()
            {
                active.remove(&oldest);
            }
            let mut state = self.state.take().ok_or(DemandError::StaleCursor)?;
            state.offset = end;
            active.insert(self.token, Some(state));
        }
        Ok(())
    }
}

impl SourceDirectoryPublication {
    fn restore(&mut self) -> Result<(), DemandError> {
        if !self.restore {
            self.state.take();
            return Ok(());
        }
        let mut active = self
            .source
            .inner
            .cursors
            .lock()
            .map_err(|_| DemandError::StaleCursor)?;
        if self.source.reference() != self.reference {
            return Err(DemandError::StaleSource);
        }
        let slot = active
            .get_mut(&self.token)
            .ok_or(DemandError::StaleCursor)?;
        *slot = self.state.take();
        self.restore = false;
        Ok(())
    }
}

impl NativeDemandSource {
    fn directory_publication(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        observed: SourceVersion,
        cursor: Option<SourceCursor>,
        context: &mut CallbackWork,
    ) -> Result<SourceDirectoryPublication, DemandError> {
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
            let state = slot.take().ok_or(DemandError::CursorBusy)?;
            (cursor.token, state)
        } else {
            let token = self
                .inner
                .next_cursor
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    current.checked_add(1)
                })
                .map_err(|_| DemandError::StaleCursor)?
                + 1;
            (
                token,
                self.open_directory_state(directory, observed, context)?,
            )
        };
        Ok(SourceDirectoryPublication {
            source: self.clone(),
            reference: source,
            token,
            state: Some(state),
            restore,
            end: 0,
            original: None,
            _ownership: SourcePayloadOwnership::default(),
        })
    }

    fn open_directory_state(
        &self,
        directory: &NamespacePath,
        observed: SourceVersion,
        context: &mut CallbackWork,
    ) -> Result<DirectoryState, DemandError> {
        let (relative, allocation) = self.relative(directory, context)?;
        #[cfg(unix)]
        let result = self.inner.root.read_dir_stats(&relative, context);
        #[cfg(windows)]
        let result = self.inner.root.read_dir_stats(&relative, context);
        drop(relative);
        context.release(allocation)?;
        let (entries, allocation) = result?;
        let ownership = SourcePayloadOwnership::new(context)?;
        ownership.retain(allocation, context)?;
        let bytes = directory.retained_bytes() + std::mem::size_of::<DirectoryState>() as u64;
        let allocation = context.claim(bytes)?;
        ownership.retain(allocation, context)?;
        Ok(DirectoryState {
            directory: directory.clone(),
            version: observed,
            entries,
            ownership,
            pending: None,
            replay: None,
            offset: 0,
            eof: false,
        })
    }
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
impl Drop for SourceDirectoryPublication {
    fn drop(&mut self) {
        let _ = self.restore();
        let _ = self.retire();
    }
}
impl SourceDirectoryPublication {
    fn retire(&mut self) -> Result<(), DemandError> {
        if let Some(original) = self.original.take() {
            original.with_original_leaf(|work| {
                work.complete(publication_cleanup(), publication_cleanup())
            })?;
        }
        Ok(())
    }
    fn check_owner(
        &self,
        accounting: &crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        match (&self.original, accounting.native_work()) {
            (Some(owner), Some(work)) if owner.shares_allocation_context(work) => Ok(()),
            (None, None) => Ok(()),
            _ => Err(DemandError::NativeWorkUnavailable),
        }
    }
    /// Commits the producer position after consumer projection succeeds.
    pub fn accept(
        &mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.check_owner(accounting)?;
        accounting.check().map_err(DemandError::from)?;
        if self.original.is_none() {
            accounting
                .charge(publication_cleanup())
                .map_err(DemandError::from)?;
        }
        if let Some(state) = &self.state {
            let has_next = !state.eof
                || state
                    .replay
                    .as_ref()
                    .is_some_and(|backing| self.end < backing.value.len());
            if has_next {
                self.commit(self.end)?;
            } else {
                self.discard();
            }
        }
        self.retire()
    }
    /// Restores the producer position even if the consumer was cancelled.
    pub fn abort(
        &mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        self.check_owner(accounting)?;
        if let Some((_, work, budget, _)) = accounting.standalone_parts() {
            work.charge(&publication_cleanup(), &budget)?;
        }
        self.restore()?;
        self.retire()
    }
}

impl NativeDemandSource {
    fn read_entry(
        &self,
        state: &mut DirectoryState,
        context: &mut CallbackWork,
    ) -> Result<(), DemandError> {
        if state.pending.is_some() || state.eof {
            return Ok(());
        }
        match state.entries.next(context)? {
            None => state.eof = true,
            Some((entry, allocation)) => {
                state.ownership.retain(allocation, context)?;
                state.pending = Some((entry, allocation));
                context.record(WorkCounters {
                    source_entries_visited: 1,
                    ..WorkCounters::UNCHARGED
                })?;
            }
        }
        Ok(())
    }

    fn next_entry(
        &self,
        directory: &DemandPath,
        entry: &HostListedEntry,
        backing: &mut crate::kernel::Owned<Vec<SourceDirectoryEntry>>,
        context: &mut CallbackWork,
    ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
        let stat = match &entry.stat {
            Some(stat) => stat.clone(),
            None => {
                #[cfg(unix)]
                let result = self.inner.root.stat(&directory.join(&entry.name), context);
                #[cfg(windows)]
                let result = self
                    .inner
                    .root
                    .open_dir_held(directory, context)
                    .and_then(|parent| {
                        crate::native_host::stat_windows_leaf(&parent, &entry.name, context)
                    });
                match result {
                    Ok(stat) => stat,
                    Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                        return Ok(None);
                    }
                    Err(error) => return Err(error),
                }
            }
        };
        #[cfg(unix)]
        let ((encoding, bytes), allocation) = crate::native_name::host_name_bytes_with_source_work(
            &entry.name,
            self.inner.profile,
            self.inner.limits.maximum_component_bytes,
            context,
        )?;
        #[cfg(windows)]
        let ((encoding, bytes), allocation) =
            crate::native_name::host_wide_name_bytes_with_source_work(
                &entry.name,
                self.inner.profile,
                self.inner.limits.maximum_component_bytes,
                context,
            )?;
        backing.bytes += bytes.capacity() as u64;
        if let Some(allocation) = allocation {
            backing
                .allocation
                .absorb(&mut crate::kernel::KernelOwnedAllocation::NativeOriginal(
                    allocation,
                ))
                .map_err(DemandError::from)?;
        }
        let name = crate::kernel::LogicalName::new(
            encoding,
            bytes,
            self.inner.limits.maximum_component_bytes,
        )
        .map_err(|_| DemandError::InvalidRequest)?;
        Ok(Some(SourceDirectoryEntry {
            name,
            node: Self::node(&stat),
        }))
    }

    fn page_entries(
        &self,
        state: &mut DirectoryState,
        maximum: usize,
        context: &mut CallbackWork,
    ) -> Result<SourceDirectoryEntries, DemandError> {
        if let Some(backing) = &state.replay {
            if state.offset < backing.value.len() {
                let end = (state.offset + maximum).min(backing.value.len());
                return Ok(SourceDirectoryEntries {
                    backing: backing.clone(),
                    range: state.offset..end,
                });
            }
        }
        state.replay.take();
        state.offset = 0;
        let mut backing = account_source(context, |accounting, _| {
            let mut backing =
                crate::kernel::Owned::<Vec<SourceDirectoryEntry>>::allocate(maximum, accounting)
                    .map_err(DemandError::from)?;
            let bytes = crate::source_watch::arc_control_bytes::<
                crate::kernel::Owned<Vec<SourceDirectoryEntry>>,
            >();
            let mut header = accounting
                .claim_owned(bytes, 1)
                .map_err(DemandError::from)?;
            backing
                .allocation
                .absorb(&mut header)
                .map_err(DemandError::from)?;
            backing.bytes += bytes;
            Ok(backing)
        })?;
        let (relative, allocation) = self.relative(&state.directory, context)?;
        let result = (|| {
            for _ in 0..maximum {
                self.read_entry(state, context)?;
                let Some((entry, _)) = &state.pending else {
                    break;
                };
                let converted = self.next_entry(&relative, entry, &mut backing, context)?;
                let (entry, bytes) = state.pending.take().ok_or(DemandError::StaleCursor)?;
                drop(entry);
                state.ownership.retire(bytes)?;
                if !context.is_native_original() {
                    context.release(bytes)?;
                }
                if let Some(entry) = converted {
                    if !self.excluded.excludes_child(&state.directory, &entry.name) {
                        backing.value.push(entry);
                    }
                }
            }
            self.read_entry(state, context)
        })();
        drop(relative);
        let released = context.release(allocation);
        let end = backing.value.len();
        let backing = Arc::new(backing);
        state.replay = Some(backing.clone());
        result.and(released)?;
        Ok(SourceDirectoryEntries {
            backing,
            range: 0..end,
        })
    }

    fn directory_page(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum: u32,
        context: &mut CallbackWork,
    ) -> Result<SourceDirectoryPage, DemandError> {
        if maximum == 0 || maximum > 4096 {
            return Err(DemandError::InvalidRequest);
        }
        context.admit(WorkCounters {
            items_examined: maximum as u64 * self.excluded.paths.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        context.record(WorkCounters {
            items_examined: maximum as u64 * self.excluded.paths.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        self.check(source, context)?;
        if self.excluded.excludes(directory) {
            return Err(DemandError::Absent);
        }
        self.observe_directory(directory, context)?;
        let metadata = self
            .metadata(directory, context)?
            .ok_or(DemandError::Absent)?;
        if !metadata.is_dir() {
            return Err(DemandError::NotDirectory);
        }
        let observed = version(&metadata);
        let mut lease = self.directory_publication(source, directory, observed, cursor, context)?;
        let state = lease.state.as_mut().ok_or(DemandError::StaleCursor)?;
        let entries = self.page_entries(state, maximum as usize, context)?;
        let has_next = !state.eof || entries.range.end < entries.backing.value.len();
        self.check(source, context)?;
        if self.metadata(directory, context)?.as_ref().map(version) != Some(observed) {
            lease.discard();
            return Err(DemandError::StaleVersion);
        }
        context.record(WorkCounters {
            items_returned: entries.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let ownership = SourcePayloadOwnership::new(context)?;
        let allocation = context.claim(std::mem::size_of::<SourceDirectoryPublication>() as u64)?;
        ownership.retain(allocation, context)?;
        let next = has_next.then_some(SourceCursor {
            source,
            token: lease.token,
        });
        let original = context.native_source_work();
        if let Some(original) = &original {
            original.with_original_leaf(|work| work.reserve(publication_cleanup()))?;
        }
        lease.end = entries.range.end;
        lease.original = original;
        lease._ownership = ownership;
        let publication = Some(Box::new(lease));
        Ok(SourceDirectoryPage {
            entries,
            next,
            version: observed,
            publication,
        })
    }
}
