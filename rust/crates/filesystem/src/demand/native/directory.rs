use super::*;

pub(in crate::demand) struct DirectoryState {
        pub(in crate::demand) source: SourceReference,
        pub(in crate::demand) directory: NamespacePath,
        pub(in crate::demand) version: SourceVersion,
        pub(in crate::demand) entries: Option<DirectoryReader>,
        pub(in crate::demand) pending: Option<DirectoryPending>,
        pub(in crate::demand) replay: Option<Arc<SourceDirectoryBacking>>,
        pub(in crate::demand) replay_offset: usize,
        pub(in crate::demand) eof: bool,
        pub(in crate::demand) in_use: bool,
        pub(in crate::demand) unpublished_start: bool,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

impl DirectoryState {
    pub(in crate::demand) fn private_bytes(&self) -> u64 {
                std::mem::size_of::<Mutex<Self>>() as u64
                    + self.directory.retained_bytes()
                    + self.entries.as_ref().map_or(0, |reader| reader.0.logical_owned_bytes())
                    + self.pending.as_ref().map_or(0, |pending| pending.0.logical_owned_bytes())
            }
}

#[derive(Default)]
    pub(in crate::demand) struct CursorSlots {
        pub(in crate::demand) slots: Vec<Option<(u64, Arc<Mutex<DirectoryState>>)>>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

pub(in crate::demand) type DirectoryReader = (HostStatReader, SourcePayloadOwnership);

pub(in crate::demand) type DirectoryPending = (HostListedEntry, SourcePayloadOwnership);

pub(in crate::demand) fn cursor_index(token: u64) -> usize {
        (token.saturating_sub(1) % 1024) as usize
    }

pub(in crate::demand) fn cursor_slot(active: &mut CursorSlots, token: u64) -> Option<&mut Arc<Mutex<DirectoryState>>> {
        active.slots.get_mut(cursor_index(token))?.as_mut()
            .filter(|(held, _)| *held == token).map(|(_, slot)| slot)
    }

pub(in crate::demand) struct CursorLease {
        pub(in crate::demand) source: NativeDemandSource,
        pub(in crate::demand) token: u64,
        pub(in crate::demand) state: Arc<Mutex<DirectoryState>>,
        pub(in crate::demand) active: bool,
    }

impl CursorLease {
    pub(in crate::demand) fn state(&self) -> Result<std::sync::MutexGuard<'_, DirectoryState>, DemandError> {
                self.state.lock().map_err(|_| DemandError::StaleCursor)
            }

    pub(in crate::demand) fn restore(&mut self) {
                if !self.active { return; }
                let mut state = self.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.in_use = false;
                self.active = false;
            }
}

impl Drop for CursorLease {
        fn drop(&mut self) {
            self.restore();
        }
    }

pub(in crate::demand) struct NativePagePublication {
        pub(in crate::demand) lease: Option<CursorLease>,
        pub(in crate::demand) end: usize,
        pub(in crate::demand) original_cleanup: Option<crate::NativeMaterializationWork>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

impl Drop for NativePagePublication {
        fn drop(&mut self) {
            if let Some(original) = self.original_cleanup.take() {
                if let Some(mut lease) = self.lease.take() {
                    lease.restore();
                    let _cleanup = original.source_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1))
                        .and_then(std::convert::identity);
                } else {
                    let _cleanup = original.source_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, 1).expect("original reservation is balanced"));
                }
            }
        }
    }

pub(in crate::demand) fn publication_cleanup() -> WorkCounters {
        WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }
    }

impl SourceDirectoryPublication for NativePagePublication {
        fn cleanup_budget(&self) -> WorkCounters {
            if self.lease.is_some() { publication_cleanup() } else { WorkCounters::UNCHARGED }
        }

        fn retained_bytes(&self) -> u64 {
            let private_bytes = self.lease.as_ref().map_or(0, |lease| {
                let active = lease.source.inner.cursors.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                let state = lease.state.lock().unwrap_or_else(std::sync::PoisonError::into_inner);
                state.private_bytes() + (active.slots.capacity() * std::mem::size_of::<Option<(u64, Arc<Mutex<DirectoryState>>)>>()) as u64
            });
            std::mem::size_of::<Self>() as u64 + private_bytes
        }

        fn accept(&mut self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError> {
            let native = accounting.native_work().map(NativeSourceWork::from_native_materialization_work);
            match (&self.original_cleanup, native) {
                (Some(original), Some(work)) if Arc::ptr_eq(&original.state, work.original)
                    && same_resource_view(original.resource.as_ref(), work.context.and_then(|context| context.resource.as_ref())) => {}
                (None, None) => {}
                _ => return Err(DemandError::NativeWorkUnavailable),
            }
            if let Some(work) = native { crate::native_host::NativeHostWork::check(&work)?; }
            else { accounting.check().map_err(SourceCursor::cursor_accounting_error)?; }
            if let Some(lease) = self.lease.as_mut() {
                let mut active = lease.source.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
                let state = lease.state.lock();
                let reference = match &state {
                    Ok(state) => state.source,
                    Err(poisoned) => poisoned.get_ref().source,
                };
                if lease.source.reference() != reference { return Err(DemandError::StaleSource); }
                let slot = cursor_slot(&mut active, lease.token).ok_or(DemandError::StaleCursor)?;
                if !Arc::ptr_eq(slot, &lease.state) { return Err(DemandError::StaleCursor); }
                let mut state = state.map_err(|_| DemandError::StaleCursor)?;
                if !state.in_use || self.end < state.replay_offset
                    || self.end > state.replay.as_ref().map_or(0, |entries| entries.entries.len()) {
                    return Err(DemandError::StaleCursor);
                }
                if let Some(work) = native { crate::native_host::NativeHostWork::check(&work)?; }
                else { accounting.check().map_err(SourceCursor::cursor_accounting_error)?; }
                if let Some(original) = &self.original_cleanup {
                    original.source_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1)).and_then(std::convert::identity)?;
                } else { accounting.charge(publication_cleanup()).map_err(SourceCursor::cursor_accounting_error)?; }
                state.replay_offset = self.end;
                state.unpublished_start = false;
                if state.replay.as_ref().is_some_and(|entries| state.replay_offset == entries.entries.len()) {
                    state.replay.take();
                    state.replay_offset = 0;
                }
                let complete = state.eof && state.replay.is_none() && state.pending.is_none();
                state.in_use = false;
                lease.active = false;
                drop(state);
                if complete { active.slots[cursor_index(lease.token)] = None; }
                drop(active);
                self.lease.take();
            }
            self.original_cleanup.take();
            Ok(())
        }

        fn abort(&mut self, accounting: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError> {
            if let Some(work) = accounting.native_work() {
                let work = NativeSourceWork::from_native_materialization_work(work);
                let original = self.original_cleanup.as_ref().ok_or(DemandError::NativeWorkUnavailable)?;
                if !Arc::ptr_eq(&original.state, work.original)
                    || !same_resource_view(original.resource.as_ref(), work.context.and_then(|context| context.resource.as_ref())) {
                    return Err(DemandError::NativeWorkUnavailable);
                }
                if let Some(mut lease) = self.lease.take() {
                    lease.restore();
                    original.source_leaf(|work| work.complete(crate::materializer::NativeResourceCredit::Items, 1, 1)).and_then(std::convert::identity)?;
                } else {
                    original.source_leaf(|work| work.refund(crate::materializer::NativeResourceCredit::Items, 1).expect("original reservation is balanced"))?;
                }
                self.original_cleanup.take();
                return Ok(());
            }
            if self.original_cleanup.is_some() { return Err(DemandError::NativeWorkUnavailable); }
            if self.lease.is_some() {
                let (_, work, budget, _) = accounting.standalone_parts().ok_or(DemandError::NativeWorkUnavailable)?;
                work.charge(&publication_cleanup(), &budget)?;
                if let Some(mut lease) = self.lease.take() { lease.restore(); }
            }
            Ok(())
        }
    }

impl NativeDemandSource {
    pub(in crate::demand) fn cursor_lease<W: SourceDemandReadWork>(
                    &self,
                    source: SourceReference,
                    directory: &NamespacePath,
                    observed: SourceVersion,
                    cursor: Option<SourceCursor>,
                    maximum_entries: u32,
                    work: &mut W,
                ) -> Result<CursorLease, DemandError> {
                    let mut active = self.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
                    let slot_bytes = std::mem::size_of::<Option<(u64, Arc<Mutex<DirectoryState>>)>>();
                    if !work.is_native_original() { work.adopt((active.slots.capacity() * slot_bytes) as u64)?; }
                    let held = if let Some(cursor) = cursor {
                        if cursor.source != source || cursor.directory != *directory || cursor.directory_version != observed {
                            return Err(DemandError::StaleCursor);
                        }
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        let slot = cursor_slot(&mut active, cursor.token).ok_or(DemandError::StaleCursor)?;
                        Some((cursor.token, Arc::clone(slot)))
                    } else {
                        let mut held = None;
                        for slot in active.slots.iter_mut() {
                            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                            let Some((token, cursor)) = slot else { continue };
                            let state = cursor.lock().map_err(|_| DemandError::StaleCursor)?;
                            if state.source != source || state.directory != *directory { continue; }
                            if state.in_use { return Err(DemandError::CursorBusy); }
                            if state.unpublished_start && state.version == observed {
                                held = Some((*token, Arc::clone(cursor)));
                                break;
                            }
                        }
                        held
                    };
                    if let Some((token, state)) = held {
                        let mut directory_state = state.lock().map_err(|_| DemandError::StaleCursor)?;
                        if directory_state.in_use { return Err(DemandError::CursorBusy); }
                        if directory_state.source != source || directory_state.directory != *directory
                            || directory_state.version != observed
                        {
                            return Err(DemandError::StaleCursor);
                        }
                        if !work.is_native_original() {
                            work.adopt(directory_state.private_bytes().checked_add(directory_state.replay.as_ref().map_or(0, |entries| entries.retained_bytes()))
                                .ok_or(crate::WorkError::Overflow)?)?;
                        }
                        directory_state.in_use = true;
                        drop(directory_state);
                        return Ok(CursorLease {
                            source: self.clone(), token, state,
                            active: true,
                        });
                    }
                    let token = self.inner.next_cursor.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                        current.checked_add(1)
                    }).map_err(|_| DemandError::StaleCursor)? + 1;
                    let index = cursor_index(token);
                    if let Some(Some((_, slot))) = active.slots.get(index) {
                        if slot.lock().map_err(|_| DemandError::StaleCursor)?.in_use {
                            return Err(DemandError::CursorBusy);
                        }
                    }
                    if index >= active.slots.capacity() {
                        let old = active.slots.capacity() * slot_bytes;
                        let capacity = active.slots.capacity().saturating_mul(2).max(index + 1).min(1024);
                        let copied = active.slots.len() * slot_bytes;
                        let copy = work.reserve_source_copy(copied as u64)?;
                        let ownership = SourcePayloadOwnership::new(work)?;
                        let (mut grown, bytes) = source_vec(capacity, work)?;
                        if let Err(error) = ownership.retain(bytes, work) {
                            drop(grown);
                            let _cleanup = work.release(bytes);
                            return Err(error);
                        }
                        grown.append(&mut active.slots);
                        let retired = std::mem::replace(&mut *active, CursorSlots { slots: grown, ownership });
                        let completed = if let Some(copy) = copy {
                            { let mut copy = copy; copy.record_completed(crate::WorkCounters { bytes_copied: copied as u64, ..crate::WorkCounters::UNCHARGED }) }.map_err(crate::native_name::original_name_error)
                        } else {
                            work.record(WorkCounters { bytes_copied: copied as u64, ..WorkCounters::UNCHARGED })
                        };
                        drop(retired);
                        if !work.is_native_original() { work.release(old as u64)?; }
                        completed?;
                    }
                    if index >= active.slots.len() {
                        work.charge(WorkCounters { items_examined: (index + 1 - active.slots.len()) as u64, ..WorkCounters::UNCHARGED })?;
                        active.slots.resize_with(index + 1, || None);
                    }
                    let ownership = SourcePayloadOwnership::new(work)?;
                    let directory = clone_namespace(directory, work)?;
                    ownership.retain(directory.retained_bytes(), work)?;
                    let replay = Self::page_buffer(maximum_entries, work)?;
                    work.allocate(std::mem::size_of::<Mutex<DirectoryState>>() as u64)?;
                    ownership.retain(std::mem::size_of::<Mutex<DirectoryState>>() as u64, work)?;
                    let state = Arc::new(Mutex::new(DirectoryState {
                        source, directory, version: observed, entries: None, pending: None,
                        replay: Some(replay), replay_offset: 0,
                        eof: false, in_use: true, unpublished_start: true, ownership,
                    }));
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    active.slots[index] = Some((token, Arc::clone(&state)));
                    Ok(CursorLease {
                        source: self.clone(), token, state,
                        active: true,
                    })
                }

    pub(in crate::demand) fn page_buffer<W: SourceDemandReadWork>(maximum_entries: u32, work: &mut W) -> Result<Arc<SourceDirectoryBacking>, DemandError> {
                    let capacity = maximum_entries as usize;
                    let arc_bytes = std::mem::size_of::<SourceDirectoryBacking>() as u64;
                    let ownership = SourcePayloadOwnership::new(work)?;
                    let (entries, buffer_bytes) = source_vec(capacity, work)?;
                    if let Err(error) = ownership.retain(buffer_bytes, work) {
                        drop(entries);
                        let _cleanup = work.release(buffer_bytes);
                        return Err(error);
                    }
                    work.allocate(arc_bytes)?;
                    ownership.retain(arc_bytes, work)?;
                    Ok(Arc::new(SourceDirectoryBacking { entries, ownership }))
                }

    pub(in crate::demand) fn open_directory_state<W: SourceDemandReadWork>(&self, state: &mut DirectoryState, work: &mut W) -> Result<(), DemandError> {
                    if state.entries.is_some() { return Ok(()); }
                    let ownership = SourcePayloadOwnership::new(work)?;
                    let relative = self.relative(&state.directory, work)?;
                    #[cfg(unix)]
                    let result = self.inner.root.read_dir_stats(&relative, work);
                    #[cfg(windows)]
                    let result = self.inner.root.read_dir_stats_wide_with_work(&relative, work);
                    match result {
                        Ok((reader, allocation)) => {
                            ownership.retain(allocation, work)?;
                            state.entries = Some((reader, ownership));
                        }
                        Err(error) => {
                            let bytes = path_owned_bytes(&relative);
                            drop(relative);
                            work.release(bytes)?;
                            return Err(error);
                        }
                    }
                    let bytes = path_owned_bytes(&relative);
                    drop(relative);
                    work.release(bytes)?;
                    Ok(())
                }

    pub(in crate::demand) fn next_entry<W: SourceDemandReadWork>(
                    &self,
                    directory: &DemandPath,
                    entry: &HostListedEntry,
                    work: &mut W,
                ) -> Result<Option<SourceDirectoryEntry>, DemandError> {
                    #[cfg(unix)]
                    let file_name = &entry.name;
                    #[cfg(unix)]
                    let ((encoding, bytes), allocation) = crate::native_name::host_name_bytes_with_source_work(
                        file_name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
                    )?;
                    #[cfg(windows)]
                    let ((encoding, bytes), allocation) = crate::native_name::host_wide_name_bytes_with_source_work(
                        &entry.name, self.inner.profile, self.inner.limits.maximum_component_bytes, work,
                    )?;
                    let ownership = if let Some(allocation) = allocation {
                        SourcePayloadOwnership::from_native_allocation(allocation)
                    } else {
                        let ownership = SourcePayloadOwnership::new(work)?;
                        ownership.retain(bytes.capacity() as u64, work)?;
                        ownership
                    };
                    let name = crate::kernel::LogicalName::new(
                        encoding,
                        bytes,
                        self.inner.limits.maximum_component_bytes,
                    )
                    .map_err(|_| DemandError::InvalidRequest)?;
                    let node = match &entry.stat {
                        Some(stat) => Self::node(stat, work)?,
                        None => {
                            #[cfg(unix)]
                            let result = self.inner.root.open_dir_held(directory, work)
                                .and_then(|parent| crate::native_host::stat_leaf_with_work(&parent, file_name, work));
                            #[cfg(windows)]
                            let result = self.inner.root.open_dir_held_wide_with_work(directory, work)
                                .and_then(|parent| crate::native_host::stat_windows_leaf_with_work(&parent, &entry.name, work));
                            match result {
                                Ok(stat) => Self::node(&stat, work)?,
                                Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {
                                    let bytes = name.as_bytes().len() as u64;
                                    drop(name);
                                    drop(ownership);
                                    if !work.is_native_original() { work.release(bytes)?; }
                                    return Ok(None);
                                }
                                Err(error) => return Err(error),
                            }
                        }
                    };
                    Ok(Some(SourceDirectoryEntry { name, node, ownership }))
                }

    pub(in crate::demand) fn read_entry<W: SourceDemandReadWork>(reader: &mut DirectoryReader, work: &mut W) -> Result<Option<DirectoryPending>, DemandError> {
                    let ownership = SourcePayloadOwnership::new(work)?;
                    #[cfg(unix)]
                    let result = reader.0.next_with_work(work)?;
                    #[cfg(windows)]
                    let result = reader.0.next_wide_with_work(work)?;
                    let Some((entry, allocation)) = result else { return Ok(None) };
                    ownership.retain(allocation, work)?;
                    Ok(Some((entry, ownership)))
                }

    pub(in crate::demand) fn page_entries<W: SourceDemandReadWork>(
                    &self,
                    state: &mut DirectoryState,
                    maximum_entries: u32,
                    work: &mut W,
                    mut on_entry: impl FnMut(),
                ) -> Result<std::ops::Range<usize>, DemandError> {
                    if let Some(entries) = &state.replay
                        && state.replay_offset < entries.entries.len()
                    {
                        let end = state.replay_offset.saturating_add(maximum_entries as usize).min(entries.entries.len());
                        for index in state.replay_offset..end {
                            work.admit(WorkCounters { items_returned: (index - state.replay_offset + 1) as u64, ..WorkCounters::UNCHARGED })?;
                            work.charge(WorkCounters { source_entries_visited: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
                            on_entry();
                        }
                        work.check()?;
                        return Ok(state.replay_offset..end);
                    }
                    if state.replay.is_none() {
                        state.replay = Some(Self::page_buffer(maximum_entries, work)?);
                        state.replay_offset = 0;
                    }
                    let limit = (maximum_entries as usize).min(state.replay.as_ref().map_or(0, |entries| entries.entries.capacity()));
                    self.open_directory_state(state, work)?;
                    let directory = self.relative(&state.directory, work)?;
                    let answer = (|| {
                                while state.replay.as_ref().map_or(0, |entries| entries.entries.len()) < limit && !state.eof {
                                    work.check()?;
                                    if state.pending.is_none() {
                                        state.pending = Self::read_entry(state.entries.as_mut().ok_or(DemandError::StaleCursor)?, work)?;
                                    }
                                    let Some(entry) = state.pending.as_ref() else { state.eof = true; break };
                                    let count = state.replay.as_ref().map_or(0, |entries| entries.entries.len());
                                    work.admit(WorkCounters {
                                        source_entries_visited: 1, items_returned: count as u64 + 1,
                                        ..WorkCounters::UNCHARGED
                                    })?;
                                    work.charge(WorkCounters { source_entries_visited: 1, items_examined: 1, ..WorkCounters::UNCHARGED })?;
                                    let converted = self.next_entry(&directory, &entry.0, work)?;
                                    let raw_bytes = entry.0.logical_owned_bytes();
                                    if let Some(converted) = converted {
                                        let entries = Arc::get_mut(state.replay.as_mut().ok_or(DemandError::StaleCursor)?)
                                            .ok_or(DemandError::CursorBusy)?;
                                        entries.entries.push(converted);
                                    }
                                    drop(state.pending.take());
                                    if !work.is_native_original() { work.release(raw_bytes)?; }
                                    on_entry();
                                }
                                if !state.eof && state.pending.is_none() {
                                    state.pending = Self::read_entry(state.entries.as_mut().ok_or(DemandError::StaleCursor)?, work)?;
                                    if state.pending.is_none() { state.eof = true; }
                                }
                                work.check()?;
                                Ok(0..state.replay.as_ref().map_or(0, |entries| entries.entries.len()))
                    })();
                    let bytes = path_owned_bytes(&directory);
                    drop(directory);
                    work.release(bytes)?;
                    answer
                }

    pub(in crate::demand) fn directory_page<W: SourceDemandReadWork>(
                    &self, source: SourceReference, directory: &NamespacePath,
                    cursor: Option<SourceCursor>, maximum_entries: u32, work: &mut W,
                ) -> Result<SourceDirectoryPage, DemandError> {
                    if maximum_entries == 0 || maximum_entries > 4096 { return Err(DemandError::InvalidRequest); }
                    let ownership = SourcePayloadOwnership::new(work)?;
                    let guard_bytes = std::mem::size_of::<NativePagePublication>() as u64;
                    work.allocate(guard_bytes)?;
                    let mut publication = Box::new(NativePagePublication {
                        lease: None, end: 0,
                        original_cleanup: None,
                        ownership,
                    });
                    publication.ownership.retain(guard_bytes, work)?;
                    let original_budget = work.with_original_leaf(|actual| actual.budget.checked_add(actual.cancellation_wait.pending_work)
                        .expect("original additive completion credits came from this grant"));
                    if let Some(original) = work.native_source_work().map(|work| work.owned().allocation_work()) {
                        work.with_original_leaf(|actual| actual.reserve(crate::materializer::NativeResourceCredit::Items, 1))?;
                        publication.original_cleanup = Some(original);
                    } else {
                        work.with_original_leaf(|actual| {
                            actual.budget = publication_cleanup().remaining(actual.budget)?;
                            Ok::<_, DemandError>(())
                        })?;
                    }
                    let answer = (|| {
                                self.check(source, work)?;
                                self.observe_directory(directory, work)?;
                                let metadata = self.metadata(directory, work)?.ok_or(DemandError::Absent)?;
                                if !metadata.is_dir() { return Err(DemandError::NotDirectory); }
                                let observed = version(&metadata, work)?;
                                publication.lease = Some(self.cursor_lease(source, directory, observed, cursor, maximum_entries, work)?);
                                let lease = publication.lease.as_ref().ok_or(DemandError::StaleCursor)?;
                                let mut state = lease.state()?;
                                let range = self.page_entries(&mut state, maximum_entries, work, || {})?;
                                let backing = state.replay.as_ref().ok_or(DemandError::StaleCursor)?;
                                let has_next = !state.eof || state.pending.is_some() || range.end < backing.entries.len();
                                let entries = SourceDirectoryEntries::shared(Arc::clone(backing), range.clone());
                                drop(state);
                                self.check(source, work)?;
                                let current = self.metadata(directory, work)?;
                                if current.as_ref().map(|metadata| version(metadata, work))
                                    .transpose()? != Some(observed) {
                                    return Err(DemandError::StaleVersion);
                                }
                                let next = if has_next {
                                    let name_ownership = SourcePayloadOwnership::new(work)?;
                                    let directory = clone_namespace(directory, work)?;
                                    name_ownership.retain(directory.retained_bytes(), work)?;
                                    Some(SourceCursor {
                                        source, directory, directory_version: observed, token: lease.token,
                                        #[cfg(not(target_arch = "wasm32"))]
                                        native_ownership: Some(name_ownership),
                                        borrowed: false,
                                    })
                                } else { None };
                                work.check()?;
                                work.record(WorkCounters { items_returned: range.len() as u64, ..WorkCounters::UNCHARGED })?;
                                publication.end = range.end;
                                Ok((entries, next, observed))
                    })();
                    if !work.is_native_original() {
                        work.with_original_leaf(|actual| actual.restore_budget(original_budget));
                    }
                    match answer {
                        Ok((entries, next, version)) => Ok(SourceDirectoryPage {
                            entries, next, version, publication: Some(publication),
                        }),
                        Err(error) => {
                            if !work.is_native_original() && publication.lease.is_some() {
                                work.record(publication_cleanup())?;
                                publication.lease.take();
                            }
                            drop(publication);
                            Err(error)
                        }
                    }
                }
}
