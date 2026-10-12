use super::*;

pub(in crate::demand) struct NativeChanges {
        pub(in crate::demand) inner: std::sync::Weak<NativeDemandInner>,
        pub(in crate::demand) sink: Arc<dyn SourceChangeSink>,
        pub(in crate::demand) ownership: SourcePayloadOwnership,
    }

#[derive(Clone, Copy)]
    pub(in crate::demand) enum NativeChangedName<'a> {
        Os(&'a std::ffi::OsStr),
        #[cfg(windows)]
        Wide(&'a [u16]),
    }

impl NativeDemandInner {
    pub(in crate::demand) fn names<'a, W: SourceDemandReadWork>(
                &self, parts: impl Iterator<Item = Option<NativeChangedName<'a>>> + Clone,
                work: &mut W,
            ) -> Result<(NamespacePath, bool, Option<crate::NativeAllocation>), DemandError> {
                let mut count = 0_usize;
                for _ in parts.clone() {
                    work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                    count = count.checked_add(1).ok_or(crate::WorkError::Overflow)?;
                }
                let capacity = count.min(usize::from(self.limits.maximum_path_depth));
                let (mut names, vector_bytes) = source_vec::<crate::kernel::LogicalName, _>(capacity, work)?;
                let mut owned_bytes = vector_bytes;
                let mut native_names: Option<crate::NativeAllocation> = None;
                let result = (|| {
                    let mut exact = true;
                    let mut encoded_bytes = 1_u32;
                    for part in parts {
                        work.charge(WorkCounters { items_examined: 1, source_path_components: 1, ..WorkCounters::UNCHARGED })?;
                        if names.len() == capacity { exact = false; break; }
                        let Some(part) = part else { exact = false; break; };
                        let encoded = match part {
                            NativeChangedName::Os(name) => crate::native_name::host_name_bytes_with_source_work(
                                name, self.profile, self.limits.maximum_component_bytes, work,
                            ),
                            #[cfg(windows)]
                            NativeChangedName::Wide(name) => crate::native_name::host_wide_name_bytes_with_source_work(
                                name, self.profile, self.limits.maximum_component_bytes, work,
                            ),
                        };
                        let ((encoding, bytes), mut guard) = match encoded {
                            Ok(encoded) => encoded,
                            Err(DemandError::InvalidRequest) => { exact = false; break; }
                            Err(error) => return Err(error),
                        };
                        let retained = bytes.capacity() as u64;
                        let next = encoded_bytes.checked_add(u32::from(!names.is_empty()))
                            .and_then(|length| length.checked_add(u32::try_from(bytes.len()).ok()?));
                        if next.is_none_or(|length| length > self.limits.maximum_path_bytes) {
                            drop(bytes);
                            if guard.is_none() { work.release(retained)?; }
                            drop(guard);
                            exact = false;
                            break;
                        }
                        let name = match crate::kernel::LogicalName::new(
                            encoding, bytes, self.limits.maximum_component_bytes,
                        ) {
                            Ok(name) => name,
                            Err(_) => {
                                if guard.is_none() { work.release(retained)?; }
                                drop(guard);
                                exact = false;
                                break;
                            }
                        };
                        if let Some(mut incoming) = guard.take() {
                            if let Some(owned) = native_names.as_mut() {
                                if let Err(error) = owned.absorb(&mut incoming) {
                                    drop(name);
                                    drop(incoming);
                                    return Err(crate::native_name::original_name_error(error));
                                }
                            } else { native_names = Some(incoming); }
                        } else {
                            owned_bytes = owned_bytes.checked_add(retained).ok_or(crate::WorkError::Overflow)?;
                        }
                        encoded_bytes = next.expect("checked native namespace length");
                        names.push(name);
                    }
                    work.charge(WorkCounters { items_examined: names.len() as u64, ..WorkCounters::UNCHARGED })?;
                    let path = NamespacePath::new(std::mem::take(&mut names), self.limits)?;
                    Ok((path, exact, native_names.take()))
                })();
                if result.is_err() {
                    drop(names);
                    drop(native_names);
                    work.release(owned_bytes)?;
                }
                result
            }
}

pub(in crate::demand) enum NativeChangedItem<'a> {
        Everything,
        #[cfg(any(target_os = "macos", windows))]
        Unconfirmed,
        #[cfg(windows)]
        File(u64),
        Path { relative: &'a Path, altered: bool },
        #[cfg(windows)]
        WidePath { relative: &'a [u16], altered: bool },
    }

pub(in crate::demand) enum NativeChangeReport {
        Inline { entries: [SourceChange; 2], len: usize },
        Heap(Vec<SourceChange>),
    }

impl NativeChangeReport {
    pub(in crate::demand) fn push(&mut self, change: SourceChange) {
                match self {
                    Self::Inline { entries, len } => {
                        entries[*len] = change;
                        *len += 1;
                    }
                    Self::Heap(entries) => entries.push(change),
                }
            }

    pub(in crate::demand) fn as_slice(&self) -> &[SourceChange] {
                match self {
                    Self::Inline { entries, len } => &entries[..*len],
                    Self::Heap(entries) => entries,
                }
            }
}

impl NativeChanges {
    pub(in crate::demand) fn append_path_change<W: SourceDemandReadWork>(
                reported: &mut NativeChangeReport, name: NamespacePath, exact: bool,
                mut name_guard: Option<crate::NativeAllocation>,
                native_names: &mut Option<crate::NativeAllocation>,
                ownership: &SourcePayloadOwnership,
                altered: bool, metadata: Result<HostStat, DemandError>, work: &mut W,
            ) -> Result<(), DemandError> {
                let guarded = name_guard.as_ref().map_or(0, crate::NativeAllocation::bytes);
                let owned = name.retained_bytes().checked_sub(guarded).ok_or(crate::WorkError::Overflow)?;
                let observed = (|| {
                    Ok(match metadata {
                        Ok(metadata) => {
                            work.charge(WorkCounters { bytes_hashed: identity_hash_bytes(&metadata), ..WorkCounters::UNCHARGED })?;
                            Some(file_identity(&metadata))
                        }
                        Err(DemandError::Io(error)) if matches!(error.kind(),
                            std::io::ErrorKind::NotFound | std::io::ErrorKind::NotADirectory) => None,
                        Err(error) => return Err(error),
                    })
                })();
                let node = match observed {
                    Ok(node) => node,
                    Err(error) => {
                        drop(name);
                        drop(name_guard);
                        let cleanup = work.release(owned);
                        return Err::<(), DemandError>(error).and(cleanup);
                    }
                };
                let altered = altered && exact && (node.is_some() || name.is_root());
                if !altered && name.is_root() {
                    drop(name);
                    drop(name_guard);
                    work.release(owned)?;
                    reported.push(SourceChange::Everything);
                } else {
                    if let Err(error) = ownership.retain(owned, work) {
                        drop(name);
                        let cleanup = work.release(owned);
                        return Err::<(), DemandError>(error).and(cleanup);
                    }
                    if let Some(mut incoming) = name_guard.take() {
                        if let Some(existing) = native_names.as_mut() {
                            if let Err(error) = existing.absorb(&mut incoming) {
                                drop(name);
                                drop(incoming);
                                let cleanup = if ownership.has_native_custody() {
                                    ownership.retire(owned)
                                } else { work.release(owned) };
                                return Err::<(), DemandError>(crate::native_name::original_name_error(error)).and(cleanup);
                            }
                        } else { *native_names = Some(incoming); }
                    }
                    reported.push(if altered { SourceChange::Entry(name) } else { SourceChange::Name(name) });
                }
                if let Some(node) = node { reported.push(SourceChange::Node(node)); }
                Ok(())
            }

    pub(in crate::demand) fn deliver_changes<'a, W: SourceDemandReadWork>(
                &self, changes: impl ExactSizeIterator<Item = NativeChangedItem<'a>>,
                work: &mut W,
            ) -> Result<(), DemandError> {
                work.check()?;
                let Some(inner) = self.inner.upgrade() else { return Ok(()); };
                let mut native_names: Option<crate::NativeAllocation> = None;
                let ownership = SourcePayloadOwnership::new(work)?;
                let capacity = changes.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                let (mut reported, vector_bytes) = if capacity <= 2 {
                    (NativeChangeReport::Inline { entries: [SourceChange::Everything, SourceChange::Everything], len: 0 }, 0)
                } else {
                    let (entries, vector_bytes) = source_vec::<SourceChange, _>(capacity, work)?;
                    if let Err(error) = ownership.retain(vector_bytes, work) {
                        drop(entries);
                        let cleanup = work.release(vector_bytes);
                        return Err::<(), DemandError>(error).and(cleanup);
                    }
                    (NativeChangeReport::Heap(entries), vector_bytes)
                };
                let result = (|| {
                    for change in changes {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                        match change {
                            NativeChangedItem::Everything => reported.push(SourceChange::Everything),
                            #[cfg(any(target_os = "macos", windows))]
                            NativeChangedItem::Unconfirmed => reported.push(SourceChange::Unconfirmed),
                            #[cfg(windows)]
                            NativeChangedItem::File(index) => {
                                work.charge(WorkCounters {
                                    bytes_hashed: b"acyclic-fs-native-source-file-v1\0".len() as u64 + 12,
                                    ..WorkCounters::UNCHARGED
                                })?;
                                reported.push(SourceChange::Node(windows_file_identity(inner.root.identity().device, index)));
                            }
                            NativeChangedItem::Path { relative, altered } => {
                                let (name, exact, guard) = inner.names(relative.components().map(|component| match component {
                                    std::path::Component::Normal(name) => Some(NativeChangedName::Os(name)),
                                    _ => None,
                                }), work)?;
                                let metadata = inner.root.stat(relative, work);
                                Self::append_path_change(&mut reported, name, exact, guard, &mut native_names, &ownership, altered, metadata, work)?;
                            }
                            #[cfg(windows)]
                            NativeChangedItem::WidePath { relative, altered } => {
                                let (name, exact, guard) = inner.names(relative.split(|unit| *unit == u16::from(b'\\') || *unit == u16::from(b'/'))
                                    .filter(|name| !name.is_empty()).map(|name| Some(NativeChangedName::Wide(name))), work)?;
                                let metadata = inner.root.stat_wide_with_work(relative, work);
                                Self::append_path_change(&mut reported, name, exact, guard, &mut native_names, &ownership, altered, metadata, work)?;
                            }
                        }
                    }
                    if !reported.as_slice().is_empty() {
                        if let Some(source) = work.native_source_work() {
                            self.sink.source_changed_with_native_work(reported.as_slice(), source)?;
                        } else {
                            work.with_original_leaf(|work| {
                                let remaining = work.remaining()?;
                                let cancellation = work.cancellation().clone();
                                let prior_live = work.allocations.live_bytes();
                                match self.sink.source_changed(reported.as_slice(), remaining, &cancellation) {
                                    Ok(receipt) => work.merge_with_prior_live(receipt.work, prior_live),
                                    Err(failure) => {
                                        work.merge_with_prior_live(*failure.work, prior_live)?;
                                        Err(failure.error)
                                    }
                                }
                            })?;
                        }
                    }
                    Ok(())
                })();
                let names = reported.as_slice().iter().try_fold(0_u64, |sum, change| {
                    let bytes = match change {
                        SourceChange::Name(path) | SourceChange::Entry(path) => path.retained_bytes(),
                        _ => 0,
                    };
                    sum.checked_add(bytes).ok_or(crate::WorkError::Overflow)
                })?;
                let owned = vector_bytes.checked_add(names).and_then(|bytes|
                    bytes.checked_sub(native_names.as_ref().map_or(0, crate::NativeAllocation::bytes)))
                    .ok_or(crate::WorkError::Overflow)?;
                drop(reported);
                drop(native_names);
                let cleanup = if ownership.has_native_custody() {
                    drop(ownership);
                    Ok(())
                } else {
                    drop(ownership);
                    work.release(owned)
                };
                result.and(cleanup)
            }
}

impl<'a> From<&'a crate::source_watch::HostChange> for NativeChangedItem<'a> {
        fn from(change: &'a crate::source_watch::HostChange) -> Self {
            use crate::source_watch::HostChange;
            match change {
                HostChange::Everything => NativeChangedItem::Everything,
                #[cfg(any(target_os = "macos", windows))]
                HostChange::Unconfirmed => NativeChangedItem::Unconfirmed,
                #[cfg(windows)]
                HostChange::File(index) => NativeChangedItem::File(*index),
                HostChange::Rebound(relative) => NativeChangedItem::Path { relative, altered: false },
                HostChange::Altered(relative) => NativeChangedItem::Path { relative, altered: true },
            }
        }
    }

#[cfg(windows)]
    impl<'a> From<&'a crate::source_watch::HostChangeWide<'_>> for NativeChangedItem<'a> {
        fn from(change: &'a crate::source_watch::HostChangeWide<'_>) -> Self {
            use crate::source_watch::HostChangeWide;
            match change {
                HostChangeWide::Everything => NativeChangedItem::Everything,
                HostChangeWide::Unconfirmed => NativeChangedItem::Unconfirmed,
                HostChangeWide::File(index) => NativeChangedItem::File(*index),
                HostChangeWide::Path { action, path } => NativeChangedItem::WidePath {
                    relative: path, altered: *action == 3,
                },
            }
        }
    }

impl crate::source_watch::HostChangeSink for NativeChanges {
        fn logical_owned_bytes(&self) -> u64 {
            std::mem::size_of::<Self>() as u64 + self.sink.logical_owned_bytes()
        }

        fn host_changed(
            &self, changes: &[crate::source_watch::HostChange], work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            self.deliver_changes(changes.iter().map(NativeChangedItem::from), work)
        }

        #[cfg(windows)]
        fn host_changed_wide(
            &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut DemandWork,
        ) -> Result<(), DemandError> {
            self.deliver_changes(changes.iter().map(NativeChangedItem::from), work)
        }

        fn host_changed_with_native_work(
            &self, changes: &[crate::source_watch::HostChange], work: &mut NativeSourceWork<'_>,
        ) -> Result<(), DemandError> {
            self.deliver_changes(changes.iter().map(NativeChangedItem::from), work)
        }

        #[cfg(windows)]
        fn host_changed_wide_with_native_work(
            &self, changes: &[crate::source_watch::HostChangeWide<'_>], work: &mut NativeSourceWork<'_>,
        ) -> Result<(), DemandError> {
            self.deliver_changes(changes.iter().map(NativeChangedItem::from), work)
        }
    }
