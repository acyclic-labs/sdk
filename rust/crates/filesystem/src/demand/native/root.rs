use super::*;

#[cfg(unix)]
    pub(in crate::demand) type DemandPath = PathBuf;

#[cfg(windows)]
    pub(in crate::demand) type DemandPath = Vec<u16>;

#[cfg(unix)]
    pub(in crate::demand) fn path_owned_bytes(path: &DemandPath) -> u64 {
        path.as_os_str().as_encoded_bytes().len() as u64
    }

#[cfg(windows)]
    pub(in crate::demand) fn path_owned_bytes(path: &DemandPath) -> u64 {
        (path.len() * std::mem::size_of::<u16>()) as u64
    }

pub(in crate::demand) struct SourceRootInput {
        pub(in crate::demand) path: DemandPath,
        pub(in crate::demand) absolute: bool,
        pub(in crate::demand) custody: SourceRootPathCustody,
    }

pub(in crate::demand) fn source_vec<T, W: SourceDemandReadWork>(
        capacity: usize, work: &mut W,
    ) -> Result<(Vec<T>, u64), DemandError> {
        let width = std::mem::size_of::<T>();
        let mut bytes = capacity.checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        work.allocate(bytes)?;
        let mut values = Vec::new();
        if let Err(error) = values.try_reserve_exact(capacity) {
            drop(values);
            work.release(bytes)?;
            return Err(DemandError::Allocation(error));
        }
        let actual = values.capacity().checked_mul(width).ok_or(crate::WorkError::Overflow)? as u64;
        if let Err(error) = work.reconcile_allocation(&mut bytes, actual) {
            drop(values);
            work.release(bytes)?;
            return Err(error);
        }
        Ok((values, bytes))
    }

pub(in crate::demand) struct SourceRootPathCustody {
        pub(in crate::demand) ownership: SourcePayloadOwnership,
        pub(in crate::demand) original: std::sync::Weak<NativeDemandReadMeter>,
        pub(in crate::demand) bytes: u64,
    }

impl SourceRootPathCustody {
    pub(in crate::demand) fn new<W: SourceDemandReadWork>(work: &mut W) -> Result<Self, DemandError> {
                Ok(Self {
                    ownership: SourcePayloadOwnership::new(work)?,
                    original: work.original_meter().map(Arc::downgrade).unwrap_or_default(),
                    bytes: 0,
                })
            }

    pub(in crate::demand) fn retain<W: SourceDemandReadWork>(&mut self, bytes: u64, work: &mut W) -> Result<(), DemandError> {
                self.ownership.retain(bytes, work)?;
                self.bytes = bytes;
                Ok(())
            }
}

impl Drop for SourceRootPathCustody {
        fn drop(&mut self) {
            if self.ownership.has_native_custody() {
                self.ownership.retire(self.bytes).expect("native root retires only its physical path");
                return;
            }
            if let Some(original) = self.original.upgrade() {
                original.with_original_leaf(|work| work.release(self.bytes))
                    .expect("ordinary root path retires after its physical buffer");
            }
        }
    }

impl SourceRootInput {
    pub(in crate::demand) fn copy<W: SourceDemandReadWork>(path: &Path, work: &mut W) -> Result<Self, DemandError> {
                let mut custody = SourceRootPathCustody::new(work)?;
                #[cfg(unix)]
                let (owned, bytes) = crate::native_host::clone_path_with_work(path, work)?;
                #[cfg(windows)]
                let (owned, bytes) = crate::native_host::encode_root_path_name_with_work(path, work)?;
                if let Err(error) = custody.retain(bytes, work) {
                    drop(owned);
                    work.release(bytes)?;
                    return Err(error);
                }
                Ok(Self { path: owned, absolute: path.is_absolute(), custody })
            }

    pub(in crate::demand) fn from_admitted(path: PathBuf, work: &mut NativeSourceWork<'_>) -> Result<Self, DemandError> {
                #[cfg(unix)]
                {
                    let mut custody = SourceRootPathCustody::new(work)?;
                    let bytes = path.capacity() as u64;
                    if let Err(error) = custody.retain(bytes, work) {
                        drop(path);
                        work.release(bytes)?;
                        return Err(error);
                    }
                    Ok(Self { absolute: path.is_absolute(), path, custody })
                }
                #[cfg(windows)]
                {
                    let bytes = path.capacity() as u64;
                    let result = Self::copy(&path, work);
                    drop(path);
                    work.release(bytes)?;
                    result
                }
            }

    pub(in crate::demand) fn normalize<W: SourceDemandReadWork>(self, work: &mut W) -> Result<Self, DemandError> {
                #[cfg(unix)]
                if self.absolute { return Ok(self); }
                #[cfg(windows)]
                if self.absolute && self.path.len() < 248
                    || self.path.starts_with(&[92, 92, 63, 92])
                    || self.path.starts_with(&[92, 63, 63, 92]) {
                    return Ok(self);
                }
                let release = !self.custody.ownership.has_native_custody() && self.custody.original.strong_count() == 0;
                let prior_bytes = self.custody.bytes;
                let result = (|| {
                let mut custody = SourceRootPathCustody {
                    ownership: SourcePayloadOwnership::new(work)?,
                    original: self.custody.original.clone(),
                    bytes: 0,
                };
                #[cfg(unix)]
                let path = crate::native_host::normalize_root_path_measured(&self.path, work)?;
                #[cfg(windows)]
                let path = crate::native_host::normalize_root_wide_name_with_work(&self.path, self.absolute, work)?;
                #[cfg(unix)]
                let bytes = path.capacity() as u64;
                #[cfg(windows)]
                let bytes = (path.capacity() * std::mem::size_of::<u16>()) as u64;
                if let Err(error) = custody.retain(bytes, work) {
                    drop(path);
                    work.release(bytes)?;
                    return Err(error);
                }
                Ok(Self { path, absolute: true, custody })
                })();
                drop(self);
                if release { let cleanup = work.release(prior_bytes); return result.and_then(|input| cleanup.map(|()| input)); }
                result
            }
}

pub(in crate::demand) fn retain_root_control<T: std::any::Any + Send + Sync, W: SourceDemandReadWork>(
        owner: &Arc<T>, ownership: &SourcePayloadOwnership, work: &mut W,
    ) -> Result<(), DemandError> {
        if let Some(custody) = &ownership.custody {
            let owner: Arc<dyn std::any::Any + Send + Sync> = owner.clone();
            let weak = Arc::downgrade(&owner);
            drop(owner);
            return work.with_original_leaf(|work| custody.attach_known_control(
                weak, work,
            ));
        }
        Ok(())
    }

impl NativeDemandSource {
    /// Returns the stable identity of the already-open native root.
                #[must_use]
                pub fn root_identity(&self) -> crate::NativeRootIdentity {
                    self.inner.root.identity()
                }

    #[cfg(test)]
                pub(in crate::demand) fn read_with_deadline(
                    &self, source: SourceReference, path: &NamespacePath, expected: SourceVersion,
                    budget: WorkBudget, cancellation: &CancellationToken, deadline: std::time::Duration,
                ) -> DemandResult<crate::kernel::OwnedSourceChunk> {
                    let mut work = DemandWork::new(budget, cancellation);
                    let file = match NativeDemandFile::open(self.clone(), source, path, expected, &mut work) {
                        Ok(file) => file,
                        Err(error) => return work.finish(Err(error)),
                    };
                    file.read_remote(0, 6, work, deadline)
                }

    pub(in crate::demand) fn observe_directory<W: SourceDemandReadWork>(&self, directory: &NamespacePath, work: &mut W) -> Result<(), DemandError> {
                    self.admit_to_watches(directory, work)?;
                    if let Some(observer) = &self.inner.observer {
                        if let Some(original) = work.native_source_work() {
                            return observer.observe_directory_with_native_work(directory, original);
                        }
                        let cancellation = work.with_original_leaf(|work| work.cancellation.clone());
                        let answer = observer.observe_directory(directory, work.remaining()?, &cancellation);
                        match answer {
                            Ok(receipt) => work.merge(receipt.work)?,
                            Err(failure) => {
                                work.merge(*failure.work)?;
                                return Err(failure.error);
                            }
                        }
                    }
                    Ok(())
                }

    #[cfg(target_os = "linux")]
                pub(in crate::demand) fn admit_to_watches<W: SourceDemandReadWork>(&self, directory: &NamespacePath, work: &mut W) -> Result<bool, DemandError> {
                    let watches = self.inner.watches.read()
                        .unwrap_or_else(std::sync::PoisonError::into_inner);
                    let mut count = 0usize;
                    for watch in watches.entries.iter() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        count += usize::from(watch.as_ref().and_then(std::sync::Weak::upgrade).is_some_and(|watch| !watch.watch.is_stopped()));
                    }
                    if count == 0 { return Ok(false); }
                    let bytes = count * std::mem::size_of::<Arc<NativeSourceWatchBacking>>();
                    work.allocate(bytes as u64)?;
                    let mut live = Vec::with_capacity(count);
                    for watch in watches.entries.iter() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if let Some(watch) = watch.as_ref().and_then(std::sync::Weak::upgrade) {
                            if watch.watch.is_stopped() { continue; }
                            work.charge(WorkCounters {
                                bytes_copied: std::mem::size_of::<Arc<NativeSourceWatchBacking>>() as u64,
                                ..WorkCounters::default()
                            })?;
                            live.push(watch);
                        }
                    }
                    drop(watches);
                    let relative = self.relative(directory, work)?;
                    let answer = (|| {
                                let mut added = false;
                                for watch in &live {
                                    if !watch.watch.is_stopped() { added |= watch.watch.admit_with_work(&relative, work)?; }
                                }
                                Ok(added)
                    })();
                    let path_bytes = path_owned_bytes(&relative);
                    drop(relative);
                    work.release(path_bytes)?;
                    drop(live);
                    work.release(bytes as u64)?;
                    answer
                }

    /// One watch covers the whole root on every other host.
                #[cfg(not(target_os = "linux"))]
                #[allow(
                    clippy::unused_self,
                    clippy::unnecessary_wraps,
                    reason = "the Linux variant admits directories to inotify watches and can fail; this one keeps its signature"
                )]
                pub(in crate::demand) fn admit_to_watches<W: SourceDemandReadWork>(&self, _directory: &NamespacePath, work: &mut W) -> Result<bool, DemandError> {
                    work.check()?;
                    Ok(false)
                }

    pub(in crate::demand) fn observe_parent<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<(), DemandError> {
                    let components = path.components();
                    let depth = components.len().saturating_sub(1);
                    work.allocate((depth * std::mem::size_of::<crate::kernel::LogicalName>()) as u64)?;
                    let mut prefix = Vec::with_capacity(depth);
                    for name in components.iter().take(depth) {
                        work.allocate_for(name.as_bytes().len() as u64, WorkCounters {
                            bytes_copied: name.as_bytes().len() as u64, ..WorkCounters::default()
                        })?;
                        prefix.push(name.clone());
                    }
                    let parent = NamespacePath::new(prefix, self.inner.limits)?;
                    let answer = self.observe_directory(&parent, work);
                    let bytes = parent.retained_bytes();
                    drop(parent);
                    work.release(bytes)?;
                    answer
                }

    /// Invalidates source references and cursors on the caller's work context.
                pub fn invalidate<W: SourceDemandReadWork>(
                    &self, work: &mut W,
                ) -> Result<SourceReference, DemandError> {
                    work.check()?;
                    if work.is_native_original() && !self.inner.root_custody.ownership.has_native_custody() {
                        return Err(DemandError::NativeWorkUnavailable);
                    }
                    let mut cursors = self.inner.cursors.lock().map_err(|_| DemandError::StaleCursor)?;
                    let visits = cursors.slots.len() as u64;
                    work.with_original_leaf(|actual| actual.reserve(crate::materializer::NativeResourceCredit::Items, visits))?;
                    let previous = match self.inner.epoch.fetch_update(Ordering::AcqRel, Ordering::Acquire, |epoch| epoch.checked_add(1)) {
                        Ok(previous) => previous,
                        Err(_) => {
                            work.with_original_leaf(|actual| actual.refund(crate::materializer::NativeResourceCredit::Items, visits).expect("original reservation is balanced"));
                            return Err(crate::WorkError::Overflow.into());
                        }
                    };
                    for slot in cursors.slots.iter_mut() {
                        work.with_original_leaf(|actual| actual.complete(crate::materializer::NativeResourceCredit::Items, 1, 1))?;
                        // Actual source data drops outside the original ledger lock.
                        drop(slot.take());
                    }
                    cursors.slots.clear();
                    Ok(SourceReference { identity: self.inner.identity, epoch: previous + 1 })
                }

    /// Binds the caller's canonical root spelling and expected real identity
                pub fn validate_root<W: SourceDemandReadWork>(
                    &self, path: &Path, expected: crate::NativeRootIdentity,
                    work: &mut W,
                ) -> Result<SourceReference, DemandError> {
                    work.check()?;
                    if work.is_native_original() && !self.inner.root_custody.ownership.has_native_custody() {
                        return Err(DemandError::NativeWorkUnavailable);
                    }
                    if !path.is_absolute() { return Err(DemandError::InvalidRequest); }
                    if self.inner.root.identity() != expected { return Err(DemandError::SourceUnavailable); }
                    #[cfg(unix)]
                    let input = path.as_os_str().as_encoded_bytes();
                    #[cfg(unix)]
                    let held = self.inner.path.as_os_str().as_encoded_bytes();
                    #[cfg(windows)]
                    let owned = SourceRootInput::copy(path, work)?.normalize(work)?;
                    #[cfg(windows)]
                    let input = owned.path.as_slice();
                    #[cfg(windows)]
                    let held = self.inner.path.as_slice();
                    let result = (|| {
                        if input.len() != held.len() { return Err(DemandError::SourceUnavailable); }
                        for (input, held) in input.iter().zip(held) {
                            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED })?;
                            if input != held { return Err(DemandError::SourceUnavailable); }
                        }
                        let source = self.reference();
                        self.validate_current(source, work)?;
                        Ok(source)
                    })();
                    #[cfg(windows)]
                    {
                        let release = !owned.custody.ownership.has_native_custody() && owned.custody.original.strong_count() == 0;
                        let bytes = owned.custody.bytes;
                        drop(owned);
                        if release { let cleanup = work.release(bytes); return result.and_then(|source| cleanup.map(|()| source)); }
                    }
                    result
                }

    /// Validates a source reference against the held root and its current name.
                pub fn validate_current<W: SourceDemandReadWork>(
                    &self, source: SourceReference, work: &mut W,
                ) -> Result<(), DemandError> {
                    work.check()?;
                    if work.is_native_original() && !self.inner.root_custody.ownership.has_native_custody() {
                        return Err(DemandError::NativeWorkUnavailable);
                    }
                    if source != self.reference() { return Err(DemandError::StaleSource); }
                    let held = self.inner.root.held_identity_with_work(work)?;
                    #[cfg(unix)]
                    let named = crate::native_host::root_path_identity_with_work(&self.inner.path, work)?;
                    #[cfg(windows)]
                    let named = crate::native_host::root_path_identity_wide_with_work(&self.inner.path, work)?;
                    if held != self.inner.root.identity() || named != held { return Err(DemandError::SourceUnavailable); }
                    if source != self.reference() { return Err(DemandError::StaleSource); }
                    work.check()
                }

    pub(in crate::demand) fn check<W: SourceDemandReadWork>(&self, source: SourceReference, work: &mut W) -> Result<(), DemandError> {
                    if source != self.reference() {
                        return Err(DemandError::StaleSource);
                    }
                    work.check()?;
                    if self.root_named_while_watched(work)? {
                        return Ok(());
                    }
                    #[cfg(unix)]
                    let identity = crate::native_host::root_path_identity_with_work(&self.inner.path, work);
                    #[cfg(windows)]
                    let identity = crate::native_host::root_path_identity_wide_with_work(&self.inner.path, work);
                    match identity {
                        Ok(identity) if identity == self.inner.root.identity() => Ok(()),
                        Err(error @ (DemandError::Work(_) | DemandError::Cancelled)) => Err(error),
                        _ => Err(DemandError::SourceUnavailable),
                    }
                }

    #[cfg(windows)]
                pub(in crate::demand) fn root_named_while_watched<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<bool, DemandError> {
                    let watches = self.inner.watches.read().unwrap_or_else(std::sync::PoisonError::into_inner);
                    for watch in watches.entries.iter() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if watch.as_ref().is_some_and(|watch| watch.strong_count() != 0) { return Ok(true); }
                    }
                    Ok(false)
                }

    /// Watches elsewhere report a moved root rather than prevent it.
                #[cfg(not(windows))]
                #[allow(
                    clippy::unused_self,
                    reason = "the Windows variant reads the live watch set; this one keeps its signature"
                )]
                pub(in crate::demand) fn root_named_while_watched<W: SourceDemandReadWork>(&self, work: &mut W) -> Result<bool, DemandError> {
                    work.check()?;
                    Ok(false)
                }

    #[cfg(unix)]
                pub(in crate::demand) fn relative<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<DemandPath, DemandError> {
                    let limits = self.inner.limits;
                    if path.depth() > usize::from(limits.maximum_path_depth)
                        || path.encoded_bytes() > limits.maximum_path_bytes
                    {
                        return Err(DemandError::InvalidRequest);
                    }
                    let mut length = path.depth().saturating_sub(1);
                    for component in path.components() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        if component.as_bytes().len() > limits.maximum_component_bytes as usize {
                            return Err(DemandError::InvalidRequest);
                        }
                        let bytes = if component.encoding() == crate::kernel::NameEncoding::WindowsUtf16Le {
                            char::decode_utf16(component.as_bytes().chunks_exact(2)
                                .map(|bytes| u16::from_le_bytes([bytes[0], bytes[1]])))
                                .try_fold(0usize, |length, value| {
                                    let value = value.map_err(|_| DemandError::InvalidRequest)?.len_utf8();
                                    Ok::<_, DemandError>(length + value)
                                })?
                        } else { component.as_bytes().len() };
                        length = length.checked_add(bytes).ok_or(crate::WorkError::Overflow)?;
                    }
                    work.allocate(length as u64)?;
                    let mut result = PathBuf::with_capacity(length);
                    for component in path.components() {
                        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::default() })?;
                        let name = work.with_original_leaf(|work| host_component(component, work))?;
                        work.charge(WorkCounters { bytes_copied: name.as_encoded_bytes().len() as u64, ..WorkCounters::default() })?;
                        result.push(&name);
                        let bytes = name.as_encoded_bytes().len() as u64;
                        drop(name);
                        work.release(bytes)?;
                    }
                    Ok(result)
                }

    #[cfg(windows)]
                pub(in crate::demand) fn relative<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<DemandPath, DemandError> {
                    work.with_original_leaf(|work| native_wide_path(path, self.inner.limits, work))
                }

    pub(in crate::demand) fn stat_relative<W: SourceDemandReadWork>(&self, relative: &DemandPath, work: &mut W) -> Result<HostStat, DemandError> {
                    #[cfg(unix)]
                    { self.inner.root.stat(relative, work) }
                    #[cfg(windows)]
                    { self.inner.root.stat_wide_with_work(relative, work) }
                }

    pub(in crate::demand) fn metadata<W: SourceDemandReadWork>(&self, path: &NamespacePath, work: &mut W) -> Result<Option<HostStat>, DemandError> {
                    let relative = self.relative(path, work)?;
                    let result = self.stat_relative(&relative, work);
                    let bytes = path_owned_bytes(&relative);
                    drop(relative);
                    work.release(bytes)?;
                    match result {
                        Ok(metadata) => Ok(Some(metadata)),
                        Err(DemandError::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
                        Err(error) => Err(error),
                    }
                }

    pub(in crate::demand) fn node<W: SourceDemandReadWork>(metadata: &HostStat, work: &mut W) -> Result<SourceNode, DemandError> {
                    work.charge(WorkCounters {
                        bytes_hashed: identity_hash_bytes(metadata),
                        items_examined: 1, ..WorkCounters::default()
                    })?;
                    let kind = node_kind(metadata.file_type());
                    Ok(SourceNode {
                        kind,
                        file_identity: file_identity(metadata),
                        link_count: link_count(metadata),
                        device: device_identity(metadata, kind),
                        logical_bytes: (kind == SourceNodeKind::RegularFile
                            || cfg!(unix) && kind == SourceNodeKind::SymbolicLink)
                            .then_some(metadata.len()),
                        version: version(metadata, work)?,
                        metadata: source_metadata(metadata),
                    })
                }

    pub(in crate::demand) fn lookup_node<W: SourceDemandReadWork>(
                    &self, source: SourceReference, path: &NamespacePath, work: &mut W,
                ) -> Result<Option<SourceNode>, DemandError> {
                    self.check(source, work)?;
                    self.observe_parent(path, work)?;
                    let mut node = self.metadata(path, work)?.as_ref()
                        .map(|metadata| Self::node(metadata, work)).transpose()?;
                    if node.is_some_and(|node| node.kind == SourceNodeKind::Directory)
                        && self.admit_to_watches(path, work)?
                    {
                        node = self.metadata(path, work)?.as_ref()
                            .map(|metadata| Self::node(metadata, work)).transpose()?;
                    }
                    self.check(source, work)?;
                    Ok(node)
                }

    pub(in crate::demand) fn link_target<W: SourceDemandReadWork>(
                    &self, source: SourceReference, path: &NamespacePath,
                    expected: SourceVersion, maximum_bytes: u64, work: &mut W,
                ) -> Result<crate::kernel::OwnedSourceChunk, DemandError> {
                    self.check(source, work)?;
                    self.observe_parent(path, work)?;
                    let before = self.metadata(path, work)?.ok_or(DemandError::Absent)?;
                    if !before.file_type().is_symlink() { return Err(DemandError::InvalidRequest); }
                    #[cfg(unix)]
                    if before.len() > maximum_bytes { return Err(DemandError::InvalidRequest); }
                    if version(&before, work)? != expected {
                        return Err(DemandError::StaleVersion);
                    }
                    #[cfg(unix)]
                    work.admit(WorkCounters { output_bytes: before.len(), ..WorkCounters::UNCHARGED })?;
                    #[cfg(unix)]
                    if work.is_native_original() {
                        work.admit(WorkCounters { source_bytes_read: before.len(), ..WorkCounters::UNCHARGED })?;
                    }
                    let relative = self.relative(path, work)?;
                    let result = (|| {
                        let remaining = work.remaining()?;
                        let maximum = remaining.output_bytes.min(remaining.source_bytes_read).min(maximum_bytes);
                        let bytes = if work.is_native_original() {
                            self.inner.root.read_source_link_with_work(&relative, maximum, work)?
                        } else {
                        let ownership = SourcePayloadOwnership::new(work)?;
                        #[cfg(windows)]
                        let target_ownership = SourcePayloadOwnership::new(work)?;
                        #[cfg(unix)]
                        let (target, target_bytes) = self.inner.root.read_link(&relative, maximum, work)?;
                        #[cfg(windows)]
                        let (target, target_bytes) = self.inner.root.read_link_wide_with_work(&relative, maximum, work)?;
                        #[cfg(unix)]
                        ownership.retain(target_bytes, work)?;
                        #[cfg(windows)]
                        target_ownership.retain(target_bytes, work)?;
                        #[cfg(unix)]
                        let bytes = target;
                        #[cfg(windows)]
                        let bytes = {
                            let length = target.len().checked_mul(2).ok_or(crate::WorkError::Overflow)?;
                            work.admit(WorkCounters { output_bytes: length as u64, ..WorkCounters::UNCHARGED })?;
                            work.allocate(length as u64)?;
                            let mut bytes = Vec::new();
                            if let Err(error) = bytes.try_reserve_exact(length) {
                                drop(bytes);
                                let _released = work.release(length as u64);
                                return Err(DemandError::Allocation(error));
                            }
                            let mut owned = length as u64;
                            if let Err(error) = work.with_original_leaf(|work|
                                work.reconcile_native_allocation(&mut owned, bytes.capacity() as u64)) {
                                drop(bytes);
                                let _released = work.release(owned);
                                return Err(error);
                            }
                            if let Err(error) = ownership.retain(owned, work) {
                                drop(bytes);
                                let _released = work.release(owned);
                                return Err(error);
                            }
                            for unit in &target {
                                if let Err(error) = work.charge(WorkCounters {
                                    bytes_encoded: 2, bytes_copied: 2, items_examined: 1,
                                    ..WorkCounters::UNCHARGED
                                }) {
                                    drop(bytes);
                                    if work.is_native_original() { ownership.retire(owned)?; }
                                    else { work.release(owned)?; }
                                    return Err(error);
                                }
                                bytes.extend_from_slice(&unit.to_le_bytes());
                            }
                            drop(target);
                            drop(target_ownership);
                            if !work.is_native_original() { work.release(target_bytes)?; }
                            bytes
                        };
                        bytes.into()
                        };
                        let after = self.metadata(path, work)?.ok_or(DemandError::Absent)?;
                        if version(&after, work)? != expected {
                            return Err(DemandError::StaleVersion);
                        }
                        self.check(source, work)?;
                        work.charge(WorkCounters { output_bytes: bytes.len() as u64, ..WorkCounters::UNCHARGED })?;
                        let logical_owned_bytes = bytes.retained_capacity().unwrap_or(0) as u64;
                        if work.is_native_original() {
                            Ok(crate::kernel::OwnedSourceChunk::intrinsic(bytes, logical_owned_bytes))
                        } else {
                            Ok(crate::kernel::OwnedSourceChunk::claimed(bytes, logical_owned_bytes))
                        }
                    })();
                    let bytes = path_owned_bytes(&relative);
                    drop(relative);
                    work.release(bytes)?;
                    result
                }
}
