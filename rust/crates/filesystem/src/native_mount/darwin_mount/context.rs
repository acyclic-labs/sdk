use super::*;

pub(super) struct FileHandle {
    file_id: FileId,
    file: Option<(
        Arc<dyn MountOpenFile>,
        Option<crate::demand::NativeMountAllocationCustody>,
    )>,
    written: Arc<DarwinOwned<AtomicU64>>,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
pub(super) struct CacheStamp {
    pub(super) forgotten: u64,
    pub(super) view: ViewStamp,
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub(super) struct CacheEpochs {
    pub(super) view: CacheStamp,
    pub(super) binding: u64,
}

pub(super) struct FileTarget {
    pub(super) file: (
        Arc<dyn MountOpenFile>,
        Option<crate::demand::NativeMountAllocationCustody>,
    ),
    written: Option<Arc<DarwinOwned<AtomicU64>>>,
}

pub(super) struct DarwinMountContext {
    pub(super) source: Arc<dyn MountFilesystem>,
    pub(super) handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    pub(super) handler_budget: crate::WorkBudget,
    pub(super) handler_cancellation: crate::CancellationToken,
    pub(super) startup_meter: Mutex<crate::native_mount::NativeMountMeterActivation>,
    pub(super) writable: bool,
    pub(super) mount_uid: u32,
    pub(super) mount_gid: u32,
    pub(super) next_handle: AtomicU64,
    pub(super) inodes: Mutex<DarwinOwned<HashMap<FileId, u64>>>,
    pub(super) forgotten: AtomicU64,
    pub(super) files: RwLock<DarwinOwned<HashMap<u64, FileHandle>>>,
    pub(super) directories:
        Mutex<DarwinOwned<HashMap<u64, Arc<DarwinOwned<Mutex<DirectoryHandle>>>>>>,
    pub(super) ledger: FlushLedger,
    pub(super) changes: ChangeLabels,
    pub(super) origin: ViewOrigin,
    pub(super) callbacks: CallbackGate,
    pub(super) around: AroundChanges,
    pub(super) callback_guard: Option<Arc<dyn crate::native_mount::NativeMountCallbackGuard>>,
    pub(super) source_original_work: Option<Work>,
    pub(super) native_allocation: NativeAllocation,
}

#[derive(Default)]
pub(super) struct DarwinCallbackState {
    pub(super) completed_future_request_work: WorkCounters,
    pub(super) first_fatal: Option<(Option<NativeMountError>, WorkCounters)>,
}

impl DarwinCallbackState {
    pub(super) fn take_failure(&mut self) -> Option<(NativeMountError, WorkCounters)> {
        let (cause, work) = self.first_fatal.as_mut()?;
        cause.take().map(|cause| (cause, *work))
    }
}

pub(super) struct AroundChanges {
    /// Changes another origin reported, plus views forgotten.
    pub(super) made: AtomicU64,
    /// `made` as of the latest barrier that waited them out.
    pub(super) settled: AtomicU64,
    /// Fences that could not confirm every change was reported.
    pub(super) unconfirmed: AtomicU64,
    /// `unconfirmed` as of the latest barrier that verified or waited them
    /// out.
    pub(super) verified: AtomicU64,
}

impl ViewObserver for DarwinMountContext {
    fn view_changed(&self, _position: ViewStamp, origin: ViewOrigin) {
        // The client learns of this mount's own changes from their replies.
        if origin != self.origin {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
    }

    fn view_unconfirmed(&self, _position: ViewStamp) {
        self.around.unconfirmed.fetch_add(1, Ordering::AcqRel);
    }
}

pub(super) fn uptime(work: &Work) -> Result<Duration, NativeMountError> {
    work.charge(READ_WORK)?;
    let mut now = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    // SAFETY: `now` is a valid, writable timespec; the clock always exists.
    if unsafe { libc::clock_gettime(libc::CLOCK_UPTIME_RAW, &raw mut now) } != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(Duration::new(
        u64::try_from(now.tv_sec).unwrap_or(0),
        u32::try_from(now.tv_nsec).unwrap_or(0),
    ))
}

impl DarwinMountContext {
    pub(super) fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        uid: u32,
        gid: u32,
        root_file_id: FileId,
        work: &Work,
    ) -> Result<Self, NativeMountError> {
        let source_original_work = original_work(source.as_ref(), Some(work))?;
        let callback_guard = source.native_callback_guard();
        verify_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        let mut inodes = DarwinOwned::empty(HashMap::new());
        grow_map(&mut inodes, work)?;

        inodes.insert(root_file_id, ROOT_INODE);
        let (handler_budget, handler_cancellation) = {
            let meter = work.state.lock();
            (meter.budget, meter.cancellation().clone())
        };
        Ok(Self {
            source,
            writable,
            mount_uid: uid,
            mount_gid: gid,
            handler_failure: shared(
                work,
                DarwinOwned::empty(Mutex::new(DarwinCallbackState::default())),
            )?,
            handler_budget,
            handler_cancellation,
            startup_meter: Mutex::new(crate::native_mount::NativeMountMeterActivation::default()),
            next_handle: AtomicU64::new(1),
            inodes: Mutex::new(inodes),
            forgotten: AtomicU64::new(0),
            files: RwLock::new(DarwinOwned::empty(HashMap::new())),
            directories: Mutex::new(DarwinOwned::empty(HashMap::new())),
            ledger: FlushLedger::new(),
            changes: ChangeLabels::new(),
            origin: ViewOrigin::new(),
            callbacks: CallbackGate::new(),
            around: AroundChanges {
                made: AtomicU64::new(0),
                settled: AtomicU64::new(0),
                unconfirmed: AtomicU64::new(0),
                verified: AtomicU64::new(0),
            },
            callback_guard,
            source_original_work,
            native_allocation: work.claim(
                std::mem::size_of::<Self>() as u64,
                1,
                WorkCounters::UNCHARGED,
            )?,
        })
    }

    pub(in crate::native_mount) fn allocate_handle(&self) -> Result<u64, i32> {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        if handle == 0 || handle == u64::MAX {
            return Err(libc::EMFILE);
        }
        Ok(handle)
    }

    pub(super) fn cache_stamp(&self, work: &Work) -> Result<Option<CacheStamp>, NativeMountError> {
        let forgotten = self.forgotten.load(Ordering::Acquire);
        Ok(self
            .source
            .view_stamp_with_meter(work)?
            .map(|view| CacheStamp { forgotten, view }))
    }

    pub(super) fn unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
        work: &Work,
    ) -> Result<bool, NativeMountError> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) {
            return Ok(false);
        }
        Ok(self
            .source
            .unchanged_since_with_meter(path, file_id, stamp.view, work)?)
    }

    pub(super) fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
        work: &Work,
    ) -> Result<bool, NativeMountError> {
        if stamp.forgotten != self.forgotten.load(Ordering::Acquire) {
            return Ok(false);
        }
        Ok(self
            .source
            .reported_unchanged_since_with_meter(path, file_id, stamp.view, work)?)
    }

    pub(super) fn cache_epochs(
        &self,
        work: &Work,
    ) -> Result<Option<CacheEpochs>, NativeMountError> {
        if !self.source.view_is_stable_with_meter(work)? {
            return Ok(None);
        }
        let (Some(view), Some(binding)) = (
            self.cache_stamp(work)?,
            self.source.binding_epoch_with_meter(work)?,
        ) else {
            return Ok(None);
        };
        Ok(Some(CacheEpochs { view, binding }))
    }

    pub(super) fn forget_view(&self) {
        self.forgotten.fetch_add(1, Ordering::AcqRel);
        self.around.made.fetch_add(1, Ordering::AcqRel);
    }

    pub(super) fn revalidate(&self, work: &Work) -> Result<bool, NativeMountError> {
        work.admit(WorkCounters::UNCHARGED)?;
        let unconfirmed = self.around.unconfirmed.load(Ordering::Acquire);
        if self.around.verified.load(Ordering::Acquire) < unconfirmed
            && !self.verify_labels(work)?
        {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
        self.around
            .verified
            .fetch_max(unconfirmed, Ordering::AcqRel);
        let made = self.around.made.load(Ordering::Acquire);
        if self.around.settled.load(Ordering::Acquire) >= made {
            return Ok(false);
        }
        self.callbacks.drain(work)?;
        let arrived_by = uptime(work)? + REPLY_DELIVERY_SLACK;
        work.charge(READ_WORK)?;
        // SAFETY: the transport reports its fixed client-cache timeout.
        let expired = Duration::from_secs(
            arrived_by.as_secs()
                + u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() }),
        );
        loop {
            let now = uptime(work)?;
            if now >= expired {
                break;
            }
            work.charge(READ_WORK)?;
            std::thread::sleep((expired - now).min(Duration::from_millis(10)));
        }
        self.around.settled.fetch_max(made, Ordering::AcqRel);
        Ok(true)
    }

    pub(super) fn verify_labels(&self, work: &Work) -> Result<bool, NativeMountError> {
        let cleared = self.changes.cleared.load(Ordering::Acquire);
        let Some(issued) = self.changes.issued(work)? else {
            return Ok(false);
        };
        for (path, lookup) in issued.iter() {
            work.charge(scan_work(1))?;
            match self.lookup(path, work) {
                Ok(current) if same_attributes(current, *lookup) => {}
                Err(cause) if fatal(&cause) => return Err(cause),
                _ => return Ok(false),
            }
        }
        Ok(self.changes.cleared.load(Ordering::Acquire) == cleared)
    }

    pub(super) fn inode(&self, file_id: FileId, work: &Work) -> Result<u64, NativeMountError> {
        let mut inodes = self.inodes.lock().map_err(|_| driver_errno(libc::EIO))?;

        if let Some(inode) = inodes.get(&file_id) {
            return Ok(*inode);
        }
        let inode = (inodes.len() as u64)
            .checked_add(1)
            .ok_or_else(|| driver_errno(libc::EOVERFLOW))?;
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(driver_errno(libc::EOVERFLOW));
        }
        grow_map(&mut inodes, work)?;

        inodes.insert(file_id, inode);
        Ok(inode)
    }

    pub(super) fn lookup(
        &self,
        path: &MountPath,
        work: &Work,
    ) -> Result<MountLookup, NativeMountError> {
        self.source
            .lookup_with_meter(path, work)?
            .ok_or_else(|| driver_errno(libc::ENOENT))
    }

    pub(super) fn may_have_named_attributes(
        &self,
        path: &MountPath,
        work: &Work,
    ) -> Result<bool, NativeMountError> {
        Ok(self.lookup(path, work)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    pub(super) fn admit_write(&self, work: &Work) -> Result<(), NativeMountError> {
        if !self.writable {
            return Err(driver_errno(libc::EROFS));
        }
        if let Some(guard) = &self.callback_guard {
            crate::native_mount::adapter::CallbackRuntime::existing()?
                .verify_native_callback_write_guard(guard.as_ref(), work)?;
        }
        Ok(work.admit(WorkCounters::UNCHARGED)?)
    }

    pub(super) fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), NativeMountError>,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        self.admit_write(work)?;
        let mut metadata = self.lookup_handle(path, handle, work)?.metadata;
        mutate(&mut metadata)?;
        if handle == 0 {
            self.mutate(
                || self.source.set_attributes(path, metadata, None, work),
                work,
            )
        } else {
            self.mutate_file(
                path,
                handle,
                |file| file.set_attributes(metadata, None, work),
                work,
            )
        }
    }

    pub(super) fn flush_on_close(&self, handle: u64, work: &Work) -> Result<(), NativeMountError> {
        if handle == 0 {
            return Ok(());
        }
        let written = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;

            files
                .get(&handle)
                .ok_or_else(|| driver_errno(libc::ESTALE))?
                .written
                .load(Ordering::Acquire)
        };
        if written == 0 || !self.source.flush_on_handle_close() {
            return Ok(());
        }
        self.admit_write(work)?;
        self.ledger
            .flush_through(written, self.source.as_ref(), work)?;
        self.admit_write(work)
    }

    pub(super) fn sync(&self, work: &Work) -> Result<(), NativeMountError> {
        self.admit_write(work)?;
        self.ledger
            .flush_through(self.ledger.latest(), self.source.as_ref(), work)?;
        self.admit_write(work)
    }

    pub(super) fn open(
        &self,
        path: &MountPath,
        written: u64,
        work: &Work,
    ) -> Result<u64, NativeMountError> {
        let lookup = self.lookup(path, work)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(driver_errno(libc::EISDIR)),
            _ => return Err(driver_errno(libc::EINVAL)),
        }
        let handle = self.allocate_handle().map_err(driver_errno)?;
        let entry = FileHandle {
            file_id: lookup.node.file_id,
            file: None,
            written: shared(work, DarwinOwned::empty(AtomicU64::new(written)))?,
        };
        let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
        grow_map(&mut files, work)?;
        files.insert(handle, entry);
        Ok(handle)
    }

    pub(super) fn file_target(
        &self,
        path: &MountPath,
        handle: u64,
        work: &Work,
    ) -> Result<FileTarget, NativeMountError> {
        if handle == 0 {
            return Ok(FileTarget {
                file: retained_file(work, self.source.open_file_with_meter(path, work)?)?,
                written: None,
            });
        }
        let file_id = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            let entry = files
                .get(&handle)
                .ok_or_else(|| driver_errno(libc::ESTALE))?;
            if let Some(file) = &entry.file {
                return Ok(FileTarget {
                    file: file.clone(),
                    written: Some(Arc::clone(&entry.written)),
                });
            }
            entry.file_id
        };
        let (file, lookup) = self.source.open_file_with_lookup_with_meter(path, work)?;
        let file = retained_file(work, file)?;
        if lookup.node.file_id != file_id {
            return Err(driver_errno(libc::ESTALE));
        }
        let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
        // Recheck after source I/O: close or unlink may have changed the entry.
        let entry = files
            .get_mut(&handle)
            .ok_or_else(|| driver_errno(libc::ESTALE))?;
        Ok(FileTarget {
            file: entry.file.get_or_insert(file).clone(),
            written: Some(Arc::clone(&entry.written)),
        })
    }

    pub(super) fn lookup_handle(
        &self,
        path: &MountPath,
        handle: u64,
        work: &Work,
    ) -> Result<MountLookup, NativeMountError> {
        if handle == 0 {
            return self.lookup(path, work);
        }
        let (file_id, file) = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            let entry = files
                .get(&handle)
                .ok_or_else(|| driver_errno(libc::ESTALE))?;
            (entry.file_id, entry.file.clone())
        };
        let lookup = match file {
            Some((file, _custody)) => file.lookup_with_meter(work)?,
            None => self.lookup(path, work)?,
        };
        if lookup.node.file_id != file_id {
            return Err(driver_errno(libc::ESTALE));
        }
        Ok(lookup)
    }

    pub(super) fn unlink(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        unlink: impl FnOnce() -> Result<(), MountSourceError>,
        work: &Work,
    ) -> Result<(), NativeMountError> {
        let has_open = lookup.node.kind == MountNodeKind::Regular
            && lookup.node.link_count == 1
            && self
                .files
                .read()
                .map_err(|_| driver_errno(libc::EIO))?
                .values()
                .any(|entry| entry.file_id == lookup.node.file_id);
        let detached = self.mutate(
            || {
                let detached = if has_open {
                    Some(self.source.detach_file_with_meter(path, work)?)
                } else {
                    None
                };
                unlink()?;
                Ok(detached)
            },
            work,
        )?;
        if let Some(detached) = detached {
            let detached = retained_file(work, detached)?;
            let mut files = self.files.write().map_err(|_| driver_errno(libc::EIO))?;
            for entry in files
                .values_mut()
                .filter(|entry| entry.file_id == lookup.node.file_id)
            {
                entry.file = Some(detached.clone());
            }
        }
        Ok(())
    }

    pub(super) fn mutate<T>(
        &self,
        mutation: impl FnOnce() -> Result<T, MountSourceError>,
        work: &Work,
    ) -> Result<T, NativeMountError> {
        self.admit_write(work)?;
        let result = mutation().map_err(NativeMountError::from);
        self.ledger.record();
        result.and_then(|value| {
            self.admit_write(work)?;
            Ok(value)
        })
    }

    pub(super) fn mutate_file<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
        work: &Work,
    ) -> Result<T, NativeMountError> {
        self.admit_write(work)?;
        let target = self.file_target(path, handle, work)?;
        let result = mutation(target.file.0.as_ref()).map_err(NativeMountError::from);
        let sequence = self.ledger.record();
        if let Some(written) = target.written {
            written.fetch_max(sequence, Ordering::AcqRel);
        }
        result.and_then(|value| {
            self.admit_write(work)?;
            Ok(value)
        })
    }
}
