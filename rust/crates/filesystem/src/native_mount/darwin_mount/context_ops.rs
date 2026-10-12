use super::*;

impl DarwinMountContext {
    pub(super) fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        uid: u32,
        gid: u32,
        root_file_id: FileId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Self, NativeMountError> {
        let source_original_work = original_work(source.as_ref(), Some(work))?;
        let callback_guard = source.native_callback_guard();
        verify_guard(callback_guard.as_ref(), source_original_work.as_ref())?;
        let mut inodes = DarwinOwned::empty(HashMap::new());
        grow_map(&mut inodes, work)?;
        copying(
            work,
            copy_work(std::mem::size_of::<(FileId, u64)>() as u64),
            || inodes.insert(root_file_id, ROOT_INODE),
        )?;
        let (handler_budget, handler_cancellation) = {
            let meter = (&work.state).lock();
            (meter.budget, meter.cancellation().clone())
        };
        Ok(Self {
            source,
            writable,
            mount_uid: uid,
            mount_gid: gid,
            handler_failure: shared(work, Mutex::new(DarwinCallbackState::default()))?,
            handler_budget,
            handler_cancellation,
            startup_meter: Mutex::new(crate::native_mount::NativeMountMeterActivation::default()),
            next_handle: AtomicU64::new(1),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            inodes: Mutex::new(inodes),
            forgotten: AtomicU64::new(0),
            files: RwLock::new(DarwinOwned::empty(HashMap::new())),
            directories: Mutex::new(DarwinOwned::empty(HashMap::new())),
            directory_checkpoints: Mutex::new(DarwinOwned::empty(HashMap::new())),
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
                crate::WorkCounters::UNCHARGED,
            )?,
            allocation_custody: None,
        })
    }

    pub(in crate::native_mount) fn allocate_handle(&self) -> Result<u64, i32> {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        if handle == 0 || handle == u64::MAX {
            return Err(libc::EMFILE);
        }
        Ok(handle)
    }

    pub(super) fn cache_stamp(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<Option<CacheStamp>, NativeMountError> {
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
        work: &crate::NativeMaterializationWork,
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
        work: &crate::NativeMaterializationWork,
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
        work: &crate::NativeMaterializationWork,
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

    pub(super) fn revalidate(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        work.admit(crate::WorkCounters::UNCHARGED)?;
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

    pub(super) fn verify_labels(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
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

    pub(super) fn inode(
        &self,
        file_id: FileId,
        work: &crate::NativeMaterializationWork,
    ) -> Result<u64, NativeMountError> {
        let mut inodes = self.inodes.lock().map_err(|_| driver_errno(libc::EIO))?;
        work.charge(scan_work(inodes.capacity() as u64))?;
        if let Some(inode) = inodes.get(&file_id) {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(driver_errno(libc::EOVERFLOW));
        }
        grow_map(&mut inodes, work)?;
        copying(
            work,
            copy_work(std::mem::size_of::<(FileId, u64)>() as u64),
            || inodes.insert(file_id, inode),
        )?;
        Ok(inode)
    }

    pub(super) fn lookup(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<MountLookup, NativeMountError> {
        self.source
            .lookup_with_meter(path, work)?
            .ok_or_else(|| driver_errno(libc::ENOENT))
    }

    pub(super) fn may_have_named_attributes(
        &self,
        path: &MountPath,
        work: &crate::NativeMaterializationWork,
    ) -> Result<bool, NativeMountError> {
        Ok(self.lookup(path, work)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    pub(super) fn admit_write(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if !self.writable {
            return Err(driver_errno(libc::EROFS));
        }
        if let Some(guard) = &self.callback_guard {
            crate::native_mount::adapter::CallbackRuntime::existing()?
                .verify_native_callback_write_guard(guard.as_ref(), work)?;
        }
        Ok(work.admit(crate::WorkCounters::UNCHARGED)?)
    }

    pub(super) fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), NativeMountError>,
        work: &crate::NativeMaterializationWork,
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

    pub(super) fn flush_on_close(
        &self,
        handle: u64,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        if handle == 0 {
            return Ok(());
        }
        let written = {
            let files = self.files.read().map_err(|_| driver_errno(libc::EIO))?;
            work.charge(scan_work(files.capacity() as u64))?;
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

    pub(super) fn sync(
        &self,
        work: &crate::NativeMaterializationWork,
    ) -> Result<(), NativeMountError> {
        self.admit_write(work)?;
        self.ledger
            .flush_through(self.ledger.latest(), self.source.as_ref(), work)?;
        self.admit_write(work)
    }
}
