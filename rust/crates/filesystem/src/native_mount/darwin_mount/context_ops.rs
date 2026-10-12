use super::*;

impl DarwinMountContext {
    pub(in crate::native_mount) fn new(
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        metadata: &Metadata,
        root_file_id: FileId,
    ) -> Self {
        Self {
            source,
            writable,
            mount_uid: metadata.uid(),
            mount_gid: metadata.gid(),
            next_handle: AtomicU64::new(1),
            next_inode: AtomicU64::new(ROOT_INODE + 1),
            inodes: Mutex::new(HashMap::from([(root_file_id, ROOT_INODE)])),
            forgotten: AtomicU64::new(0),
            lookups: Mutex::new(LookupCache {
                entries: HashMap::new(),
            }),
            files: RwLock::new(HashMap::new()),
            directories: Mutex::new(HashMap::new()),
            directory_checkpoints: Mutex::new(HashMap::new()),
            namespace_revision: AtomicU64::new(0),
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
        }
    }

    /// Shares this context, told of every change to its source's view.
    pub(in crate::native_mount) fn observing_source(self) -> Arc<Self> {
        let context = Arc::new(self);
        let observer: Weak<dyn ViewObserver> = Arc::<Self>::downgrade(&context);
        context.source.observe_view(observer);
        context
    }

    pub(in crate::native_mount) fn allocate_handle(&self) -> Result<u64, i32> {
        let handle = self.next_handle.fetch_add(1, Ordering::Relaxed);
        if handle == 0 || handle == u64::MAX {
            return Err(libc::EMFILE);
        }
        Ok(handle)
    }

    pub(in crate::native_mount) fn namespace_changed(&self) {
        self.namespace_revision.fetch_add(1, Ordering::Release);
    }

    /// Samples the view before observing facts a later callback may reuse;
    /// `None` when the source cannot invalidate exactly.
    pub(in crate::native_mount) fn cache_stamp(&self) -> Option<CacheStamp> {
        let forgotten = self.forgotten.load(Ordering::Acquire);
        Some(CacheStamp {
            forgotten,
            view: self.source.view_stamp()?,
        })
    }

    /// Whether facts about `path`, which resolved to `file_id` (`None`: to
    /// nothing), observed after `stamp` still describe the view.
    pub(in crate::native_mount) fn unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
    ) -> bool {
        stamp.forgotten == self.forgotten.load(Ordering::Acquire)
            && self.source.unchanged_since(path, file_id, stamp.view)
    }

    /// [`Self::unchanged_since`], counting only changes the source reported.
    pub(in crate::native_mount) fn reported_unchanged_since(
        &self,
        path: &MountPath,
        file_id: Option<FileId>,
        stamp: CacheStamp,
    ) -> bool {
        stamp.forgotten == self.forgotten.load(Ordering::Acquire)
            && self
                .source
                .reported_unchanged_since(path, file_id, stamp.view)
    }

    pub(in crate::native_mount) fn cache_epochs(&self) -> Option<CacheEpochs> {
        if !self.source.view_is_stable() {
            return None;
        }
        Some(CacheEpochs {
            view: self.cache_stamp()?,
            binding: self.source.binding_epoch()?,
        })
    }

    /// Stops trusting every fact this mount observed so far, because a
    /// change the source did not record may have superseded any of them:
    /// cached lookups, handle observations, and directory pages all fail
    /// validation, and every object's next change attribute is fresh, so
    /// the client discards what it cached at its next revalidation.
    pub(in crate::native_mount) fn forget_view(&self) {
        self.forgotten.fetch_add(1, Ordering::AcqRel);
        self.namespace_changed();
        self.lookups
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entries
            .clear();
        self.around.made.fetch_add(1, Ordering::AcqRel);
    }

    /// Waits until the NFS client holds nothing that a change made around
    /// the mount before this call superseded. Changes made through the mount
    /// need no wait: the client learns of them from their replies.
    ///
    /// Without delegations, which a local-socket mount cannot take, the
    /// server cannot recall what the client cached. The client trusts
    /// cached attributes, names, and directory pages until the attribute
    /// timeout expires, measured in whole seconds of uptime from when each
    /// reply arrived, and then revalidates them against change attributes,
    /// which every such change moved (see [`ChangeLabels`]). Every callback
    /// that began before this call is waited out; the replies they sent
    /// are assumed to arrive within [`REPLY_DELIVERY_SLACK`], so all expire
    /// by the uptime second one timeout after that. The wait is at most the
    /// timeout plus the slack and averages half a second less.
    ///
    /// A fence that could not confirm every change was reported (macOS)
    /// needs no wait when nothing the client holds changed: each object it
    /// holds a label for is read again, and the wait follows only if one no
    /// longer matches what its label was issued for.
    pub(in crate::native_mount) fn revalidate(&self) {
        self.revalidate_with_wait();
    }

    /// Revalidates the mount and reports whether a client-cache expiry wait
    /// was required. The result is kept private to the driver; tests use it
    /// to verify the selective unconfirmed-fence policy without making
    /// assertions about scheduler timing.
    pub(in crate::native_mount) fn revalidate_with_wait(&self) -> bool {
        let unconfirmed = self.around.unconfirmed.load(Ordering::Acquire);
        if self.around.verified.load(Ordering::Acquire) < unconfirmed && !self.verify_labels() {
            self.around.made.fetch_add(1, Ordering::AcqRel);
        }
        self.around
            .verified
            .fetch_max(unconfirmed, Ordering::AcqRel);
        let made = self.around.made.load(Ordering::Acquire);
        if self.around.settled.load(Ordering::Acquire) >= made {
            return false;
        }
        self.callbacks.drain();
        let arrived_by = uptime() + REPLY_DELIVERY_SLACK;
        // SAFETY: the bridge reports a constant.
        let timeout = u64::from(unsafe { acyclic_fs_darwin_mount_attribute_timeout() });
        let expired = Duration::from_secs(arrived_by.as_secs() + timeout);
        loop {
            let now = uptime();
            if now >= expired {
                break;
            }
            std::thread::sleep(expired - now);
        }
        self.around.settled.fetch_max(made, Ordering::AcqRel);
        true
    }

    /// Whether every object the client holds a label for still shows what
    /// its label was issued for, read again from the source. False when any
    /// differs, or when what the client holds is unknown.
    pub(in crate::native_mount) fn verify_labels(&self) -> bool {
        let cleared = self.changes.cleared.load(Ordering::Acquire);
        let Some(issued) = self.changes.issued() else {
            return false;
        };
        issued.iter().all(|(file_id, observed)| {
            self.lookup(&observed.path)
                .is_ok_and(|current| current.node.file_id == *file_id && observed.matches(&current))
        }) && self.changes.cleared.load(Ordering::Acquire) == cleared
    }

    pub(in crate::native_mount) fn inode(&self, file_id: FileId) -> Result<u64, i32> {
        let mut inodes = self.inodes.lock().map_err(|_| libc::EIO)?;
        if let Some(inode) = inodes.get(&file_id) {
            return Ok(*inode);
        }
        let inode = self.next_inode.fetch_add(1, Ordering::Relaxed);
        if inode <= ROOT_INODE || inode == u64::MAX {
            return Err(libc::EOVERFLOW);
        }
        inodes.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        inodes.insert(file_id, inode);
        Ok(inode)
    }

    pub(in crate::native_mount) fn lookup(&self, path: &MountPath) -> Result<MountLookup, i32> {
        // Sampled first: a change to anything the lookup reads records a
        // later position, so its result can never validate over the change.
        let stamp = self.cache_stamp();
        if stamp.is_some() {
            let cache = self.lookups.lock().map_err(|_| libc::EIO)?;
            if let Some((cached, cached_stamp)) = cache.entries.get(path).copied()
                && self.unchanged_since(
                    path,
                    cached.map(|lookup| lookup.node.file_id),
                    cached_stamp,
                )
            {
                return cached.ok_or(libc::ENOENT);
            }
        }
        let lookup = self.source.lookup(path).map_err(errno)?;
        if let Some(stamp) = stamp {
            self.remember_lookup(path, lookup, stamp)?;
        }
        lookup.ok_or(libc::ENOENT)
    }

    pub(in crate::native_mount) fn remember_current_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
    ) -> Result<(), i32> {
        let Some(stamp) = self.cache_stamp() else {
            return Ok(());
        };
        self.remember_lookup(path, lookup, stamp)?;
        Ok(())
    }

    pub(in crate::native_mount) fn remember_lookup(
        &self,
        path: &MountPath,
        lookup: Option<MountLookup>,
        stamp: CacheStamp,
    ) -> Result<(), i32> {
        let mut lookups = self.lookups.lock().map_err(|_| libc::EIO)?;
        if lookups.entries.len() >= MAXIMUM_LOOKUP_CACHE_ENTRIES {
            lookups.entries.clear();
        }
        lookups.entries.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        lookups.entries.insert(path.clone(), (lookup, stamp));
        Ok(())
    }

    /// Opens a handle on the regular file at `path`. The source file is
    /// bound on first use (see [`Self::bind`]).
    pub(in crate::native_mount) fn open(&self, path: &MountPath, state: InitialHandleState) -> Result<u64, i32> {
        let written = match state {
            InitialHandleState::Clean => 0,
            InitialHandleState::Written => self.ledger.latest(),
        };
        // Sampled first, so a change after it cannot validate the lookup.
        let stamp = self.cache_stamp();
        let lookup = self.lookup(path)?;
        match lookup.node.kind {
            MountNodeKind::Regular => {}
            MountNodeKind::Directory => return Err(libc::EISDIR),
            _ => return Err(libc::EINVAL),
        }
        let handle = self.allocate_handle()?;
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        files.try_reserve(1).map_err(|_| libc::ENOMEM)?;
        files.insert(
            handle,
            FileHandle {
                file_id: lookup.node.file_id,
                file: None,
                observation: Arc::new(Mutex::new(FileObservation { stamp, lookup })),
                written: Arc::new(AtomicU64::new(written)),
            },
        );
        Ok(handle)
    }

    /// The source file behind `handle`, opened at `path` on first use.
    ///
    /// A handle names one file identity. Until a callback needs its source
    /// file, `path` (the kernel's current name for the handle's object) still
    /// names that identity unless something outside this mount removed or
    /// replaced it, which fails the handle as a stale NFS handle; a removal
    /// or replacement through this mount detaches its file first (see [`Self::unlink`]).
    pub(in crate::native_mount) fn bind(&self, path: &MountPath, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        let file_id = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            let entry = files.get(&handle).ok_or(libc::ESTALE)?;
            if let Some(file) = &entry.file {
                return Ok(Arc::clone(file));
            }
            entry.file_id
        };
        let file = self.source.open_file(path).map_err(errno)?;
        if file.lookup().map_err(errno)?.node.file_id != file_id {
            return Err(libc::ESTALE);
        }
        let mut files = self.files.write().map_err(|_| libc::EIO)?;
        let entry = files.get_mut(&handle).ok_or(libc::ESTALE)?;
        Ok(Arc::clone(entry.file.get_or_insert(file)))
    }

    /// Runs `unlink`, which removes `path`, the name `lookup` resolved.
    /// When that is the last name of a regular file with open handles, the
    /// file is detached first and, once the name is gone, those handles use
    /// it, so an unlinked open file stays readable and writable.
    pub(in crate::native_mount) fn unlink(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        unlink: impl FnOnce() -> Result<(), MountSourceError>,
    ) -> Result<(), i32> {
        let file_id = lookup.node.file_id;
        let has_open = lookup.node.kind == MountNodeKind::Regular
            && lookup.node.link_count == 1
            && self
                .files
                .read()
                .map_err(|_| libc::EIO)?
                .values()
                .any(|entry| entry.file_id == file_id);
        let detached = self.mutate(|| {
            let detached = has_open
                .then(|| self.source.detach_file(path))
                .transpose()?;
            unlink()?;
            Ok(detached)
        })?;
        if let Some(detached) = detached {
            for entry in self
                .files
                .write()
                .map_err(|_| libc::EIO)?
                .values_mut()
                .filter(|entry| entry.file_id == file_id)
            {
                entry.file = Some(Arc::clone(&detached));
            }
        }
        Ok(())
    }

    /// The open handle's file and written marker, or a path-bound file
    /// without a marker when `handle` is zero.
    pub(in crate::native_mount) fn file_target(&self, path: &MountPath, handle: u64) -> Result<FileTarget, i32> {
        if handle == 0 {
            let file = self.source.open_file(path).map_err(errno)?;
            return Ok(FileTarget {
                file,
                written: None,
            });
        }
        let file = self.bind(path, handle)?;
        let files = self.files.read().map_err(|_| libc::EIO)?;
        let entry = files.get(&handle).ok_or(libc::ESTALE)?;
        Ok(FileTarget {
            file,
            written: Some(Arc::clone(&entry.written)),
        })
    }

    pub(in crate::native_mount) fn file_or_open(&self, path: &MountPath, handle: u64) -> Result<Arc<dyn MountOpenFile>, i32> {
        self.file_target(path, handle).map(|target| target.file)
    }

    /// Runs one mutating callback and records its completion before the
    /// reply, including a failed attempt that may have applied partially, so
    /// a later sync covers it and the change attribute advances.
    pub(in crate::native_mount) fn mutate<T>(&self, mutation: impl FnOnce() -> Result<T, MountSourceError>) -> Result<T, i32> {
        self.admit_write()?;
        let result = mutation().map_err(errno);
        self.ledger.record();
        result
    }

    /// Mutates one regular file through `handle` (or a path-bound file when
    /// it is zero) and marks that handle written, so its close publishes
    /// when the source publishes on close.
    pub(in crate::native_mount) fn mutate_file<T>(
        &self,
        path: &MountPath,
        handle: u64,
        mutation: impl FnOnce(&dyn MountOpenFile) -> Result<T, MountSourceError>,
    ) -> Result<T, i32> {
        self.admit_write()?;
        let target = self.file_target(path, handle)?;
        let result = mutation(target.file.as_ref()).map_err(errno);
        let sequence = self.ledger.record();
        if let Some(written) = target.written {
            written.fetch_max(sequence, Ordering::AcqRel);
        }
        result
    }

    /// The attributes of `handle`'s file, or of the file at `path` when
    /// `handle` is zero.
    pub(in crate::native_mount) fn lookup_handle(&self, path: &MountPath, handle: u64) -> Result<MountLookup, i32> {
        if handle == 0 {
            return self.lookup(path);
        }
        let observation = {
            let files = self.files.read().map_err(|_| libc::EIO)?;
            Arc::clone(&files.get(&handle).ok_or(libc::ESTALE)?.observation)
        };
        {
            let observation = observation.lock().map_err(|_| libc::EIO)?;
            if observation.stamp.is_some_and(|stamp| {
                self.unchanged_since(path, Some(observation.lookup.node.file_id), stamp)
            }) {
                return Ok(observation.lookup);
            }
        }
        let stamp = self.cache_stamp();
        let lookup = self.bind(path, handle)?.lookup().map_err(errno)?;
        if stamp.is_some() {
            let mut observation = observation.lock().map_err(|_| libc::EIO)?;
            observation.stamp = stamp;
            observation.lookup = lookup;
        }
        Ok(lookup)
    }

    pub(in crate::native_mount) fn attributes(&self, path: &MountPath, handle: u64) -> Result<NativeStat, i32> {
        // Sampled before observing: a change after it cannot keep the label.
        let stamp = self.cache_stamp();
        let lookup = self.lookup_handle(path, handle)?;
        self.attributes_from_lookup(path, lookup, stamp)
    }

    /// Native attributes of `lookup`, observed at `path` after `stamp`.
    pub(in crate::native_mount) fn attributes_from_lookup(
        &self,
        path: &MountPath,
        lookup: MountLookup,
        stamp: Option<CacheStamp>,
    ) -> Result<NativeStat, i32> {
        let node = lookup.node;
        let file_kind = match node.kind {
            MountNodeKind::Regular => mode::IFREG,
            MountNodeKind::Directory => mode::IFDIR,
            MountNodeKind::SymbolicLink => mode::IFLNK,
            MountNodeKind::Fifo => mode::IFIFO,
            MountNodeKind::Socket => mode::IFSOCK,
            MountNodeKind::CharacterDevice => mode::IFCHR,
            MountNodeKind::BlockDevice => mode::IFBLK,
            MountNodeKind::Unsupported => return Err(libc::EOPNOTSUPP),
        };
        let default_mode = if self.writable { 0o755 } else { 0o555 };
        let mode = metadata_or(lookup.metadata.posix_mode, default_mode) & 0o7777;
        let (accessed_seconds, accessed_nanoseconds) = metadata_time(lookup.metadata.accessed_ns);
        let (modified_seconds, modified_nanoseconds) = metadata_time(lookup.metadata.modified_ns);
        let (changed_seconds, changed_nanoseconds) = metadata_time(lookup.metadata.changed_ns);
        let (created_seconds, created_nanoseconds) = metadata_time(lookup.metadata.created_ns);
        Ok(NativeStat {
            inode: self.inode(node.file_id)?,
            logical_bytes: node.logical_bytes,
            blocks: node.logical_bytes.div_ceil(512),
            accessed_seconds,
            accessed_nanoseconds,
            modified_seconds,
            modified_nanoseconds,
            changed_seconds,
            changed_nanoseconds,
            created_seconds,
            created_nanoseconds,
            mode: file_kind | mode,
            link_count: u32::try_from(node.link_count).unwrap_or(u32::MAX),
            uid: metadata_or(lookup.metadata.posix_uid, self.mount_uid),
            gid: metadata_or(lookup.metadata.posix_gid, self.mount_gid),
            device: node
                .device
                .map(|(major, minor)| {
                    super::device::join_device(major, minor)
                        .map(|device| u64::from(u32::from_ne_bytes(device.to_ne_bytes())))
                })
                .transpose()?
                .unwrap_or(0),
            block_size: 4096,
            flags: u32::try_from(metadata_or(lookup.metadata.posix_flags, 0)).unwrap_or(u32::MAX),
            change: self.changes.label(
                node.file_id,
                stamp,
                self.source.binding_epoch(),
                Observed {
                    path: path.clone(),
                    lookup,
                },
                |stamp| self.unchanged_since(path, Some(node.file_id), stamp),
                |stamp| self.reported_unchanged_since(path, Some(node.file_id), stamp),
            ),
        })
    }

    /// Whether the node at `path` may carry named attributes. A node's
    /// metadata records its named attributes; without that record it has
    /// none, which every source reads the same way, so the probes the macOS
    /// client makes on each new file (provenance, Finder info) need no
    /// source call.
    pub(in crate::native_mount) fn may_have_named_attributes(&self, path: &MountPath) -> Result<bool, i32> {
        Ok(self.lookup(path)?.metadata.named_attributes != MetadataField::Unavailable)
    }

    pub(in crate::native_mount) fn admit_write(&self) -> Result<(), i32> {
        self.writable.then_some(()).ok_or(libc::EROFS)
    }

    pub(in crate::native_mount) fn mutate_metadata(
        &self,
        path: &MountPath,
        handle: u64,
        mutate: impl FnOnce(&mut FileMetadata) -> Result<(), i32>,
    ) -> Result<(), i32> {
        let current = self.lookup_handle(path, handle)?;
        let mut metadata = current.metadata;
        mutate(&mut metadata)?;
        // Metadata of any node kind: only a handle names a regular file.
        if handle == 0 {
            self.mutate(|| self.source.set_attributes(path, metadata, None))
        } else {
            self.mutate_file(path, handle, |file| file.set_attributes(metadata, None))
        }
    }

    /// Publishes this handle's mutations when the source publishes on close.
    pub(in crate::native_mount) fn flush_on_close(&self, handle: u64) -> Result<(), i32> {
        if handle == 0 {
            // Named-attribute opens carry no handle; their writes are
            // recorded mutations that the next sync covers.
            return Ok(());
        }
        let written = self
            .files
            .read()
            .map_err(|_| libc::EIO)?
            .get(&handle)
            .ok_or(libc::ESTALE)?
            .written
            .load(Ordering::Acquire);
        if written == 0 || !self.source.flush_on_handle_close() {
            return Ok(());
        }
        self.ledger.flush_through(written, self.source.as_ref())
    }

    /// Publishes every mutation this mount has acknowledged.
    pub(in crate::native_mount) fn sync(&self) -> Result<(), i32> {
        self.ledger
            .flush_through(self.ledger.latest(), self.source.as_ref())
    }
}

