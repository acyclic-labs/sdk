//! Names and retained-handle routing for the one Darwin provider instance.
//! Routing takes the shared core's lease before choosing a source or meter.
use super::*;
use crate::native_mount::shared::view_table::ViewLease;
use crate::native_mount::shared::views::{NamedView, NamedViews};

pub(super) struct SharedViews {
    names: Arc<NamedViews<Vec<u8>, Arc<DarwinMountContext>>>,
    attributes: NativeStat,
    changed: AtomicU64,
    handler_failure: Arc<DarwinOwned<Mutex<DarwinCallbackState>>>,
    handles: RwLock<DarwinOwned<HashMap<u64, u64>>>,
    work: Work,
    _allocation: NativeAllocation,
}

pub(super) struct ViewRouting {
    views: std::sync::Weak<SharedViews>,
    serial: u64,
}

pub(super) struct SharedView {
    entry: NamedView<Vec<u8>, Arc<DarwinMountContext>>,
    pub(super) context: Arc<DarwinMountContext>,
    views: Arc<SharedViews>,
}

pub(super) struct Routed<'a> {
    pub(super) context: ViewLease<Arc<DarwinMountContext>>,
    pub(super) path: Option<&'a CStr>,
    serial: u64,
}

impl SharedViews {
    pub(super) fn new(destination: &Path, work: &Work) -> Result<Arc<Self>, NativeMountError> {
        let metadata =
            crate::native_host::unix_name(destination.as_os_str(), &mut &*work, |path, work| {
                start_stat(path, work)
            })?;
        let attributes = NativeStat {
            inode: ROOT_INODE,
            logical_bytes: 0,
            blocks: 0,
            accessed_seconds: 0,
            accessed_nanoseconds: 0,
            modified_seconds: 0,
            modified_nanoseconds: 0,
            changed_seconds: 0,
            changed_nanoseconds: 0,
            created_seconds: 0,
            created_nanoseconds: 0,
            mode: u32::from(libc::S_IFDIR) | 0o555,
            link_count: 2,
            uid: metadata.st_uid,
            gid: metadata.st_gid,
            device: 0,
            block_size: 4096,
            flags: 0,
            change: 1,
        };
        Ok(Arc::new(Self {
            attributes,
            changed: AtomicU64::new(1),
            handler_failure: shared(
                work,
                DarwinOwned::empty(Mutex::new(DarwinCallbackState::default())),
            )?,
            names: Arc::new(NamedViews::new(i64::MAX as u64 - 2)),
            handles: RwLock::new(DarwinOwned::empty(HashMap::new())),
            work: work.clone(),
            _allocation: work.claim(
                acyclic_native_runtime::arc_allocation_bytes::<Self>() as u64,
                1,
                WorkCounters::UNCHARGED,
            )?,
        }))
    }

    /// The caller transfers a fresh, claimed issuance; names are never reused.
    pub(super) fn insert(
        self: &Arc<Self>,
        name: Vec<u8>,
        source: Arc<dyn MountFilesystem>,
        writable: bool,
        destination: &Path,
        work: &Work,
    ) -> Result<SharedView, NativeMountError> {
        let mut context = None;
        let entry = self.names.insert(name, |serial| {
            let mut view = DarwinMountContext::new(source, writable, destination, work)?;
            view.routing = Some(ViewRouting {
                views: Arc::downgrade(self),
                serial,
            });
            let view = Arc::new(view);
            let observer: Arc<dyn ViewObserver> = view.clone();
            view.source
                .observe_view_with_meter(Arc::downgrade(&observer), work)?;
            context = Some(Arc::clone(&view));
            Ok(view)
        })?;
        self.changed.fetch_add(1, Ordering::Release);
        Ok(SharedView {
            entry,
            context: context.ok_or(NativeMountError::ControlPending)?,
            views: Arc::clone(self),
        })
    }

    /// Resolve a name or an original open handle in constant table work.
    /// A handle never falls back to a pathname, even after unlink or retirement.
    pub(super) fn route<'a>(&self, path: Option<&'a CStr>, handle: u64) -> Result<Routed<'a>, i32> {
        let owner = if handle == 0 {
            None
        } else {
            Some(
                *self
                    .handles
                    .read()
                    .unwrap_or_else(PoisonError::into_inner)
                    .get(&handle)
                    .ok_or(libc::ESTALE)?,
            )
        };
        let (serial, context, path) = match path {
            Some(path) => {
                let (name, relative) = relative_path(path)?;
                let (serial, context) = self.names.enter_name(name).ok_or(libc::ESTALE)?;
                if owner.is_some_and(|owner| owner != serial) {
                    return Err(libc::EXDEV);
                }
                (serial, context, Some(relative))
            }
            None => {
                let serial = owner.ok_or(libc::ESTALE)?;
                let context = self.names.enter(serial).ok_or(libc::ESTALE)?;
                (serial, context, None)
            }
        };
        Ok(Routed {
            context,
            path,
            serial,
        })
    }

    /// Keep one original lease for both sides of a namespace mutation.
    pub(super) fn route_pair<'a>(
        &self,
        source: &'a CStr,
        destination: &'a CStr,
    ) -> Result<(Routed<'a>, &'a CStr), i32> {
        let source = self.route(Some(source), 0)?;
        let destination = self.route(Some(destination), 0)?;
        if source.serial != destination.serial {
            return Err(libc::EXDEV);
        }
        let destination = destination.path.ok_or(libc::ESTALE)?;
        if source.path.is_some_and(|path| path.to_bytes() == b"/") || destination.to_bytes() == b"/"
        {
            return Err(libc::EBUSY);
        }
        Ok((source, destination))
    }

    pub(super) fn listed_from(&self, first: u64, limit: usize) -> Vec<(u64, Vec<u8>)> {
        self.names.listed_from(first, limit)
    }
}

impl ViewRouting {
    pub(super) fn opened(&self, handle: u64) -> Result<(), NativeMountError> {
        let views = self
            .views
            .upgrade()
            .ok_or_else(|| driver_errno(libc::ESTALE))?;
        let mut handles = views
            .handles
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        grow_map(&mut handles, &views.work)?;
        handles.insert(handle, self.serial);
        Ok(())
    }

    pub(super) fn closed(&self, handle: u64) {
        if let Some(views) = self.views.upgrade() {
            views
                .handles
                .write()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&handle);
        }
    }
}

impl SharedView {
    /// Unlist before draining callbacks, then forget only this view's handles.
    pub(super) fn retire(&mut self) {
        if let Some((_, context)) = self.entry.retire_after(|| {}) {
            self.views.changed.fetch_add(1, Ordering::Release);
            let files = context.files.read().unwrap_or_else(PoisonError::into_inner);
            let mut handles = self
                .views
                .handles
                .write()
                .unwrap_or_else(PoisonError::into_inner);
            for handle in files.keys() {
                handles.remove(handle);
            }
        }
    }
}

impl Drop for SharedView {
    fn drop(&mut self) {
        self.retire();
    }
}

fn relative_path(path: &CStr) -> Result<(&[u8], &CStr), i32> {
    let bytes = path.to_bytes().strip_prefix(b"/").ok_or(libc::EINVAL)?;
    let end = bytes
        .iter()
        .position(|byte| *byte == b'/')
        .unwrap_or(bytes.len());
    let name = &bytes[..end];
    if name.is_empty() {
        return Err(libc::ESTALE);
    }
    let relative = if end == bytes.len() {
        c"/"
    } else {
        // SAFETY: end names a slash inside the original terminated CStr. The
        // suffix borrows that same allocation and contains its original NUL.
        unsafe { CStr::from_ptr(path.as_ptr().add(end + 1)) }
    };
    Ok((name, relative))
}
impl SharedViews {
    pub(super) fn attributes(&self) -> NativeStat {
        NativeStat {
            change: self.changed.load(Ordering::Acquire),
            ..self.attributes
        }
    }

    pub(super) fn record_failure(&self, cause: NativeMountError, work: WorkCounters) -> i32 {
        let status = callback_errno(&cause);
        if fatal(&cause) {
            let mut state = self
                .handler_failure
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            if state.first_fatal.is_none() {
                state.first_fatal = Some((Some(cause), work));
            }
        }
        status
    }

    pub(super) fn run<T>(
        &self,
        operation: impl FnOnce(&Work) -> Result<T, NativeMountError>,
    ) -> Result<T, i32> {
        self.work
            .admit(WorkCounters::UNCHARGED)
            .map_err(|cause| self.record_failure(cause.into(), self.work.receipt()))?;
        if let Some((cause, _)) = &self
            .handler_failure
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .first_fatal
        {
            return Err(cause.as_ref().map_or(libc::EIO, callback_errno));
        }
        operation(&self.work).map_err(|cause| self.record_failure(cause, self.work.receipt()))
    }

    pub(super) fn read_directory(
        &self,
        buffer: *mut c_void,
        filler: DirectoryFiller,
        offset: i64,
        work: &Work,
    ) -> Result<c_int, NativeMountError> {
        if offset < 0 {
            return Err(driver_errno(libc::EINVAL));
        }
        for (name, after) in [(c".", 1), (c"..", 2)] {
            if offset < after {
                // SAFETY: the synchronous bridge owns the output and filler.
                if unsafe {
                    acyclic_fs_darwin_mount_fill_directory(
                        buffer,
                        filler,
                        name.as_ptr(),
                        ptr::null(),
                        after,
                    )
                } != 0
                {
                    return Ok(0);
                }
            }
        }
        // Bound each table read, but fill the caller's frame before reporting EOF.
        // Metadata is not fetched from any volume while the root names lock is held.
        let mut first = offset.saturating_sub(1).max(1) as u64;
        loop {
            let entries = self.listed_from(first, 64);
            if entries.is_empty() {
                return Ok(0);
            }
            for (serial, name) in entries {
                work.charge(scan_work(1))?;
                let name = native_name(&name, work)?;
                let after = i64::try_from(serial)
                    .map_err(|_| driver_errno(libc::EOVERFLOW))?
                    .checked_add(2)
                    .ok_or_else(|| driver_errno(libc::EOVERFLOW))?;
                // SAFETY: the bridge consumes the funded terminated name synchronously.
                if unsafe {
                    acyclic_fs_darwin_mount_fill_directory(
                        buffer,
                        filler,
                        name.as_ptr(),
                        ptr::null(),
                        after,
                    )
                } != 0
                {
                    return Ok(0);
                }
                first = serial + 1;
            }
        }
    }
}
