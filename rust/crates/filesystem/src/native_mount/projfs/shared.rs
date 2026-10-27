//! The process's shared ProjFS instances, one virtualization root each.
//!
//! Every view is a directory `root\name` of one instance. ProjFS reports
//! each callback by its path from the virtualization root, so the first
//! component names the view: [`shared_callbacks`] resolves it through the
//! shared core's [`NamedViews`], which admits the callback under a lease on
//! a live view only, and hands the standalone callback the rest of the path
//! with the view's own runtime as its instance context. A retired view's
//! name resolves to nothing, and serials are never reused, so a late
//! callback never reaches another view's runtime. Every name a view passes
//! to ProjFS carries its prefix ([`Runtime::projfs_name`]).
//!
//! - A view's root, and anything beside the views, is never renamed or
//!   linked; a rename or link across views fails as across volumes.
//! - Views write through staging directories beside the root, outside the
//!   virtualization root.
//! - One projector service thread runs every view's projector passes.
//! - Retiring a view unlists it, waits for its admitted callbacks and its
//!   running pass, clears its tombstones and deletes its directory. Its
//!   callbacks find no view from then on, so no tombstone is left behind.
//! - The shared core's [`Instances`] registry starts an instance for a
//!   root's first view, and the last view's retirement stops virtualizing,
//!   joins the service and detaches the root. A process that dies leaves its
//!   root to the shared-root reaper, which detaches and removes it.

use super::super::shared::instances::{Instance, Instances};
use super::super::shared::view_table::ViewLease;
use super::super::shared::views::{NamedView, NamedViews};
use super::*;
use std::path::Path;

const SEPARATOR: u16 = 92;

/// One live view as its callbacks see it.
pub(super) struct ViewCore {
    runtime: Arc<Runtime>,
    /// Paths from the view's root whose deletion or rename may have left a
    /// tombstone, which only ProjFS can remove.
    vacated: Mutex<HashSet<Vec<u16>>>,
}

/// Views keyed by their folded names.
pub(super) type SharedViews = NamedViews<Vec<u16>, ViewCore>;

/// Windows names compare case-insensitively; shared view names are ASCII.
fn fold(name: &[u16]) -> Vec<u16> {
    name.iter()
        .map(|&unit| {
            if (65..=90).contains(&unit) {
                unit + 32
            } else {
                unit
            }
        })
        .collect()
}

/// Admits a callback to the view whose name is `name`.
fn enter(views: &SharedViews, name: &[u16]) -> Option<ViewLease<ViewCore>> {
    views.enter_name(&fold(name)).map(|(_, view)| view)
}

/// A callback name inside the instance, split at its view: the view's name
/// and the rest, which is empty (a pointer to the terminator) for the view's
/// root or an entry beside the views. `None` for an empty or absent name.
unsafe fn split<'a>(name: PCWSTR) -> Option<(&'a [u16], PCWSTR, bool)> {
    if name.is_null() {
        return None;
    }
    // SAFETY: ProjFS passes NUL-terminated names live for the callback.
    let wide: &'a [u16] = unsafe { std::slice::from_raw_parts(name.as_ptr(), name.len()) };
    if wide.is_empty() {
        return None;
    }
    let (view, offset) = match wide.iter().position(|unit| *unit == SEPARATOR) {
        Some(index) => (&wide[..index], index + 1),
        None => (wide, wide.len()),
    };
    // SAFETY: `offset` is within the name or at its terminator.
    let rest = PCWSTR::from_raw(unsafe { name.as_ptr().add(offset) });
    Some((view, rest, offset >= wide.len()))
}

fn route(data: &PRJ_CALLBACK_DATA, runtime: &Arc<Runtime>, path: PCWSTR) -> PRJ_CALLBACK_DATA {
    let mut routed = *data;
    routed.FilePathName = path;
    routed.InstanceContext = Arc::as_ptr(runtime).cast_mut().cast();
    routed
}

unsafe extern "system" fn shared_query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    // SAFETY: the instance context is its views until virtualizing stops.
    let Some((data, views)) = (unsafe { shared_views(callback_data) }) else {
        return HR_UNEXPECTED;
    };
    // SAFETY: as above.
    let Some((name, rest, root_level)) = (unsafe { split(data.FilePathName) }) else {
        return HR_FILE_NOT_FOUND;
    };
    // A view's root, an entry beside the views, and anything of a retired
    // view exist in no source: ProjFS leaves no tombstone for them.
    if root_level {
        return HR_FILE_NOT_FOUND;
    }
    let Some(view) = enter(views, name) else {
        return HR_FILE_NOT_FOUND;
    };
    let routed = route(data, &view.runtime, rest);
    match callbacks().QueryFileNameCallback {
        // SAFETY: the routed data names the leased view's live runtime.
        Some(callback) => unsafe { callback(&raw const routed) },
        None => HR_UNEXPECTED,
    }
}

unsafe extern "system" fn shared_notification(
    callback_data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    // SAFETY: the instance context is its views until virtualizing stops.
    let Some((data, views)) = (unsafe { shared_views(callback_data) }) else {
        return HR_UNEXPECTED;
    };
    // SAFETY: ProjFS passes these names as null or NUL-terminated strings.
    let (source, destination) = unsafe { (split(data.FilePathName), split(destination_filename)) };
    let ignored = || {
        keep_file_notifications(notification, operation_parameters);
        HR_OK
    };
    if notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
    {
        if [source, destination]
            .iter()
            .any(|name| name.is_some_and(|(_, _, root_level)| root_level))
        {
            return HR_ACCESS_DENIED;
        }
        if let (Some((from, ..)), Some((to, ..))) = (source, destination)
            && fold(from) != fold(to)
        {
            return HR_NOT_SAME_DEVICE;
        }
    }
    // A name from outside the instance routes by the other one.
    let Some((name, _, root_level)) = source.or(destination) else {
        return if notification == PRJ_NOTIFICATION_PRE_RENAME
            || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
        {
            HR_NOT_SAME_DEVICE
        } else {
            ignored()
        };
    };
    if root_level {
        return ignored();
    }
    let Some(view) = enter(views, name) else {
        return ignored();
    };
    if let Some((_, rest, _)) = source
        && matches!(
            notification,
            PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED | PRJ_NOTIFICATION_FILE_RENAMED
        )
    {
        // SAFETY: as above.
        let rest = unsafe { rest.as_wide() }.to_vec();
        lock_recover(&view.vacated).insert(rest);
    }
    let routed = route(
        data,
        &view.runtime,
        source.map_or(data.FilePathName, |(_, rest, _)| rest),
    );
    let destination = destination.map_or(destination_filename, |(_, rest, _)| rest);
    match callbacks().NotificationCallback {
        // SAFETY: the routed data names the leased view's live runtime.
        Some(callback) => unsafe {
            callback(
                &raw const routed,
                is_directory,
                notification,
                destination,
                operation_parameters,
            )
        },
        None => HR_UNEXPECTED,
    }
}

unsafe extern "system" fn shared_start_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

unsafe extern "system" fn shared_end_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
) -> HRESULT {
    HR_OK
}

// Every entry is a real file or directory, so enumeration adds nothing: a
// listing of the root shows the views' directories as they are on disk.
unsafe extern "system" fn shared_get_directory(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _enumeration_id: *const GUID,
    _search_expression: PCWSTR,
    _buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE,
) -> HRESULT {
    HR_OK
}

unsafe extern "system" fn shared_placeholder(_callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    HR_FILE_NOT_FOUND
}

unsafe extern "system" fn shared_file_data(
    _callback_data: *const PRJ_CALLBACK_DATA,
    _byte_offset: u64,
    _length: u32,
) -> HRESULT {
    HR_UNEXPECTED
}

unsafe extern "system" fn shared_cancel(_callback_data: *const PRJ_CALLBACK_DATA) {}

fn shared_callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(shared_start_directory),
        EndDirectoryEnumerationCallback: Some(shared_end_directory),
        GetDirectoryEnumerationCallback: Some(shared_get_directory),
        GetPlaceholderInfoCallback: Some(shared_placeholder),
        GetFileDataCallback: Some(shared_file_data),
        QueryFileNameCallback: Some(shared_query_name),
        NotificationCallback: Some(shared_notification),
        CancelCommandCallback: Some(shared_cancel),
    }
}

static INSTANCES: Instances<SharedProjFsInstance> = Instances::new();

#[cfg(test)]
pub(super) fn acquire_instance(
    root: &Path,
) -> Result<Arc<Instance<SharedProjFsInstance>>, NativeMountError> {
    INSTANCES.acquire(root, SharedProjFsInstance::start)
}

pub(super) struct SharedProjFsInstance {
    views: Arc<SharedViews>,
    root: PathBuf,
    /// Beside the root, outside the virtualization root: every view's
    /// staging directory is inside it.
    staging: PathBuf,
    context: Option<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT>,
    service: Option<super::service::ProjectorService>,
}

// SAFETY: the context is an opaque handle valid until the instance stops
// virtualizing in its drop; ProjFS synchronizes calls on it.
unsafe impl Send for SharedProjFsInstance {}
// SAFETY: as above; nothing mutates the instance after start but its drop.
unsafe impl Sync for SharedProjFsInstance {}

impl SharedProjFsInstance {
    fn start(root: &Path) -> Result<Self, NativeMountError> {
        let mut instance = Self {
            views: Arc::new(SharedViews::new(u64::MAX)),
            root: root.to_path_buf(),
            staging: staging_path(root),
            context: None,
            service: None,
        };
        // Every failure below drops `instance`, whose teardown undoes the
        // steps already taken.
        match std::fs::create_dir(&instance.staging) {
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => {
                return Err(error.into());
            }
            _ => {}
        }
        // A previous instance of this process that failed to detach left
        // its tag; nothing else serves this process's root.
        super::cleanup::detach_root_by_path(root)?;
        let work = crate::NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(cause, _)| NativeMountError::from(cause))?;
        let identity = HostRoot::open(root, &mut &work)?.identity();
        // ProjFS keys cached state by instance ID, even past detach: the
        // same directory always gets the same ID.
        let guid = guid_from_key(u128::from_le_bytes(identity.to_bytes()));
        let root_name = HSTRING::from(root.as_os_str());
        // SAFETY: the root name and GUID outlive this synchronous call.
        unsafe { PrjMarkDirectoryAsPlaceholder(&root_name, PCWSTR::null(), None, &raw const guid) }
            .map_err(|error| driver_error(&error))?;
        let mut mapping = PRJ_NOTIFICATION_MAPPING {
            NotificationBitMask: FILE_NOTIFICATIONS,
            NotificationRoot: PCWSTR::from_raw(NOTIFICATION_ROOT.as_ptr()),
        };
        let options = PRJ_STARTVIRTUALIZING_OPTIONS {
            Flags: PRJ_FLAG_USE_NEGATIVE_PATH_CACHE,
            PoolThreadCount: crate::native_mount::CALLBACK_THREADS,
            ConcurrentThreadCount: crate::native_mount::CALLBACK_THREADS,
            NotificationMappings: &raw mut mapping,
            NotificationMappingsCount: 1,
        };
        let callbacks = shared_callbacks();
        // SAFETY: the views allocation stays live until the drop stops
        // virtualizing, which drains every callback.
        let context = unsafe {
            PrjStartVirtualizing(
                &root_name,
                &raw const callbacks,
                Some(Arc::as_ptr(&instance.views).cast::<c_void>()),
                Some(&raw const options),
            )
        }
        .map_err(|error| driver_error(&error))?;
        instance.context = Some(context);
        instance.service = Some(super::service::ProjectorService::start()?);
        Ok(instance)
    }
}

impl Drop for SharedProjFsInstance {
    fn drop(&mut self) {
        // Every view has detached from the service and its callbacks.
        drop(self.service.take());
        if let Some(context) = self.context.take() {
            // SAFETY: no view remains; stopping drains every callback.
            unsafe { PrjStopVirtualizing(context) };
            if let Err(error) = super::cleanup::detach_root_by_path(&self.root) {
                tracing::warn!(%error, root = %self.root.display(), "shared ProjFS root detach failed");
            }
            // ProjFS keeps a stopped root's directory bound to the instance
            // even once detached: a lookup of any name in it, such as a
            // retired view's, fails as if its provider were away. A fresh
            // directory in its place answers every lookup itself.
            match std::fs::remove_dir(&self.root) {
                Ok(()) => {
                    if let Err(error) = std::fs::create_dir(&self.root) {
                        tracing::warn!(%error, root = %self.root.display(), "shared ProjFS root was not recreated");
                    }
                }
                Err(error) => {
                    tracing::warn!(%error, root = %self.root.display(), "shared ProjFS root was not replaced");
                }
            }
        }
        if let Err(error) = std::fs::remove_dir(&self.staging)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(%error, staging = %self.staging.display(), "shared ProjFS staging removal failed");
        }
    }
}

/// One view of a shared instance: its directory, its listing and its place
/// in the projector service. Dropping it retires the view.
pub(in crate::native_mount) struct SharedProjFsView {
    // Field order: the instance reference drops last.
    view: Option<NamedView<Vec<u16>, ViewCore>>,
    service: Option<u64>,
    name: Vec<u16>,
    directory: PathBuf,
    vacated: HashSet<Vec<u16>>,
    finished: bool,
    instance: Arc<Instance<SharedProjFsInstance>>,
}

impl SharedProjFsView {
    /// Creates the view's directory in the shared instance at the
    /// destination's parent, starting the instance if none is live.
    pub(super) fn create(destination: &Path) -> Result<Self, NativeMountError> {
        let (Some(root), Some(name)) = (destination.parent(), destination.file_name()) else {
            return Err(NativeMountError::InvalidDestination);
        };
        // A view is named once: only a fresh name from this process's root
        // is listed, so no callback for a retired view can reach a later one.
        if !super::super::shared::claim_issued_destination(destination) {
            return Err(NativeMountError::InvalidDestination);
        }
        use std::os::windows::ffi::OsStrExt as _;
        let name = name.encode_wide().collect::<Vec<_>>();
        let instance = INSTANCES.acquire(root, SharedProjFsInstance::start)?;
        let directory = instance
            .root
            .join(destination.file_name().unwrap_or_default());
        std::fs::create_dir(&directory)?;
        Ok(Self {
            view: None,
            service: None,
            name,
            directory,
            vacated: HashSet::new(),
            finished: false,
            instance,
        })
    }

    pub(super) fn directory(&self) -> &Path {
        &self.directory
    }

    /// The view's prefix in ProjFS names.
    pub(super) fn prefix(&self) -> Vec<u16> {
        let mut prefix = self.name.clone();
        prefix.push(SEPARATOR);
        prefix
    }

    pub(super) fn staging_anchor(&self) -> PathBuf {
        self.instance
            .staging
            .join(&self.directory.file_name().unwrap_or_default())
    }

    pub(super) fn context(&self) -> Result<PRJ_NAMESPACE_VIRTUALIZATION_CONTEXT, NativeMountError> {
        self.instance
            .context
            .ok_or(NativeMountError::ControlPending)
    }

    /// Lists the view, so its callbacks reach `runtime`, and makes the
    /// instance's service its projector's runner.
    pub(super) fn list(&mut self, runtime: &Arc<Runtime>) -> Result<(), NativeMountError> {
        let context = self.context()?;
        let service = self
            .instance
            .service
            .as_ref()
            .ok_or(NativeMountError::ControlPending)?;
        self.view = Some(self.instance.views.insert(fold(&self.name), |_| {
            Ok(ViewCore {
                runtime: Arc::clone(runtime),
                vacated: Mutex::new(HashSet::new()),
            })
        })?);
        self.service = Some(service.attach(runtime, context)?);
        Ok(())
    }

    /// Stops serving the view: once this returns, no callback and no
    /// projector pass reaches its runtime, and none holds a reference to it.
    pub(super) fn quiesce(&mut self) {
        if let (Some(id), Some(service)) = (self.service.take(), self.instance.service.as_ref()) {
            service.detach(id);
        }
        if let Some((_, core)) = self
            .view
            .take()
            .and_then(|mut view| view.retire_after(|| {}))
        {
            self.vacated.extend(
                core.vacated
                    .into_inner()
                    .unwrap_or_else(std::sync::PoisonError::into_inner),
            );
        }
    }

    /// Retires the view and, unless `preserve`, clears its tombstones and
    /// deletes its directory. The caller has dropped the view's runtime.
    pub(super) fn finish(&mut self, preserve: bool) -> Result<(), NativeMountError> {
        self.quiesce();
        if self.finished || preserve {
            self.finished = true;
            return Ok(());
        }
        let context = self.context()?;
        let mut vacated = std::mem::take(&mut self.vacated)
            .into_iter()
            .collect::<Vec<_>>();
        // Deepest first: a directory's tombstone goes after its children's.
        vacated.sort_by_key(|path| {
            std::cmp::Reverse(path.iter().filter(|unit| **unit == SEPARATOR).count())
        });
        let prefix = self.prefix();
        for path in vacated {
            let mut name = prefix.clone();
            name.extend(path);
            // A path that holds no tombstone (an ordinary entry, or nothing)
            // fails here and is the directory removal's to delete.
            // SAFETY: the name outlives this synchronous call on a live context.
            let _ = unsafe {
                PrjDeleteFile(
                    context,
                    &HSTRING::from_wide(&name),
                    Some(PRJ_UPDATE_ALLOW_TOMBSTONE),
                    None,
                )
            };
        }
        if let Err(error) = std::fs::remove_dir_all(&self.directory)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return Err(error.into());
        }
        self.finished = true;
        Ok(())
    }
}

impl Drop for SharedProjFsView {
    fn drop(&mut self) {
        self.quiesce();
        if !self.finished {
            // Only a view whose start failed gets here: its directory
            // holds nothing yet.
            let _ = std::fs::remove_dir(&self.directory);
        }
    }
}
