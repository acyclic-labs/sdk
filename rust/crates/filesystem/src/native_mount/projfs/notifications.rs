use super::*;

pub(super) unsafe fn query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let work = match runtime.native_work(false) {
        Ok(work) => work,
        Err(error) => return native_hresult(&error),
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let basis = {
        let creating = runtime
            .creating_name
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let (path, Some(basis)) = &*creating {
            // SAFETY: ProjFS retains this NUL-terminated path throughout the callback.
            let name = if data.FilePathName.is_null() {
                &[]
            } else {
                data.FilePathName.as_wide()
            };
            callback_name_matches(path, name).then_some(*basis)
        } else {
            None
        }
    };
    // Release the slot before calling the source: a concurrent creator may
    // need its write lock to return, and source calls can cause more callbacks.
    if let Some((file_id, basis)) = basis {
        match creating_name_is_current(runtime.source.as_ref(), &path, file_id, basis, &work) {
            Ok(true) => return HR_OK,
            Ok(false) => {}
            Err(error) => return source_hresult(&error),
        }
    }
    lookup_name(runtime.source.as_ref(), &path, &work)
}

fn creating_name_is_current(
    source: &dyn MountFilesystem,
    path: &MountPath,
    file_id: FileId,
    basis: ReadBasis,
    work: &crate::NativeMaterializationWork,
) -> Result<bool, MountSourceError> {
    // Scalar facts can only reject this shortcut. Admitting a hit still needs
    // the metered lease and checks below; rejected facts take the old lookup.
    if !source.reports_changes_to(file_id)
        || source.binding_epoch() != Some(basis.binding)
        || source.view_stamp() != Some(basis.stamp)
    {
        return Ok(false);
    }
    let _lease = match source.acquire_binding_lease_with_meter(Some(basis.binding), work) {
        Ok(lease) => lease,
        Err(MountSourceError::Stale) => return Ok(false),
        Err(error) => return Err(error),
    };
    let current = source.view_is_stable_with_meter(work)?
        && source.binding_epoch_with_meter(work)? == Some(basis.binding)
        && source.reports_changes_to(file_id)
        && source.view_stamp_with_meter(work)? == Some(basis.stamp);
    if current {
        source.authorize_with_meter(&[(crate::access::Action::List, path)], work)?;
    }
    Ok(current)
}

fn lookup_name(
    source: &dyn MountFilesystem,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> HRESULT {
    match source.lookup_with_meter(path, work) {
        Ok(Some(_)) => HR_OK,
        Ok(None) => HR_FILE_NOT_FOUND,
        Err(error) => source_hresult(&error),
    }
}

pub(super) fn record_post_operation_failure(
    failure: &Mutex<PostOperationFailures>,
    notification: PRJ_NOTIFICATION,
    path: &MountPath,
    retry_path: Option<&MountPath>,
    result: &Result<(), MountSourceError>,
) {
    if !matches!(
        notification,
        PRJ_NOTIFICATION_NEW_FILE_CREATED
            | PRJ_NOTIFICATION_FILE_OVERWRITTEN
            | PRJ_NOTIFICATION_FILE_RENAMED
            | PRJ_NOTIFICATION_HARDLINK_CREATED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        return;
    }
    let mut failure = lock_recover(failure);
    match (retry_path, result) {
        (_, Ok(())) => {}
        (None, Err(_)) if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION => {}
        (Some(retry_path), Err(error))
            if notification != PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION =>
        {
            failure.queue_capture(retry_path.clone(), error.to_string());
        }
        (_, Err(error)) => {
            // A metadata-only failure cannot be replayed as a full-file capture:
            // doing so could replace a concurrent source content change.
            if failure.unreplayable.is_none() {
                failure.unreplayable = Some(format!("{notification:?} at {path:?}: {error}"));
            }
        }
    }
}

#[allow(
    clippy::too_many_lines,
    reason = "notification admission and its exact authored operation share one callback boundary"
)]
pub(super) unsafe fn notification(
    callback_data: *const PRJ_CALLBACK_DATA,
    is_directory: bool,
    notification: PRJ_NOTIFICATION,
    destination_filename: PCWSTR,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    if matches!(
        notification,
        PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        runtime.projector.retry();
    }
    // SAFETY: `runtime` accepted `callback_data` above as the live ProjFS
    if unsafe { (*callback_data).TriggeringProcessId } == std::process::id()
        && matches!(
            notification,
            PRJ_NOTIFICATION_FILE_OPENED | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION
        )
        && MetadataProbeGuard::is_active(&runtime.metadata_probes, &path, |active, path| {
            matches!(projfs_path_suffix(active, path), Some([]))
        })
    {
        return HR_OK;
    }
    let work = match runtime.native_work(notification != PRJ_NOTIFICATION_FILE_OPENED) {
        Ok(work) => work,
        Err(error) => {
            let status = native_hresult(&error);
            let mut failures = lock_recover(&runtime.post_operation_failure);
            if failures.callback_failure.is_none() {
                failures.callback_failure = Some((error, lock_recover(&runtime.work).receipt()));
            }
            return status;
        }
    };
    if notification == PRJ_NOTIFICATION_FILE_OPENED {
        if let Err(error) = authorize_open(runtime.source.as_ref(), &path, is_directory, &work) {
            return source_hresult(&error);
        }
        if !runtime.writable {
            return HR_OK;
        }
    }
    if !runtime.writable {
        return HR_NOT_SUPPORTED;
    }
    let file_id = file_id(data);
    // SAFETY: ProjFS passes these names as null or NUL-terminated strings
    let source_is_external = unsafe { empty_destination(data.FilePathName) };
    // SAFETY: as above.
    let destination_is_external = unsafe { empty_destination(destination_filename) };
    if notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
        && (source_is_external
            || destination_is_external
            || path_from(destination_filename).is_none())
    {
        return HR_NOT_SAME_DEVICE;
    }
    let invalid_rename = source_is_external && destination_is_external
        || is_directory && (source_is_external || destination_is_external)
        || !destination_is_external && path_from(destination_filename).is_none();
    if notification == PRJ_NOTIFICATION_PRE_RENAME && invalid_rename {
        return HR_NOT_SAME_DEVICE;
    }
    if matches!(
        notification,
        PRJ_NOTIFICATION_PRE_RENAME
            | PRJ_NOTIFICATION_PRE_SET_HARDLINK
            | PRJ_NOTIFICATION_PRE_DELETE
    ) {
        let destination = (!destination_is_external)
            .then(|| path_from(destination_filename))
            .flatten();
        return match authorize_pre_operation(
            runtime.source.as_ref(),
            (!source_is_external).then_some(&path),
            destination.as_ref(),
            notification,
            &work,
        ) {
            Ok(()) if notification == PRJ_NOTIFICATION_PRE_DELETE => {
                match runtime.callbacks.call_observed(
                    file_id,
                    || admit_delete(runtime, file_id, &path, &work),
                    |_| {},
                ) {
                    Ok(()) => HR_OK,
                    Err(error) => source_hresult(&error),
                }
            }
            Ok(()) => HR_OK,
            Err(error) => source_hresult(&error),
        };
    }
    if notification == PRJ_NOTIFICATION_NEW_FILE_CREATED {
        if is_directory {
            lock_recover(runtime.post_operation_failure.as_ref())
                .queue_subtree(&path, "new directory awaiting capture".to_owned());
        } else {
            defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        }
        keep_file_notifications(notification, operation_parameters);
        return HR_OK;
    }
    if notification == PRJ_NOTIFICATION_FILE_OVERWRITTEN {
        if is_directory {
            defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        }
        keep_file_notifications(notification, operation_parameters);
        return HR_OK;
    }
    let destination = (!destination_is_external)
        .then(|| path_from(destination_filename))
        .flatten();
    if path.components().is_empty() && notification == PRJ_NOTIFICATION_FILE_OPENED {
        return HR_OK;
    }
    if notification == PRJ_NOTIFICATION_HARDLINK_CREATED {
        let Some(destination) = destination else {
            lock_recover(runtime.post_operation_failure.as_ref()).unreplayable = Some(format!(
                "{path:?}: hard-link destination is invalid after host completion"
            ));
            return HR_INVALID_DATA;
        };
        defer_host_capture(runtime.post_operation_failure.as_ref(), path);
        defer_host_capture(runtime.post_operation_failure.as_ref(), destination);
        return HR_OK;
    }
    let source = &runtime.source;
    let metadata_baselines = &runtime.metadata_baselines;
    let projection = &runtime.projection;
    let metadata_probes = &runtime.metadata_probes;
    let metadata_edits = &runtime.metadata_edits;
    let metadata_root = &runtime.metadata_root;
    let operation_failures = &runtime.post_operation_failure;
    let projector = &runtime.projector;
    let retry_path = std::cell::RefCell::new(None);
    let operation = || {
        if path.components().is_empty()
            && (notification == PRJ_NOTIFICATION_FILE_OPENED
                || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION)
        {
            return Ok(());
        }
        if notification == PRJ_NOTIFICATION_FILE_OPENED {
            if has_pending_capture(operation_failures.as_ref(), &path) {
                return Ok(());
            }
            let baseline =
                probe_host_windows_metadata(metadata_root, &path, metadata_probes, &work)?;
            let projected = projected_source_id(runtime, &path, Some(baseline), &work)?;
            record_metadata_open(metadata_baselines, file_id, baseline, projected)
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
        {
            let (final_close, expected) = match close_metadata_handle(metadata_baselines, file_id) {
                MetadataClose::Pending(binding) => (false, binding),
                MetadataClose::Final(state) => (true, state.and_then(|state| state.source_binding)),
            };
            let result = if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED {
                // Capture custody belongs to flush_callback_gate. A delayed
                // close cannot cancel a newer capture of this same name.
                complete_delete(source.as_ref(), &path, expected, &work)
            } else {
                defer_host_capture(operation_failures.as_ref(), path.clone());
                Ok(())
            };
            if result.is_ok()
                && !final_close
                && notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            {
                let baseline =
                    probe_host_windows_metadata(metadata_root, &path, metadata_probes, &work)?;
                refresh_metadata_baseline(metadata_baselines, file_id, baseline);
            }
            result
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION {
            let baseline = match close_metadata_handle(metadata_baselines, file_id) {
                MetadataClose::Pending(_) => return Ok(()),
                MetadataClose::Final(state) => state.and_then(|state| state.baseline),
            };
            if has_pending_capture(operation_failures.as_ref(), &path) {
                return Ok(());
            }
            let Some(baseline) = baseline else {
                defer_host_capture(operation_failures.as_ref(), path.clone());
                return Ok(());
            };
            let host = probe_host_windows_metadata(metadata_root, &path, metadata_probes, &work)?;
            if baseline.identity != host.identity
                || baseline.links != host.links
                || baseline.size != host.size
            {
                lock_recover(projection).remove(&path);
                defer_host_capture(operation_failures.as_ref(), path.clone());
                return Ok(());
            }
            let metadata_changed = metadata_changed_since_open(baseline, host);
            if metadata_changed {
                *retry_path.borrow_mut() = Some(path.clone());
            }
            if !metadata_changed && !has_pending_capture(operation_failures.as_ref(), &path) {
                match crate::native_mount::cached_binding::cached_binding_matches(
                    projection,
                    |bindings| {
                        bindings
                            .get(&path)
                            .copied()
                            .filter(|binding| binding.host == host && host.links == Some(1))
                    },
                    || source.acquire_binding_lease_with_meter(None, &work),
                    |binding| {
                        source.reports_changes_to(binding.file_id)
                            && ReadBasis::sample(source.as_ref()) == Some(binding.basis)
                    },
                    |binding| {
                        binding
                            .basis
                            .still_describes(source.as_ref(), &path, Some(binding.file_id))
                    },
                ) {
                    Ok(true) => return Ok(()),
                    Err(error)
                        if matches!(
                            error.semantic_error(),
                            MountSourceError::Work(_)
                                | MountSourceError::NativeWork(_)
                                | MountSourceError::Cancelled
                        ) =>
                    {
                        return Err(error);
                    }
                    Ok(false) | Err(_) => {}
                }
            }
            let basis = ReadBasis::sample(source.as_ref());
            let lookup = source.lookup(&path);
            match lookup {
                Ok(Some(_)) if metadata_changed => metadata_edits.with(&host.identity, || {
                    let Some(lookup) = source.lookup(&path)? else {
                        defer_host_capture(operation_failures.as_ref(), path.clone());
                        return Ok(());
                    };
                    capture_changed_windows_metadata(
                        source.as_ref(),
                        &path,
                        lookup.metadata,
                        baseline,
                        host,
                        &work,
                    )
                }),
                Ok(Some(lookup)) => {
                    if let Some(basis) = basis
                        && !has_pending_capture(operation_failures.as_ref(), &path)
                        && host.links == Some(1)
                    {
                        let mut bindings = lock_recover(projection);
                        if bindings.len() >= MAXIMUM_CACHED_BINDINGS {
                            bindings.clear();
                        }
                        bindings.insert(
                            path.clone(),
                            ReadOnlyBinding {
                                host,
                                file_id: lookup.node.file_id,
                                basis,
                            },
                        );
                    }
                    Ok(())
                }
                Ok(None) => {
                    lock_recover(projection).remove(&path);
                    defer_host_capture(operation_failures.as_ref(), path.clone());
                    Ok(())
                }
                Err(error) => Err(error),
            }
        } else if notification == PRJ_NOTIFICATION_FILE_RENAMED {
            let _tree = lock_recover(&projector.tree);
            let current = projector.current_beneath(source.as_ref(), &path);
            let result = handle_rename_source(
                runtime,
                &path,
                source_is_external,
                destination_is_external,
                is_directory,
                destination.clone(),
                &work,
            );
            if result.is_ok()
                && !source_is_external
                && !destination_is_external
                && let Some(destination) = destination.as_ref()
            {
                lock_recover(operation_failures.as_ref())
                    .rename_pending_captures(&path, destination);
                let carried = Carried::read(source.as_ref(), &path, destination, &current);
                projector.renamed(&path, destination, carried);
            }
            result
        } else {
            Err(MountSourceError::Unsupported(
                "ProjFS emitted an unadmitted write notification"
                    .to_owned()
                    .into(),
            ))
        }
    };
    let result = runtime
        .callbacks
        .call_observed(file_id, operation, |result| {
            let retry_path = retry_path.borrow();
            record_post_operation_failure(
                runtime.post_operation_failure.as_ref(),
                notification,
                &path,
                retry_path.as_ref(),
                result,
            );
        });
    keep_file_notifications(notification, operation_parameters);
    match result {
        Ok(()) => HR_OK,
        Err(error) => {
            let status = source_hresult(&error);
            if matches!(
                error.semantic_error(),
                MountSourceError::Work(_)
                    | MountSourceError::NativeWork(_)
                    | MountSourceError::Cancelled
            ) {
                let mut failures = lock_recover(&runtime.post_operation_failure);
                if failures.callback_failure.is_none() {
                    failures.callback_failure = Some((error.into(), work.receipt()));
                }
            }
            status
        }
    }
}

pub(super) fn keep_file_notifications(
    notification: PRJ_NOTIFICATION,
    operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS,
) {
    if operation_parameters.is_null() {
        return;
    }
    // SAFETY: ProjFS supplies the writable union member matching each of
    unsafe {
        match notification {
            PRJ_NOTIFICATION_NEW_FILE_CREATED | PRJ_NOTIFICATION_FILE_OVERWRITTEN => {
                (*operation_parameters).PostCreate.NotificationMask = FILE_NOTIFICATIONS;
            }
            PRJ_NOTIFICATION_FILE_RENAMED => {
                (*operation_parameters).FileRenamed.NotificationMask = FILE_NOTIFICATIONS;
            }
            _ => {}
        }
    }
}

fn admit_delete(
    runtime: &Runtime,
    file_id: u128,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    if lock_recover(&runtime.metadata_baselines)
        .get(&file_id)
        .is_some_and(|state| state.source_binding.is_some())
    {
        return Ok(());
    }
    let binding = bind_delete(runtime, path, work)?;
    let allocation = work.claim(
        size_of::<(u128, OpenMetadataState)>() as u64,
        1,
        crate::WorkCounters::UNCHARGED,
    )?;
    let mut handles = lock_recover(&runtime.metadata_baselines);
    handles
        .try_reserve(1)
        .map_err(MountSourceError::Allocation)?;
    // The callback FileId groups opens of one host file. Delete admission
    // shares its metadata owner and retires with the last observed close.
    let state = handles.entry(file_id).or_insert(OpenMetadataState {
        baseline: None,
        source_binding: None,
        open_handles: std::num::NonZeroUsize::MIN,
        _delete_allocation: None,
    });
    state.source_binding = Some(binding);
    state._delete_allocation = Some(allocation);
    Ok(())
}

fn bind_delete(
    runtime: &Runtime,
    path: &MountPath,
    work: &crate::NativeMaterializationWork,
) -> Result<DeleteBinding, MountSourceError> {
    // PRE_DELETE is the admission boundary even for Windows deletes which do
    // not send FILE_OPENED. Keep an older projected/open incarnation instead
    // of binding a delayed host operation to its replacement in the source.
    if let Some(id) = projected_source_id(runtime, path, None, work)? {
        return Ok(DeleteBinding::Present(id));
    }
    let _lease = runtime
        .source
        .acquire_binding_lease_with_meter(None, work)?;
    Ok(match runtime.source.lookup_with_meter(path, work)? {
        Some(entry) => DeleteBinding::Present(entry.node.file_id),
        None => DeleteBinding::Absent,
    })
}

fn projected_source_id(
    runtime: &Runtime,
    path: &MountPath,
    host: Option<HostWindowsMetadata>,
    work: &crate::NativeMaterializationWork,
) -> Result<Option<FileId>, MountSourceError> {
    let entry = {
        let state = lock_recover(&runtime.projector.state);
        state
            .written
            .get(path)
            .or_else(|| state.pending.get(path))
            .copied()
    };
    let Some(entry) = entry else { return Ok(None) };
    if let Some(Owned::File(owned)) = entry.owned {
        let host = match host {
            Some(host) => host,
            None => probe_host_windows_metadata(
                &runtime.metadata_root,
                path,
                &runtime.metadata_probes,
                work,
            )?,
        };
        if owned.identity != host.identity {
            // An external replacement is no longer the projected object.
            // Its operation must bind at admission to its own source entry.
            return Ok(None);
        }
    }
    Ok(entry.facts.map(|facts| facts.lookup.node.file_id))
}

fn complete_delete(
    source: &dyn MountFilesystem,
    path: &MountPath,
    expected: Option<DeleteBinding>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let expected = match expected {
        Some(DeleteBinding::Absent) => return Ok(()),
        Some(DeleteBinding::Present(id)) => id,
        None => {
            return match source.lookup_with_meter(path, work)? {
                None => Ok(()),
                Some(_) => Err(MountSourceError::Stale),
            };
        }
    };
    // The shared mutation compares the original identity atomically. Absence
    // or a different incarnation means this old completion has no work left.
    match source.remove(path, Some(expected), work) {
        Err(error)
            if matches!(
                error.semantic_error(),
                MountSourceError::NotFound
                    | MountSourceError::Filesystem(crate::FsError::Mutation(
                        crate::kernel::GenerationMutationError::FileIdentityConflict
                    ))
            ) =>
        {
            Ok(())
        }
        result => result,
    }
}

pub(super) fn handle_rename_source(
    runtime: &Runtime,
    source: &MountPath,
    source_is_external: bool,
    destination_is_external: bool,
    is_directory: bool,
    destination: Option<MountPath>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    let source_fs = runtime.source.as_ref();
    let post_operation_failures = runtime.post_operation_failure.as_ref();
    if source_is_external {
        let destination = destination
            .ok_or_else(|| MountSourceError::Invalid("rename destination is invalid".into()))?;
        if is_directory {
            lock_recover(post_operation_failures).queue_subtree(
                &destination,
                "imported directory awaiting capture".to_owned(),
            );
        } else {
            defer_host_capture(post_operation_failures, destination);
        }
        return Ok(());
    }
    if destination_is_external {
        defer_host_capture(post_operation_failures, source.clone());
        return Ok(());
    }
    let destination = destination
        .ok_or_else(|| MountSourceError::Invalid("rename destination is invalid".into()))?;
    if source_fs.lookup_with_meter(source, work)?.is_none() {
        if is_directory {
            lock_recover(post_operation_failures).queue_subtree(
                &destination,
                "renamed directory awaiting capture".to_owned(),
            );
        } else {
            defer_host_capture(post_operation_failures, destination);
        }
        return Ok(());
    }
    source_fs.rename(source, &destination, true, work)?;
    source_fs.flush(work)
}

unsafe extern "system" fn cancel(_callback_data: *const PRJ_CALLBACK_DATA) {}

macro_rules! on_callback {
    ($stack:ident, $level:ident, $callback:ident($data:ident: $data_type:ty $(, $argument:ident: $type:ty)*)) => {{
        unsafe extern "system" fn entry($data: $data_type $(, $argument: $type)*) -> HRESULT {
            // SAFETY: the SDK retains its installed instance context through this callback.
            let Some((_, runtime)) = (unsafe { runtime($data) }) else { return HR_UNEXPECTED; };
            let Some(_active) = runtime.callbacks.enter() else { return HR_FILE_INVALID; };
            let span = callback_span!($level, $callback);
            // SAFETY: `ProjFS` passes the arguments its callback contract
            let result = on_callback!(@run $stack, $callback($data $(, $argument)*));
            if result.is_err() {
                span.record("errno", result.0);
            }
            result
        }
        entry
    }};
    (@run provider, $call:expr) => {
        provider_stack::run(|| {
            // SAFETY: the SDK callback entry retains admission and validates
            // the context; switching stacks preserves the callback arguments.
            unsafe { $call }
        }).unwrap_or_else(|error| error.code())
    };
    // Only constant-return callbacks use the caller's stack; they neither
    // inspect their arguments nor invoke a source operation.
    (@run caller, $call:expr) => {
        // SAFETY: the SDK entry retains admission; these callbacks ignore all
        // uninspected arguments and return their fixed protocol status.
        unsafe { $call }
    };
}

pub(super) fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(on_callback!(provider, TRACE, start_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        EndDirectoryEnumerationCallback: Some(on_callback!(caller, TRACE, end_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        GetDirectoryEnumerationCallback: Some(on_callback!(caller, TRACE, get_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID,
            search_expression: PCWSTR,
            buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE
        ))),
        GetPlaceholderInfoCallback: Some(on_callback!(caller, TRACE, placeholder(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        GetFileDataCallback: Some(on_callback!(caller, TRACE, file_data(
            callback_data: *const PRJ_CALLBACK_DATA,
            byte_offset: u64,
            length: u32
        ))),
        QueryFileNameCallback: Some(on_callback!(provider, TRACE, query_name(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        NotificationCallback: Some(on_callback!(provider, DEBUG, notification(
            callback_data: *const PRJ_CALLBACK_DATA,
            is_directory: bool,
            kind: PRJ_NOTIFICATION,
            destination_filename: PCWSTR,
            operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS
        ))),
        CancelCommandCallback: Some(cancel),
    }
}

pub(super) fn open_notifications(writable: bool) -> PRJ_NOTIFY_TYPES {
    if writable {
        FILE_NOTIFICATIONS
    } else {
        PRJ_NOTIFY_FILE_OPENED
    }
}

pub(super) fn authorize_open(
    source: &dyn MountFilesystem,
    path: &MountPath,
    directory: bool,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    use crate::access::Action;
    if directory {
        source.authorize_with_meter(&[(Action::List, path)], work)
    } else {
        source.authorize_with_meter(&[(Action::List, path), (Action::Read, path)], work)
    }
}

pub(super) fn authorize_pre_operation(
    source: &dyn MountFilesystem,
    path: Option<&MountPath>,
    destination: Option<&MountPath>,
    notification: PRJ_NOTIFICATION,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
    use crate::native_mount::actions::Mutation;
    let mutation = match (notification, path, destination) {
        (PRJ_NOTIFICATION_PRE_DELETE, Some(_), _) => Mutation::Delete,
        (PRJ_NOTIFICATION_PRE_RENAME, None, Some(_)) => Mutation::Create,
        (PRJ_NOTIFICATION_PRE_RENAME, Some(_), _) => Mutation::Rename,
        (PRJ_NOTIFICATION_PRE_SET_HARDLINK, Some(_), Some(_)) => Mutation::HardLink,
        _ => return Err(MountSourceError::AccessDenied),
    };
    let actions = mutation.actions();
    match (path, destination) {
        (Some(path), Some(destination)) => {
            source.authorize_with_meter(&[(actions.0, path), (actions.1, destination)], work)
        }
        (Some(path), None) => source.authorize_with_meter(&[(actions.0, path)], work),
        (None, Some(destination)) => source.authorize_with_meter(&[(actions.1, destination)], work),
        _ => Err(MountSourceError::AccessDenied),
    }
}
