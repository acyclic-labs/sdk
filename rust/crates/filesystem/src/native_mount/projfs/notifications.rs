use super::*;

pub(super) unsafe fn query_name(callback_data: *const PRJ_CALLBACK_DATA) -> HRESULT {
    let Some((data, runtime)) = runtime(callback_data) else {
        return HR_UNEXPECTED;
    };
    let Some(path) = path_from(data.FilePathName) else {
        return HR_INVALID_DATA;
    };
    let work = match runtime.native_work(false) {
        Ok(work) => work,
        Err(error) => return native_hresult(&error),
    };
    match runtime.source.lookup_with_meter(&path, &work) {
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
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            | PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
    ) {
        return;
    }
    let mut failure = lock_recover(failure);
    match (retry_path, result) {
        (_, Ok(())) => {}
        (Some(retry_path), Err(error)) => {
            failure.queue_capture(retry_path.clone(), error.to_string());
        }
        (None, Err(error)) => {
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
    let work = match runtime.native_work(true) {
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
    if !runtime.writable {
        return HR_NOT_SUPPORTED;
    }
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
        && MetadataProbeGuard::is_active(runtime.metadata_probes.as_ref(), &path)
    {
        return HR_OK;
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
    if notification == PRJ_NOTIFICATION_PRE_RENAME
        || notification == PRJ_NOTIFICATION_PRE_SET_HARDLINK
    {
        return HR_OK;
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
            let baseline = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &path,
                metadata_probes.as_ref(),
                &work,
            )?;
            record_metadata_open(metadata_baselines.as_ref(), file_id, baseline)
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            || notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED
        {
            let close = close_metadata_handle(metadata_baselines.as_ref(), file_id);
            let final_close = matches!(close, MetadataClose::Final(_));
            let result = if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_DELETED {
                lock_recover(operation_failures.as_ref())
                    .pending_captures
                    .remove(&path);
                *retry_path.borrow_mut() = Some(path.clone());
                match source.lookup(&path)? {
                    Some(_) => source.remove(&path, None, &work),
                    None => Ok(()),
                }
            } else {
                defer_host_capture(operation_failures.as_ref(), path.clone());
                Ok(())
            };
            if result.is_ok()
                && !final_close
                && notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_FILE_MODIFIED
            {
                let baseline = probe_host_windows_metadata(
                    metadata_root.as_ref(),
                    &path,
                    metadata_probes.as_ref(),
                    &work,
                )?;
                refresh_metadata_baseline(metadata_baselines.as_ref(), file_id, baseline);
            }
            result
        } else if notification == PRJ_NOTIFICATION_FILE_HANDLE_CLOSED_NO_MODIFICATION {
            let baseline = match close_metadata_handle(metadata_baselines.as_ref(), file_id) {
                MetadataClose::Pending => return Ok(()),
                MetadataClose::Final(baseline) => baseline,
            };
            if has_pending_capture(operation_failures.as_ref(), &path) {
                return Ok(());
            }
            let capture_path = path.clone();
            let Some(baseline) = baseline else {
                defer_host_capture(operation_failures.as_ref(), capture_path);
                return Ok(());
            };
            *retry_path.borrow_mut() = Some(capture_path.clone());
            let host = probe_host_windows_metadata(
                metadata_root.as_ref(),
                &capture_path,
                metadata_probes.as_ref(),
                &work,
            )?;
            if baseline.identity != host.identity
                || baseline.links != host.links
                || baseline.size != host.size
            {
                lock_recover(projection.as_ref())
                    .bindings
                    .remove(&capture_path);
                defer_host_capture(operation_failures.as_ref(), capture_path);
                return Ok(());
            }
            let cached_binding = lock_recover(projection.as_ref())
                .bindings
                .get(&capture_path)
                .copied();
            if !metadata_changed_since_open(baseline, host)
                && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                && let Some(binding) = cached_binding
                && binding.host == host
                && host.links == Some(1)
                && let Ok(_view_lease) = source.acquire_view_lease()
                && binding.basis.still_describes(
                    source.as_ref(),
                    &capture_path,
                    Some(binding.file_id),
                )
            {
                return Ok(());
            }
            let basis = ReadBasis::sample(source.as_ref());
            let lookup = source.lookup(&capture_path);
            match lookup {
                Ok(Some(_)) if metadata_changed_since_open(baseline, host) => {
                    metadata_edits.with(&host.identity, || {
                        let Some(lookup) = source.lookup(&capture_path)? else {
                            defer_host_capture(operation_failures.as_ref(), capture_path);
                            return Ok(());
                        };
                        capture_changed_windows_metadata(
                            source.as_ref(),
                            &capture_path,
                            lookup.metadata,
                            baseline,
                            host,
                            &work,
                        )
                    })
                }
                Ok(Some(lookup)) => {
                    if let Some(basis) = basis
                        && !has_pending_capture(operation_failures.as_ref(), &capture_path)
                        && host.links == Some(1)
                    {
                        lock_recover(projection.as_ref()).remember_binding(
                            capture_path,
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
                    lock_recover(projection.as_ref())
                        .bindings
                        .remove(&capture_path);
                    defer_host_capture(operation_failures.as_ref(), capture_path);
                    Ok(())
                }
                Err(error) => Err(error),
            }
        } else if notification == PRJ_NOTIFICATION_FILE_RENAMED {
            let _tree = lock_recover(&projector.tree);
            let current = projector.current_beneath(source.as_ref(), &path);
            let result = handle_rename_source(
                source.as_ref(),
                &path,
                source_is_external,
                destination_is_external,
                is_directory,
                destination.clone(),
                operation_failures.as_ref(),
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
        Some(Ok(())) => HR_OK,
        Some(Err(error)) => {
            let status = source_hresult(&error);
            if matches!(
                error.semantic_error(),
                MountSourceError::Work(_)
                    | MountSourceError::NativeWork(_)
                    | MountSourceError::Cancelled
            ) {
                let mut failures = lock_recover(&runtime.post_operation_failure);
                if failures.callback_failure.is_none() {
                    failures.callback_failure =
                        Some((error.into(), lock_recover(&runtime.work).receipt()));
                }
            }
            status
        }
        None => HR_UNEXPECTED,
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

pub(super) fn handle_rename_source(
    source_fs: &dyn MountFilesystem,
    source: &MountPath,
    source_is_external: bool,
    destination_is_external: bool,
    is_directory: bool,
    destination: Option<MountPath>,
    post_operation_failures: &Mutex<PostOperationFailures>,
    work: &crate::NativeMaterializationWork,
) -> Result<(), MountSourceError> {
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

macro_rules! on_provider_stack {
    ($level:ident, $callback:ident($data:ident: $data_type:ty $(, $argument:ident: $type:ty)*)) => {{
        unsafe extern "system" fn entry($data: $data_type $(, $argument: $type)*) -> HRESULT {
            // SAFETY: the SDK retains its installed instance context through this callback.
            let Some((_, runtime)) = (unsafe { runtime($data) }) else { return HR_UNEXPECTED; };
            let Some(_active) = runtime.callbacks.enter() else { return HR_FILE_INVALID; };
            let span = callback_span!($level, $callback);
            // SAFETY: `ProjFS` passes the arguments its callback contract
            let result = provider_stack::run(|| unsafe { $callback($data $(, $argument)*) })
                .unwrap_or_else(|error| error.code());
            if result.is_err() {
                span.record("errno", result.0);
            }
            result
        }
        entry
    }};
}

pub(super) fn callbacks() -> PRJ_CALLBACKS {
    PRJ_CALLBACKS {
        StartDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, start_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        EndDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, end_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID
        ))),
        GetDirectoryEnumerationCallback: Some(on_provider_stack!(TRACE, get_directory(
            callback_data: *const PRJ_CALLBACK_DATA,
            enumeration_id: *const GUID,
            search_expression: PCWSTR,
            buffer: PRJ_DIR_ENTRY_BUFFER_HANDLE
        ))),
        GetPlaceholderInfoCallback: Some(on_provider_stack!(TRACE, placeholder(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        GetFileDataCallback: Some(on_provider_stack!(TRACE, file_data(
            callback_data: *const PRJ_CALLBACK_DATA,
            byte_offset: u64,
            length: u32
        ))),
        QueryFileNameCallback: Some(on_provider_stack!(TRACE, query_name(
            callback_data: *const PRJ_CALLBACK_DATA
        ))),
        NotificationCallback: Some(on_provider_stack!(DEBUG, notification(
            callback_data: *const PRJ_CALLBACK_DATA,
            is_directory: bool,
            kind: PRJ_NOTIFICATION,
            destination_filename: PCWSTR,
            operation_parameters: *mut PRJ_NOTIFICATION_PARAMETERS
        ))),
        CancelCommandCallback: Some(cancel),
    }
}
