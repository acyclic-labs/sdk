use super::*;

fn start_task_view(
    destination: &Path,
    source: Arc<dyn MountFilesystem>,
) -> Result<crate::native_mount::NativeMountSession, NativeMountError> {
    crate::native_mount::mount_native(
        NativeMountRequest {
            mount_id: crate::MountId::new(),
            volume_id: crate::VolumeId::new(),
            destination: destination.to_path_buf(),
            writable: true,
            shared: true,
        },
        source,
    )
}

fn shared_callbacks_keep_their_view(substitute_handle: bool) -> TestResult {
    use super::super::file_ops::acyclic_fs_darwin_mount_fsync;
    use super::super::requests::CallbackTarget;
    use super::super::shared::SharedViews;

    let (first_source, _, first_work) = checkout_context(MountPublication::Manual)?;
    let (second_source, _, second_work) = checkout_context(MountPublication::Manual)?;
    let root = tempfile::tempdir()?;
    let views = SharedViews::new(root.path(), &first_work)?;
    let mut first = views.insert(b"first".to_vec(), first_source.clone(), true, &first_work)?;
    let second = views.insert(
        b"second".to_vec(),
        second_source.clone(),
        true,
        &second_work,
    )?;
    let target = CallbackTarget::Shared(Arc::clone(&views));
    let address = &target as *const CallbackTarget as usize;
    let create = |name: &CStr, context: &DarwinMountContext| {
        let mut handle = 0;
        // SAFETY: the target, terminated name and output live through the call.
        let result = unsafe {
            acyclic_fs_darwin_mount_create(
                address,
                name.as_ptr(),
                0o644,
                context.mount_uid,
                context.mount_gid,
                libc::O_RDWR,
                &mut handle,
            )
        };
        assert_eq!(result, 0);
        handle
    };
    let first_handle = create(c"/first/same", &first.context);
    let second_handle = create(c"/second/same", &second.context);
    assert_ne!(first_handle, second_handle);
    for (name, handle, value) in [
        (c"/first/same", first_handle, b"first".as_slice()),
        (c"/second/same", second_handle, b"second".as_slice()),
    ] {
        // SAFETY: the target and input bytes live through the callback.
        assert_eq!(
            unsafe {
                acyclic_fs_darwin_mount_write(
                    address,
                    name.as_ptr(),
                    handle,
                    value.as_ptr().cast(),
                    value.len(),
                    0,
                )
            },
            c_int::try_from(value.len())?
        );
    }
    let path = MountPath::root().child(b"same".to_vec());
    assert_eq!(
        first_source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
        b"first"
    );
    assert_eq!(
        second_source.open_file(&path)?.read_up_to(0, 16)?.as_ref(),
        b"second"
    );
    // A retained handle cannot select another view even when a path does.
    let foreign = if substitute_handle {
        second_handle
    } else {
        first_handle
    };
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_fsync(address, c"/second/same".as_ptr(), foreign) },
        -libc::EXDEV,
        "foreign handle crossed a view boundary"
    );
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_rename(
                address,
                c"/first/same".as_ptr(),
                c"/second/moved".as_ptr(),
            )
        },
        -libc::EXDEV
    );
    first.retire();
    assert!(views.route(Some(c"/first/same"), 0).is_err());
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_fsync(address, ptr::null(), first_handle) },
        -libc::ESTALE
    );
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_fsync(address, ptr::null(), second_handle) },
        0
    );
    assert_eq!(views.listed_from(1, 64).len(), 1);
    views.close();
    // SAFETY: the retained target and handle remain allocated, but the
    // provider's closed admission must prevent a pathless callback entering.
    assert_eq!(
        unsafe { acyclic_fs_darwin_mount_fsync(address, ptr::null(), second_handle) },
        -libc::ESTALE
    );
    Ok(())
}

#[test]
fn shared_callbacks_route_paths_and_handles_and_retire_only_their_view() -> TestResult {
    shared_callbacks_keep_their_view(false)
}

#[test]
#[should_panic(expected = "foreign handle crossed a view boundary")]
fn shared_callback_negative_control_substitutes_the_receivers_handle() {
    shared_callbacks_keep_their_view(true).expect("real callback fixture");
}

#[test]
fn root_listing_fills_the_native_frame_across_internal_pages() -> TestResult {
    use super::super::requests::CallbackTarget;
    use super::super::shared::SharedViews;

    unsafe extern "C" fn collect(
        output: *mut c_void,
        name: *const c_char,
        _metadata: *const libc::stat,
        after: libc::off_t,
        _flags: c_int,
    ) -> c_int {
        // SAFETY: this test retains the collection and the bridge's terminated
        // name through the synchronous callback, which never stores a pointer.
        let (output, name) = unsafe {
            (
                &mut *output.cast::<Vec<(Vec<u8>, i64)>>(),
                CStr::from_ptr(name),
            )
        };
        output.push((name.to_bytes().to_vec(), after));
        0
    }
    let (source, _, work) = checkout_context(MountPublication::Manual)?;
    let root = tempfile::tempdir()?;
    let views = SharedViews::new(root.path(), &work)?;
    let mut owners = Vec::new();
    for number in 0..65 {
        owners.push(views.insert(number.to_string().into_bytes(), source.clone(), true, &work)?);
    }
    let target = CallbackTarget::Shared(Arc::clone(&views));
    let address = &target as *const CallbackTarget as usize;
    // SAFETY: the root exposes no named attributes; size probes need no buffer.
    assert_eq!(
        unsafe {
            super::super::attribute_ops::acyclic_fs_darwin_mount_listxattr(
                address,
                c"/".as_ptr(),
                ptr::null_mut(),
                0,
            )
        },
        0
    );
    assert_eq!(
        unsafe {
            acyclic_fs_darwin_mount_getxattr(
                address,
                c"/".as_ptr(),
                c"missing".as_ptr(),
                ptr::null_mut(),
                0,
            )
        },
        -libc::ENOATTR
    );
    let mut output: Vec<(Vec<u8>, i64)> = Vec::new();
    // SAFETY: the typed target, output vector and filler live through the call.
    assert_eq!(
        unsafe {
            super::super::directory::acyclic_fs_darwin_mount_readdir(
                address,
                c"/".as_ptr(),
                ptr::from_mut(&mut output).cast(),
                collect,
                0,
            )
        },
        0
    );
    assert_eq!(
        output.len(),
        67,
        "one internal page was mistaken for native EOF"
    );
    let after = output.last().ok_or("empty listing")?.1;
    output.clear();
    // SAFETY: same live owner and buffer, now resumed from its last cookie.
    assert_eq!(
        unsafe {
            super::super::directory::acyclic_fs_darwin_mount_readdir(
                address,
                c"/".as_ptr(),
                ptr::from_mut(&mut output).cast(),
                collect,
                after,
            )
        },
        0
    );
    assert!(output.is_empty(), "resume cookie repeated a listed view");
    views.close();
    assert!(views.insert(b"late".to_vec(), source, true, &work).is_err());
    assert!(
        views.route(Some(c"/0"), 0).is_err(),
        "dead provider admitted a callback"
    );
    Ok(())
}

#[test]
fn task_views_share_one_real_nfs_session_and_retire_independently() -> TestResult {
    use std::os::unix::fs::MetadataExt;

    let (first_source, _, _) = checkout_context(MountPublication::Manual)?;
    let (second_source, _, _) = checkout_context(MountPublication::Manual)?;
    crate::native_mount::initialize_native_mount_services()?;
    acyclic_native_runtime::initialize_native_io_services()?;
    let namespace = tempfile::tempdir()?;
    let admitted = crate::hold_root(namespace.path())?;
    let first_name = crate::native_mount::shared_view_destination(Some(&admitted))?;
    let second_name = crate::native_mount::shared_view_destination(Some(&admitted))?;
    let mut first = start_task_view(first_name.path(), first_source)?;
    let mut second = start_task_view(second_name.path(), second_source)?;
    let first_driver = first.driver.as_ref().ok_or("first driver missing")?;
    let second_driver = second.driver.as_ref().ok_or("second driver missing")?;
    assert!(Arc::ptr_eq(&first_driver.unmount, &second_driver.unmount));
    let physical = Arc::clone(&first_driver.unmount);
    let first_path = first_name.path().join("same");
    let second_path = second_name.path().join("same");
    std::fs::write(&first_path, b"first")?;
    std::fs::write(&second_path, b"second")?;
    assert_eq!(std::fs::read(&first_path)?, b"first");
    assert_eq!(std::fs::read(&second_path)?, b"second");
    assert_ne!(
        std::fs::metadata(&first_path)?.ino(),
        std::fs::metadata(&second_path)?.ino()
    );
    first.stop_with_context(&crate::native_mount::NativeSessionStopWork::AcceptedDrop)?;
    assert!(!first_name.path().exists());
    assert_ne!(
        physical.pointer(),
        0,
        "one view detached its peer's physical session"
    );
    assert_eq!(std::fs::read(&second_path)?, b"second");
    second.stop_with_context(&crate::native_mount::NativeSessionStopWork::AcceptedDrop)?;
    assert_eq!(
        physical.pointer(),
        0,
        "last view retained the physical session"
    );
    Ok(())
}

fn native_provider_exit_closes_admission(keep_provider_running: bool) -> TestResult {
    let (source, _, _) = checkout_context(MountPublication::Manual)?;
    let (late_source, _, _) = checkout_context(MountPublication::Manual)?;
    crate::native_mount::initialize_native_mount_services()?;
    acyclic_native_runtime::initialize_native_io_services()?;
    let namespace = tempfile::tempdir()?;
    let admitted = crate::hold_root(namespace.path())?;
    let name = crate::native_mount::shared_view_destination(Some(&admitted))?;
    let mut view = start_task_view(name.path(), source)?;
    let physical = Arc::clone(&view.driver.as_ref().ok_or("driver missing")?.unmount);
    if !keep_provider_running {
        physical.interrupt(None)?;
        let mut state = physical.state();
        while !state.loop_finished {
            state = physical
                .changed
                .wait(state)
                .unwrap_or_else(PoisonError::into_inner);
        }
        assert_eq!(
            physical.pointer(),
            0,
            "exited provider retained its native owner"
        );
    }
    // Keep the original view alive, so acquisition finds this exact instance
    // instead of legitimately starting a replacement after its last observer.
    let late = crate::native_mount::shared_view_destination(Some(&admitted))?;
    let outcome = start_task_view(late.path(), late_source);
    assert!(outcome.is_err(), "dead provider admitted a view");
    drop(outcome);
    view.stop_with_context(&crate::native_mount::NativeSessionStopWork::AcceptedDrop)?;
    Ok(())
}

#[test]
fn real_provider_exit_refuses_new_views_while_existing_views_retain_the_instance() -> TestResult {
    native_provider_exit_closes_admission(false)
}

#[test]
#[should_panic(expected = "dead provider admitted a view")]
fn provider_exit_negative_control_keeps_the_real_provider_running() {
    native_provider_exit_closes_admission(true).expect("real native provider fixture");
}
