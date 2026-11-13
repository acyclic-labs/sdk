use super::*;

fn shared_callbacks_keep_their_view(substitute_handle: bool) -> TestResult {
    use super::super::file_ops::acyclic_fs_darwin_mount_fsync;
    use super::super::requests::CallbackTarget;
    use super::super::shared::SharedViews;

    let (first_source, _, first_work) = checkout_context(MountPublication::Manual)?;
    let (second_source, _, second_work) = checkout_context(MountPublication::Manual)?;
    let root = tempfile::tempdir()?;
    let views = SharedViews::new(root.path(), &first_work)?;
    let mut first = views.insert(
        b"first".to_vec(),
        first_source.clone(),
        true,
        root.path(),
        &first_work,
    )?;
    let second = views.insert(
        b"second".to_vec(),
        second_source.clone(),
        true,
        root.path(),
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
