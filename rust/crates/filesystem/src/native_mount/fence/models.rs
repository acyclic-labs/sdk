use super::*;

fn cleanup_after_contended_registry(
    destination: &Path,
    base: rustix::fs::Statx,
    helper_stat: rustix::fs::Stat,
    broken: bool,
) {
    let kernel =
        super::super::disconnected::kernel::Kernel::install(base, helper_stat, false, false);
    let state = destination
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("state");
    kernel.set_registry_directory(state);
    if broken {
        kernel.disable_registry_wait();
    }
    let work = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| error)
    .unwrap();
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(destination, false, &work, &mut guard, false).unwrap();
    let mut guard = guard.unwrap();
    let (fence, registry) = MountDestinationGuard::paths_for_canonical(destination).unwrap();
    let registry: MountLockFile = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(registry)
        .unwrap()
        .into();
    // A distinct starter owns the registry before retirement begins. Closing
    // this actual descriptor wakes the same kernel wait queue used by cleanup.
    fence_io::try_lock(&registry).unwrap();
    let before = work.receipt();
    let retiring = loom::thread::spawn(move || {
        guard.release_cleanup(None).unwrap();
        assert!(guard.file.is_none());
        assert_eq!(
            work.receipt().backend_write_operations,
            before.backend_write_operations + 3,
            "contention must use exactly lock, unlink and unlock"
        );
    });
    loom::thread::yield_now();
    drop(registry);
    retiring.join().unwrap();
    assert!(!fence.exists(), "cleanup left an unowned destination fence");
}

#[test]
fn registry_contention_retires_with_original_credit_and_missing_wait_control() {
    registry::initialize().unwrap();
    let temporary = tempfile::tempdir().unwrap();
    let checkout = temporary.path().join("checkout");
    std::fs::create_dir(&checkout).unwrap();
    std::fs::create_dir(temporary.path().join("state")).unwrap();
    let destination = checkout.join("mount");
    std::fs::create_dir(&destination).unwrap();
    let base = rustix::fs::statx(
        rustix::fs::CWD,
        &destination,
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW | rustix::fs::AtFlags::STATX_DONT_SYNC,
        rustix::fs::StatxFlags::TYPE | rustix::fs::StatxFlags::MNT_ID,
    )
    .unwrap();
    let helper_stat = rustix::fs::stat(std::env::current_exe().unwrap()).unwrap();
    let candidate = destination.clone();
    loom::model(move || {
        cleanup_after_contended_registry(&candidate, base, helper_stat, false);
    });
    rejected(
        move || {
            loom::model(move || {
                cleanup_after_contended_registry(&destination, base, helper_stat, true);
            })
        },
        "mount destination is already owned by another process",
    );
}

fn admission(file: MountLockFile, active: loom::sync::Arc<loom::sync::Mutex<u8>>) {
    fence_io::lock(&file).unwrap();
    {
        let mut active = active.lock().unwrap();
        *active += 1;
        assert_eq!(*active, 1, "registry inode split concurrent admission");
    }
    loom::thread::yield_now();
    *active.lock().unwrap() -= 1;
    fence_io::unlock(&file).unwrap();
}

#[test]
fn retirement_preserves_registry_identity_for_pending_and_new_starters() {
    let temporary = tempfile::tempdir().unwrap();
    let checkout = temporary.path().join("checkout");
    std::fs::create_dir(&checkout).unwrap();
    let state = temporary.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let destination = checkout.join("mount");
    std::fs::create_dir(&destination).unwrap();
    let base = rustix::fs::statx(
        rustix::fs::CWD,
        &destination,
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW | rustix::fs::AtFlags::STATX_DONT_SYNC,
        rustix::fs::StatxFlags::TYPE | rustix::fs::StatxFlags::MNT_ID,
    )
    .unwrap();
    let helper_stat = rustix::fs::stat(std::env::current_exe().unwrap()).unwrap();
    let scenario = move |broken| {
        let kernel =
            super::super::disconnected::kernel::Kernel::install(base, helper_stat, false, false);
        kernel.set_registry_directory(state.clone());
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)
        .unwrap();
        let mut guard = None;
        MountDestinationGuard::acquire_with_work(&destination, false, &work, &mut guard, false)
            .unwrap();
        // This opener retains the old inode before the final mount retires.
        let pending = registry::open(&mut &work).unwrap();
        guard.as_mut().unwrap().release_cleanup(None).unwrap();
        drop(guard);
        if broken {
            // Restore the old Harness cleanup of the exact registry file.
            std::fs::remove_file(registry::path().unwrap()).unwrap();
        }
        let fresh = registry::open(&mut &work).unwrap();
        let active = loom::sync::Arc::new(loom::sync::Mutex::new(0));
        let peer = {
            let active = active.clone();
            loom::thread::spawn(move || admission(pending, active))
        };
        admission(fresh, active.clone());
        peer.join().unwrap();
        assert_eq!(*active.lock().unwrap(), 0);
    };
    let candidate = scenario.clone();
    loom::model(move || candidate(false));
    rejected(
        move || loom::model(move || scenario(true)),
        "registry inode split concurrent admission",
    );
}

fn rejected(control: impl FnOnce() + std::panic::UnwindSafe, expected: &str) {
    let error =
        std::panic::catch_unwind(control).expect_err("registry mutation escaped its oracle");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains(expected),
        "unexpected control failure: {message}"
    );
}
