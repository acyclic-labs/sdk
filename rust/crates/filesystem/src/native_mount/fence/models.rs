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
    let cancellation = crate::CancellationToken::new();
    let work = NativeMaterializationWork::new(crate::WorkBudget::UNBOUNDED, &cancellation)
        .map_err(|(error, _)| error)
        .unwrap();
    let mut guard = None;
    MountDestinationGuard::acquire_with_work(destination, false, &work, &mut guard, false).unwrap();
    let mut guard = guard.unwrap();
    let (fence, registry) = MountDestinationGuard::paths_for_canonical(destination).unwrap();
    // A real failed unlink consumes the original transaction. Retry after
    // caller cancellation must replenish that same ledger before contending
    // with the starter; every following lock schedule remains exhaustive.
    let saved = fence.with_extension("retained");
    std::fs::rename(&fence, &saved).unwrap();
    std::fs::create_dir(&fence).unwrap();
    cancellation.cancel();
    assert!(guard.release_cleanup(None).is_err());
    std::fs::remove_dir(&fence).unwrap();
    std::fs::rename(saved, &fence).unwrap();
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

#[test]
fn threads_sharing_the_registry_descriptor_are_admitted_one_at_a_time() {
    let temporary = tempfile::tempdir().unwrap();
    let state = temporary.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let base = rustix::fs::statx(
        rustix::fs::CWD,
        temporary.path(),
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW | rustix::fs::AtFlags::STATX_DONT_SYNC,
        rustix::fs::StatxFlags::TYPE | rustix::fs::StatxFlags::MNT_ID,
    )
    .unwrap();
    let helper_stat = rustix::fs::stat(std::env::current_exe().unwrap()).unwrap();
    let scenario = move |broken| {
        let kernel =
            super::super::disconnected::kernel::Kernel::install(base, helper_stat, false, false);
        kernel.set_registry_directory(state.clone());
        if broken {
            // The flock alone: every thread shares the descriptor that owns it.
            kernel.disable_registry_admission();
        }
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)
        .unwrap();
        // One descriptor, as every mount in a process shares.
        let registry = loom::sync::Arc::new({
            let (directory, file) = registry::open_with_directory(&mut &work).unwrap();
            registry::Registry::new(directory, file)
        });
        let active = loom::sync::Arc::new(loom::sync::Mutex::new(0));
        let peer = {
            let (registry, active) = (registry.clone(), active.clone());
            let work = work.clone();
            loom::thread::spawn(move || shared_admission(&registry, &active, &work))
        };
        shared_admission(&registry, &active, &work);
        peer.join().unwrap();
    };
    let candidate = scenario.clone();
    loom::model(move || candidate(false));
    rejected(
        move || loom::model(move || scenario(true)),
        "a shared registry descriptor admitted two threads",
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

#[test]
fn shared_registry_descriptor_serializes_threads_before_the_kernel_lock() {
    registry::initialize().unwrap();
    let fixture = tempfile::tempdir().unwrap();
    let destination = fixture.path().join("checkout");
    std::fs::create_dir(&destination).unwrap();
    let state = fixture.path().join("state");
    std::fs::create_dir(&state).unwrap();
    let base = rustix::fs::statx(
        rustix::fs::CWD,
        &destination,
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW | rustix::fs::AtFlags::STATX_DONT_SYNC,
        rustix::fs::StatxFlags::TYPE | rustix::fs::StatxFlags::MNT_ID,
    )
    .unwrap();
    let helper = rustix::fs::stat(std::env::current_exe().unwrap()).unwrap();
    loom::model(move || {
        let kernel =
            super::super::disconnected::kernel::Kernel::install(base, helper, false, false);
        kernel.set_registry_directory(state.clone());
        let work = NativeMaterializationWork::new(
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .map_err(|(error, _)| error)
        .unwrap();
        let registry = registry::shared(&mut &work).unwrap();
        let active = loom::sync::Arc::new(loom::sync::Mutex::new(0_u8));
        let peer = {
            let registry = registry.clone();
            let active = active.clone();
            let work = work.clone();
            loom::thread::spawn(move || shared_admission(&registry, &active, &work))
        };
        shared_admission(&registry, &active, &work);
        peer.join().unwrap();
        assert_eq!(*active.lock().unwrap(), 0);
    });
}

fn shared_admission(
    registry: &registry::Registry,
    active: &loom::sync::Mutex<u8>,
    work: &NativeMaterializationWork,
) {
    let held = registry.lock(&mut &*work).unwrap();
    {
        let mut active = active.lock().unwrap();
        *active += 1;
        assert_eq!(
            *active, 1,
            "shared descriptor admitted concurrent registry owners"
        );
    }
    loom::thread::yield_now();
    *active.lock().unwrap() -= 1;
    held.unlock(&mut &*work).unwrap();
}
