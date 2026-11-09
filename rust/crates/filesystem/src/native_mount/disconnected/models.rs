use super::*;

fn check_case(name: &str, scenario: impl Fn() + Send + Sync + 'static) {
    let mut model = loom::model::Builder::new();
    // Each independent scenario needs its own saved schedule on replay.
    model.checkpoint_file = model.checkpoint_file.map(|path| path.with_extension(name));
    if model.log {
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_env("LOOM_LOG"))
            .with_test_writer()
            .without_time()
            .finish();
        tracing::subscriber::with_default(subscriber, || model.check(scenario));
    } else {
        model.check(scenario);
    }
}

#[inline(never)]
fn work() -> Result<Box<NativeMaterializationWork>, NativeMountError> {
    NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map(Box::new)
    .map_err(|(error, _)| NativeMountError::from(error))
}

// Keep independent model phases out of the caller's coroutine frame.
#[inline(never)]
fn remount(
    destination: &Path,
    kernel: &kernel::Kernel,
) -> Result<Box<Option<MountDestinationGuard>>, NativeMountError> {
    let work = work()?;
    // Recovery runs while this owner remains live. Keep both its construction
    // slot and return value off the coroutine stack, without releasing custody.
    let mut guard = Box::new(None);
    // The startup path's actual acquisition, including registry, physical fence
    // files and work accounting. Only the subsequent mount syscall is fake.
    MountDestinationGuard::acquire_with_work(destination, true, &work, &mut guard, false)?;
    kernel.mount();
    if guard.is_some() {
        Ok(guard)
    } else {
        Err(NativeMountError::ControlPending)
    }
}

#[inline(never)]
fn scenario(destination: &Path, base: rustix::fs::Statx, helper_stat: rustix::fs::Stat) {
    let kernel = kernel::Kernel::install(base, helper_stat, true);
    let pinned = linux::Pinned::open(destination).unwrap().unwrap();
    let peer_kernel = kernel.clone();
    let peer_destination = destination.to_path_buf();
    let peer = loom::thread::spawn(move || {
        peer_kernel.wait_for_helper();
        match remount(&peer_destination, &peer_kernel) {
            Ok(guard) => Some(guard),
            Err(NativeMountError::DestinationBusy) => None,
            Err(error) => panic!("{error}"),
        }
    });
    match recover(&pinned) {
        Ok(_) | Err(NativeMountError::DestinationBusy) => {}
        Err(error) => panic!("{error}"),
    }
    // The join result retains the mounted session's real fence until recovery
    // returns, without adding a test-only coordination lock.
    drop(peer.join().unwrap());
    let _remounted = remount(destination, &kernel).unwrap();
    kernel.assert_safe();
}

#[inline(never)]
fn remount_first(
    destination: &Path,
    base: rustix::fs::Statx,
    helper_stat: rustix::fs::Stat,
    retained: bool,
) {
    let kernel = kernel::Kernel::install(base, helper_stat, false);
    let pinned = linux::Pinned::open(destination).unwrap().unwrap();
    let mounted = remount(destination, &kernel).unwrap();
    if retained {
        assert!(matches!(
            recover(&pinned),
            Err(NativeMountError::DestinationBusy)
        ));
        drop(mounted);
    } else {
        drop(mounted);
        // Detection used the old disconnected descriptor. The real fenced
        // recheck must preserve the new mount after its fence is released.
        assert!(!recover(&pinned).unwrap());
    }
    kernel.wait_for_retirement();
    kernel.assert_safe();
}

#[test]
fn real_recovery_owner_keeps_fence_through_helper_exit_with_negative_control() {
    crate::native_mount::registry::initialize().unwrap();
    // These ordinary filesystem objects provide descriptor identities only;
    // connectivity, flock, process execution and mount syscalls are modeled.
    let root = tempfile::tempdir().unwrap();
    let destination = root.path().join("scratch");
    std::fs::create_dir(&destination).unwrap();
    let base = rustix::fs::statx(
        rustix::fs::CWD,
        &destination,
        rustix::fs::AtFlags::SYMLINK_NOFOLLOW | rustix::fs::AtFlags::STATX_DONT_SYNC,
        rustix::fs::StatxFlags::TYPE | rustix::fs::StatxFlags::MNT_ID,
    )
    .unwrap();
    let helper_stat = rustix::fs::stat(std::env::current_exe().unwrap()).unwrap();
    // Registry serialization admits exactly two acquisition orders. Check the
    // remount-first order, then check custody from the actual fence acquisition
    // through exit and explore competing admission over the helper interval.
    // These independent rejection/recheck invocations each have one real
    // retirement worker; their unrelated worker tails need no cross-product.
    for retained in [true, false] {
        let contended_destination = destination.clone();
        check_case(
            if retained {
                "busy-remount"
            } else {
                "stale-remount"
            },
            move || remount_first(&contended_destination, base, helper_stat, retained),
        );
    }
    check_case("recovery-first", move || {
        scenario(&destination, base, helper_stat)
    });
    // The kept recovery-early-release source mutation runs these same custody
    // assertions against the production ordering, without a test-only branch.
}
