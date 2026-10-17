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

fn remount(
    destination: &Path,
    kernel: &kernel::Kernel,
) -> Result<MountDestinationGuard, NativeMountError> {
    let work = NativeMaterializationWork::new(
        crate::WorkBudget::UNBOUNDED,
        &crate::CancellationToken::new(),
    )
    .map_err(|(error, _)| NativeMountError::from(error))?;
    let mut guard = None;
    // The startup path's actual acquisition, including registry, physical fence
    // files and work accounting. Only the subsequent mount syscall is fake.
    MountDestinationGuard::acquire_with_work(destination, true, &work, &mut guard, false)?;
    kernel.mount();
    guard.ok_or(NativeMountError::ControlPending)
}

fn scenario(
    destination: &Path,
    base: rustix::fs::Statx,
    helper_stat: rustix::fs::Stat,
    broken: bool,
) {
    let kernel = kernel::Kernel::install(base, helper_stat, broken);
    let pinned = linux::Pinned::open(destination).unwrap().unwrap();
    let peer_kernel = kernel.clone();
    let peer_destination = destination.to_path_buf();
    let peer = loom::thread::spawn(move || {
        peer_kernel.wait_for_recovery_fence();
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
    // remount-first order, then explore all syscall interleavings after the
    // recovery-first acquisition, including real owner/worker retirement.
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
    let positive_destination = destination.clone();
    check_case("recovery-first", move || {
        scenario(&positive_destination, base, helper_stat, false)
    });
    let error = std::panic::catch_unwind(move || {
        check_case("early-release", move || {
            scenario(&destination, base, helper_stat, true)
        })
    })
    .expect_err("early release of the real fence was accepted");
    let message = error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(
        message.contains("helper detached a live remount after early fence release")
            || message.contains("remount acquired the fence before helper exit"),
        "{message}"
    );
}
