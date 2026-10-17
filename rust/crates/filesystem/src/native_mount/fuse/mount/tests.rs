use super::*;
use proptest::prelude::*;
use rustix::net::ReturnFlags;
use std::{cell::Cell, sync::OnceLock};

std::thread_local! {
    static FORKS: Cell<usize> = const { Cell::new(0) };
    static FORCE_FORK: Cell<bool> = const { Cell::new(false) };
    static FORGET_PENDING: Cell<bool> = const { Cell::new(false) };
    static OLD_UNMOUNT: Cell<bool> = const { Cell::new(false) };
}
pub(super) fn forget_pending() -> bool {
    FORGET_PENDING.get()
}
pub(super) fn old_unmount() -> bool {
    OLD_UNMOUNT.get()
}
pub(super) fn force_fork() -> bool {
    FORCE_FORK.get()
}
unsafe extern "C" fn before_fork() {
    let _ = FORKS.try_with(|count| count.set(count.get() + 1));
}
struct Restore(&'static std::thread::LocalKey<Cell<bool>>, bool);
impl Drop for Restore {
    fn drop(&mut self) {
        self.0.set(self.1);
    }
}

fn real_helper_spawn_never_forks_at_n_and_two_n_and_pre_exec_fails_that_bound() -> io::Result<()> {
    static REGISTERED: OnceLock<i32> = OnceLock::new();
    // SAFETY: the parent-side callback only touches constant-initialized TLS.
    // No child callback, shared mutable state or allocator is involved.
    assert_eq!(
        *REGISTERED.get_or_init(|| unsafe { libc::pthread_atfork(Some(before_fork), None, None) }),
        0
    );
    let diagnostic = File::from(rustix::fs::memfd_create(
        c"fork-test",
        rustix::fs::MemfdFlags::CLOEXEC,
    )?);
    for count in [1, 2] {
        for broken in [false, true] {
            let restore = Restore(&FORCE_FORK, FORCE_FORK.replace(broken));
            FORKS.set(0);
            for _ in 0..count {
                let (receive, child) = UnixStream::pair()?;
                let mut process = spawn_mount_helper(
                    c"/usr/bin/true",
                    Path::new("/unused"),
                    "unused",
                    child,
                    &diagnostic,
                )?;
                assert!(process.wait()?.success());
                drop(receive);
            }
            drop(restore);
            let forks = FORKS.get();
            assert_eq!(forks, if broken { count } else { 0 });
            assert_eq!(
                std::panic::catch_unwind(|| assert_eq!(forks, 0)).is_ok(),
                !broken
            );
        }
    }
    Ok(())
}

fn descriptor() -> OwnedFd {
    File::open("/dev/null").unwrap().into()
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Normal unmount happens once; only permission refusal admits the helper.
    // Preserve the complete errno and ordered calls against the original rule.
    #[test]
    fn unmount_policy_preserves_refusal_and_only_delegates_permission(
        kernel in proptest::option::of(proptest::sample::select(vec![
            nix::errno::Errno::EPERM, nix::errno::Errno::EBUSY,
            nix::errno::Errno::EINTR, nix::errno::Errno::EINVAL,
            nix::errno::Errno::EIO,
        ])), helper_fails in any::<bool>(),
    ) {
        let normalize = |result: io::Result<()>| result.map_err(|error|
            (error.kind(), error.raw_os_error(), error.to_string()));
        for broken in [false, true] {
            let _restore = Restore(&OLD_UNMOUNT, OLD_UNMOUNT.replace(broken));
            let trace = std::cell::RefCell::new(Vec::new());
            let actual = unmount_with(|flags| {
                trace.borrow_mut().push((0, flags.bits()));
                kernel.map_or(Ok(()), Err)
            }, || {
                trace.borrow_mut().push((1, 0));
                if helper_fails { Err(nix::errno::Errno::EACCES.into()) } else { Ok(()) }
            });
            let mut expected_trace = vec![(0, 0)];
            let expected = match kernel {
                None => Ok(()),
                Some(nix::errno::Errno::EPERM) => {
                    expected_trace.push((1, 0));
                    if helper_fails { Err(io::Error::from_raw_os_error(libc::EACCES)) } else { Ok(()) }
                }
                Some(error) => Err(io::Error::from_raw_os_error(error as i32)),
            };
            let equal = normalize(actual) == normalize(expected) && trace.into_inner() == expected_trace;
            prop_assert_eq!(std::panic::catch_unwind(|| assert!(equal)).is_ok(), !broken);
        }
    }
    #[test]
    fn reply_requires_one_descriptor_nonempty_payload_and_complete_control(count in 0usize..5, bytes in 0usize..6, truncated in any::<bool>()) {
        let flags = if truncated { ReturnFlags::CTRUNC } else { ReturnFlags::empty() };
        let files = (0..count).map(|_| descriptor()).collect::<Vec<_>>();
        let result = reply::one_device(bytes, flags, files.into_iter());
        prop_assert_eq!(result.is_ok(), count == 1 && bytes > 0 && !truncated);
        if let Err(error) = result { prop_assert_eq!(error.kind(), io::ErrorKind::InvalidData); }
    }
}

#[test]
fn helper_spawn_and_socket_custody() -> io::Result<()> {
    // The deliberately broken fork duplicates every thread's open descriptors
    // until exec. Finish that control before opening the custody oracle's flock
    // descriptions, so the control itself cannot keep a rejected fd alive.
    real_helper_spawn_never_forks_at_n_and_two_n_and_pre_exec_fails_that_bound()?;
    for count in 0u8..5 {
        for length in [0, 1, 5, 32] {
            let mut input = vec![count];
            input.resize(length + 1, b'x');
            reply::check::check(&input);
        }
    }
    proptest::test_runner::TestRunner::new(ProptestConfig::with_cases(64))
        .run(&proptest::collection::vec(any::<u8>(), 0..40), |input| {
            reply::check::check(&input);
            Ok(())
        })
        .expect("descriptor roundtrip/custody property");
    Ok(())
}

#[test]
fn accepting_the_first_of_two_descriptors_fails_the_same_reply_property() {
    for broken in [false, true] {
        let run = || {
            reply::one_device(
                1,
                ReturnFlags::empty(),
                vec![descriptor(), descriptor()].into_iter(),
            )
        };
        let result = if broken {
            reply::accepting_extra(run)
        } else {
            run()
        };
        let rejected = result.is_err();
        assert_eq!(
            std::panic::catch_unwind(|| assert!(rejected)).is_ok(),
            !broken
        );
    }
}

fn retry_history(failures: &[bool], broken: bool) -> io::Result<bool> {
    let _restore = Restore(&FORGET_PENDING, FORGET_PENDING.replace(broken));
    let mut mount = Mount {
        pending: true,
        ..Mount::default()
    };
    let mut attempts = 0;
    let mut actual = Vec::new();
    let mut expected = Vec::new();
    // Always include an exited helper that returned no syscall error but did
    // not detach. Execute the whole history, even when the control diverges.
    for (index, fails) in failures.iter().copied().chain([false]).enumerate() {
        let result = mount.attempt_unmount(
            |mount| mount.observe_detachment(|| Ok(false)),
            |_| {
                attempts += 1;
                if fails {
                    Err(io::Error::from_raw_os_error(libc::EIO))
                } else {
                    Ok(())
                }
            },
        );
        let error = result
            .err()
            .map(|error| (error.kind(), error.raw_os_error(), error.to_string()));
        let expected_error = fails.then(|| {
            let error = io::Error::from_raw_os_error(libc::EIO);
            (error.kind(), error.raw_os_error(), error.to_string())
        });
        // The kernel has not signalled device destruction. The same production
        // transition consumes this external observation; helper exit is not it.
        let detached = mount.observe_detachment(|| Ok(false))?;
        actual.push((error, mount.pending(), attempts, detached));
        expected.push((expected_error, true, index + 1, false));
    }
    mount.attempt_unmount(
        |mount| mount.observe_detachment(|| Ok(true)),
        |_| panic!("detached device was unmounted again"),
    )?;
    assert!(mount.observe_detachment(|| Ok(true))?);
    assert!(!mount.pending());
    Ok(actual == expected && attempts == failures.len() + 1)
}

#[test]
fn unsuccessful_unmounts_remain_retryable_at_n_and_two_n() -> io::Result<()> {
    for count in [1, 2] {
        for broken in [false, true] {
            let equal = retry_history(&vec![false; count - 1], broken)?;
            assert_eq!(std::panic::catch_unwind(|| assert!(equal)).is_ok(), !broken);
        }
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Complete failed-helper histories preserve exact errors and retry admission
    // until an actual device signal retires the mount. Restore the original
    // premature clear in the production attempt path to reject the same property.
    #[test]
    fn helper_failure_histories_keep_retry_and_retire_only_on_device_signal(
        failures in prop::collection::vec(any::<bool>(), 0..8),
    ) {
        for broken in [false, true] {
            prop_assert_eq!(retry_history(&failures, broken).unwrap(), !broken);
        }
    }
}
