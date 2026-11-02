use super::*;
use crate::native_mount::{MountSourceError, NativeMountError};

fn check_callback(error: &NativeMountError, expected: CallbackFailure) {
    assert_eq!(callback_failure(error), expected, "{error:?}");
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    assert_eq!(crate::native_mount::callback_errno(error), expected.errno());
    #[cfg(target_os = "linux")]
    assert_eq!(
        crate::native_mount::fuse::callback_errno(error),
        expected.errno()
    );
    #[cfg(target_os = "macos")]
    assert_eq!(
        crate::native_mount::darwin_mount::callback_errno(error),
        expected.errno()
    );
    #[cfg(windows)]
    assert_eq!(
        crate::native_mount::projfs::native_hresult(error),
        windows::core::HRESULT::from_win32(match expected {
            CallbackFailure::Cancelled => {
                windows::Win32::Foundation::ERROR_OPERATION_ABORTED.0
            }
            CallbackFailure::Stale => 1006,
            CallbackFailure::NotFound => 2,
            CallbackFailure::Denied => 5,
            CallbackFailure::Os(code) => u32::from_ne_bytes(code.to_ne_bytes()),
            _ => panic!("use an explicit Windows status for this contract"),
        })
    );
}

#[test]
fn namespace_causes_keep_their_semantics_through_source_and_workspace() {
    use crate::kernel::NamespacePathError as E;
    for (error, expected) in [
        (E::Cancelled, CallbackFailure::Cancelled),
        (E::AllocationFailed, CallbackFailure::OutOfMemory),
        (E::Work(crate::WorkError::Overflow), CallbackFailure::Io),
        (E::TooDeep, CallbackFailure::Invalid),
    ] {
        assert_eq!(
            source_failure(&MountSourceError::NamespacePath(error.clone())),
            expected
        );
        assert_eq!(
            workspace_failure(&crate::WorkspaceError::NamespacePath(error.clone())),
            expected
        );
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        check_callback(
            &NativeMountError::Source(MountSourceError::NamespacePath(error)),
            expected,
        );
    }
}

#[test]
fn typed_invocation_failures_are_terminal_in_every_transport() {
    use crate::kernel::NamespacePathError as E;
    for namespace in [
        E::Cancelled,
        E::AllocationFailed,
        E::Work(crate::WorkError::Overflow),
    ] {
        for source in [
            MountSourceError::NamespacePath(namespace.clone()),
            MountSourceError::WorkspaceCause(crate::WorkspaceError::NamespacePath(
                namespace.clone(),
            )),
        ] {
            assert!(stops_mount(&NativeMountError::Demand(
                crate::DemandError::Mount(Box::new(source))
            )));
        }
    }
    for cause in [
        crate::NativeTreeMaterializationError::Cancelled,
        crate::NativeTreeMaterializationError::OriginalWorkPoisoned,
        crate::NativeTreeMaterializationError::MissingCustody,
    ] {
        assert!(stops_mount(&NativeMountError::Materialization(cause)));
    }
}

#[test]
fn failed_completion_is_not_erased_by_a_reply_only_primary() {
    let failure = NativeMountError::Source(MountSourceError::MutationFinalization {
        primary: Box::new(Some(MountSourceError::NotFound)),
        cleanup: Box::new(Some(crate::DemandError::Cancelled)),
        custody: None,
    });
    assert_eq!(callback_failure(&failure), CallbackFailure::NotFound);
    assert!(stops_mount(&failure));
    #[cfg(target_os = "macos")]
    assert!(crate::native_mount::darwin_mount::fatal(&failure));
}

#[test]
fn workspace_causes_keep_their_semantics_through_source_and_demand() {
    for (workspace, expected) in [
        (crate::WorkspaceError::AccessDenied, CallbackFailure::Denied),
        (crate::WorkspaceError::NotFound, CallbackFailure::NotFound),
        (
            crate::WorkspaceError::Filesystem(crate::FsError::NotFound),
            CallbackFailure::NotFound,
        ),
    ] {
        let source = MountSourceError::WorkspaceCause(workspace);
        assert_eq!(source_failure(&source), expected);
        check_callback(
            &NativeMountError::Demand(crate::DemandError::Mount(Box::new(source))),
            expected,
        );
    }
}

#[test]
fn real_host_error_keeps_its_native_code_through_every_wrapper()
-> Result<(), Box<dyn std::error::Error>> {
    // This fixture is captured from an actual host open, never a hand-written
    // errno/HRESULT. Each transport owns the original error returned by open.
    let root = tempfile::tempdir()?;
    let absent = root.path().join("not-created");
    let mut native_code = None;
    for transport in 0..4 {
        let error = std::fs::File::open(&absent).expect_err("fixture path is absent");
        let code = error.raw_os_error().ok_or("host open has no native code")?;
        assert_eq!(*native_code.get_or_insert(code), code);
        let error = match transport {
            0 => NativeMountError::Io(error),
            1 => NativeMountError::Source(MountSourceError::Io(error, None)),
            2 => NativeMountError::Demand(crate::DemandError::Mount(Box::new(
                MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Io(error)),
            ))),
            _ => NativeMountError::Source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(
                    crate::NativeTreeMaterializationError::Io(error),
                )),
            )),
        };
        check_callback(&error, CallbackFailure::Os(code));
    }
    Ok(())
}

/// Every way a cancellation reaches a callback is `Cancelled`, and only
/// staleness is `Stale`, on every OS; each OS spells both apart.
#[test]
fn cancellation_and_staleness_are_classified_once() {
    let cancelled = [
        NativeMountError::Demand(crate::DemandError::Cancelled),
        NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled),
        NativeMountError::Source(MountSourceError::Cancelled),
        NativeMountError::Source(MountSourceError::Filesystem(crate::FsError::Cancelled(
            crate::CancellationError,
        ))),
        NativeMountError::Source(MountSourceError::NativeWork(
            crate::NativeTreeMaterializationError::Cancelled,
        )),
        NativeMountError::Demand(crate::DemandError::Mount(Box::new(
            MountSourceError::Cancelled,
        ))),
        NativeMountError::Source(MountSourceError::WorkspaceCause(
            crate::WorkspaceError::Cancelled(crate::CancellationError),
        )),
        NativeMountError::Source(MountSourceError::WorkspaceCause(
            crate::WorkspaceError::Filesystem(crate::FsError::NativeKernel(
                crate::NativeTreeMaterializationError::Cancelled,
            )),
        )),
        NativeMountError::Source(MountSourceError::MutationFinalization {
            primary: Box::new(Some(MountSourceError::Cancelled)),
            cleanup: Box::new(None),
            custody: None,
        }),
    ];
    for error in &cancelled {
        assert_eq!(
            callback_failure(error),
            CallbackFailure::Cancelled,
            "{error:?}"
        );
    }
    let stale = [
        NativeMountError::Source(MountSourceError::Stale),
        NativeMountError::Source(MountSourceError::Filesystem(
            crate::FsError::SourceSnapshotStale,
        )),
        NativeMountError::Source(MountSourceError::WorkspaceCause(
            crate::WorkspaceError::StaleSource,
        )),
        NativeMountError::Source(MountSourceError::WorkspaceCause(
            crate::WorkspaceError::StaleIdentity,
        )),
        NativeMountError::Source(MountSourceError::WorkspaceCause(
            crate::WorkspaceError::Filesystem(crate::FsError::SourceSnapshotStale),
        )),
    ];
    for error in &stale {
        assert_eq!(callback_failure(error), CallbackFailure::Stale, "{error:?}");
    }
    #[cfg(any(target_os = "linux", target_os = "macos"))]
    {
        assert_eq!(CallbackFailure::Cancelled.errno(), libc::ECANCELED);
        assert_eq!(CallbackFailure::Stale.errno(), libc::ESTALE);
    }
    // ERROR_OPERATION_ABORTED and ERROR_FILE_INVALID.
    #[cfg(windows)]
    {
        assert_eq!(CallbackFailure::Cancelled.hresult().0 & 0xffff, 995);
        assert_eq!(CallbackFailure::Stale.hresult().0 & 0xffff, 1006);
    }
}

/// One outcome sequence through every backend's adapter: each answers
/// its kernel with the spelling of the same classification, and stops
/// its mount on exactly the same outcomes. FUSE and NFS answer through
/// `callback_errno`, `ProjFS` through `native_hresult` and
/// `source_hresult`; FUSE's `finish`, the NFS `fatal` rule and `ProjFS`'s
/// notification rule are `stops_mount` and `source_stops_mount`.
#[test]
fn every_backend_adapter_answers_one_outcome_sequence_alike() {
    use CallbackFailure::{
        Cancelled, Denied, Invalid, Io, NotFound, OutOfMemory, ReadOnly, Stale, Unsupported,
    };
    let source = |error| NativeMountError::Source(error);
    let sequence: [(NativeMountError, CallbackFailure, bool); 15] = [
        (source(MountSourceError::NotFound), NotFound, false),
        (source(MountSourceError::AccessDenied), Denied, false),
        (source(MountSourceError::ReadOnly), ReadOnly, false),
        (
            source(MountSourceError::Invalid("bad".into())),
            Invalid,
            false,
        ),
        (
            source(MountSourceError::Unsupported("no".into())),
            Unsupported,
            false,
        ),
        (source(MountSourceError::Stale), Stale, false),
        (
            source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::AccessDenied,
            )),
            Denied,
            false,
        ),
        (
            source(MountSourceError::Filesystem(
                crate::FsError::SourceSnapshotStale,
            )),
            Stale,
            false,
        ),
        (source(MountSourceError::Cancelled), Cancelled, true),
        (
            source(MountSourceError::WorkspaceCause(
                crate::WorkspaceError::StaleSource,
            )),
            Stale,
            false,
        ),
        (
            NativeMountError::Demand(crate::DemandError::Cancelled),
            Cancelled,
            true,
        ),
        (
            NativeMountError::Materialization(crate::NativeTreeMaterializationError::Cancelled),
            Cancelled,
            true,
        ),
        (
            NativeMountError::Io(std::io::Error::other("host")),
            Io,
            false,
        ),
        (NativeMountError::ControlPending, Io, true),
        (
            NativeMountError::Allocation(Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err()),
            OutOfMemory,
            true,
        ),
    ];
    for (error, expected, stops) in &sequence {
        assert_eq!(callback_failure(error), *expected, "{error:?}");
        assert_eq!(stops_mount(error), *stops, "{error:?}");
        #[cfg(any(target_os = "linux", target_os = "macos"))]
        assert_eq!(
            super::super::super::callback_errno(error),
            expected.errno(),
            "{error:?}"
        );
        #[cfg(windows)]
        assert_eq!(
            super::super::super::projfs::native_hresult(error),
            expected.hresult(),
            "{error:?}"
        );
        if let NativeMountError::Source(error) = error {
            assert_eq!(source_stops_mount(error), *stops, "{error:?}");
            #[cfg(windows)]
            assert_eq!(
                super::super::super::projfs::source_hresult(error),
                expected.hresult(),
                "{error:?}"
            );
        }
    }
}
