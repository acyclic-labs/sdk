use super::*;

fn convert(error: DemandError, lazy: bool) -> MountSourceError {
    if lazy {
        super::super::metadata::lazy_error(LazyWorkspaceError::Demand(error))
    } else {
        error.into()
    }
}

#[test]
fn demand_causes_keep_their_type_without_a_diagnostic_allocation() {
    use std::error::Error;
    for lazy in [false, true] {
        for cause in [
            DemandError::StaleSource,
            DemandError::StaleVersion,
            DemandError::StaleCursor,
            DemandError::NativeWorkUnavailable,
            DemandError::OriginalWorkPoisoned,
            DemandError::WorkerUnavailable,
            DemandError::SourceUnavailable,
            DemandError::CursorBusy,
            DemandError::NotRegularFile,
            DemandError::InvalidRequest,
        ] {
            let expected = std::mem::discriminant(&cause);
            let mut converted = None;
            let allocations = acyclic_test_support::measure(|| {
                converted = Some(convert(cause, lazy));
            });
            let converted = converted.expect("conversion completed");
            let retained = converted
                .source()
                .and_then(|error| error.downcast_ref::<DemandError>())
                .expect("original demand cause must remain typed");
            assert_eq!(std::mem::discriminant(retained), expected);
            assert_eq!(allocations.count_total, 0, "{converted:?}");
        }
    }
}

#[cfg(feature = "native-watch")]
#[test]
fn observation_causes_keep_their_original_box_without_allocating() {
    for lazy in [false, true] {
        let original = Box::new(crate::watch::NativeWatchError::BoundedObservationUnavailable);
        let identity = std::ptr::from_ref(original.as_ref());
        let mut converted = None;
        let allocations = acyclic_test_support::measure(|| {
            converted = Some(convert(DemandError::Observation(original), lazy));
        });
        let converted = converted.expect("conversion completed");
        let MountSourceError::Demand(DemandError::Observation(retained)) = &converted else {
            panic!("original observation cause must remain typed: {converted:?}");
        };
        assert_eq!(std::ptr::from_ref(retained.as_ref()), identity);
        assert_eq!(allocations.count_total, 0);
    }
}

#[test]
fn lazy_and_direct_demand_conversion_use_the_same_callback_semantics() {
    use crate::native_mount::shared::outcome::{CallbackFailure, callback_failure, stops_mount};
    for transport in 0..3 {
        for (cause, expected, terminal) in [
            (DemandError::Cancelled, CallbackFailure::Cancelled, true),
            #[cfg(feature = "native-watch")]
            (
                DemandError::Observation(Box::new(crate::watch::NativeWatchError::Cancelled(
                    crate::CancellationError,
                ))),
                CallbackFailure::Cancelled,
                true,
            ),
            (DemandError::StaleSource, CallbackFailure::Stale, false),
            (DemandError::StaleVersion, CallbackFailure::Stale, false),
            (DemandError::StaleCursor, CallbackFailure::Stale, false),
            (DemandError::Absent, CallbackFailure::NotFound, false),
            (
                DemandError::NotDirectory,
                CallbackFailure::NotDirectory,
                false,
            ),
            (DemandError::CursorBusy, CallbackFailure::Io, false),
            (
                DemandError::NativeWorkUnavailable,
                CallbackFailure::Io,
                true,
            ),
            (
                DemandError::Allocation(Vec::<u8>::new().try_reserve(usize::MAX).unwrap_err()),
                CallbackFailure::OutOfMemory,
                true,
            ),
            #[cfg(feature = "native-watch")]
            (
                DemandError::Observation(Box::new(
                    crate::watch::NativeWatchError::BoundedObservationUnavailable,
                )),
                CallbackFailure::Io,
                true,
            ),
            (
                DemandError::Work(crate::WorkError::Overflow),
                CallbackFailure::Io,
                true,
            ),
            (
                DemandError::Mount(Box::new(MountSourceError::AccessDenied)),
                CallbackFailure::Denied,
                false,
            ),
        ] {
            let callback = if transport < 2 {
                NativeMountError::Source(convert(cause, transport == 1))
            } else {
                NativeMountError::Demand(cause)
            };
            let allocations = acyclic_test_support::measure(|| {
                assert_eq!(callback_failure(&callback), expected, "{callback:?}");
                assert_eq!(stops_mount(&callback), terminal, "{callback:?}");
            });
            assert_eq!(allocations.count_total, 0);
            #[cfg(any(target_os = "linux", target_os = "macos"))]
            assert_eq!(
                crate::native_mount::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(target_os = "linux")]
            assert_eq!(
                crate::native_mount::fuse::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(target_os = "macos")]
            assert_eq!(
                crate::native_mount::darwin_mount::callback_errno(&callback),
                expected.errno()
            );
            #[cfg(windows)]
            {
                let code = match expected {
                    CallbackFailure::Cancelled => 995,
                    CallbackFailure::Stale => 1006,
                    CallbackFailure::NotFound => 2,
                    CallbackFailure::NotDirectory => 267,
                    CallbackFailure::Denied => 5,
                    CallbackFailure::OutOfMemory => 8,
                    CallbackFailure::Io => 1117,
                    _ => unreachable!("fixture uses explicit Windows statuses"),
                };
                assert_eq!(
                    crate::native_mount::projfs::native_hresult(&callback),
                    windows::core::HRESULT::from_win32(code)
                );
            }
        }
    }
}

#[test]
fn real_demand_io_keeps_its_host_code_through_both_conversions()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let absent = root.path().join("not-created");
    let transports = if cfg!(feature = "native-watch") { 6 } else { 3 };
    for transport in 0..transports {
        // Capture the actual host failure; no fabricated OS-tool fixture.
        let error = std::fs::File::open(&absent).expect_err("fixture path is absent");
        let code = error.raw_os_error().ok_or("host open has no native code")?;
        let cause = match transport / 3 {
            #[cfg(feature = "native-watch")]
            1 => DemandError::Observation(Box::new(crate::watch::NativeWatchError::Io(error))),
            _ => DemandError::Io(error),
        };
        let callback = if transport % 3 < 2 {
            NativeMountError::Source(convert(cause, transport % 3 == 1))
        } else {
            NativeMountError::Demand(cause)
        };
        assert_eq!(
            crate::native_mount::shared::outcome::callback_failure(&callback),
            crate::native_mount::shared::outcome::CallbackFailure::Os(code)
        );
        let retained = match &callback {
            NativeMountError::Source(MountSourceError::Io(error, None))
            | NativeMountError::Demand(DemandError::Io(error)) => error,
            #[cfg(feature = "native-watch")]
            NativeMountError::Demand(DemandError::Observation(error)) => match error.as_ref() {
                crate::watch::NativeWatchError::Io(error) => error,
                _ => panic!("original OS cause was replaced: {callback:?}"),
            },
            _ => panic!("original OS cause was replaced: {callback:?}"),
        };
        assert_eq!(retained.raw_os_error(), Some(code));
        assert!(!crate::native_mount::shared::outcome::stops_mount(
            &callback
        ));
    }
    Ok(())
}

#[test]
fn demand_round_trips_do_not_nest_transports_or_allocate() {
    for cause in [
        DemandError::StaleSource,
        DemandError::StaleVersion,
        DemandError::StaleCursor,
        DemandError::NativeWorkUnavailable,
        DemandError::OriginalWorkPoisoned,
        DemandError::WorkerUnavailable,
        DemandError::SourceUnavailable,
        DemandError::CursorBusy,
        DemandError::NotRegularFile,
        DemandError::InvalidRequest,
    ] {
        let expected = std::mem::discriminant(&cause);
        let mut retained = Some(cause);
        let allocations = acyclic_test_support::measure(|| {
            for lazy in [false, true] {
                let mounted = convert(retained.take().expect("cause retained"), lazy);
                retained = Some(DemandError::from(mounted));
            }
        });
        assert_eq!(
            std::mem::discriminant(&retained.expect("cause returned")),
            expected,
            "reverse demand transport must unwrap its original cause"
        );
        assert_eq!(allocations.count_total, 0);
    }
}

#[cfg(feature = "native-watch")]
#[test]
fn observation_round_trip_returns_the_original_box() {
    let original = Box::new(crate::watch::NativeWatchError::BoundedObservationUnavailable);
    let identity = std::ptr::from_ref(original.as_ref());
    let mut retained = None;
    let allocations = acyclic_test_support::measure(|| {
        retained = Some(DemandError::from(convert(
            DemandError::Observation(original),
            true,
        )));
    });
    let Some(DemandError::Observation(retained)) = retained else {
        panic!("reverse observation transport must return its original box");
    };
    assert_eq!(std::ptr::from_ref(retained.as_ref()), identity);
    assert_eq!(allocations.count_total, 0);
}

#[test]
fn mount_work_and_cancellation_use_the_shared_demand_conversion() {
    for cause in [
        MountSourceError::Work(crate::WorkError::Overflow),
        MountSourceError::Cancelled,
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Work(
            crate::WorkError::Overflow,
        )),
        MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled),
    ] {
        let cancelled = matches!(
            cause,
            MountSourceError::Cancelled
                | MountSourceError::NativeWork(crate::NativeTreeMaterializationError::Cancelled)
        );
        let mut retained = None;
        let allocations = acyclic_test_support::measure(|| {
            retained = Some(DemandError::from(cause));
        });
        let retained = retained.expect("conversion completed");
        assert_eq!(
            matches!(retained, DemandError::Cancelled),
            cancelled,
            "all demand callers must share cancellation semantics"
        );
        assert!(
            matches!(
                retained,
                DemandError::Cancelled | DemandError::Work(crate::WorkError::Overflow)
            ),
            "all demand callers must share work semantics"
        );
        assert_eq!(allocations.count_total, 0);
    }
}

#[test]
fn capture_io_keeps_the_actual_host_error_without_formatting()
-> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    // Capture the actual host error before measuring the transport itself.
    let error =
        std::fs::File::open(root.path().join("not-created")).expect_err("fixture path is absent");
    let code = error.raw_os_error().ok_or("host open has no native code")?;
    let mut retained = None;
    let allocations = acyclic_test_support::measure(|| {
        retained = Some(crate::native_mount::adapter::capture_io_error(error));
    });
    let retained = retained.expect("conversion completed");
    assert_eq!(
        crate::native_mount::shared::outcome::source_failure(&retained),
        crate::native_mount::shared::outcome::CallbackFailure::Os(code),
        "capture I/O must keep its actual host error"
    );
    assert!(
        matches!(retained, MountSourceError::Io(error, None) if error.raw_os_error() == Some(code))
    );
    assert_eq!(allocations.count_total, 0);
    Ok(())
}
