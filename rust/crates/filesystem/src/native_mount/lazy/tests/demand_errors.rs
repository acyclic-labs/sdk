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
    use crate::native_mount::outcome::{NativeFailure, source_failure};
    for lazy in [false, true] {
        for (cause, expected) in [
            (DemandError::Cancelled, NativeFailure::Cancelled),
            (DemandError::StaleSource, NativeFailure::Stale),
            (DemandError::StaleVersion, NativeFailure::Stale),
            (DemandError::StaleCursor, NativeFailure::Stale),
            (DemandError::Absent, NativeFailure::NotFound),
            (DemandError::NotDirectory, NativeFailure::NotDirectory),
            (
                DemandError::Work(crate::WorkError::Overflow),
                NativeFailure::Io,
            ),
            (
                DemandError::Mount(Box::new(MountSourceError::AccessDenied)),
                NativeFailure::Denied,
            ),
        ] {
            let converted = convert(cause, lazy);
            assert_eq!(source_failure(&converted), expected, "{converted:?}");
            let callback = NativeMountError::Source(converted);
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
                    NativeFailure::Cancelled => 995,
                    NativeFailure::Stale => 1006,
                    NativeFailure::NotFound => 2,
                    NativeFailure::NotDirectory => 267,
                    NativeFailure::Denied => 5,
                    NativeFailure::Io => 1117,
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
    for lazy in [false, true] {
        // Capture the actual host failure; no fabricated OS-tool fixture.
        let error = std::fs::File::open(&absent).expect_err("fixture path is absent");
        let code = error.raw_os_error().ok_or("host open has no native code")?;
        let converted = convert(DemandError::Io(error), lazy);
        assert_eq!(
            crate::native_mount::outcome::source_failure(&converted),
            crate::native_mount::outcome::NativeFailure::Os(code)
        );
        assert!(
            matches!(converted, MountSourceError::Io(error, None) if error.raw_os_error() == Some(code))
        );
    }
    Ok(())
}

#[test]
fn demand_round_trips_do_not_nest_transports_or_allocate() {
    for cause in [
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
        crate::native_mount::outcome::source_failure(&retained),
        crate::native_mount::outcome::NativeFailure::Os(code),
        "capture I/O must keep its actual host error"
    );
    assert!(
        matches!(retained, MountSourceError::Io(error, None) if error.raw_os_error() == Some(code))
    );
    assert_eq!(allocations.count_total, 0);
    Ok(())
}
