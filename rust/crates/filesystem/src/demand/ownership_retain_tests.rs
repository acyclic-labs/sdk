use super::*;
use proptest::prelude::*;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Outcome {
    MissingAdmission,
    Overflow,
    Retained,
}

fn outcome(result: Result<(), DemandError>) -> Outcome {
    match result {
        Ok(()) => Outcome::Retained,
        Err(DemandError::NativeMountAdmissionMissing) => Outcome::MissingAdmission,
        Err(DemandError::Work(crate::WorkError::Overflow)) => Outcome::Overflow,
        Err(error) => panic!("unexpected retain cause: {error}"),
    }
}

fn check_outcomes(
    retain: fn(&NativeMountAllocationCustody, u64, &mut DemandWork) -> Result<(), DemandError>,
    adopted: u64,
    retained: u64,
    bytes: u64,
) {
    let original = NativeMaterializationWork::new(
        crate::performance::BOUNDARY_BUDGET,
        &crate::CancellationToken::new(),
    )
    .unwrap();
    for has_gauge in [false, true] {
        // Empty, foreign (same counter values, different identity), exact.
        for admission in 0..3 {
            let admitted = admission == 2;
            for (adopted, retained, bytes) in [
                (adopted, retained, bytes),
                (u64::MAX, 0, 1),
                (0, u64::MAX, 1),
                (0, 0, 1),
                (0, 0, 0),
            ] {
                for (has_original, available, live) in [
                    (false, None, false),
                    (true, None, false),
                    (true, Some(0), false),
                    (true, Some(0), true),
                    (true, Some(u64::MAX), true),
                ] {
                    let mut capture = available.map(|available| {
                        let capture =
                            super::super::CaptureTransientCustody::new_native(&original).unwrap();
                        capture.claim(available);
                        capture
                    });
                    let gauge = Arc::new(AtomicU64::new(retained));
                    let mut captured_original = original.clone();
                    captured_original.capture_origin = if live { capture.clone() } else { None };
                    if !live {
                        drop(capture.take());
                    }
                    let custody = NativeMountAllocationCustody {
                        bytes: has_gauge.then(|| gauge.clone()),
                        accounting_dirty: Arc::new(AtomicBool::new(false)),
                        original: has_original.then_some(captured_original),
                    };
                    let expected = if !has_gauge || !admitted {
                        Outcome::MissingAdmission
                    } else if adopted.checked_add(bytes).is_none()
                        || retained.checked_add(bytes).is_none()
                        || (has_original
                            && live
                            && available.is_some_and(|available| available < bytes))
                    {
                        Outcome::Overflow
                    } else {
                        Outcome::Retained
                    };
                    original.with_original_leaf(|work| {
                        assert!(work.roots.native_mount_admissions.0.is_empty());
                        if admission != 0 {
                            work.roots.native_mount_admissions.0.push((
                                if admitted {
                                    gauge.clone()
                                } else {
                                    Arc::new(AtomicU64::new(retained))
                                },
                                adopted,
                            ));
                        }
                        let actual = outcome(retain(&custody, bytes, work));
                        assert_eq!(actual, expected, "retain cause");
                        let successful = actual == Outcome::Retained;
                        assert_eq!(
                            gauge.load(Ordering::Acquire),
                            if successful {
                                retained + bytes
                            } else {
                                retained
                            }
                        );
                        if admission != 0 {
                            let (entry_gauge, after) =
                                work.roots.native_mount_admissions.0.pop().unwrap();
                            assert_eq!(after, if successful { adopted + bytes } else { adopted });
                            assert_eq!(
                                entry_gauge.load(Ordering::Acquire),
                                if successful {
                                    retained + bytes
                                } else {
                                    retained
                                }
                            );
                        }
                        if let Some(capture) = &capture {
                            assert_eq!(
                                capture.take_retained_bytes(),
                                available.unwrap()
                                    - if successful && has_original { bytes } else { 0 }
                            );
                        }
                    });
                }
            }
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]
    // Exhaust every admission and transfer branch for each generated counter
    // triple; boundary triples force both arithmetic failures and success.
    #[test]
    fn retain_preserves_every_admission_and_arithmetic_cause(
        adopted in any::<u64>(), retained in any::<u64>(), bytes in any::<u64>(),
    ) {
        check_outcomes(NativeMountAllocationCustody::retain, adopted, retained, bytes);
    }
}

#[test]
#[should_panic(expected = "retain cause")]
fn negative_control_rejects_missing_admission_reported_as_overflow() {
    check_outcomes(
        |custody, bytes, work| {
            // Reintroduce the former missing-gauge/admission mapping while keeping
            // the real production retain path for admitted owners.
            if !work
                .roots
                .native_mount_admissions
                .0
                .iter()
                .any(|entry| custody.accounts_for(&entry.0))
            {
                return Err(crate::WorkError::Overflow.into());
            }
            custody.retain(bytes, work)
        },
        0,
        0,
        0,
    );
}
