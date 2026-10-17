use super::*;
use crate::kernel::codec::ReadAllocationStep;
use proptest::prelude::*;

// Independent scalar model: before-effects denial leaves the prefix unchanged;
// cancellation after an admitted effect retains that completed effect's charge.
fn prefix(
    steps: &[(u8, u8, bool)],
    budget: WorkBudget,
    cancel_at: usize,
    recheck: bool,
) -> (Option<(usize, bool, &'static str)>, (u64, u64, u64)) {
    let (mut copied, mut hashed, mut items) = (0, 0, 0);
    for (index, &(kind, bytes, _)) in steps.iter().enumerate() {
        if cancel_at == index * 2 {
            return (Some((index, false, "cancel")), (copied, hashed, items));
        }
        let exceeded = match kind {
            0 if copied + u64::from(bytes) > budget.bytes_copied => Some("bytes_copied"),
            1 if hashed + u64::from(bytes) > budget.bytes_hashed => Some("bytes_hashed"),
            2 if !recheck && items + 1 > budget.items_examined => Some("items_examined"),
            _ => None,
        };
        if let Some(counter) = exceeded {
            return (Some((index, false, counter)), (copied, hashed, items));
        }
        match kind {
            0 => copied += u64::from(bytes),
            1 => hashed += u64::from(bytes),
            _ => items += u64::from(!recheck),
        }
        if cancel_at == index * 2 + 1 {
            return (Some((index, true, "cancel")), (copied, hashed, items));
        }
    }
    (None, (copied, hashed, items))
}

fn replay<const OMIT_COMPLETION: bool, const PREFIX_MUTANT: u8, const CHECK_PREFIX: bool>(
    steps: &[(u8, u8, bool)],
    budget: WorkBudget,
    cancel_at: usize,
    mode: u8,
) -> Result<(), TestCaseError> {
    let cancellation = CancellationToken::new();
    #[cfg(not(target_arch = "wasm32"))]
    let original = (mode != 0).then(|| {
        crate::NativeMaterializationWork::new(
            if mode == 2 || mode == 4 {
                WorkBudget::UNBOUNDED
            } else {
                budget
            },
            &cancellation,
        )
        .unwrap()
    });
    #[cfg(not(target_arch = "wasm32"))]
    let context = original.as_ref().map(|work| {
        let work = if mode == 2 || mode == 4 {
            work.native_volume_work(budget).unwrap()
        } else {
            work.clone()
        };
        if mode >= 3 {
            work.recheck_admitted()
        } else {
            work
        }
    });
    let mut accounting = match mode {
        #[cfg(not(target_arch = "wasm32"))]
        1..=4 => KernelAccounting::native(context.as_ref().unwrap(), &cancellation),
        _ => KernelAccounting::standalone(budget, &cancellation),
    };
    let mut observed = OriginalReadAccounting::new(&mut accounting).unwrap();
    let mut expected = observed.accounting.spent();
    let mut failure = None;
    for (index, &(kind, bytes, direct)) in steps.iter().enumerate() {
        if cancel_at == index * 2 {
            cancellation.cancel();
        }
        let delta = match kind {
            0 => WorkCounters {
                bytes_copied: u64::from(bytes),
                ..WorkCounters::UNCHARGED
            },
            1 => WorkCounters {
                bytes_hashed: u64::from(bytes),
                ..WorkCounters::UNCHARGED
            },
            _ => WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            },
        };
        let before = (|| {
            if direct {
                observed.admit(delta)
            } else {
                observed.observe(match kind {
                    0 => ReadAllocationStep::BeforeCopy(u64::from(bytes)),
                    1 => ReadAllocationStep::BeforeHash(u64::from(bytes)),
                    _ => ReadAllocationStep::BeforeItem,
                })
            }
        })();
        if let Err(error) = before {
            failure = Some((index, false, error.to_string()));
            break;
        }
        if cancel_at == index * 2 + 1 {
            cancellation.cancel();
        }
        let complete = (|| {
            if OMIT_COMPLETION {
                return Ok(());
            }
            if direct {
                observed.record_completed(delta)?;
            } else {
                observed.observe(match kind {
                    0 => ReadAllocationStep::Copied(u64::from(bytes)),
                    1 => ReadAllocationStep::Hashed(u64::from(bytes)),
                    _ => ReadAllocationStep::Item,
                })?;
            }
            Ok::<_, FsError>(())
        })();
        if let Err(error) = complete {
            failure = Some((index, true, error.to_string()));
            break;
        }
    }
    let (allocation, bytes) = observed.into_parts();
    assert_eq!(bytes, 0);
    drop(allocation);
    let actual = accounting.spent();
    let (stop, (copied, hashed, items)) = prefix(steps, budget, cancel_at, mode >= 3);
    expected.bytes_copied += copied;
    expected.bytes_hashed += hashed;
    expected.items_examined += items;
    prop_assert_eq!(actual, expected, "exact independent completed-work receipt");
    // These controls change only error location, leaving real execution and
    // its exact receipt untouched. Only the prefix assertion can detect them.
    if let Some((index, after, _)) = &mut failure {
        if PREFIX_MUTANT == 1 {
            *index += 1;
        }
        if PREFIX_MUTANT == 2 {
            *after = !*after;
        }
    }
    if CHECK_PREFIX {
        prop_assert_eq!(
            failure.as_ref().map(|(i, after, _)| (*i, *after)),
            stop.map(|(i, after, _)| (i, after)),
            "exact independent failure prefix"
        );
    }
    if let (Some((_, _, error)), Some((_, _, counter))) = (&failure, stop) {
        prop_assert!(error.contains(counter), "{error}: expected {counter}");
    }
    Ok(())
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(128))]
    #[test]
    fn wrapper_checks_preserve_all_limits_receipts_and_cancellation_prefixes(
        steps in prop::collection::vec((0_u8..3, 0_u8..33, any::<bool>()), 0..9),
        copy_budget in 0_u64..129, hash_budget in 0_u64..129,
        item_budget in 0_u64..9, cancel_at in 0_usize..20,
    ) {
        let budget = WorkBudget { bytes_copied: copy_budget, bytes_hashed: hash_budget,
            items_examined: item_budget, ..WorkBudget::UNBOUNDED };
        for mode in [0,
            #[cfg(not(target_arch = "wasm32"))] 1,
            #[cfg(not(target_arch = "wasm32"))] 2,
            #[cfg(not(target_arch = "wasm32"))] 3,
            #[cfg(not(target_arch = "wasm32"))] 4,
        ] {
            replay::<false, 0, true>(&steps, budget, cancel_at, mode)?;
        }
    }
}

#[test]
fn prefix_oracle_rejects_missing_completed_charges() {
    let steps = [(0, 8, false), (1, 7, true), (2, 0, false)];
    replay::<false, 0, true>(&steps, WorkBudget::UNBOUNDED, usize::MAX, 0).unwrap();
    assert!(replay::<true, 0, true>(&steps, WorkBudget::UNBOUNDED, usize::MAX, 0).is_err());
}

fn check_prefix_control<const MUTANT: u8>() {
    let steps = [(0, 8, false), (1, 7, true), (2, 0, false)];
    for mode in [
        0,
        #[cfg(not(target_arch = "wasm32"))]
        1,
        #[cfg(not(target_arch = "wasm32"))]
        2,
        #[cfg(not(target_arch = "wasm32"))]
        3,
        #[cfg(not(target_arch = "wasm32"))]
        4,
    ] {
        for (budget, cancel) in [
            (
                WorkBudget {
                    bytes_copied: 7,
                    ..WorkBudget::UNBOUNDED
                },
                usize::MAX,
            ),
            (WorkBudget::UNBOUNDED, 1),
        ] {
            replay::<false, 0, true>(&steps, budget, cancel, mode).unwrap();
            let failure = replay::<false, MUTANT, true>(&steps, budget, cancel, mode).unwrap_err();
            assert!(
                failure
                    .to_string()
                    .contains("exact independent failure prefix"),
                "{failure}"
            );
            // Deleting that assertion makes the identical mutant pass; neither
            // the receipt assertion nor the error-counter check detects it.
            replay::<false, MUTANT, false>(&steps, budget, cancel, mode).unwrap();
        }
    }
}

#[test]
fn wrong_failure_index_requires_the_prefix_assertion() {
    check_prefix_control::<1>();
}

#[test]
fn wrong_failure_phase_requires_the_prefix_assertion() {
    check_prefix_control::<2>();
}
