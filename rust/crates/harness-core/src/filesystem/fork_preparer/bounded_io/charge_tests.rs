//! The faster standalone path must retain the original accounting semantics.
use super::*;

#[test]
fn standalone_charge_matches_kernel_for_every_counter_and_failure_boundary() -> Result<()> {
    let fields: Vec<String> = serde_json::to_value(WorkCounters::default())
        .expect("work counters serialize")
        .as_object()
        .expect("work counters are an object")
        .keys()
        .cloned()
        .collect();
    for field in fields {
        for initial in [0, 7, u64::MAX] {
            for proposed in [0, 1, 8, u64::MAX] {
                for live in [0, 3] {
                    for cancelled in [false, true] {
                        for invalid_ledger in [false, true] {
                            let cancellation = CancellationToken::new();
                            let budget = acyclic_fs::performance::work_budget(8, 8);
                            let fast = PreparationIo::from_budget(budget, &cancellation)?;
                            let reference = PreparationIo::from_budget(budget, &cancellation)?;
                            let mut initial_json = serde_json::to_value(WorkCounters::default())
                                .expect("work counters serialize");
                            initial_json[&field] = initial.into();
                            let initial: WorkCounters = serde_json::from_value(initial_json)
                                .expect("initial work counters decode");
                            for io in [&fast, &reference] {
                                io.retain_owned(live)?;
                                let PreparationIo::Owned { work, .. } = io else {
                                    unreachable!()
                                };
                                let mut state = work.lock().expect("initial accounting ledger");
                                state.0 = initial;
                                if invalid_ledger {
                                    state.1 = Err(acyclic_fs::WorkError::Overflow);
                                }
                            }
                            let mut delta_json = serde_json::to_value(WorkCounters::default())
                                .expect("work counters serialize");
                            delta_json[&field] = proposed.into();
                            let mut delta: WorkCounters = serde_json::from_value(delta_json)
                                .expect("delta work counters decode");
                            if cancelled {
                                cancellation.cancel();
                            }
                            let actual = fast.charge(delta);
                            let expected = reference.account(|accounting| {
                                if let Some((allocations, _, _, _)) = accounting.standalone_parts()
                                {
                                    delta.peak_allocation_bytes = delta
                                        .peak_allocation_bytes
                                        .checked_add(allocations.live_bytes())
                                        .ok_or(acyclic_fs::WorkError::Overflow)?;
                                }
                                accounting.charge(delta)
                            });
                            assert_eq!(
                                format!("{actual:?}"),
                                format!("{expected:?}"),
                                "{field} initial={initial:?} delta={delta:?} live={live} cancelled={cancelled} invalid_ledger={invalid_ledger}"
                            );
                            let PreparationIo::Owned { work: actual, .. } = &fast else {
                                unreachable!()
                            };
                            let PreparationIo::Owned { work: expected, .. } = &reference else {
                                unreachable!()
                            };
                            let actual = actual.lock().expect("actual accounting ledger");
                            let expected = expected.lock().expect("reference accounting ledger");
                            assert_eq!(
                                actual.0, expected.0,
                                "rejected work must leave the same counters"
                            );
                            assert_eq!(
                                actual.1.as_ref().map(|a| a.live_bytes()),
                                expected.1.as_ref().map(|a| a.live_bytes())
                            );
                        }
                    }
                }
            }
        }
    }
    Ok(())
}
