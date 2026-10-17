use super::*;
use crate::CancellationToken;
use proptest::prelude::*;

struct Fixture {
    root: NativeMaterializationWork,
    view: NativeMaterializationWork,
    cancellation: CancellationToken,
    invocation: CancellationToken,
    claims: Vec<NativeAllocation>,
}

impl Fixture {
    fn compare(&self, other: &Self) -> proptest::test_runner::TestCaseResult {
        prop_assert_eq!(self.root.receipt(), other.root.receipt());
        prop_assert_eq!(
            self.root.live_allocation_bytes(),
            other.root.live_allocation_bytes()
        );
        prop_assert_eq!(
            self.view.remaining().map_err(|error| format!("{error:?}")),
            other.view.remaining().map_err(|error| format!("{error:?}"))
        );
        Ok(())
    }

    fn new(limits: &[(u8, u16)], invocation: bool) -> Self {
        let cancellation = CancellationToken::new();
        let root =
            NativeMaterializationWork::new(crate::performance::BOUNDARY_BUDGET, &cancellation)
                .unwrap();
        let (mut view, invocation) = if invocation {
            root.invocation_view().unwrap()
        } else {
            (root.clone(), cancellation.clone())
        };
        for &(units, bytes) in limits {
            view = view
                .native_volume_work(crate::performance::work_budget(
                    u64::from(units),
                    u64::from(bytes),
                ))
                .unwrap();
        }
        Self {
            root,
            view,
            cancellation,
            invocation,
            claims: Vec::new(),
        }
    }

    fn apply(
        &mut self,
        kind: u8,
        recheck: bool,
        delta: WorkCounters,
        bytes: u8,
        legacy: bool,
    ) -> Result<(), String> {
        let view = if recheck {
            self.view.recheck_admitted()
        } else {
            self.view.clone()
        };
        // Replay the old outer projection using the production helper still
        // required by reservations. The ledger applies its own projection.
        let delta = if legacy {
            view.item_charge(delta)
        } else {
            delta
        };
        let result = match kind {
            0 => view.admit(delta),
            1 => view.charge(delta),
            2 => view.spent(delta),
            3 => view.record_admitted(delta),
            4 => view
                .claim(u64::from(bytes), 1, delta)
                .map(|claim| self.claims.push(claim)),
            5 => view.reserve_work(delta).map(drop),
            _ => {
                drop(self.claims.pop());
                Ok(())
            }
        };
        result.map_err(|error| format!("{error:?}"))
    }
}

proptest! {
    // Property: for arbitrary operation/cancellation traces and resource
    // ancestor chains, removing the outer projection preserves every result,
    // completed counter, live allocation and remaining allowance after each step.
    #[test]
    fn item_projection_is_once_per_ledger_operation(
        limits in prop::collection::vec((0_u8..65, 0_u16..513), 0..5),
        invocation in any::<bool>(),
        actions in prop::collection::vec((0_u8..7, any::<bool>(), 0_u8..24,
            prop_oneof![8 => 0_u64..257, 1 => Just(u64::MAX)], 0_u8..5, 0_u8..17, any::<u8>()), 1..33),
    ) {
        let mut current = Fixture::new(&limits, invocation);
        let mut legacy = Fixture::new(&limits, invocation);
        for (kind, recheck, field, amount, items, cancel, bytes) in actions {
            match cancel {
                1 => { current.cancellation.cancel(); legacy.cancellation.cancel(); }
                2 => { current.invocation.cancel(); legacy.invocation.cancel(); }
                _ => {}
            }
            let delta = delta(field, amount, items);
            prop_assert_eq!(current.apply(kind, recheck, delta, bytes, false), legacy.apply(kind, recheck, delta, bytes, true));
            current.compare(&legacy)?;
        }
        current.claims.clear();
        legacy.claims.clear();
        prop_assert_eq!(current.root.live_allocation_bytes(), legacy.root.live_allocation_bytes());
        prop_assert_eq!(current.root.receipt(), legacy.root.receipt());
    }
}

#[test]
fn comparison_detects_bypassing_the_recheck_projection() {
    let mut correct = Fixture::new(&[], false);
    let broken = Fixture::new(&[], false);
    let delta = WorkCounters {
        items_examined: 3,
        ..WorkCounters::UNCHARGED
    };
    correct.apply(1, true, delta, 0, false).unwrap();
    broken
        .view
        .recheck_admitted()
        .state(|state| {
            // Broken variant: ignore the installed recheck policy in the ledger.
            state.scope(
                |work| &mut work.recheck_items,
                false,
                |state| state.charge(delta),
            )
        })
        .unwrap();
    assert_eq!(correct.root.receipt().items_examined, 0);
    assert_eq!(broken.root.receipt().items_examined, 3);
    assert!(correct.compare(&broken).is_err());
}

#[test]
fn every_entrypoint_charges_the_normalized_delta_once() {
    for resource in [false, true] {
        for invocation in [false, true] {
            for recheck in [false, true] {
                for kind in 0..6 {
                    let limits = if resource { &[(64, 4096)][..] } else { &[] };
                    let mut fixture = Fixture::new(limits, invocation);
                    let before = fixture.root.receipt();
                    let live = fixture.root.live_allocation_bytes();
                    let mut delta = crate::performance::work_budget(1, 1);
                    delta.items_examined = 3;
                    delta.allocation_operations = 0;
                    delta.peak_allocation_bytes = 0;
                    fixture.apply(kind, recheck, delta, 2, false).unwrap();
                    let mut expected = before;
                    if (1..=4).contains(&kind) {
                        if recheck {
                            delta.items_examined = 0;
                        }
                        if kind == 4 {
                            delta.allocation_operations = 1;
                            delta.peak_allocation_bytes = live + 2;
                        }
                        expected = expected.checked_add(delta).unwrap();
                    }
                    assert_eq!(fixture.root.receipt(), expected);
                    fixture.claims.clear();
                    assert_eq!(fixture.root.live_allocation_bytes(), live);
                }
            }
        }
    }
}

fn delta(field: u8, amount: u64, items: u8) -> WorkCounters {
    let mut delta = WorkCounters {
        items_examined: u64::from(items),
        ..WorkCounters::UNCHARGED
    };
    let counter = match field {
        0 => &mut delta.authority_records_read,
        1 => &mut delta.authority_records_appended,
        2 => &mut delta.authority_bytes_read,
        3 => &mut delta.authority_bytes_written,
        4 => &mut delta.object_probes,
        5 => &mut delta.backend_read_operations,
        6 => &mut delta.backend_write_operations,
        7 => &mut delta.durability_operations,
        8 => &mut delta.page_reads,
        9 => &mut delta.page_writes,
        10 => &mut delta.object_bytes_read,
        11 => &mut delta.object_bytes_written,
        12 => &mut delta.bytes_hashed,
        13 => &mut delta.bytes_copied,
        14 => &mut delta.bytes_encoded,
        15 => &mut delta.source_bytes_read,
        16 => &mut delta.source_path_components,
        17 => &mut delta.source_entries_visited,
        18 => &mut delta.output_bytes,
        19 => &mut delta.items_examined,
        20 => &mut delta.items_returned,
        21 => &mut delta.allocation_operations,
        22 => &mut delta.peak_allocation_bytes,
        _ => &mut delta.materializations,
    };
    *counter = amount;
    delta
}
