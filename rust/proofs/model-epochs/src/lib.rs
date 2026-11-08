#![cfg(kani)]

//! Verify the production epoch kernels without translating the async runtime.

#[path = "../../../crates/harness-core/src/conversation/epochs/kernel.rs"]
pub mod kernel;
#[path = "../../../crates/harness-core/src/conversation/epochs/protocol.rs"]
pub mod protocol;

mod proofs {
    use super::{kernel, protocol};

    #[kani::proof]
    #[kani::unwind(4)]
    fn cuts_only_advance_at_complete_contiguous_exchanges() {
        let before: u64 = kani::any();
        let mut cut = kernel::Cut::after(before);
        let mut observed = u128::from(before);
        let mut selected = before;
        for _ in 0..3 {
            let next: u64 = kani::any();
            let closed: bool = kani::any();
            let previous = cut;
            let boundary = if closed {
                kernel::Boundary::Closed
            } else {
                kernel::Boundary::Open
            };
            let expected = observed + 1 == u128::from(next);
            let accepted = cut.push(next, boundary);
            assert_eq!(accepted, expected);
            if expected {
                observed = u128::from(next);
                if closed {
                    selected = next;
                }
            } else {
                assert_eq!(cut, previous);
            }
            assert_eq!(cut.cursor(), selected);
            kani::cover!(accepted && !closed && selected < next);
            kani::cover!(accepted && closed && selected > before);
            kani::cover!(!accepted && observed > u128::from(selected));
        }
    }

    #[kani::proof]
    fn token_arithmetic_never_wraps_or_underestimates() {
        let reported: u64 = kani::any();
        let bytes: u64 = kani::any();
        let seams: u64 = kani::any();
        let exact = u128::from(reported) + u128::from(bytes) + u128::from(seams);
        let bound = kernel::token_bound(reported, bytes, seams);
        assert_eq!(
            bound.map(u128::from),
            (exact <= u128::from(u64::MAX)).then_some(exact)
        );
        kani::cover!(bound.is_none());
        kani::cover!(bound.is_some() && bytes > 0 && seams > 0);
    }

    #[kani::proof]
    fn output_reservation_and_fork_coverage_are_exact() {
        let input: u64 = kani::any();
        let output: u64 = kani::any();
        let capacity: u64 = kani::any();
        assert_eq!(
            kernel::fits(input, output, capacity),
            u128::from(input) + u128::from(output) <= u128::from(capacity)
        );
        assert_eq!(kernel::inherited(input, capacity), input <= capacity);
        kani::cover!(kernel::fits(input, output, capacity) && output > 0);
        kani::cover!(!kernel::inherited(input, capacity));
    }

    #[kani::proof]
    fn failed_summary_never_swaps_or_changes_its_outcome() {
        let cursor: u64 = kani::any();
        let end: u64 = kani::any();
        let generation: u64 = kani::any();
        kani::assume(end > cursor);
        let id = [0; 32];
        let mut state = protocol::State::restore(generation, cursor, None, end).unwrap();
        assert!(state.prepare(id, end, end).is_ok());
        assert!(state.complete(id, protocol::Completion::Failed).is_ok());
        let failed = state;
        assert!(state.swap(id, generation, end).is_err());
        assert_eq!(state, failed);
        assert_eq!(state.demand(), protocol::Demand::Failed(id));
        assert!(state.complete(id, protocol::Completion::Ready).is_err());
        assert_eq!(state, failed);
        kani::cover!(generation == u64::MAX);
    }

    #[kani::proof]
    fn restore_and_swap_preserve_the_exact_prepared_identity() {
        let generation: u64 = kani::any();
        let cursor: u64 = kani::any();
        let head: u64 = kani::any();
        let pending = if kani::any() {
            Some(protocol::Pending {
                summary: kani::any(),
                generation: kani::any(),
                through: kani::any(),
                completion: if kani::any() {
                    Some(if kani::any() {
                        protocol::Completion::Ready
                    } else {
                        protocol::Completion::Failed
                    })
                } else {
                    None
                },
            })
        } else {
            None
        };
        let refusal = if cursor > head {
            Some(protocol::Refusal::Range)
        } else {
            match pending {
                Some(value) if value.generation != generation => Some(protocol::Refusal::Stale),
                Some(value) if value.through <= cursor || value.through > head => {
                    Some(protocol::Refusal::Range)
                }
                _ => None,
            }
        };
        let restored = protocol::State::restore(generation, cursor, pending, head);
        assert_eq!(restored.as_ref().err().copied(), refusal);
        kani::cover!(refusal == Some(protocol::Refusal::Range));
        kani::cover!(refusal == Some(protocol::Refusal::Stale));
        let Ok(mut state) = restored else { return };
        assert_eq!(state.generation(), generation);
        assert_eq!(state.cursor(), cursor);
        assert_eq!(state.pending(), pending);
        let demand = match pending {
            None => protocol::Demand::Unprepared,
            Some(value) => match value.completion {
                None => protocol::Demand::Await(value.summary),
                Some(protocol::Completion::Ready) => protocol::Demand::Swap(value.summary),
                Some(protocol::Completion::Failed) => protocol::Demand::Failed(value.summary),
            },
        };
        assert_eq!(state.demand(), demand);

        let summary: protocol::SummaryId = kani::any();
        let expected: u64 = kani::any();
        let swap_head: u64 = kani::any();
        let refusal = match pending {
            None => Some(protocol::Refusal::Stale),
            Some(value)
                if expected != generation
                    || value.generation != expected
                    || value.summary != summary =>
            {
                Some(protocol::Refusal::Stale)
            }
            Some(value) if value.completion != Some(protocol::Completion::Ready) => {
                Some(protocol::Refusal::NotReady)
            }
            Some(value) if value.through > swap_head => Some(protocol::Refusal::Range),
            Some(_) if generation == u64::MAX => Some(protocol::Refusal::Exhausted),
            Some(_) => None,
        };
        let before = state;
        let observed = state.swap(summary, expected, swap_head);
        if let Some(refusal) = refusal {
            assert_eq!(observed, Err(refusal));
            assert_eq!(state, before);
        } else {
            assert_eq!(observed, Ok(()));
            assert_eq!(state.generation().checked_sub(1), Some(generation));
            assert_eq!(
                state.cursor(),
                pending.map_or(cursor, |value| value.through)
            );
            assert_eq!(state.pending(), None);
            assert_eq!(state.demand(), protocol::Demand::Unprepared);
        }
        kani::cover!(refusal == Some(protocol::Refusal::Stale) && pending.is_none());
        kani::cover!(
            refusal == Some(protocol::Refusal::Stale)
                && pending.is_some_and(|value| value.summary != summary && expected == generation)
        );
        kani::cover!(refusal == Some(protocol::Refusal::Stale) && expected != generation);
        kani::cover!(
            refusal == Some(protocol::Refusal::NotReady)
                && pending.is_some_and(|value| value.completion.is_none())
        );
        kani::cover!(
            refusal == Some(protocol::Refusal::NotReady)
                && pending
                    .is_some_and(|value| value.completion == Some(protocol::Completion::Failed))
        );
        kani::cover!(refusal == Some(protocol::Refusal::Range));
        kani::cover!(refusal == Some(protocol::Refusal::Exhausted));
        kani::cover!(refusal.is_none());
    }

    #[kani::proof]
    fn fanout_preserves_settled_coverage_and_pending_identity() {
        let cursor: u64 = kani::any();
        let end: u64 = kani::any();
        let common: u64 = kani::any();
        kani::assume(cursor < end);
        let mut state = protocol::State::restore(0, cursor, None, end).unwrap();
        assert!(state.prepare([0; 32], end, end).is_ok());
        let pending = state;
        assert!(state.fork(common).is_err());
        assert_eq!(state, pending);
        assert!(state.complete([0; 32], protocol::Completion::Ready).is_ok());
        assert!(state.fork(common).is_err());
        assert!(state.swap([0; 32], 0, end).is_ok());
        assert_eq!(state.fork(common).is_ok(), end <= common);
        if let Ok(child) = state.fork(common) {
            assert_eq!(child.cursor(), end);
            assert!(child.pending().is_none());
        }
        kani::cover!(cursor <= common && common < end);
        kani::cover!(end <= common);
    }
}
