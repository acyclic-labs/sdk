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
    fn preparation_and_completion_preserve_the_current_intent() {
        let generation: u64 = kani::any();
        let cursor: u64 = kani::any();
        let head: u64 = kani::any();
        let Ok(mut state) = protocol::State::restore(generation, cursor, None, head) else {
            return;
        };
        if kani::any() {
            let summary: protocol::SummaryId = kani::any();
            let through: u64 = kani::any();
            kani::assume(cursor < through && through <= head);
            assert_eq!(state.prepare(summary, through, head), Ok(()));
            if kani::any() {
                let completion = if kani::any() {
                    protocol::Completion::Ready
                } else {
                    protocol::Completion::Failed
                };
                assert_eq!(state.complete(summary, completion), Ok(()));
            }
        }

        let summary: protocol::SummaryId = kani::any();
        let through: u64 = kani::any();
        let head: u64 = kani::any();
        let before = state;
        let pending = before.pending();
        let refusal = if through <= cursor || through > head {
            Some(protocol::Refusal::Range)
        } else if pending.is_some_and(|value| value.summary != summary || value.through != through)
        {
            Some(protocol::Refusal::Busy)
        } else {
            None
        };
        assert_eq!(state.prepare(summary, through, head).err(), refusal);
        assert_eq!(state.generation(), generation);
        assert_eq!(state.cursor(), cursor);
        if refusal.is_some() || pending.is_some() {
            assert_eq!(state, before);
        } else {
            assert_eq!(
                state.pending(),
                Some(protocol::Pending {
                    summary,
                    generation,
                    through,
                    completion: None,
                })
            );
        }
        kani::cover!(refusal == Some(protocol::Refusal::Range));
        kani::cover!(refusal == Some(protocol::Refusal::Busy));
        kani::cover!(refusal.is_none() && pending.is_none());
        kani::cover!(refusal.is_none() && pending.is_some_and(|value| value.completion.is_none()));
        kani::cover!(
            refusal.is_none()
                && pending
                    .is_some_and(|value| value.completion == Some(protocol::Completion::Ready))
        );
        kani::cover!(
            refusal.is_none()
                && pending
                    .is_some_and(|value| value.completion == Some(protocol::Completion::Failed))
        );

        let summary: protocol::SummaryId = kani::any();
        let completion = if kani::any() {
            protocol::Completion::Ready
        } else {
            protocol::Completion::Failed
        };
        let before = state;
        let pending = before.pending();
        let refusal = match pending {
            None => Some(protocol::Refusal::Stale),
            Some(value) if value.summary != summary => Some(protocol::Refusal::Stale),
            Some(value) if value.completion.is_some_and(|prior| prior != completion) => {
                Some(protocol::Refusal::Contradiction)
            }
            Some(_) => None,
        };
        assert_eq!(state.complete(summary, completion).err(), refusal);
        if refusal.is_some() {
            assert_eq!(state, before);
        } else {
            assert_eq!(state.generation(), before.generation());
            assert_eq!(state.cursor(), before.cursor());
            assert_eq!(
                state.pending(),
                pending.map(|value| protocol::Pending {
                    completion: Some(completion),
                    ..value
                })
            );
            let demand = match completion {
                protocol::Completion::Ready => protocol::Demand::Swap(summary),
                protocol::Completion::Failed => protocol::Demand::Failed(summary),
            };
            assert_eq!(state.demand(), demand);
        }
        kani::cover!(refusal == Some(protocol::Refusal::Stale) && pending.is_none());
        kani::cover!(
            refusal == Some(protocol::Refusal::Stale)
                && pending.is_some_and(|value| value.summary != summary)
        );
        kani::cover!(
            refusal == Some(protocol::Refusal::Contradiction)
                && completion == protocol::Completion::Ready
        );
        kani::cover!(
            refusal == Some(protocol::Refusal::Contradiction)
                && completion == protocol::Completion::Failed
        );
        kani::cover!(
            refusal.is_none()
                && pending.is_some_and(|value| value.completion.is_none())
                && completion == protocol::Completion::Ready
        );
        kani::cover!(
            refusal.is_none()
                && pending.is_some_and(|value| value.completion.is_none())
                && completion == protocol::Completion::Failed
        );
        kani::cover!(
            refusal.is_none()
                && pending.is_some_and(|value| value.completion == Some(completion))
                && completion == protocol::Completion::Ready
        );
        kani::cover!(
            refusal.is_none()
                && pending.is_some_and(|value| value.completion == Some(completion))
                && completion == protocol::Completion::Failed
        );
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
        let generation: u64 = kani::any();
        let cursor: u64 = kani::any();
        let head: u64 = kani::any();
        let pending = if kani::any() {
            Some(protocol::Pending {
                summary: kani::any(),
                generation,
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
        let Ok(state) = protocol::State::restore(generation, cursor, pending, head) else {
            return;
        };
        let common: u64 = kani::any();
        let refusal = if cursor > common {
            Some(protocol::Refusal::Range)
        } else if pending.is_some() {
            Some(protocol::Refusal::NotReady)
        } else {
            None
        };
        let fork = state.fork(common);
        assert_eq!(fork.as_ref().err().copied(), refusal);
        if let Ok(child) = fork {
            assert_eq!(child, state);
        }
        kani::cover!(refusal == Some(protocol::Refusal::Range) && pending.is_none());
        kani::cover!(refusal == Some(protocol::Refusal::Range) && pending.is_some());
        kani::cover!(
            refusal == Some(protocol::Refusal::NotReady)
                && pending.is_some_and(|value| value.completion.is_none())
        );
        kani::cover!(
            refusal == Some(protocol::Refusal::NotReady)
                && pending
                    .is_some_and(|value| value.completion == Some(protocol::Completion::Ready))
        );
        kani::cover!(
            refusal == Some(protocol::Refusal::NotReady)
                && pending
                    .is_some_and(|value| value.completion == Some(protocol::Completion::Failed))
        );
        kani::cover!(refusal.is_none() && generation == 0 && cursor == 0);
        kani::cover!(refusal.is_none() && generation > 0 && cursor > 0);
        kani::cover!(refusal.is_none() && generation == u64::MAX);
    }
}
