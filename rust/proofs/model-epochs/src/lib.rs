#[path = "../../../crates/harness-core/src/conversation/epochs/kernel.rs"]
mod kernel;
#[path = "../../../crates/harness-core/src/conversation/epochs/protocol.rs"]
mod protocol;

#[kani::proof]
fn cuts_only_advance_at_complete_contiguous_exchanges() {
    let before: u64 = kani::any();
    let next: u64 = kani::any();
    let closed: bool = kani::any();
    let mut cut = kernel::Cut::after(before);
    let boundary = if closed {
        kernel::Boundary::Closed
    } else {
        kernel::Boundary::Open
    };
    let accepted = cut.push(next, boundary);
    assert_eq!(accepted, before.checked_add(1) == Some(next));
    assert_eq!(cut.cursor(), if accepted && closed { next } else { before });
    kani::cover!(accepted && closed);
    kani::cover!(accepted && !closed);
    kani::cover!(!accepted);
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
fn stale_generation_and_exhaustion_leave_ready_state_unchanged() {
    let generation: u64 = kani::any();
    let expected: u64 = kani::any();
    kani::assume(expected != generation);
    let id = [0; 32];
    let mut state = protocol::State::restore(generation, 0, None, 1).unwrap();
    assert!(state.prepare(id, 1, 1).is_ok());
    assert!(state.complete(id, protocol::Completion::Ready).is_ok());
    let ready = state;
    assert_eq!(state.swap(id, expected, 1), Err(protocol::Refusal::Stale));
    assert_eq!(state, ready);
    if generation == u64::MAX {
        assert_eq!(
            state.swap(id, generation, 1),
            Err(protocol::Refusal::Exhausted)
        );
        assert_eq!(state, ready);
    } else {
        assert!(state.swap(id, generation, 1).is_ok());
        assert_eq!(state.cursor(), 1);
        assert_eq!(state.generation(), generation + 1);
    }
    kani::cover!(generation == u64::MAX);
    kani::cover!(generation < u64::MAX);
}

#[kani::proof]
fn fanout_preserves_settled_coverage_and_never_drops_a_pending_producer() {
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
