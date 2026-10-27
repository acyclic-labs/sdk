#[path = "../../../crates/harness-core/src/agent/slots.rs"]
mod slots;

#[kani::proof]
fn client_cannot_take_a_recorded_slot() {
    let open: usize = kani::any();
    let reserved: usize = kani::any();
    kani::assume(open <= slots::MAX_OPEN_CHILDREN);
    kani::assume(reserved <= slots::MAX_OPEN_CHILDREN - open);
    let ceiling = slots::client_ceiling(true, reserved);
    if slots::admits(open, ceiling, false) {
        assert!(open + 1 + reserved <= slots::MAX_OPEN_CHILDREN);
    }
}

#[kani::proof]
fn every_recorded_child_fits_after_client_admission() {
    let open: usize = kani::any();
    let reserved: usize = kani::any();
    let clients: usize = kani::any();
    kani::assume(open <= slots::MAX_OPEN_CHILDREN);
    kani::assume(reserved <= slots::MAX_OPEN_CHILDREN - open);
    kani::assume(clients <= slots::client_ceiling(true, reserved) - open);
    let after_clients = open + clients;
    assert!(after_clients + reserved <= slots::MAX_OPEN_CHILDREN);
    // The last distinct recorded spawn can still be admitted at settlement.
    if reserved > 0 {
        assert!(slots::admits(after_clients + reserved - 1, slots::MAX_OPEN_CHILDREN, false));
    }
}

#[kani::proof]
fn completing_a_child_does_not_release_a_paused_turns_slot() {
    let open: usize = kani::any();
    let reserved: u16 = kani::any();
    kani::assume(open > 0 && open <= slots::MAX_OPEN_CHILDREN);
    kani::assume(usize::from(reserved) <= slots::MAX_OPEN_CHILDREN - open);
    let after = slots::reserve_vacated(reserved);
    assert!(open - 1 + usize::from(after) == open + usize::from(reserved));
}

#[kani::proof]
fn replay_and_untrusted_reservation_counts_do_not_overflow() {
    let reserved: usize = kani::any();
    let open: usize = kani::any();
    assert!(slots::client_ceiling(true, reserved) <= slots::MAX_OPEN_CHILDREN);
    assert!(slots::admits(open, 0, true));
    assert!(slots::client_ceiling(false, reserved) == slots::MAX_OPEN_CHILDREN);
}
