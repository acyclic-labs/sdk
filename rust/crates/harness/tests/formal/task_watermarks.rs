#[path = "../../src/task_volume/watermarks.rs"]
mod production;

#[kani::proof]
#[kani::unwind(1)]
fn watermark_updates_preserve_the_cut_and_are_atomic_on_rejection() {
    let position: u64 = kani::any();
    let old_accepted: u64 = kani::any();
    let old_durable: u64 = kani::any();
    let next_accepted: u64 = kani::any();
    let next_durable: u64 = kani::any();
    let mut accepted = old_accepted;
    let mut durable = old_durable;
    let committed = production::advance(
        position,
        &mut accepted,
        &mut durable,
        next_accepted,
        next_durable,
    );
    if committed {
        assert!(accepted >= position && accepted >= old_accepted);
        assert!(durable <= accepted && durable >= old_durable);
        assert!(accepted == next_accepted && durable == next_durable);
    } else {
        assert!(accepted == old_accepted && durable == old_durable);
        assert!(
            next_accepted < position
                || next_accepted < old_accepted
                || next_durable < old_durable
                || next_durable > next_accepted
        );
    }
}
