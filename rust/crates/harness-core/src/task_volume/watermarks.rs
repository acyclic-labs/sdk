pub(super) fn advance(
    position: u64,
    accepted: &mut u64,
    durable: &mut u64,
    next_accepted: u64,
    next_durable: u64,
) -> bool {
    if next_durable > next_accepted
        || next_accepted < *accepted
        || next_durable < *durable
        || next_accepted < position
    {
        return false;
    }
    *accepted = next_accepted;
    *durable = next_durable;
    true
}
