pub(super) fn room(
    frames: usize,
    bytes: usize,
    pending: usize,
    used: usize,
    largest: usize,
    credit: bool,
) -> (bool, bool, bool) {
    (
        pending < frames.saturating_sub(1)
            && bytes
                .checked_sub(2 * largest)
                .is_some_and(|remaining| used <= remaining),
        pending < frames && used <= bytes.saturating_sub(largest),
        credit,
    )
}

pub(super) fn enabled(
    latest: bool,
    pending: usize,
    room: (bool, bool, bool),
    channel_credit: bool,
) -> bool {
    (latest && pending < 3 && room.0) || (pending == 0 && room.1 && room.2 && channel_credit)
}
