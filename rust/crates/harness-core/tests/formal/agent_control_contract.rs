// One runnable obligation shared by the solver and original execution traces.
use super::production::{Control, Mode};

pub fn runnable(control: Control, queued: bool, approval_queued: bool, pending: usize) -> bool {
    if control.closed {
        pending > 0
    } else {
        match control.mode {
            Mode::Done => false,
            Mode::Approval => control.turn && approval_queued,
            Mode::Running => control.turn,
            Mode::Await(deadline) => control.turn && deadline.is_some(),
            Mode::Idle => queued,
        }
    }
}
