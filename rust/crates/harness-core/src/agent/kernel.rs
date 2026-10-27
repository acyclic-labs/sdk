//! Dependency-free control kernel of one agent step.
//!
//! Every control decision of [`super::step`] is made here, over a `Copy` summary
//! of the agent, so the solver harness in `tests/formal/agent_control.rs` checks
//! the production code exhaustively: exactly one `ChildDone`, no turn after
//! completion or archive, no lost wake-up, and a cascade that terminates.

/// Alarm deadline that is due immediately: continue in a fresh step.
pub const CONTINUE_NOW: u64 = 1;
/// Most archive requests sent to children in one step.
pub const MAX_FANOUT: usize = 32;

/// Control phase of an agent.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Mode {
    /// No turn in progress.
    Idle,
    /// A turn continues in the next step.
    Running,
    /// A turn waits for its approval response or its host's event hook.
    Approval,
    /// A turn waits until this Unix millisecond deadline.
    Alarm(u64),
    /// The agent's task is complete.
    Done,
}

/// `Copy` summary of the agent the kernel decides over.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Control {
    /// Phase.
    pub mode: Mode,
    /// Archived.
    pub closed: bool,
    /// `ChildDone` has been sent.
    pub reported: bool,
    /// A turn is in progress (its driver checkpoint is retained).
    pub turn: bool,
}

impl Control {
    /// A freshly adopted agent.
    pub const ADOPTED: Self = Self {
        mode: Mode::Idle,
        closed: false,
        reported: false,
        turn: false,
    };
}

/// Class of an admitted inbox record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admit {
    /// Carries turn input (message, approval, hook, child result).
    Input,
    /// Stop the current turn.
    Cancel,
    /// Close the agent.
    Archive,
}

/// The effect of admitting a record.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Admitted {
    /// Next control.
    pub control: Control,
    /// Drop every queued input.
    pub clear_queue: bool,
    /// Queue the record's input.
    pub enqueue: bool,
}

/// Admits one inbox record.
#[must_use]
pub const fn admit(control: Control, kind: Admit) -> Admitted {
    match kind {
        Admit::Input => Admitted {
            control,
            clear_queue: false,
            enqueue: !control.closed && !matches!(control.mode, Mode::Done),
        },
        Admit::Cancel => Admitted {
            control: Control {
                mode: if matches!(control.mode, Mode::Done) {
                    Mode::Done
                } else {
                    Mode::Idle
                },
                turn: false,
                ..control
            },
            clear_queue: true,
            enqueue: false,
        },
        Admit::Archive => Admitted {
            control: Control {
                closed: true,
                turn: false,
                ..control
            },
            clear_queue: true,
            enqueue: false,
        },
    }
}

/// Which input the next turn step runs with.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Pick {
    /// No turn step.
    Nothing,
    /// Continue the turn in progress without input.
    Resume,
    /// Start or feed the turn with the front of the queue.
    Front,
    /// Resume the waiting turn with its queued approval response.
    Approval,
}

/// Chooses the turn step. `alarm_wake` is true when the actor alarm fired.
#[must_use]
pub const fn pick(control: Control, alarm_wake: bool, queued: bool, approval_ready: bool) -> Pick {
    if control.closed || matches!(control.mode, Mode::Done) {
        return Pick::Nothing;
    }
    match control.mode {
        Mode::Approval if control.turn => {
            if approval_ready {
                Pick::Approval
            } else {
                Pick::Nothing
            }
        }
        Mode::Running if control.turn => Pick::Resume,
        Mode::Alarm(_) if control.turn => {
            if alarm_wake {
                Pick::Resume
            } else {
                Pick::Nothing
            }
        }
        _ => {
            if queued {
                Pick::Front
            } else {
                Pick::Nothing
            }
        }
    }
}

/// What a turn step waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Wait {
    /// Continue at once.
    Continue,
    /// Wait for the approval response.
    Approval,
    /// Wait until this Unix millisecond deadline.
    Until(u64),
    /// Turn finished; wait for input.
    Input,
    /// Task complete.
    Done,
}

/// Next control after a turn step, and whether to send `ChildDone`.
#[must_use]
pub const fn settle(control: Control, wait: Wait) -> (Control, bool) {
    const fn next(control: Control, mode: Mode, turn: bool) -> Control {
        Control {
            mode,
            turn,
            ..control
        }
    }
    match wait {
        Wait::Continue => (next(control, Mode::Running, true), false),
        Wait::Approval => (next(control, Mode::Approval, true), false),
        Wait::Until(deadline) => (
            next(
                control,
                Mode::Alarm(if deadline < CONTINUE_NOW {
                    CONTINUE_NOW
                } else {
                    deadline
                }),
                true,
            ),
            false,
        ),
        Wait::Input => (next(control, Mode::Idle, false), false),
        Wait::Done => report(control),
    }
}

/// Terminal transition; `ChildDone` is sent only by the first.
#[must_use]
pub const fn report(control: Control) -> (Control, bool) {
    (
        Control {
            mode: Mode::Done,
            turn: false,
            reported: true,
            ..control
        },
        !control.reported,
    )
}

/// One archive cascade step over `pending` unarchived children.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Cascade {
    /// Next control.
    pub control: Control,
    /// Children archived in this step.
    pub archive: usize,
    /// Send `ChildDone` (outcome `Archived`) now.
    pub report: bool,
}

/// Archives at most [`MAX_FANOUT`] children; reports once none remain.
#[must_use]
pub const fn cascade(control: Control, pending: usize) -> Cascade {
    let archive = if pending < MAX_FANOUT {
        pending
    } else {
        MAX_FANOUT
    };
    if pending > archive || control.reported {
        return Cascade {
            control,
            archive,
            report: false,
        };
    }
    let (control, report) = report(control);
    Cascade {
        control,
        archive,
        report,
    }
}

/// The single actor alarm implied by the state after a step.
///
/// Derived from state, never from the step's history, so an input that runs
/// nothing cannot drop a pending deadline or continuation.
///
/// `approval_ready` is true when the response the waiting turn needs is
/// already queued.
#[must_use]
pub const fn alarm(
    control: Control,
    queued: bool,
    approval_ready: bool,
    pending: usize,
) -> Option<u64> {
    if control.closed {
        return if pending > 0 {
            Some(CONTINUE_NOW)
        } else {
            None
        };
    }
    match control.mode {
        Mode::Done => None,
        Mode::Approval => {
            if approval_ready {
                Some(CONTINUE_NOW)
            } else {
                None
            }
        }
        Mode::Running => Some(CONTINUE_NOW),
        Mode::Alarm(deadline) => Some(deadline),
        Mode::Idle => {
            if queued {
                Some(CONTINUE_NOW)
            } else {
                None
            }
        }
    }
}
