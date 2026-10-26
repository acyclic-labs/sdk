// Shared agent control model, `include!`d by `agent_control.rs` (production
// kernel, must verify) and `agent_control_negative.rs` (mutated kernels, must
// fail). The includer declares `mod production` for `src/agent/kernel.rs`.

use production::{Admit, Admitted, CONTINUE_NOW, Cascade, Control, MAX_FANOUT, Mode, Pick, Wait};

const STEPS: usize = 6;
const MAX_CHILDREN: usize = 2 * MAX_FANOUT + 1;

/// The kernel entry points the shell in `src/agent/machine.rs` calls.
#[derive(Clone, Copy)]
struct Kernel {
    admit: fn(Control, Admit) -> Admitted,
    pick: fn(Control, bool, bool, bool) -> Pick,
    settle: fn(Control, Wait) -> (Control, bool),
    cascade: fn(Control, usize) -> Cascade,
    alarm: fn(Control, bool, bool, usize) -> Option<u64>,
}

const PRODUCTION: Kernel = Kernel {
    admit: production::admit,
    pick: production::pick,
    settle: production::settle,
    cascade: production::cascade,
    alarm: production::alarm,
};

#[derive(Clone, Copy)]
struct Model {
    control: Control,
    queued: u8,
    approval_queued: bool,
    pending: usize,
    reports: u8,
}

fn any_wait() -> Wait {
    match kani::any::<u8>() % 5 {
        0 => Wait::Continue,
        1 => Wait::Approval,
        2 => Wait::Until(kani::any()),
        3 => Wait::Input,
        _ => Wait::Done,
    }
}

fn any_wake() -> Option<Admit> {
    match kani::any::<u8>() % 4 {
        0 => Some(Admit::Input),
        1 => Some(Admit::Cancel),
        2 => Some(Admit::Archive),
        _ => None,
    }
}

/// One step of the shell in `src/agent/machine.rs`, over counts instead of data.
/// `wake == None` is the actor alarm.
fn step(kernel: Kernel, model: &mut Model, wake: Option<Admit>, open_children: usize) {
    if let Some(kind) = wake {
        let admitted = (kernel.admit)(model.control, kind);
        model.control = admitted.control;
        if admitted.clear_queue {
            model.queued = 0;
            model.approval_queued = false;
        }
        if kind == Admit::Archive {
            model.pending = open_children;
        }
        if admitted.enqueue && model.queued < 3 {
            model.queued += 1;
            model.approval_queued |=
                matches!(model.control.mode, Mode::Approval) && kani::any::<bool>();
        }
    }
    if model.control.closed {
        let cascade = (kernel.cascade)(model.control, model.pending);
        assert!(cascade.archive <= MAX_FANOUT && cascade.archive <= model.pending);
        model.pending -= cascade.archive;
        if cascade.report {
            assert!(
                model.pending == 0,
                "archive reported before every child was archived"
            );
        }
        model.control = cascade.control;
        model.reports += u8::from(cascade.report);
        return;
    }
    let picked = (kernel.pick)(
        model.control,
        wake.is_none(),
        model.queued > 0,
        model.approval_queued,
    );
    if matches!(model.control.mode, Mode::Done) {
        assert!(picked == Pick::Nothing, "turn after completion");
    }
    match picked {
        Pick::Nothing => return,
        Pick::Resume => {}
        Pick::Front => model.queued -= 1,
        Pick::Approval => {
            model.queued -= 1;
            model.approval_queued = false;
        }
    }
    let (next, send) = (kernel.settle)(model.control, any_wait());
    model.control = next;
    model.reports += u8::from(send);
    if matches!(next.mode, Mode::Done) {
        model.queued = 0;
        model.approval_queued = false;
    }
    if matches!(next.mode, Mode::Approval) {
        // A new request: an earlier queued response may or may not match it.
        model.approval_queued = model.queued > 0 && kani::any::<bool>();
    }
}

fn check_alarm(kernel: Kernel, model: &Model) {
    let set = (kernel.alarm)(
        model.control,
        model.queued > 0,
        model.approval_queued,
        model.pending,
    );
    if let Some(deadline) = set {
        assert!(deadline >= CONTINUE_NOW, "alarm is not a real deadline");
    }
    let control = model.control;
    let runnable = if control.closed {
        model.pending > 0
    } else {
        match control.mode {
            Mode::Done => false,
            Mode::Approval => control.turn && model.approval_queued,
            Mode::Running | Mode::Alarm(_) => control.turn,
            Mode::Idle => model.queued > 0,
        }
    };
    assert!(set.is_some() == runnable, "lost or spurious wake-up");
    if control.closed || matches!(control.mode, Mode::Done) {
        assert!(!control.turn, "turn retained after completion or archive");
    }
}

fn exactly_once_and_never_loses_a_wake(kernel: Kernel) {
    let open_children: usize = kani::any();
    kani::assume(open_children <= MAX_CHILDREN);
    let mut model = Model {
        control: Control::ADOPTED,
        queued: kani::any::<u8>() % 2,
        approval_queued: false,
        pending: 0,
        reports: 0,
    };
    for _ in 0..STEPS {
        let closed_before = model.control.closed;
        step(kernel, &mut model, any_wake(), open_children);
        assert!(model.reports <= 1, "ChildDone sent twice");
        if closed_before {
            assert!(model.control.closed, "archive reopened");
        }
        check_alarm(kernel, &model);
    }
}

fn archive_drains_and_reports_exactly_once(kernel: Kernel) {
    let open_children: usize = kani::any();
    kani::assume(open_children <= MAX_CHILDREN);
    let reported: bool = kani::any();
    let mut model = Model {
        control: Control {
            reported,
            mode: if reported { Mode::Done } else { Mode::Idle },
            ..Control::ADOPTED
        },
        queued: 0,
        approval_queued: false,
        pending: 0,
        reports: 0,
    };
    step(kernel, &mut model, Some(Admit::Archive), open_children);
    // Only the alarm wakes an archived agent that receives no further input.
    for _ in 0..(MAX_CHILDREN / MAX_FANOUT + 1) {
        if (kernel.alarm)(model.control, false, false, model.pending).is_none() {
            break;
        }
        step(kernel, &mut model, None, open_children);
    }
    assert!(model.pending == 0, "cascade stalled");
    assert!(model.control.reported, "archive never reported");
    assert!(
        model.reports == u8::from(!reported),
        "archive reported wrongly"
    );
}
