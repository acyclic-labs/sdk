//! Negative controls for `agent_control.rs`: each harness replaces one
//! production kernel decision with a plausible defect and must find a
//! counterexample (`kani tests/formal/agent_control_negative.rs` reports every
//! harness FAILED).

#[path = "../../src/agent/kernel.rs"]
mod production;

include!("agent_control_model.rs");

/// The pre-kernel shell: an inbox wake that ran no turn cleared the alarm.
fn alarm_from_history(
    control: Control,
    queued: bool,
    approval: bool,
    pending: usize,
) -> Option<u64> {
    match control.mode {
        Mode::Alarm(_) if !control.closed => None,
        _ => production::alarm(control, queued, approval, pending),
    }
}

/// Archive reports even when the agent already reported its completion.
fn cascade_unguarded(control: Control, pending: usize) -> Cascade {
    let mut cascade = production::cascade(control, pending);
    if pending <= MAX_FANOUT {
        cascade.report = true;
    }
    cascade
}

/// Archive keeps a retained turn checkpoint and may resume it.
fn admit_keeps_turn(control: Control, kind: Admit) -> Admitted {
    let mut admitted = production::admit(control, kind);
    if kind == Admit::Archive {
        admitted.control.turn = control.turn;
    }
    admitted
}

/// Reports the archive before every child has been archived.
fn cascade_reports_early(control: Control, pending: usize) -> Cascade {
    let mut cascade = production::cascade(control, pending);
    if !control.reported && pending > MAX_FANOUT {
        let (next, report) = production::report(cascade.control);
        cascade.control = next;
        cascade.report = report;
    }
    cascade
}

#[kani::proof]
#[kani::unwind(8)]
fn history_alarm_loses_a_deadline() {
    exactly_once_and_never_loses_a_wake(Kernel {
        alarm: alarm_from_history,
        ..PRODUCTION
    });
}

#[kani::proof]
#[kani::unwind(8)]
fn unguarded_archive_report_sends_twice() {
    exactly_once_and_never_loses_a_wake(Kernel {
        cascade: cascade_unguarded,
        ..PRODUCTION
    });
}

#[kani::proof]
#[kani::unwind(8)]
fn archive_retaining_turn_is_caught() {
    exactly_once_and_never_loses_a_wake(Kernel {
        admit: admit_keeps_turn,
        ..PRODUCTION
    });
}

#[kani::proof]
#[kani::unwind(8)]
fn early_archive_report_is_caught() {
    archive_drains_and_reports_exactly_once(Kernel {
        cascade: cascade_reports_early,
        ..PRODUCTION
    });
}

fn main() {}
