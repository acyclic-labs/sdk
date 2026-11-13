//! Deterministic source-cut simulations. The index models durable point cells;
//! a transition's effects become visible together, or none become visible.
use super::*;

#[derive(Default)]
struct Simulation {
    state: State,
    cells: BTreeMap<[u8; 32], Cell>,
    signals: Vec<Signal>,
}

impl Simulation {
    fn deliver(&mut self, input: Observation<'_>, policy: &Policy) -> Transition {
        let cell = input.key().and_then(|key| self.cells.get(&key).copied());
        let transition = self
            .state
            .apply(self.state.through, &input, cell, policy)
            .unwrap();
        if let Some(cell) = transition.cell {
            self.cells.insert(cell.key, cell);
        }
        self.state = transition.state.clone();
        self.signals.extend(transition.signals.iter().cloned());
        transition
    }
}

fn start(turn: u64, command: &str) -> Observation<'_> {
    Observation::ToolStarted {
        turn,
        command,
        check: true,
    }
}

fn failed<'a>(command: &'a str, error: &'a [u8]) -> Observation<'a> {
    Observation::ToolFinished {
        command,
        error: Some(error),
    }
}

fn done(progress: bool) -> Observation<'static> {
    Observation::TurnDone {
        spent_tokens: true,
        progress,
    }
}

#[test]
fn failures_and_checks_count_exact_signatures_without_scanning_history() {
    let policy = Policy::default();
    let mut run = Simulation::default();
    assert!(
        run.deliver(failed("cargo test", b"compile error"), &policy)
            .signals
            .is_empty()
    );
    // Neither another error nor another program repeats the original failure.
    assert!(
        run.deliver(failed("cargo test", b"test failed"), &policy)
            .signals
            .is_empty()
    );
    assert!(
        run.deliver(failed("cargo check", b"compile error"), &policy)
            .signals
            .is_empty()
    );
    assert!(
        run.deliver(failed(" cargo   test ", b"compile error"), &policy)
            .signals
            .is_empty()
    );
    let twice = run.deliver(failed("cargo test", b"compile error"), &policy);
    assert_eq!(
        (twice.signals[0].kind, twice.signals[0].count, twice.action),
        (Kind::RepeatFailure, 2, Action::Alert)
    );
    let thrice = run.deliver(failed("cargo test", b"compile error"), &policy);
    assert_eq!((thrice.signals[0].count, thrice.action), (3, Action::Pause));

    let mut run = Simulation::default();
    for turn in 0..4 {
        for _ in 0..2 {
            assert!(
                run.deliver(start(turn, "cargo check"), &policy)
                    .signals
                    .is_empty()
            );
        }
    }
    assert!(
        run.deliver(start(3, "cargo   check"), &policy)
            .signals
            .is_empty()
    );
    let looped = run.deliver(start(3, "cargo check"), &policy);
    assert_eq!(
        (
            looped.signals[0].kind,
            looped.signals[0].count,
            looped.action
        ),
        (Kind::ValidationLoop, 3, Action::Pause)
    );
    // Ordinary shell calls are not classified as validation by a repeated read.
    for _ in 0..5 {
        assert!(
            run.deliver(
                Observation::ToolStarted {
                    turn: 3,
                    command: "cat plan.md",
                    check: false,
                },
                &policy
            )
            .signals
            .is_empty()
        );
    }
}

#[test]
fn occurrence_thresholds_and_null_disable_have_no_hidden_fallbacks() {
    let policy = Policy {
        thresholds: Thresholds {
            repeat_failure: Some(4),
            repeat_failure_pause: Some(5),
            validation_loop: None,
            loop_output: None,
        },
        ..Policy::default()
    };
    let mut run = Simulation::default();
    for _ in 0..3 {
        assert!(
            run.deliver(failed("check", b"same error"), &policy)
                .signals
                .is_empty()
        );
        assert!(run.deliver(start(0, "check"), &policy).signals.is_empty());
        assert!(
            run.deliver(
                Observation::ModelOutput {
                    turn: 0,
                    output: Some([1; 32]),
                },
                &policy,
            )
            .signals
            .is_empty()
        );
    }
    assert_eq!(
        run.deliver(failed("check", b"same error"), &policy).action,
        Action::Alert
    );
    assert_eq!(
        run.deliver(failed("check", b"same error"), &policy).action,
        Action::Pause
    );
    let off = Policy {
        thresholds: Thresholds {
            repeat_failure: None,
            ..policy.thresholds
        },
        ..policy
    };
    assert!(
        run.deliver(failed("check", b"same error"), &off)
            .signals
            .is_empty()
    );
}

#[test]
fn source_progress_and_zero_spending_break_unproductive_turn_runs() {
    let policy = Policy {
        no_progress_turns: Some(3),
        ..Policy::default()
    };
    let mut run = Simulation::default();
    for _ in 0..2 {
        assert!(run.deliver(done(false), &policy).signals.is_empty());
    }
    assert!(run.deliver(done(true), &policy).signals.is_empty());
    for _ in 0..2 {
        run.deliver(done(false), &policy);
    }
    run.deliver(
        Observation::TurnDone {
            spent_tokens: false,
            progress: false,
        },
        &policy,
    );
    for _ in 0..2 {
        assert!(run.deliver(done(false), &policy).signals.is_empty());
    }
    let stopped = run.deliver(done(false), &policy);
    assert_eq!(
        (
            stopped.signals[0].kind,
            stopped.signals[0].count,
            stopped.action
        ),
        (Kind::NoProgress, 3, Action::Alert)
    );
    // Null is unbounded even after the configured case has fired.
    assert!(
        run.deliver(done(false), &Policy::default())
            .signals
            .is_empty()
    );
}

#[test]
fn output_loop_uses_model_publications_and_settlement_does_not_count_again() {
    let mut policy = Policy {
        no_progress_turns: Some(3),
        ..Policy::default()
    };
    policy.actions.insert(Kind::NoProgress, Action::Stop);
    let mut run = Simulation::default();
    assert!(
        run.deliver(
            Observation::ModelOutput {
                turn: 0,
                output: Some([1; 32]),
            },
            &policy,
        )
        .signals
        .is_empty()
    );
    assert!(run.deliver(done(false), &policy).signals.is_empty());
    assert!(
        run.deliver(
            Observation::ModelOutput {
                turn: 1,
                output: Some([2; 32]),
            },
            &policy,
        )
        .signals
        .is_empty()
    );
    assert!(run.deliver(done(false), &policy).signals.is_empty());
    let looped = run.deliver(
        Observation::ModelOutput {
            turn: 2,
            output: Some([1; 32]),
        },
        &policy,
    );
    assert_eq!(
        looped
            .signals
            .iter()
            .map(|signal| signal.kind)
            .collect::<Vec<_>>(),
        [Kind::LoopOutput]
    );
    assert_eq!(looped.action, Action::Pause);
    let settled = run.deliver(done(false), &policy);
    assert_eq!(
        settled
            .signals
            .iter()
            .map(|signal| signal.kind)
            .collect::<Vec<_>>(),
        [Kind::NoProgress]
    );
    assert_eq!(settled.action, Action::Stop);
    // A changed output with actual file progress is healthy.
    assert!(
        run.deliver(
            Observation::ModelOutput {
                turn: 3,
                output: Some([3; 32]),
            },
            &policy,
        )
        .signals
        .is_empty()
    );
    assert!(run.deliver(done(true), &policy).signals.is_empty());
}

#[test]
fn quiet_query_rejects_stale_settled_and_early_alarm_deliveries() {
    let policy = Policy {
        quiet_ms: Some(20),
        ..Policy::default()
    };
    let mut run = Simulation::default();
    for (deadline_ms, in_flight, now_ms) in [(120, true, 130), (130, false, 130), (130, true, 129)]
    {
        assert!(
            run.deliver(
                Observation::ToolAlarm {
                    call: [1; 32],
                    deadline_ms,
                    last_activity_ms: 110,
                    in_flight,
                    now_ms,
                },
                &policy
            )
            .signals
            .is_empty()
        );
    }
    let quiet = run.deliver(
        Observation::ToolAlarm {
            call: [1; 32],
            deadline_ms: 130,
            last_activity_ms: 110,
            in_flight: true,
            now_ms: 130,
        },
        &policy,
    );
    assert_eq!(
        (quiet.signals[0].kind, quiet.action),
        (Kind::Stuck, Action::Alert)
    );
    // Disabling the owner's quiet period invalidates already queued alarms.
    assert!(
        run.deliver(
            Observation::ToolAlarm {
                call: [1; 32],
                deadline_ms: 130,
                last_activity_ms: 110,
                in_flight: true,
                now_ms: 150,
            },
            &Policy::default()
        )
        .signals
        .is_empty()
    );
}

#[test]
fn spending_uses_original_measurements_and_enforces_only_owner_reservations() {
    let mut run = Simulation::default();
    let unbounded = Policy::default();
    assert!(
        run.deliver(
            Observation::Usage {
                tokens: Some(100),
                usd_nanos: Some(10_000)
            },
            &unbounded
        )
        .signals
        .is_empty()
    );
    let policy = Policy {
        reservation: Reservation {
            tokens: Some(150),
            usd_micros: Some(20),
        },
        ..unbounded
    };
    assert!(
        run.deliver(
            Observation::Usage {
                tokens: Some(49),
                usd_nanos: Some(9_000)
            },
            &policy
        )
        .signals
        .is_empty()
    );
    let spent = run.deliver(
        Observation::Usage {
            tokens: Some(1),
            usd_nanos: Some(1_000),
        },
        &policy,
    );
    assert_eq!(
        (
            spent.signals[0].kind,
            spent.action,
            spent.state.tokens.measured
        ),
        (Kind::Spend, Action::Pause, 150)
    );
    let mut unknown = Simulation::default();
    let missing = unknown.deliver(
        Observation::Usage {
            tokens: Some(1),
            usd_nanos: None,
        },
        &policy,
    );
    assert_eq!(missing.signals[0].kind, Kind::Spend);
    assert!(!missing.state.usd_micros.complete);
    // A token-only reservation does not require a provider's cost measurement.
    let token_only = Policy {
        reservation: Reservation {
            tokens: Some(5),
            usd_micros: None,
        },
        ..Policy::default()
    };
    let mut unknown = Simulation::default();
    assert!(
        unknown
            .deliver(
                Observation::Usage {
                    tokens: Some(1),
                    usd_nanos: None
                },
                &token_only
            )
            .signals
            .is_empty()
    );
    let zero = Policy {
        reservation: Reservation {
            tokens: Some(0),
            usd_micros: None,
        },
        ..Policy::default()
    };
    assert_eq!(
        Simulation::default()
            .deliver(Observation::PolicyChanged, &zero)
            .action,
        Action::Pause
    );
    // Neither per-call truncation nor per-call microdollar rounding may
    // erase or prematurely exhaust fractional original charges.
    let fractional = Policy {
        reservation: Reservation {
            tokens: None,
            usd_micros: Some(1),
        },
        ..Policy::default()
    };
    let mut run = Simulation::default();
    for nanos in [333, 333] {
        assert!(
            run.deliver(
                Observation::Usage {
                    tokens: None,
                    usd_nanos: Some(nanos)
                },
                &fractional
            )
            .signals
            .is_empty()
        );
    }
    let spent = run.deliver(
        Observation::Usage {
            tokens: None,
            usd_nanos: Some(334),
        },
        &fractional,
    );
    assert_eq!(spent.signals[0].kind, Kind::Spend);
    assert_eq!(spent.state.usd_micros.measured, 1);
    assert_eq!(spent.state.usd_nano_remainder, 0);
}

#[test]
fn crash_replay_has_no_effects_and_refused_cuts_cannot_mutate_the_checkpoint() {
    let mut run = Simulation::default();
    let policy = Policy::default();
    let observation = failed("check", b"error");
    let first = run.deliver(observation, &policy);
    let replayed = run
        .state
        .apply(0, &failed("check", b"error"), first.cell, &policy)
        .unwrap();
    assert!(replayed.cell.is_none() && replayed.signals.is_empty());
    assert_eq!(replayed.state, run.state);
    let before = run.state.clone();
    assert!(matches!(
        run.state
            .apply(2, &failed("check", b"error"), first.cell, &policy),
        Err(Error::SourceGap)
    ));
    let wrong = Cell {
        key: [0; 32],
        count: 999,
    };
    assert!(matches!(
        run.state
            .apply(1, &failed("check", b"error"), Some(wrong), &policy),
        Err(Error::CellMismatch)
    ));
    let unknown = Policy {
        version: 2,
        ..policy
    };
    assert!(matches!(
        run.state
            .apply(1, &failed("check", b"error"), None, &unknown),
        Err(Error::Version)
    ));
    assert_eq!(run.state, before);
}
