//! Exhaustive progress model of the runtime's alarm and delivery scheduling.
//!
//! The production [`ReadySet`] (registration wakers, due index, coalescing
//! queue and running-entry requeue), the production delivery arming
//! [`Registration::observe_delivery`] and the production refusal-retry rule
//! (`Inner::refused`) are driven through every interleaving of external events
//! and delivery outcomes up to a bound. The kernel proof assumes the runtime
//! delivers once per event; this checks that assumption:
//!
//! - every handler run is justified by an unsettled event (no self re-arm);
//! - at quiescence, every unsettled event with work behind it was delivered
//!   or settled as a named failure (no lost wake-up);
//! - every delivery ends in progress (its input consumed), a retryable wait
//!   (an uncertain or handler failure keeps it for the next attempt), or a
//!   failure its handler records and reports: a refusal the production
//!   `terminal` rule names never strands its input;
//! - the host reaches quiescence (no delivery loop).
//!
//! Negative controls: reinstalling the delivered alarm as a due wake (the
//! pre-fix arming) violates the first property, and a handler that settles
//! no terminal refusal (the default `on_failure`) strands its input; the
//! runtime tests in `retry.rs` are the end-to-end controls for both delivery
//! loops.
use super::super::Inner;
use super::*;
use crate::local::Alarm;
use crate::local::ready::{ReadySet, Registration};

const ACTORS: usize = 2;
/// External events per trace.
const EVENTS: usize = 3;
/// Nondeterministic delivery outcomes per trace; later attempts fail.
const OUTCOMES: usize = 4;
/// Deliveries without an external event before the host must be quiet.
const QUIET: usize = 64;

type Arm = fn(&Registration<usize>, &Alarm, u64) -> Result<bool, Error>;

/// Replays one choice sequence; `next` advances to the following one in
/// depth-first order, so every sequence within the bounds is visited once.
#[derive(Default)]
struct Choices {
    trace: Vec<(usize, usize)>,
    position: usize,
    outcomes: usize,
}

impl Choices {
    fn choose(&mut self, arity: usize) -> usize {
        let choice = match self.trace.get(self.position) {
            Some(&(choice, _)) => choice,
            None => {
                self.trace.push((0, arity));
                0
            }
        };
        self.position += 1;
        choice
    }

    fn outcome(&mut self) -> Outcome {
        if self.outcomes == OUTCOMES {
            return Outcome::Fail;
        }
        self.outcomes += 1;
        Outcome::ALL[self.choose(Outcome::ALL.len())]
    }

    fn next(&mut self) -> bool {
        self.position = 0;
        self.outcomes = 0;
        while let Some((choice, arity)) = self.trace.pop() {
            if choice + 1 < arity {
                self.trace.push((choice + 1, arity));
                return true;
            }
        }
        false
    }
}

#[derive(Clone, Copy)]
enum Outcome {
    /// A failure with no publication; the input stays unconsumed.
    Fail,
    /// Consume the trigger.
    Consume,
    /// Consume the trigger and set the next alarm one tick ahead.
    ConsumeAndArm,
    /// A composed-guard refusal identical to the previous one.
    RefuseSame,
    /// A composed-guard refusal evidencing an external append.
    RefuseChanged,
    /// Storage refuses the transition for good.
    Refused,
    /// Storage left the outcome undecided.
    Uncertain,
    /// A new input commits while this handler runs.
    InputDuring,
}

impl Outcome {
    const ALL: [Self; 8] = [
        Self::Fail,
        Self::Consume,
        Self::ConsumeAndArm,
        Self::RefuseSame,
        Self::RefuseChanged,
        Self::Refused,
        Self::Uncertain,
        Self::InputDuring,
    ];
}

struct Actor {
    registration: Registration<usize>,
    inner: Inner,
    alarm: Alarm,
    inputs: u32,
    /// External events so far, and how many of them are delivered or settled.
    events: u32,
    settled: u32,
    /// A terminal refusal left its input unconsumed and unreported.
    stranded: bool,
}

struct World {
    ready: ReadySet<usize>,
    actors: Vec<Actor>,
    now: u64,
    evidence: u64,
    guard: StreamPath,
    arm: Arm,
    /// Whether the handler settles a terminal refusal (`on_failure`).
    settles: bool,
}

impl World {
    fn new(arm: Arm, settles: bool) -> Result<Self, String> {
        let ready = ReadySet::default();
        let mut actors = Vec::new();
        for id in 0..ACTORS {
            let registration = ready.register(id).map_err(|error| error.to_string())?;
            // Actor 0 starts with an alarm due at the first tick.
            let alarm = Alarm {
                generation: u64::from(id == 0),
                deadline_unix_ms: (id == 0).then_some(1),
                delivered_generation: None,
            };
            registration
                .update_alarm(&alarm)
                .map_err(|error| error.to_string())?;
            // Registration replays the actor once: that is its first event.
            actors.push(Actor {
                registration,
                inner: Inner::default(),
                alarm,
                inputs: 0,
                events: 1,
                settled: 0,
                stranded: false,
            });
        }
        Ok(Self {
            ready,
            actors,
            now: 0,
            evidence: 0,
            guard: StreamPath::new("guard").map_err(|error| error.to_string())?,
            arm,
            settles,
        })
    }

    fn input(&mut self, id: usize) {
        let actor = &mut self.actors[id];
        actor.inputs += 1;
        actor.events += 1;
        actor.registration.waker().wake_by_ref();
    }

    fn advance(&mut self) {
        self.now += 1;
        for actor in &mut self.actors {
            if actor.alarm.pending() == Some(self.now) {
                actor.events += 1;
            }
        }
    }

    /// One dispatcher turn: wake due alarms, then run one ready actor.
    fn step(&mut self, choices: &mut Choices) -> Result<bool, String> {
        self.ready.wake_due(self.now, usize::MAX);
        let Some(work) = self.ready.take() else {
            return Ok(false);
        };
        let id = *work.key();
        self.deliver(id, choices)?;
        drop(work);
        Ok(true)
    }

    /// The runtime's delivery loop for one actor, over the production arming
    /// and refusal-retry rules.
    fn deliver(&mut self, id: usize, choices: &mut Choices) -> Result<(), String> {
        let now = self.now;
        let arm = self.arm;
        let start = self.actors[id].events;
        loop {
            let actor = &mut self.actors[id];
            let due = arm(&actor.registration, &actor.alarm, now).map_err(|e| e.to_string())?;
            if !due && actor.inputs == 0 {
                actor.settled = actor.events;
                return Ok(());
            }
            if actor.events == actor.settled {
                return Err(format!(
                    "actor {id} delivered at {now} without an unsettled event"
                ));
            }
            loop {
                let actor = &mut self.actors[id];
                match choices.outcome() {
                    Outcome::Fail => {
                        actor.settled = start;
                        return Ok(());
                    }
                    outcome @ (Outcome::Consume | Outcome::ConsumeAndArm) => {
                        actor.inner.refused = None;
                        if due {
                            actor.alarm.delivered_generation = Some(actor.alarm.generation);
                        } else {
                            actor.inputs -= 1;
                        }
                        if matches!(outcome, Outcome::ConsumeAndArm) {
                            actor.alarm = Alarm {
                                generation: actor.alarm.generation + 1,
                                deadline_unix_ms: Some(now + 1),
                                delivered_generation: None,
                            };
                        }
                        break;
                    }
                    refusal @ (Outcome::RefuseSame
                    | Outcome::RefuseChanged
                    | Outcome::Refused
                    | Outcome::Uncertain) => {
                        let refusal = match refusal {
                            Outcome::Refused => Error::Stream(StreamError::AccessDenied),
                            Outcome::Uncertain => Error::Stream(StreamError::Unavailable),
                            changed => {
                                if matches!(changed, Outcome::RefuseChanged) {
                                    self.evidence += 1;
                                }
                                let conflict = acyclic_stream::CommitConflict::Tail {
                                    path: self.guard.clone(),
                                    expected: self.evidence,
                                    actual: Some(self.evidence + 1),
                                };
                                match self.actors[id].inner.refused(vec![conflict]) {
                                    Ok(()) => break,
                                    Err(refusal) => refusal,
                                }
                            }
                        };
                        let actor = &mut self.actors[id];
                        if !super::super::terminal(&refusal) {
                            // A retryable wait: the input stays for the next attempt.
                            actor.settled = start;
                            return Ok(());
                        }
                        if !self.settles {
                            actor.stranded = true;
                            actor.settled = start;
                            return Ok(());
                        }
                        // The handler's failure transition consumes the
                        // trigger and reports the failure once.
                        actor.inner.refused = None;
                        if due {
                            actor.alarm.delivered_generation = Some(actor.alarm.generation);
                        } else {
                            actor.inputs -= 1;
                        }
                        break;
                    }
                    Outcome::InputDuring => self.input(id),
                }
            }
        }
    }

    fn run(&mut self, choices: &mut Choices) -> Result<(), String> {
        for _ in 0..EVENTS {
            match choices.choose(ACTORS + 2) {
                id if id < ACTORS => self.input(id),
                id if id == ACTORS => self.advance(),
                _ => {
                    self.step(choices)?;
                }
            }
        }
        let mut quiet = 0;
        while self.step(choices)? {
            quiet += 1;
            if quiet == QUIET {
                return Err("the host delivers without reaching quiescence".into());
            }
        }
        for (id, actor) in self.actors.iter().enumerate() {
            if actor.stranded {
                return Err(format!(
                    "actor {id} stranded a terminally refused input unreported"
                ));
            }
            let work = actor.inputs > 0
                || actor
                    .alarm
                    .pending()
                    .is_some_and(|deadline| deadline <= self.now);
            if work && actor.events > actor.settled {
                return Err(format!("actor {id} lost the wake-up of an unsettled event"));
            }
        }
        Ok(())
    }
}

fn explore(arm: Arm, settles: bool) -> Result<(), String> {
    let mut choices = Choices::default();
    loop {
        World::new(arm, settles)?
            .run(&mut choices)
            .map_err(|violation| format!("{violation}; choices {:?}", choices.trace))?;
        if !choices.next() {
            return Ok(());
        }
    }
}

fn production(registration: &Registration<usize>, alarm: &Alarm, now: u64) -> Result<bool, Error> {
    registration.observe_delivery(alarm, now)
}

/// The arming before the fix: the delivered alarm stays a due wake.
fn reinstalled(registration: &Registration<usize>, alarm: &Alarm, now: u64) -> Result<bool, Error> {
    registration.update_alarm(alarm)?;
    Ok(alarm.pending().is_some_and(|deadline| deadline <= now))
}

#[test]
fn every_event_is_delivered_settled_or_reported_and_no_attempt_rearms_itself() -> TestResult {
    explore(production, true)?;
    Ok(())
}

#[test]
fn a_terminal_refusal_no_handler_settles_is_found_stranded() {
    let violation = explore(production, false).err();
    assert!(
        violation
            .as_deref()
            .is_some_and(|violation| violation.contains("stranded")),
        "{violation:?}"
    );
}

#[test]
fn a_delivered_alarm_left_due_is_found_redelivering_itself() {
    let violation = explore(reinstalled, true).err();
    assert!(
        violation
            .as_deref()
            .is_some_and(|violation| violation.contains("without an unsettled event")),
        "{violation:?}"
    );
}
