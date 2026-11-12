//! Deterministic simulation of alarm ownership. Every interleaving of
//! registration, retirement (hibernation), arming, window opening, timer
//! wakes, authority refusal (deferral), consumption and crash, to a bounded
//! depth, keeps two properties: a checkpoint at any point never passes the
//! window of a durably pending alarm, and every pending alarm is still woken.
use super::*;
use crate::local::scheduling::{window, window_start};
use std::collections::BTreeSet;

const WINDOW: u64 = 60_000;
const TIMES: [u64; 4] = [0, WINDOW + 1_000, 2 * WINDOW + 1_000, 3 * WINDOW + 1_000];
const DEADLINES: [u64; 2] = [WINDOW + 500, 2 * WINDOW + 500];
const DEPTH: usize = 6;
const ACTOR: u8 = 1;

#[derive(Clone, Copy, Debug)]
enum Op {
    /// The runtime tracks the actor; an older registration may still live.
    Register,
    /// Hibernation retires the oldest or newest live registration.
    Retire(bool),
    /// A delivery commits a new alarm, filed in its deadline window.
    Arm(u64),
    Tick,
    /// The dispatcher opens the earliest filed window that has started.
    Open,
    /// The timer fires: parked keys are hosted again and wakes delivered.
    Due,
    /// A delivery of the due alarm the authority accepts or refuses.
    Deliver(bool),
    /// The host checkpoints and dies; a new host resumes from the checkpoint.
    Crash,
}

struct World {
    set: ReadySet<u8>,
    live: Vec<Registration<u8>>,
    alarm: crate::local::Alarm,
    filed: BTreeSet<u64>,
    consumed: u64,
    time: usize,
    crashed: bool,
}

type Outcome = Result<(), String>;

impl World {
    fn new() -> Self {
        let set = ReadySet::default();
        set.dispatch();
        Self {
            set,
            live: Vec::new(),
            alarm: crate::local::Alarm::default(),
            filed: BTreeSet::new(),
            consumed: 0,
            time: 0,
            crashed: false,
        }
    }

    fn now(&self) -> u64 {
        TIMES.get(self.time).copied().unwrap_or(u64::MAX)
    }

    fn openable(&self) -> Option<u64> {
        self.filed
            .range(self.consumed..)
            .next()
            .copied()
            .filter(|&filed| window_start(filed) <= self.now())
    }

    fn due(&self) -> bool {
        self.alarm
            .pending()
            .is_some_and(|deadline| deadline <= self.now())
    }

    fn enabled(&self, op: Op) -> bool {
        match op {
            Op::Register => self.live.len() < 2,
            Op::Retire(oldest) => self.live.len() > usize::from(oldest),
            Op::Arm(_) => !self.live.is_empty(),
            Op::Tick => self.time + 1 < TIMES.len(),
            Op::Open => self.openable().is_some(),
            Op::Due => true,
            Op::Deliver(_) => !self.live.is_empty() && self.due(),
            Op::Crash => !self.crashed,
        }
    }

    /// Host the actor as the runtime does: one registration per tracked key.
    fn host(&mut self) -> Result<&Registration<u8>, String> {
        if self.live.is_empty() {
            self.live.push(
                self.set
                    .register(ACTOR)
                    .map_err(|error| error.to_string())?,
            );
        }
        self.live.last().ok_or_else(|| "no registration".to_owned())
    }

    fn apply(&mut self, op: Op) -> Outcome {
        let now = self.now();
        match op {
            Op::Register => {
                let registration = self
                    .set
                    .register(ACTOR)
                    .map_err(|error| error.to_string())?;
                self.live.push(registration);
            }
            Op::Retire(oldest) => {
                let index = if oldest { 0 } else { self.live.len() - 1 };
                drop(self.live.remove(index));
            }
            Op::Arm(deadline) => {
                self.alarm = crate::local::Alarm {
                    generation: self.alarm.generation + 1,
                    deadline_unix_ms: Some(deadline),
                    delivered_generation: None,
                };
                self.filed.insert(window(deadline));
                let alarm = self.alarm.clone();
                self.host()?
                    .update_alarm(&alarm)
                    .map_err(|error| error.to_string())?;
            }
            Op::Tick => self.time += 1,
            Op::Open => {
                if let Some(opened) = self.openable() {
                    let registration = self.host()?;
                    registration.claim_alarm(window_start(opened));
                    registration.waker().wake_by_ref();
                    self.consumed = opened + 1;
                }
            }
            Op::Due => {
                if !self.set.parked_due(now, usize::MAX).is_empty() {
                    self.host()?.adopt_parked();
                }
                self.set.wake_due(now, usize::MAX);
            }
            Op::Deliver(accepted) => {
                let alarm = self.alarm.clone();
                let registration = self.host()?;
                let due = registration
                    .observe_delivery(&alarm, now)
                    .map_err(|error| error.to_string())?;
                if due && accepted {
                    let consumed = crate::local::Alarm {
                        delivered_generation: Some(alarm.generation),
                        ..alarm
                    };
                    registration
                        .observe_delivery(&consumed, now)
                        .map_err(|error| error.to_string())?;
                    self.alarm = consumed;
                } else if due {
                    registration
                        .defer_alarm(&alarm, now + 2 * WINDOW)
                        .map_err(|error| error.to_string())?;
                }
            }
            Op::Crash => {
                let checkpoint = self.checkpoint();
                self.live.clear();
                self.set = ReadySet::default();
                self.set.dispatch();
                self.consumed = checkpoint;
                self.crashed = true;
            }
        }
        Ok(())
    }

    fn checkpoint(&self) -> u64 {
        self.consumed.min(self.set.watermark(self.now()))
    }

    /// A restart from a checkpoint taken now reopens every window at or past
    /// it, so it must not pass a durably pending alarm's window.
    fn check(&self) -> Outcome {
        match self.alarm.pending() {
            Some(deadline) if window(deadline) < self.checkpoint() => Err(format!(
                "checkpoint {} passed pending window {}",
                self.checkpoint(),
                window(deadline)
            )),
            _ => Ok(()),
        }
    }

    /// Run the host to the end of time: a pending alarm must still be woken.
    fn finish(mut self) -> Outcome {
        self.time = TIMES.len();
        while self.openable().is_some() {
            self.apply(Op::Open)?;
        }
        self.apply(Op::Due)?;
        if self.alarm.pending().is_some() && self.set.take().is_none() {
            return Err("pending alarm never woken".to_owned());
        }
        Ok(())
    }
}

fn ops() -> Vec<Op> {
    let mut ops = vec![
        Op::Register,
        Op::Retire(true),
        Op::Retire(false),
        Op::Tick,
        Op::Open,
        Op::Due,
        Op::Deliver(true),
        Op::Deliver(false),
        Op::Crash,
    ];
    ops.extend(DEADLINES.map(Op::Arm));
    ops
}

fn explore(prefix: &mut Vec<Op>, ops: &[Op], runs: &mut usize) -> Outcome {
    let mut world = World::new();
    for &op in prefix.iter() {
        world.apply(op)?;
        world.check()?;
    }
    *runs += 1;
    let enabled: Vec<Op> = ops
        .iter()
        .copied()
        .filter(|&op| world.enabled(op))
        .collect();
    world.finish()?;
    if prefix.len() == DEPTH {
        return Ok(());
    }
    for op in enabled {
        prefix.push(op);
        explore(prefix, ops, runs)?;
        prefix.pop();
    }
    Ok(())
}

#[test]
fn no_interleaving_loses_or_skips_a_pending_alarm() -> Outcome {
    let mut prefix = Vec::new();
    let mut runs = 0;
    explore(&mut prefix, &ops(), &mut runs).map_err(|error| format!("{error} after {prefix:?}"))?;
    assert!(
        runs > 10_000,
        "the simulation explored only {runs} schedules"
    );
    Ok(())
}
