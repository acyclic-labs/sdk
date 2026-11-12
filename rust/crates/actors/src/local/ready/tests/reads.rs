//! Deterministic simulation of one shard's deadline-window reads. Two windows
//! of the shard are read concurrently and finish in either order, with an
//! alarm armed mid-read, deliveries and a crash interleaved at every point to
//! a bounded depth. Throughout, the shard's checkpoint never passes a window
//! whose read is in flight, nor a pending alarm's window, and a host
//! restarted from any checkpoint still wakes every pending alarm.
use super::*;
use crate::local::Alarm;
use crate::local::scheduling::{learn, open, read, window, window_start};
use std::collections::{BTreeMap, BTreeSet};

const WINDOW: u64 = 60_000;
/// Both windows have started, and both alarms are due.
const NOW: u64 = 3 * WINDOW + 1_000;
/// Armed before the host started: filed in window one.
const EARLY: u8 = 1;
/// Armed while the host runs: filed in window two.
const LATE: u8 = 2;
const DEPTH: usize = 8;

#[derive(Clone, Copy, Debug)]
enum Op {
    /// The timer starts reading every known window that has started.
    Open,
    /// The earliest (`false`) or latest (`true`) in-flight read finishes.
    Read(bool),
    /// A delivery commits LATE's alarm in window two.
    Arm,
    /// An actor's delivery consumes its due alarm.
    Deliver(u8),
    /// The host checkpoints and dies; a new host resumes from the checkpoint.
    Crash,
}

struct World {
    set: ReadySet<u8>,
    live: BTreeMap<u8, Registration<u8>>,
    alarms: BTreeMap<u8, Alarm>,
    /// Filed windows and their actors.
    filed: BTreeMap<u64, Vec<u8>>,
    windows: BTreeSet<(u64, u16)>,
    reading: BTreeSet<(u16, u64)>,
    consumed: BTreeMap<u16, u64>,
    crashed: bool,
}

type Outcome = Result<(), String>;

fn pending(deadline: u64) -> Alarm {
    Alarm {
        generation: 1,
        deadline_unix_ms: Some(deadline),
        delivered_generation: None,
    }
}

impl World {
    fn new() -> Self {
        let mut world = Self {
            set: ReadySet::default(),
            live: BTreeMap::new(),
            alarms: BTreeMap::from([(EARLY, pending(WINDOW + 500))]),
            filed: BTreeMap::from([(1, vec![EARLY])]),
            windows: BTreeSet::new(),
            reading: BTreeSet::new(),
            consumed: BTreeMap::new(),
            crashed: false,
        };
        world.start(0);
        world
    }

    /// The store's next filed window at or past `from`: one children page.
    fn next_window(&self, from: u64) -> Option<u64> {
        self.filed.range(from..).next().map(|(&filed, _)| filed)
    }

    /// Startup discovery from the checkpoint, as the dispatcher does.
    fn start(&mut self, consumed: u64) {
        self.live.clear();
        self.set = ReadySet::default();
        self.set.dispatch();
        self.reading.clear();
        self.consumed = BTreeMap::from([(0, consumed)]);
        self.windows = self
            .next_window(consumed)
            .map(|next| (next, 0))
            .into_iter()
            .collect();
    }

    fn host(&mut self, actor: u8) -> Result<&Registration<u8>, String> {
        if !self.live.contains_key(&actor) {
            let registration = self
                .set
                .register(actor)
                .map_err(|error| error.to_string())?;
            self.live.insert(actor, registration);
        }
        self.live
            .get(&actor)
            .ok_or_else(|| "no registration".to_owned())
    }

    fn due(&self, actor: u8) -> bool {
        self.alarms
            .get(&actor)
            .and_then(Alarm::pending)
            .is_some_and(|deadline| deadline <= NOW)
    }

    fn enabled(&self, op: Op) -> bool {
        match op {
            Op::Open => self
                .windows
                .first()
                .is_some_and(|&(known, _)| window_start(known) <= NOW),
            Op::Read(latest) => self.reading.len() > usize::from(latest),
            Op::Arm => !self.alarms.contains_key(&LATE),
            Op::Deliver(actor) => self.live.contains_key(&actor) && self.due(actor),
            Op::Crash => !self.crashed,
        }
    }

    fn apply(&mut self, op: Op) -> Outcome {
        match op {
            Op::Open => {
                open(&mut self.windows, &mut self.reading, &self.consumed, NOW);
            }
            Op::Read(latest) => {
                let (shard, opened) = if latest {
                    self.reading.last()
                } else {
                    self.reading.first()
                }
                .copied()
                .ok_or("no read in flight")?;
                for actor in self.filed.get(&opened).cloned().unwrap_or_default() {
                    self.host(actor)?.claim_alarm(window_start(opened));
                }
                let next = opened
                    .checked_add(1)
                    .and_then(|from| self.next_window(from));
                read(
                    &mut self.reading,
                    &mut self.consumed,
                    &mut self.windows,
                    (shard, opened),
                    next,
                )
                .map_err(|error| error.to_string())?;
            }
            Op::Arm => {
                let alarm = pending(2 * WINDOW + 500);
                self.alarms.insert(LATE, alarm.clone());
                self.filed.entry(2).or_default().push(LATE);
                self.host(LATE)?
                    .update_alarm(&alarm)
                    .map_err(|error| error.to_string())?;
            }
            Op::Deliver(actor) => {
                let alarm = self.alarms.get(&actor).cloned().ok_or("no alarm")?;
                let registration = self.host(actor)?;
                if registration
                    .observe_delivery(&alarm, NOW)
                    .map_err(|error| error.to_string())?
                {
                    let delivered = Alarm {
                        delivered_generation: Some(alarm.generation),
                        ..alarm
                    };
                    registration
                        .observe_delivery(&delivered, NOW)
                        .map_err(|error| error.to_string())?;
                    self.alarms.insert(actor, delivered);
                }
            }
            Op::Crash => {
                let checkpoint = self.checkpoint();
                self.start(checkpoint);
                self.crashed = true;
            }
        }
        // The dispatcher learns every newly armed deadline's window.
        let from = self.consumed.get(&0).copied().unwrap_or_default();
        for (deadline, _) in self.set.take_armed() {
            learn(&mut self.windows, from, 0, window(deadline));
        }
        Ok(())
    }

    fn checkpoint(&self) -> u64 {
        self.consumed
            .get(&0)
            .copied()
            .unwrap_or_default()
            .min(self.set.watermark(0, NOW))
    }

    /// A restart from a checkpoint taken now reopens every window at or past
    /// it, so it must not pass a window still being read or a pending alarm.
    fn check(&self) -> Outcome {
        let checkpoint = self.checkpoint();
        if let Some(&(_, opened)) = self.reading.first()
            && opened < checkpoint
        {
            return Err(format!(
                "checkpoint {checkpoint} passed window {opened} still being read"
            ));
        }
        for (actor, alarm) in &self.alarms {
            if let Some(deadline) = alarm.pending()
                && window(deadline) < checkpoint
            {
                return Err(format!(
                    "checkpoint {checkpoint} passed actor {actor}'s pending window {}",
                    window(deadline)
                ));
            }
        }
        Ok(())
    }

    /// Run the host to quiescence: every pending alarm must still be woken.
    fn finish(mut self) -> Outcome {
        loop {
            let op = [
                Op::Read(false),
                Op::Open,
                Op::Deliver(EARLY),
                Op::Deliver(LATE),
            ]
            .into_iter()
            .find(|&op| self.enabled(op));
            let Some(op) = op else { break };
            self.apply(op)?;
            self.check()?;
        }
        match self
            .alarms
            .iter()
            .find(|(_, alarm)| alarm.pending().is_some())
        {
            Some((actor, _)) => Err(format!("actor {actor}'s pending alarm never woken")),
            None => Ok(()),
        }
    }
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
fn no_interleaving_of_window_reads_passes_an_unread_window() -> Outcome {
    let ops = [
        Op::Open,
        Op::Read(false),
        Op::Read(true),
        Op::Arm,
        Op::Deliver(EARLY),
        Op::Deliver(LATE),
        Op::Crash,
    ];
    let mut prefix = Vec::new();
    let mut runs = 0;
    explore(&mut prefix, &ops, &mut runs).map_err(|error| format!("{error} after {prefix:?}"))?;
    assert!(
        runs > 1_000,
        "the simulation explored only {runs} schedules"
    );
    Ok(())
}
