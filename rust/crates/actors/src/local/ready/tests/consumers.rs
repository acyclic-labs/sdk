//! Deterministic simulation of delivery accounting across consumers. The
//! run loop and a concurrent `drain` both take work from the one shared ready
//! index, and either may stall, fail or be cancelled at any await point of
//! its delivery (before admission, or while delivering); producers commit input
//! with its activation entry, and the host checkpoints and crashes at every
//! point, to a bounded depth. The activation checkpoint advances only while
//! the index reports nothing queued or in flight, and keeps or carries
//! forward a wake entry for every failed actor (`scheduling::checkpoint`), so
//! a host restarted from it delivers every input, while a poisoned actor
//! holds back only its own entry.
use super::*;
use crate::local::scheduling;
use std::collections::{BTreeMap, BTreeSet};

const ACTORS: [u8; 2] = [1, 2];
const INPUTS: usize = 3;
const DEPTH: usize = 6;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Consumer {
    Run,
    Drain,
}

#[derive(Clone, Copy, Debug)]
enum Op {
    /// A producer commits input for an actor with its activation entry.
    Send(u8),
    /// The dispatcher observes the next activation entry and wakes its actor.
    Observe,
    /// A consumer takes one ready actor; it may then stall indefinitely.
    Take(Consumer),
    /// The consumer's delivery starts and waits for its lock and admission.
    Admit(Consumer),
    /// The consumer's future is dropped at its current await point.
    Cancel(Consumer),
    /// The consumer's delivery commits the actor's input, then settles.
    Deliver(Consumer),
    /// The consumer's delivery fails, leaving the actor's input unconsumed.
    Fail(Consumer),
    /// The dispatcher checkpoints the entries it has observed, if it may.
    Checkpoint,
    /// The host dies; a new host replays the log from its checkpoint.
    Crash,
}

struct World {
    set: ReadySet<u8>,
    registrations: BTreeMap<u8, Registration<u8>>,
    /// The activation log, by entry: its actor.
    entries: Vec<u8>,
    /// Input entries no delivery has committed yet.
    undelivered: BTreeSet<usize>,
    /// The next entry the dispatcher observes.
    seen: usize,
    /// The durable activation checkpoint.
    cursor: usize,
    /// Per actor, just past its latest entry observed or carried.
    latest: BTreeMap<u8, usize>,
    /// Each consumer's taken work and its started delivery's flight.
    taken: BTreeMap<Consumer, (Work<u8>, Option<Flight<u8>>)>,
    crashed: bool,
}

type Outcome = Result<(), String>;

fn seq(index: usize) -> Result<u64, String> {
    u64::try_from(index).map_err(|error| error.to_string())
}

impl World {
    fn new() -> Result<Self, String> {
        let mut world = Self {
            set: ReadySet::default(),
            registrations: BTreeMap::new(),
            entries: Vec::new(),
            undelivered: BTreeSet::new(),
            seen: 0,
            cursor: 0,
            latest: BTreeMap::new(),
            taken: BTreeMap::new(),
            crashed: false,
        };
        world.start()?;
        Ok(world)
    }

    /// A host starts: it tracks every actor and replays from the checkpoint.
    fn start(&mut self) -> Outcome {
        self.taken.clear();
        self.registrations.clear();
        self.latest.clear();
        self.set = ReadySet::default();
        for actor in ACTORS {
            let registration = self
                .set
                .register(actor)
                .map_err(|error| error.to_string())?;
            self.registrations.insert(actor, registration);
        }
        self.seen = self.cursor;
        Ok(())
    }

    fn enabled(&self, op: Op) -> bool {
        match op {
            Op::Send(_) => self.undelivered.len() < INPUTS && self.entries.len() < 2 * INPUTS,
            Op::Observe => self.seen < self.entries.len(),
            Op::Take(consumer) => !self.taken.contains_key(&consumer),
            Op::Admit(consumer) => self
                .taken
                .get(&consumer)
                .is_some_and(|(_, flight)| flight.is_none()),
            Op::Cancel(consumer) => self.taken.contains_key(&consumer),
            Op::Deliver(consumer) | Op::Fail(consumer) => self
                .taken
                .get(&consumer)
                .is_some_and(|(_, flight)| flight.is_some()),
            Op::Checkpoint => true,
            Op::Crash => !self.crashed,
        }
    }

    fn apply(&mut self, op: Op) -> Outcome {
        match op {
            Op::Send(actor) => {
                self.undelivered.insert(self.entries.len());
                self.entries.push(actor);
            }
            Op::Observe => {
                let actor = self.entries[self.seen];
                self.seen += 1;
                let latest = self.latest.entry(actor).or_default();
                *latest = (*latest).max(self.seen);
                self.registrations
                    .get(&actor)
                    .ok_or("untracked actor")?
                    .waker()
                    .wake_by_ref();
            }
            Op::Take(consumer) => {
                if let Some(work) = self.set.take() {
                    self.taken.insert(consumer, (work, None));
                }
            }
            Op::Admit(consumer) => {
                let (work, flight) = self.taken.get_mut(&consumer).ok_or("nothing taken")?;
                *flight = Some(self.set.flight(work.key()));
            }
            Op::Cancel(consumer) => {
                self.taken.remove(&consumer).ok_or("nothing taken")?;
            }
            Op::Deliver(consumer) => {
                let (work, flight) = self.taken.remove(&consumer).ok_or("nothing taken")?;
                // A delivery reads its actor's whole committed input.
                let actor = *work.key();
                let entries = &self.entries;
                self.undelivered.retain(|&entry| entries[entry] != actor);
                flight.ok_or("not admitted")?.finish(false);
                work.settle();
            }
            Op::Fail(consumer) => {
                let (work, flight) = self.taken.remove(&consumer).ok_or("nothing taken")?;
                flight.ok_or("not admitted")?.finish(true);
                work.settle();
            }
            Op::Checkpoint => self.checkpoint()?,
            Op::Crash => {
                self.start()?;
                self.crashed = true;
            }
        }
        Ok(())
    }

    /// The run loop's checkpoint: the shared rule over every failed actor's
    /// latest entry, with each carry committed alongside the cursor.
    fn checkpoint(&mut self) -> Outcome {
        if self.set.busy() {
            return Ok(());
        }
        let from = self.cursor;
        let failed = self.set.failed(0);
        let holds = failed
            .iter()
            .map(|actor| {
                self.latest
                    .get(actor)
                    .filter(|&&latest| latest > from)
                    .map(|&latest| seq(latest - 1))
                    .transpose()
            })
            .collect::<Result<Vec<_>, String>>()?;
        let (cursor, carry) = scheduling::checkpoint(seq(self.seen)?, &holds);
        let cursor = usize::try_from(cursor).map_err(|error| error.to_string())?;
        if cursor <= from {
            return Ok(());
        }
        if carry {
            for actor in failed {
                self.entries.push(actor);
                let latest = self.latest.entry(actor).or_default();
                *latest = (*latest).max(cursor + 1);
            }
        }
        self.cursor = cursor;
        Ok(())
    }

    /// A restart replays only entries at or past the checkpoint, so every
    /// actor with undelivered input must keep an entry there.
    fn check(&self) -> Outcome {
        for &entry in &self.undelivered {
            let actor = self.entries[entry];
            if !self.entries[self.cursor..].contains(&actor) {
                return Err(format!(
                    "checkpoint {} passed undelivered entry {entry} of actor {actor}",
                    self.cursor
                ));
            }
        }
        Ok(())
    }

    /// Run the host to quiescence, then once more from its checkpoint, as
    /// after a failed delivery: every committed input is delivered.
    fn finish(mut self) -> Outcome {
        let mut restarted = false;
        loop {
            let op = [
                Op::Deliver(Consumer::Run),
                Op::Deliver(Consumer::Drain),
                Op::Admit(Consumer::Run),
                Op::Admit(Consumer::Drain),
                Op::Observe,
                Op::Take(Consumer::Run),
            ]
            .into_iter()
            .find(|&op| self.enabled(op) && (!matches!(op, Op::Take(_)) || self.set.busy()));
            let Some(op) = op else {
                if restarted {
                    break;
                }
                restarted = true;
                self.start()?;
                continue;
            };
            self.apply(op)?;
            self.check()?;
        }
        match self.undelivered.first() {
            Some(&entry) => Err(format!(
                "input entry {entry} of actor {} was never delivered",
                self.entries[entry]
            )),
            None => Ok(()),
        }
    }
}

fn explore(prefix: &mut Vec<Op>, ops: &[Op], runs: &mut usize) -> Outcome {
    let mut world = World::new()?;
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
fn no_consumer_lets_a_checkpoint_pass_an_in_flight_delivery() -> Outcome {
    let mut ops = vec![
        Op::Observe,
        Op::Take(Consumer::Run),
        Op::Take(Consumer::Drain),
        Op::Admit(Consumer::Run),
        Op::Admit(Consumer::Drain),
        Op::Cancel(Consumer::Drain),
        Op::Deliver(Consumer::Run),
        Op::Deliver(Consumer::Drain),
        Op::Fail(Consumer::Drain),
        Op::Checkpoint,
        Op::Crash,
    ];
    ops.extend(ACTORS.map(Op::Send));
    let mut prefix = Vec::new();
    let mut runs = 0;
    explore(&mut prefix, &ops, &mut runs).map_err(|error| format!("{error} after {prefix:?}"))?;
    assert!(
        runs > 10_000,
        "the simulation explored only {runs} schedules"
    );
    Ok(())
}

/// One actor's every delivery fails while another keeps receiving input:
/// the checkpoint keeps advancing past the healthy actor's entries, and a
/// restart replays at most the poisoned actor's one carried entry.
#[test]
fn a_poisoned_actor_holds_back_only_its_own_entry() -> Outcome {
    const POISONED: u8 = 1;
    let mut world = World::new()?;
    world.apply(Op::Send(POISONED))?;
    for _ in 0..64 {
        world.apply(Op::Send(2))?;
        while world.enabled(Op::Observe) || world.set.busy() {
            if world.enabled(Op::Observe) {
                world.apply(Op::Observe)?;
            }
            world.apply(Op::Take(Consumer::Run))?;
            if world.enabled(Op::Admit(Consumer::Run)) {
                world.apply(Op::Admit(Consumer::Run))?;
            }
            if let Some((work, _)) = world.taken.get(&Consumer::Run) {
                let poisoned = *work.key() == POISONED;
                world.apply(if poisoned {
                    Op::Fail(Consumer::Run)
                } else {
                    Op::Deliver(Consumer::Run)
                })?;
            }
        }
        world.apply(Op::Checkpoint)?;
        world.check()?;
    }
    let replay = world.entries.len() - world.cursor;
    assert!(
        replay <= 1,
        "a restart replays {replay} entries behind a poisoned actor"
    );
    assert!(
        world.undelivered.contains(&0),
        "the poisoned input was lost"
    );
    Ok(())
}
