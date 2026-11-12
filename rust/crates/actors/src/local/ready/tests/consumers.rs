//! Deterministic simulation of delivery accounting across consumers. The
//! run loop and a concurrent `drain` both take work from the one shared ready
//! index, and either may stall or fail mid-delivery; producers commit input
//! with its activation entry, and the host checkpoints and crashes at every
//! point, to a bounded depth. The activation checkpoint, which advances only
//! while the index reports nothing queued, in flight or failed, never passes
//! an entry whose input is undelivered, so a host restarted from it delivers
//! every input.
use super::*;
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
    /// Entries whose input no delivery has committed yet.
    undelivered: BTreeSet<usize>,
    /// The next entry the dispatcher observes.
    seen: usize,
    /// The durable activation checkpoint.
    cursor: usize,
    /// Each consumer's taken work and its delivery's flight.
    taken: BTreeMap<Consumer, (Work<u8>, Flight<u8>)>,
    crashed: bool,
}

type Outcome = Result<(), String>;

impl World {
    fn new() -> Result<Self, String> {
        let mut world = Self {
            set: ReadySet::default(),
            registrations: BTreeMap::new(),
            entries: Vec::new(),
            undelivered: BTreeSet::new(),
            seen: 0,
            cursor: 0,
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
            Op::Send(_) => self.entries.len() < INPUTS,
            Op::Observe => self.seen < self.entries.len(),
            Op::Take(consumer) => !self.taken.contains_key(&consumer),
            Op::Deliver(consumer) | Op::Fail(consumer) => self.taken.contains_key(&consumer),
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
                self.registrations
                    .get(&actor)
                    .ok_or("untracked actor")?
                    .waker()
                    .wake_by_ref();
            }
            Op::Take(consumer) => {
                if let Some(work) = self.set.take() {
                    self.taken.insert(consumer, (work, self.set.flight()));
                }
            }
            Op::Deliver(consumer) => {
                let (work, flight) = self.taken.remove(&consumer).ok_or("nothing taken")?;
                // A delivery reads its actor's whole committed input.
                let actor = *work.key();
                let entries = &self.entries;
                self.undelivered.retain(|&entry| entries[entry] != actor);
                drop(flight);
                drop(work);
            }
            Op::Fail(consumer) => {
                let (work, flight) = self.taken.remove(&consumer).ok_or("nothing taken")?;
                flight.fail();
                drop(work);
            }
            Op::Checkpoint => {
                if !self.set.busy() && !self.set.failed() {
                    self.cursor = self.seen;
                }
            }
            Op::Crash => {
                self.start()?;
                self.crashed = true;
            }
        }
        Ok(())
    }

    /// A restart replays only entries at or past the checkpoint, so it must
    /// never pass an entry whose input is still undelivered.
    fn check(&self) -> Outcome {
        match self.undelivered.first() {
            Some(&entry) if entry < self.cursor => Err(format!(
                "checkpoint {} passed undelivered entry {entry} of actor {}",
                self.cursor, self.entries[entry]
            )),
            _ => Ok(()),
        }
    }

    /// Run the host to quiescence, then once more from its checkpoint, as
    /// after a failed delivery: every committed input is delivered.
    fn finish(mut self) -> Outcome {
        let mut restarted = false;
        loop {
            let op = [
                Op::Deliver(Consumer::Run),
                Op::Deliver(Consumer::Drain),
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
