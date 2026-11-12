//! Deterministic simulation of delivery accounting across consumers. The
//! run loop and a concurrent `drain` both take work from the one shared ready
//! index. Either may stall, fail or be cancelled at any await point of its
//! delivery (before it owns the actor, or while delivering), and the host may
//! release an actor between a take and its execution. Producers commit input
//! with its activation entry, and the host checkpoints and crashes at every
//! point. The activation checkpoint advances only while the index reports
//! nothing queued or in flight, and keeps or carries forward a wake entry for
//! every failed actor (`scheduling::checkpoint`). The invariant: no
//! checkpoint passes an input that is neither delivered nor failed and
//! carried, so a host restarted from it delivers every input, while a
//! poisoned actor holds back only its own entry.
use super::*;
use crate::local::scheduling;
use std::collections::{BTreeMap, BTreeSet};

const INPUTS: usize = 3;
const DEPTH: usize = 7;

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
    /// A consumer takes one ready actor's work; it may then stall.
    Take(Consumer),
    /// The consumer executes its work. A released actor has no slot, so the
    /// work ends without delivering; otherwise its delivery owns the actor.
    Execute(Consumer),
    /// The consumer's future is dropped at its current await point.
    Cancel(Consumer),
    /// The consumer's delivery commits the actor's input, then settles.
    Deliver(Consumer),
    /// The consumer's delivery fails, leaving the actor's input unconsumed.
    Fail(Consumer),
    /// The host releases an actor no delivery owns. Under local placement
    /// its next activation entry hosts it again.
    Release(u8),
    /// The dispatcher checkpoints the entries it has observed, if it may.
    Checkpoint,
    /// The host dies; a new host replays the log from its checkpoint.
    Crash,
}

struct World {
    actors: Vec<u8>,
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
    /// Each consumer's taken work and, once executing, its delivery's flight.
    taken: BTreeMap<Consumer, (Work<u8>, Option<Flight<u8>>)>,
    crashed: bool,
}

type Outcome = Result<(), String>;

fn seq(index: usize) -> Result<u64, String> {
    u64::try_from(index).map_err(|error| error.to_string())
}

impl World {
    fn new(actors: u8) -> Result<Self, String> {
        let mut world = Self {
            actors: (1..=actors).collect(),
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
        for &actor in &self.actors {
            let registration = self
                .set
                .register(actor)
                .map_err(|error| error.to_string())?;
            self.registrations.insert(actor, registration);
        }
        self.seen = self.cursor;
        Ok(())
    }

    /// The actor a consumer's delivery owns, once it executes.
    fn owned(&self, actor: u8) -> bool {
        self.taken
            .values()
            .any(|(work, flight)| *work.key() == actor && flight.is_some())
    }

    fn enabled(&self, op: Op) -> bool {
        match op {
            Op::Send(_) => self.undelivered.len() < INPUTS && self.entries.len() < 2 * INPUTS,
            Op::Observe => self.seen < self.entries.len(),
            Op::Take(consumer) => !self.taken.contains_key(&consumer),
            Op::Execute(consumer) => self
                .taken
                .get(&consumer)
                .is_some_and(|(_, flight)| flight.is_none()),
            Op::Cancel(consumer) => self.taken.contains_key(&consumer),
            Op::Deliver(consumer) | Op::Fail(consumer) => self
                .taken
                .get(&consumer)
                .is_some_and(|(_, flight)| flight.is_some()),
            Op::Release(actor) => self.registrations.contains_key(&actor) && !self.owned(actor),
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
                // The local placement owner hosts a released actor again
                // on its next activation entry.
                if !self.registrations.contains_key(&actor) {
                    let registration = self
                        .set
                        .register(actor)
                        .map_err(|error| error.to_string())?;
                    self.registrations.insert(actor, registration);
                }
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
            Op::Execute(consumer) => {
                let (work, _) = self.taken.get(&consumer).ok_or("nothing taken")?;
                let actor = *work.key();
                if self.registrations.contains_key(&actor) {
                    let flight = self.set.flight(&actor);
                    if let Some((_, owned)) = self.taken.get_mut(&consumer) {
                        *owned = Some(flight);
                    }
                } else {
                    // No slot: the runtime's execute returns without a
                    // delivery, dropping the work unsettled.
                    self.taken.remove(&consumer);
                }
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
                flight.ok_or("not executing")?.finish(false);
                work.settle();
            }
            Op::Fail(consumer) => {
                let (work, flight) = self.taken.remove(&consumer).ok_or("nothing taken")?;
                flight.ok_or("not executing")?.finish(true);
                work.settle();
            }
            Op::Release(actor) => {
                self.registrations.remove(&actor);
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
    /// latest entry, with each carry committed alongside the cursor. A
    /// released actor's carry is its handoff, and its mark ends there.
    fn checkpoint(&mut self) -> Outcome {
        if self.set.busy() {
            return Ok(());
        }
        let from = self.cursor;
        let failed = self.set.failed(0);
        let holds = failed
            .iter()
            .map(|actor| {
                if !self.registrations.contains_key(actor) {
                    return Ok(None);
                }
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
                if self.registrations.contains_key(&actor) {
                    let latest = self.latest.entry(actor).or_default();
                    *latest = (*latest).max(cursor + 1);
                } else {
                    self.set.forget(&actor);
                }
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
    /// after a failed delivery or a release: every committed input is
    /// delivered.
    fn finish(mut self) -> Outcome {
        let mut restarted = false;
        loop {
            let op = [
                Op::Deliver(Consumer::Run),
                Op::Deliver(Consumer::Drain),
                Op::Execute(Consumer::Run),
                Op::Execute(Consumer::Drain),
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

fn ops(actors: u8) -> Vec<Op> {
    let mut ops = vec![
        Op::Observe,
        Op::Take(Consumer::Run),
        Op::Take(Consumer::Drain),
        Op::Execute(Consumer::Run),
        Op::Execute(Consumer::Drain),
        Op::Cancel(Consumer::Drain),
        Op::Deliver(Consumer::Run),
        Op::Deliver(Consumer::Drain),
        Op::Fail(Consumer::Drain),
        Op::Checkpoint,
        Op::Crash,
    ];
    for actor in 1..=actors {
        ops.push(Op::Send(actor));
        ops.push(Op::Release(actor));
    }
    ops
}

fn explore(prefix: &mut Vec<Op>, ops: &[Op], runs: &mut usize) -> Outcome {
    let mut world = World::new(2)?;
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

/// Every schedule of two actors to the bounded depth: take, execute,
/// release, cancel, deliver, fail, checkpoint and crash in every order.
#[test]
fn no_consumer_lets_a_checkpoint_pass_an_in_flight_delivery() -> Outcome {
    let mut prefix = Vec::new();
    let mut runs = 0;
    explore(&mut prefix, &ops(2), &mut runs)
        .map_err(|error| format!("{error} after {prefix:?}"))?;
    assert!(
        runs > 100_000,
        "the simulation explored only {runs} schedules"
    );
    Ok(())
}

/// Long seeded schedules of three actors, past the exhaustive depth.
#[test]
fn seeded_schedules_of_three_actors_never_pass_an_undelivered_input() -> Outcome {
    const SEEDS: u64 = 20_000;
    const STEPS: usize = 40;
    let ops = ops(3);
    for seed in 1..=SEEDS {
        let mut state = seed;
        let mut world = World::new(3)?;
        let mut schedule = Vec::new();
        for _ in 0..STEPS {
            let enabled: Vec<Op> = ops
                .iter()
                .copied()
                .filter(|&op| world.enabled(op))
                .collect();
            // xorshift64: a fixed seed replays the same schedule.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            let index =
                usize::try_from(state % enabled.len() as u64).map_err(|error| error.to_string())?;
            let op = enabled[index];
            schedule.push(op);
            world
                .apply(op)
                .and_then(|()| world.check())
                .map_err(|error| format!("seed {seed}: {error} after {schedule:?}"))?;
        }
        world
            .finish()
            .map_err(|error| format!("seed {seed}: {error} after {schedule:?}"))?;
    }
    Ok(())
}

/// One actor's every delivery fails while another keeps receiving input:
/// the checkpoint keeps advancing past the healthy actor's entries, and a
/// restart replays at most the poisoned actor's one carried entry.
#[test]
fn a_poisoned_actor_holds_back_only_its_own_entry() -> Outcome {
    const POISONED: u8 = 1;
    let mut world = World::new(2)?;
    world.apply(Op::Send(POISONED))?;
    for _ in 0..64 {
        world.apply(Op::Send(2))?;
        while world.enabled(Op::Observe) || world.set.busy() {
            if world.enabled(Op::Observe) {
                world.apply(Op::Observe)?;
            }
            world.apply(Op::Take(Consumer::Run))?;
            if world.enabled(Op::Execute(Consumer::Run)) {
                world.apply(Op::Execute(Consumer::Run))?;
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
