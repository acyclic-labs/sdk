use super::super::{FileClientError, FileClientLimits, Pending, PendingRequests};
use super::{
    Action, BYTES, COMMANDS,
    faults::{self, Fault},
    observation::{Entry, Event, Outcome, State},
    reference::{self, Model},
};
use crate::{
    Error,
    client::FileClientDurability,
    task_volume::{FileKey, Write},
};

#[derive(Debug)]
pub(super) struct Input {
    pub(super) actions: Vec<Action>,
    pub(super) probe: Option<u64>,
}

pub(super) fn run(input: &Input, fault: Option<Fault>) -> Event {
    let _reset = faults::select(fault);
    let mut reference = Model::default();
    let mut owner = PendingRequests::new(FileClientLimits {
        commands: COMMANDS,
        bytes: BYTES,
        durability: FileClientDurability::Accepted,
    });
    let mut event = Event::default();
    event.state(reference.state(), state(&owner));
    for (step, action) in input.actions.iter().enumerate() {
        match *action {
            Some((id, size)) => {
                // ALL expected values are fixed from input before either
                // actual API call. Reference advancement never follows result.
                let entry = reference::entry(id, size, step as u64);
                let expected_check = reference.check(u64::from(size));
                let before = reference.state();
                let expected_admit = reference.admit(id, entry.clone());
                let after = reference.state();
                event.admission(expected_check, outcome(owner.check(u64::from(size))));
                event.state(before, state(&owner));
                let mut write = Write::new(FileKey::parse(&entry.key).unwrap(), entry.bytes);
                write.publication = entry.publication;
                write.channel = entry.channel;
                write.requester = entry.requester;
                event.admission(expected_admit, outcome(owner.admit(entry.cursor, write)));
                event.state(after, state(&owner));
            }
            None => {
                let expected = reference.retire();
                let after = reference.state();
                event.retirement(expected, owner.retire_front().as_ref().map(observe));
                event.state(after, state(&owner));
            }
        }
    }
    if let Some(probe) = input.probe {
        let expected = reference.check(probe);
        let after = reference.state();
        event.admission(expected, outcome(owner.check(probe)));
        event.state(after, state(&owner));
    }
    event
}

fn outcome(result: Result<(), FileClientError>) -> Outcome {
    match result {
        Ok(()) => Outcome::Accepted,
        Err(FileClientError::CommandQuota { limit }) => Outcome::CommandQuota(limit),
        Err(FileClientError::ByteQuota { limit }) => Outcome::ByteQuota(limit),
        Err(FileClientError::Harness(Error::Conflict(message))) => Outcome::Conflict(message),
        Err(error) => Outcome::Unexpected(error.to_string()),
    }
}

fn observe(pending: &Pending) -> Entry {
    let Pending { cursor, write } = pending;
    let Write {
        key,
        channel,
        requester,
        publication,
        bytes,
    } = write;
    Entry {
        key: key.path(),
        bytes: bytes.to_vec(),
        cursor: cursor.clone(),
        publication: *publication,
        channel: *channel,
        requester: *requester,
    }
}

fn state(owner: &PendingRequests) -> State {
    let PendingRequests {
        entries,
        keys,
        bytes,
        commands,
        limit,
    } = owner;
    let mut keys: Vec<_> = keys.iter().map(FileKey::path).collect();
    keys.sort();
    State {
        entries: entries.iter().map(observe).collect(),
        keys,
        bytes: *bytes,
        commands: *commands,
        limit: *limit,
        len: owner.len(),
        front: owner.front().map(observe),
    }
}
