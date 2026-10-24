//! No production queue, quota predicate, admission or retirement calls.
use super::{
    BYTES, COMMANDS,
    observation::{Entry, Outcome, State},
};
use crate::{
    OperationId, TaskId,
    task_volume::{Channel, Cursor},
};
use std::collections::VecDeque;

#[derive(Default)]
pub(super) struct Model(VecDeque<(u8, Entry)>);

pub(super) fn entry(id: u8, size: u8, step: u64) -> Entry {
    let position = step + 1;
    Entry {
        key: format!(".tasks/task{}/requests/op{}.json", id % 2, id / 2),
        bytes: (0..size)
            .map(|index| index.wrapping_mul(13) ^ id ^ step as u8)
            .collect(),
        cursor: Cursor {
            volume: [11; 32],
            epoch: uuid::Uuid::from_u128(9),
            schema: 4,
            query: [7; 32],
            position,
            accepted: position + 2,
            durable: position,
            commit: [position as u8; 32],
        },
        publication: (size % 2 == 1).then(|| OperationId::from_bytes([position as u8; 16])),
        channel: match id % 3 {
            0 => Some(Channel::Text),
            1 => Some(Channel::Reasoning),
            _ => None,
        },
        requester: (step % 2 == 0).then(|| TaskId::from_bytes([id; 16])),
    }
}

impl Model {
    pub(super) fn check(&self, bytes: u64) -> Outcome {
        if self.0.len() >= COMMANDS {
            Outcome::CommandQuota(COMMANDS)
        } else if self.used() + u128::from(bytes) > u128::from(BYTES) {
            Outcome::ByteQuota(BYTES)
        } else {
            Outcome::Accepted
        }
    }

    pub(super) fn admit(&mut self, id: u8, entry: Entry) -> Outcome {
        if self.0.iter().any(|(held, _)| *held == id) {
            return Outcome::Conflict("outbox request identity conflict".into());
        }
        let result = self.check(entry.bytes.len() as u64);
        if result == Outcome::Accepted {
            self.0.push_back((id, entry));
        }
        result
    }

    pub(super) fn retire(&mut self) -> Option<Entry> {
        self.0.pop_front().map(|(_, entry)| entry)
    }

    pub(super) fn state(&self) -> State {
        let entries: Vec<_> = self.0.iter().map(|(_, entry)| entry.clone()).collect();
        let mut keys: Vec<_> = entries.iter().map(|entry| entry.key.clone()).collect();
        keys.sort();
        State {
            len: entries.len(),
            front: entries.first().cloned(),
            bytes: self.used() as u64,
            commands: COMMANDS,
            limit: BYTES,
            entries,
            keys,
        }
    }

    fn used(&self) -> u128 {
        self.0
            .iter()
            .map(|(_, entry)| entry.bytes.len() as u128)
            .sum()
    }
}
