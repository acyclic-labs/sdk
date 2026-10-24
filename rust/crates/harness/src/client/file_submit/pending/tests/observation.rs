use crate::{OperationId, task_volume::Cursor};

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) struct Entry {
    pub(super) key: String,
    pub(super) bytes: Vec<u8>,
    pub(super) cursor: Cursor,
    pub(super) publication: Option<OperationId>,
    pub(super) channel: Option<crate::task_volume::Channel>,
    pub(super) requester: Option<crate::TaskId>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(super) enum Outcome {
    Accepted,
    CommandQuota(usize),
    ByteQuota(u64),
    Conflict(String),
    Unexpected(String),
}

#[derive(Debug, Eq, PartialEq)]
pub(super) struct State {
    pub(super) entries: Vec<Entry>,
    pub(super) keys: Vec<String>,
    pub(super) bytes: u64,
    pub(super) commands: usize,
    pub(super) limit: u64,
    pub(super) len: usize,
    pub(super) front: Option<Entry>,
}

#[derive(Debug, Default)]
pub(super) struct Event {
    trace: Vec<Observation>,
}

#[derive(Debug)]
enum Observation {
    Admission(Outcome, Outcome),
    Retirement(Option<Entry>, Option<Entry>),
    Ownership(State, State),
}

impl Event {
    pub(super) fn admission(&mut self, expected: Outcome, actual: Outcome) {
        self.trace.push(Observation::Admission(expected, actual));
    }
    pub(super) fn retirement(&mut self, expected: Option<Entry>, actual: Option<Entry>) {
        self.trace.push(Observation::Retirement(expected, actual));
    }
    pub(super) fn state(&mut self, expected: State, actual: State) {
        self.trace.push(Observation::Ownership(expected, actual));
    }
}

pub(super) fn admission(event: &Event) -> Result<(), String> {
    compare(event, |step| match step {
        Observation::Admission(expected, actual) => Some((expected, actual)),
        _ => None,
    })
}
pub(super) fn retirement(event: &Event) -> Result<(), String> {
    compare(event, |step| match step {
        Observation::Retirement(expected, actual) => Some((expected, actual)),
        _ => None,
    })
}
pub(super) fn ownership(event: &Event) -> Result<(), String> {
    compare(event, |step| match step {
        Observation::Ownership(expected, actual) => Some((expected, actual)),
        _ => None,
    })
}

fn compare<'a, T: std::fmt::Debug + Eq + 'a>(
    event: &'a Event,
    project: impl Fn(&'a Observation) -> Option<(&'a T, &'a T)>,
) -> Result<(), String> {
    for (index, step) in event.trace.iter().enumerate() {
        if let Some((expected, actual)) = project(step)
            && expected != actual
        {
            return Err(format!(
                "event {index}: expected {expected:?}, actual {actual:?}"
            ));
        }
    }
    Ok(())
}
