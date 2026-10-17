//! Projection of the existing command outcome envelope, without another store.
use super::HeadlessOutcome;
use crate::task_volume::{Component, FileKey, TaskFile};
use crate::{Error, Result, task_volume::FileValue};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, collections::BTreeMap};

#[derive(Deserialize)]
struct Outcome<'a> {
    sequence: u64,
    completion: Option<HeadlessOutcome>,
    #[serde(borrow)]
    ok: Option<&'a RawValue>,
    #[serde(borrow)]
    error: Option<&'a RawValue>,
    #[serde(default)]
    completed: bool,
    #[serde(default)]
    output_omitted: bool,
}

fn settled(outcome: &Outcome<'_>) -> Result<HeadlessOutcome> {
    let invalid = || Error::Invalid("invalid command outcome envelope".into());
    if outcome.sequence == 0 {
        return Err(invalid());
    }
    if outcome.completed && outcome.output_omitted {
        return Ok(outcome.completion.unwrap_or(HeadlessOutcome::Indeterminate));
    }
    match (outcome.ok.is_some(), outcome.error) {
        (true, None)
            if outcome
                .completion
                .is_none_or(|state| state == HeadlessOutcome::Completed) =>
        {
            Ok(HeadlessOutcome::Completed)
        }
        (false, Some(error))
            if error.get().starts_with('"')
                && outcome.completion != Some(HeadlessOutcome::Completed) =>
        {
            Ok(outcome.completion.unwrap_or(HeadlessOutcome::Indeterminate))
        }
        _ => Err(invalid()),
    }
}

pub(super) fn completion(bytes: &[u8]) -> Result<HeadlessOutcome> {
    let outcome: Outcome<'_> =
        serde_json::from_slice(bytes).map_err(|error| Error::Invalid(error.to_string()))?;
    settled(&outcome)
}

/// Select physical task files or a logical UI session across admitted tasks.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Serialize, Deserialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub enum OutputOwner {
    /// A task's execution results.
    Task(Component),
    /// A UI session's execution results.
    Session(Component),
}
/// Parsed metadata from one canonical outcome file, shared by every renderer.
#[derive(Clone, Debug, PartialEq, Serialize)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[cfg_attr(feature = "wasm", tsify(large_number_types_as_bigints))]
pub struct CommandOutputView<'a> {
    /// Canonical outcome ordering, independent of UUID/path order.
    pub sequence: u64,
    /// Terminal result from the producer's existing envelope.
    pub completion: HeadlessOutcome,
    /// Exact decoded text when this command produces displayable text.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub text: Option<Cow<'a, str>>,
    /// Exact failure text, if present.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub error: Option<Cow<'a, str>>,
    /// Stock task selected by this completed command, when supplied.
    #[cfg_attr(feature = "wasm", tsify(type = "string | null"))]
    pub task: Option<crate::TaskId>,
}
#[derive(Default, Deserialize)]
struct Display<'a> {
    #[serde(borrow)]
    text: Option<Cow<'a, str>>,
    task: Option<crate::TaskId>,
}
fn owners(key: &FileKey) -> [Option<OutputOwner>; 2] {
    match key {
        FileKey::Task {
            task,
            file: TaskFile::ExecutionResult(_),
        } => [
            Some(OutputOwner::Task(task.clone())),
            crate::task_volume::command_query::owner(key).map(OutputOwner::Session),
        ],
        FileKey::Session {
            session,
            file: TaskFile::ExecutionResult(_),
        } => [Some(OutputOwner::Session(session.clone())), None],
        _ => [None, None],
    }
}
impl<'a> CommandOutputView<'a> {
    /// Invalid envelopes remain ordinary files and cannot invent a UI outcome.
    pub fn from_file(file: &'a FileValue) -> Option<Self> {
        Self::from_bytes(&file.key, &file.bytes)
    }
    fn from_bytes(key: &FileKey, bytes: &'a [u8]) -> Option<Self> {
        owners(key)[0].as_ref()?;
        let outcome: Outcome<'_> = serde_json::from_slice(bytes).ok()?;
        let completion = settled(&outcome).ok()?;
        let value: Display = outcome
            .ok
            .and_then(|value| serde_json::from_str(value.get()).ok())
            .unwrap_or_default();
        Some(Self {
            sequence: outcome.sequence,
            completion,
            text: value.text,
            task: value.task,
            error: outcome
                .error
                .and_then(|value| serde_json::from_str(value.get()).ok()),
        })
    }
}
#[derive(Clone, Debug, PartialEq)]
struct Metadata {
    sequence: u64,
    task: Option<crate::TaskId>,
    has_text: bool,
}
#[derive(Default, Deserialize)]
struct DisplayMetadata<'a> {
    #[serde(borrow)]
    text: Option<&'a RawValue>,
    task: Option<crate::TaskId>,
}
impl Metadata {
    fn from_file(file: &FileValue) -> Option<Self> {
        owners(&file.key)[0].as_ref()?;
        let outcome: Outcome<'_> = serde_json::from_slice(&file.bytes).ok()?;
        settled(&outcome).ok()?;
        let display: DisplayMetadata<'_> = outcome
            .ok
            .and_then(|value| serde_json::from_str(value.get()).ok())
            .unwrap_or_default();
        Some(Self {
            sequence: outcome.sequence,
            task: display.task,
            has_text: display.text.is_some_and(|text| text.get().starts_with('"')),
        })
    }
}
/// Ordered selections index metadata once; payloads stay in the canonical files.
#[derive(Clone, Debug, Default, PartialEq)]
pub(super) struct Outputs {
    rows: BTreeMap<OutputOwner, BTreeMap<u64, std::collections::BTreeSet<FileKey>>>,
    keys: BTreeMap<FileKey, Metadata>,
    text_tasks: BTreeMap<(OutputOwner, crate::TaskId), BTreeMap<u64, usize>>,
}
impl Outputs {
    pub(super) fn includes(key: &FileKey) -> bool {
        owners(key)[0].is_some()
    }
    pub(super) fn remove(&mut self, key: &FileKey) {
        let Some(value) = self.keys.remove(key) else {
            return;
        };
        for owner in owners(key).into_iter().flatten() {
            let rows = self.rows.get_mut(&owner).expect("indexed owner");
            let values = rows.get_mut(&value.sequence).expect("indexed sequence");
            assert!(values.remove(key));
            if values.is_empty() {
                rows.remove(&value.sequence);
            }
            if rows.is_empty() {
                self.rows.remove(&owner);
            }
            if let Some(task) = value.task.filter(|_| value.has_text) {
                let identity = (owner, task);
                let sequences = self.text_tasks.get_mut(&identity).expect("indexed task");
                let count = sequences
                    .get_mut(&value.sequence)
                    .expect("indexed task sequence");
                *count -= 1;
                if *count == 0 {
                    sequences.remove(&value.sequence);
                }
                if sequences.is_empty() {
                    self.text_tasks.remove(&identity);
                }
            }
        }
    }
    pub(super) fn insert(&mut self, file: &FileValue) {
        self.remove(&file.key);
        let Some(value) = Metadata::from_file(file) else {
            return;
        };
        for owner in owners(&file.key).into_iter().flatten() {
            self.rows
                .entry(owner.clone())
                .or_default()
                .entry(value.sequence)
                .or_default()
                .insert(file.key.clone());
            if let Some(task) = value.task.filter(|_| value.has_text) {
                *self
                    .text_tasks
                    .entry((owner, task))
                    .or_default()
                    .entry(value.sequence)
                    .or_default() += 1;
            }
        }
        self.keys.insert(file.key.clone(), value);
    }
    fn has_text(&self, owner: &OutputOwner, task: crate::TaskId, after: u64) -> bool {
        self.text_tasks
            .get(&(owner.clone(), task))
            .and_then(BTreeMap::last_key_value)
            .is_some_and(|(sequence, _)| *sequence > after)
    }
    fn from<'a>(
        &'a self,
        owner: &OutputOwner,
        after: Option<(u64, &FileKey)>,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        use std::ops::Bound::{Excluded, Unbounded};
        let rows = self.rows.get(owner);
        let current = after.and_then(|(sequence, key)| {
            rows?
                .get(&sequence)
                .map(|values| values.range((Excluded(key), Unbounded)))
        });
        let later = rows.map(|rows| {
            rows.range((
                Excluded(after.map_or(0, |(sequence, _)| sequence)),
                Unbounded,
            ))
        });
        current
            .into_iter()
            .flatten()
            .chain(
                later
                    .into_iter()
                    .flatten()
                    .flat_map(|(_, keys)| keys.iter()),
            )
            .map(|key| (key, &self.keys[key]))
    }
    fn after<'a>(
        &'a self,
        owner: &OutputOwner,
        after: u64,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        self.rows
            .get(owner)
            .into_iter()
            .flat_map(move |rows| {
                rows.range((std::ops::Bound::Excluded(after), std::ops::Bound::Unbounded))
            })
            .flat_map(|(_, keys)| keys.iter())
            .map(|key| (key, &self.keys[key]))
    }
}
impl super::FileQueryView {
    fn output<'a>(&'a self, key: &'a FileKey) -> Option<(&'a FileKey, CommandOutputView<'a>)> {
        let file = self.files().get(key)?;
        let [bytes] = file.segments.as_slice() else {
            return None;
        };
        Some((key, CommandOutputView::from_bytes(key, bytes)?))
    }

    /// Whether a retained outcome supplies this task's canonical completed text.
    pub fn has_command_text(&self, owner: &OutputOwner, task: crate::TaskId, after: u64) -> bool {
        self.outputs.has_text(owner, task, after)
    }
    /// Resume producer order at an exact file identity, including tied sequence numbers.
    pub fn outputs_from<'a>(
        &'a self,
        owner: &OutputOwner,
        after: Option<(u64, &FileKey)>,
    ) -> impl Iterator<Item = (&'a FileKey, CommandOutputView<'a>)> + use<'a> {
        self.outputs
            .from(owner, after)
            .filter_map(|(key, _)| self.output(key))
    }
    /// Borrow outcomes in producer order, visiting only records after the supplied sequence.
    pub fn outputs<'a>(
        &'a self,
        owner: &OutputOwner,
        after: u64,
    ) -> impl Iterator<Item = (&'a FileKey, CommandOutputView<'a>)> + use<'a> {
        self.outputs
            .after(owner, after)
            .filter_map(|(key, _)| self.output(key))
    }
}
pub(in crate::client) mod follow;
#[cfg(test)]
mod tests;
