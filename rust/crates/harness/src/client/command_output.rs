//! Projection of the existing command outcome envelope, without another store.
use super::HeadlessOutcome;
use crate::task_volume::{Component, FileKey, TaskFile};
use crate::{Error, Result, task_volume::FileValue};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;
use std::{borrow::Cow, collections::BTreeMap};

pub(crate) fn command_prompt(key: &FileKey, bytes: &[u8]) -> Option<String> {
    use super::{CommandRequest, CommandResolution};
    let (FileKey::Task {
        file: TaskFile::Request(_),
        ..
    }
    | FileKey::Session {
        file: TaskFile::Request(_),
        ..
    }) = key
    else {
        return None;
    };
    crate::task_volume::command_query::related(key, TaskFile::ExecutionResult)?;
    let resolution = CommandResolution::from_request(bytes).ok()?;
    match resolution {
        CommandResolution::Prompt { text, .. }
        | CommandResolution::Submit {
            request: CommandRequest::Replace { text, .. },
        } => Some(text),
        _ => None,
    }
}

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

#[cfg(all(feature = "filesystem-local", not(target_arch = "wasm32")))]
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
fn text(value: &RawValue) -> Option<Cow<'_, str>> {
    #[derive(Deserialize)]
    struct Text<'a>(#[serde(borrow)] Cow<'a, str>);
    serde_json::from_str(value.get())
        .ok()
        .map(|Text(text)| text)
}
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Scope {
    Owner(OutputOwner),
    // The existing CommandSession query includes unscoped task commands in
    // every session. An explicit owner never inherits another session's files.
    Unscoped,
}
fn owners(key: &FileKey) -> [Option<Scope>; 2] {
    match key {
        FileKey::Task {
            task,
            file: TaskFile::ExecutionResult(_) | TaskFile::Request(_),
        } => [
            Some(Scope::Owner(OutputOwner::Task(task.clone()))),
            crate::task_volume::command_query::related(key, TaskFile::Request).map(|_| {
                crate::task_volume::command_query::owner(key)
                    .map(|owner| Scope::Owner(OutputOwner::Session(owner)))
                    .unwrap_or(Scope::Unscoped)
            }),
        ],
        FileKey::Session {
            session,
            file: TaskFile::ExecutionResult(_) | TaskFile::Request(_),
        } => [
            Some(Scope::Owner(OutputOwner::Session(session.clone()))),
            None,
        ],
        _ => [None, None],
    }
}
impl<'a> CommandOutputView<'a> {
    /// Invalid envelopes remain ordinary files and cannot invent a UI outcome.
    pub fn from_file(file: &'a FileValue) -> Option<Self> {
        Self::from_bytes(&file.key, &file.bytes)
    }
    fn from_bytes(key: &FileKey, bytes: &'a [u8]) -> Option<Self> {
        Outputs::includes(key).then_some(())?;
        let outcome: Outcome<'_> = serde_json::from_slice(bytes).ok()?;
        let completion = settled(&outcome).ok()?;
        let value: DisplayMetadata = outcome
            .ok
            .and_then(|value| serde_json::from_str(value.get()).ok())
            .unwrap_or_default();
        Some(Self {
            sequence: outcome.sequence,
            completion,
            text: value.text.and_then(text),
            task: value.task,
            error: outcome.error.and_then(text),
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
        Outputs::includes(&file.key).then_some(())?;
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
    rows: BTreeMap<Scope, BTreeMap<u64, std::collections::BTreeSet<FileKey>>>,
    keys: BTreeMap<FileKey, Metadata>,
    text_tasks: BTreeMap<(Scope, crate::TaskId), BTreeMap<u64, usize>>,
    requests: std::collections::BTreeSet<FileKey>,
    pending: BTreeMap<Scope, std::collections::BTreeSet<FileKey>>,
}
impl Outputs {
    pub(super) fn tracks(key: &FileKey) -> bool {
        Self::includes(key)
            || matches!(
                key,
                FileKey::Task {
                    file: TaskFile::Request(_),
                    ..
                } | FileKey::Session {
                    file: TaskFile::Request(_),
                    ..
                }
            )
    }
    fn pending(&mut self, key: &FileKey, include: bool) {
        for owner in owners(key).into_iter().flatten() {
            if include {
                self.pending.entry(owner).or_default().insert(key.clone());
            } else if let Some(keys) = self.pending.get_mut(&owner) {
                keys.remove(key);
                if keys.is_empty() {
                    self.pending.remove(&owner);
                }
            }
        }
    }
    pub(super) fn includes(key: &FileKey) -> bool {
        matches!(
            key,
            FileKey::Task {
                file: TaskFile::ExecutionResult(_),
                ..
            } | FileKey::Session {
                file: TaskFile::ExecutionResult(_),
                ..
            }
        )
    }
    pub(super) fn remove(&mut self, key: &FileKey) {
        if self.requests.remove(key) {
            self.pending(key, false);
            return;
        }
        let Some(value) = self.keys.remove(key) else {
            return;
        };
        if let Some(request) = crate::task_volume::command_query::related(key, TaskFile::Request)
            && self.requests.contains(&request)
        {
            self.pending(&request, true);
        }
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
        if command_prompt(&file.key, &file.bytes).is_some() {
            self.requests.insert(file.key.clone());
            if crate::task_volume::command_query::related(&file.key, TaskFile::ExecutionResult)
                .is_some_and(|result| !self.keys.contains_key(&result))
            {
                self.pending(&file.key, true);
            }
            return;
        }
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
        if let Some(request) =
            crate::task_volume::command_query::related(&file.key, TaskFile::Request)
        {
            self.pending(&request, false);
        }
    }
    fn has_text(&self, owner: &OutputOwner, task: crate::TaskId, after: u64) -> bool {
        [
            Some(Scope::Owner(owner.clone())),
            matches!(owner, OutputOwner::Session(_)).then_some(Scope::Unscoped),
        ]
        .into_iter()
        .flatten()
        .any(|scope| {
            self.text_tasks
                .get(&(scope, task))
                .and_then(BTreeMap::last_key_value)
                .is_some_and(|(sequence, _)| *sequence > after)
        })
    }
    fn from_scope<'a>(
        &'a self,
        owner: &Scope,
        sequence: u64,
        key: Option<&FileKey>,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        use std::ops::Bound::{Excluded, Unbounded};
        let rows = self.rows.get(owner);
        let current = key.and_then(|key| {
            rows?
                .get(&sequence)
                .map(|keys| keys.range((Excluded(key), Unbounded)))
        });
        let later = rows.map(|rows| rows.range((Excluded(sequence), Unbounded)));
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
    fn ordered<'a>(
        &'a self,
        owner: &OutputOwner,
        sequence: u64,
        key: Option<&FileKey>,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        let mut owned = self
            .from_scope(&Scope::Owner(owner.clone()), sequence, key)
            .peekable();
        let mut unscoped = matches!(owner, OutputOwner::Session(_))
            .then(|| self.from_scope(&Scope::Unscoped, sequence, key))
            .into_iter()
            .flatten()
            .peekable();
        std::iter::from_fn(move || match (owned.peek(), unscoped.peek()) {
            (Some((a, x)), Some((b, y))) if (x.sequence, *a) <= (y.sequence, *b) => owned.next(),
            (_, Some(_)) => unscoped.next(),
            _ => owned.next(),
        })
    }
    fn from<'a>(
        &'a self,
        owner: &OutputOwner,
        after: Option<(u64, &FileKey)>,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        self.ordered(
            owner,
            after.map_or(0, |(sequence, _)| sequence),
            after.map(|(_, key)| key),
        )
    }
    fn after<'a>(
        &'a self,
        owner: &OutputOwner,
        after: u64,
    ) -> impl Iterator<Item = (&'a FileKey, &'a Metadata)> + use<'a> {
        self.ordered(owner, after, None)
    }
}

impl super::FileQueryView {
    /// The immutable user prompt paired with a command request or outcome.
    pub fn command_prompt(&self, key: &FileKey) -> Option<String> {
        let request = crate::task_volume::command_query::related(key, TaskFile::Request)?;
        let file = self.files().get(&request)?;
        let [bytes] = file.segments.as_slice() else {
            return None;
        };
        command_prompt(&request, bytes)
    }
    #[cfg(feature = "tui")]
    pub(crate) fn pending_prompts<'a>(
        &'a self,
        owner: &OutputOwner,
    ) -> impl Iterator<Item = &'a FileKey> + use<'a> {
        [
            Some(Scope::Owner(owner.clone())),
            matches!(owner, OutputOwner::Session(_)).then_some(Scope::Unscoped),
        ]
        .into_iter()
        .flatten()
        .filter_map(|scope| self.outputs.pending.get(&scope))
        .flat_map(|keys| keys.iter())
    }
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
