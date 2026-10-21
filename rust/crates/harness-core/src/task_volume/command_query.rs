use super::{Component, Error, FileIndex, FileKey, FileValue, Index, Query, Result, TaskFile};
use std::collections::{BTreeMap, BTreeSet};

#[doc(hidden)]
pub fn identity(id: &Component) -> Option<(Option<uuid::Uuid>, crate::OperationId)> {
    let text = id.as_str();
    let (session, operation) = if let Some(owned) = text.strip_prefix("command-") {
        let (session, operation) = owned.split_once('.')?;
        (Some(uuid::Uuid::parse_str(session).ok()?), operation)
    } else {
        (None, text)
    };
    Some((session, crate::OperationId::parse(operation).ok()?))
}

#[doc(hidden)]
pub fn related(key: &FileKey, make: fn(Component) -> TaskFile) -> Option<FileKey> {
    let (FileKey::Session { file, .. } | FileKey::Task { file, .. }) = key else {
        return None;
    };
    let (TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id)) =
        file
    else {
        return None;
    };
    identity(id)?;
    let file = make(id.clone());
    Some(match key {
        FileKey::Session { session, .. } => FileKey::Session {
            session: session.clone(),
            file,
        },
        FileKey::Task { task, .. } => FileKey::Task {
            task: task.clone(),
            file,
        },
        _ => return None,
    })
}

pub(crate) fn is_result(key: &FileKey) -> bool {
    matches!(
        key,
        FileKey::Session {
            file: TaskFile::ExecutionResult(_),
            ..
        } | FileKey::Task {
            file: TaskFile::ExecutionResult(_),
            ..
        }
    ) && related(key, TaskFile::Request).is_some()
}

#[doc(hidden)]
pub fn owner(key: &FileKey) -> Option<Component> {
    match key {
        FileKey::Session { session, .. } => Some(session.clone()),
        FileKey::Task {
            file:
                TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id),
            ..
        } => identity(id)
            .and_then(|(session, _)| session)
            .map(|session| {
                Component::new(
                    session
                        .hyphenated()
                        .encode_lower(&mut uuid::Uuid::encode_buffer()),
                )
                .expect("UUID is a namespace component")
            }),
        _ => None,
    }
}

#[doc(hidden)]
pub fn matches(session: &Component, key: &FileKey) -> bool {
    match key {
        FileKey::Session { session: owner, .. } => owner == session,
        FileKey::Task {
            file:
                TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id),
            ..
        } => identity(id).is_some_and(|(owner, _)| {
            owner.is_none_or(|owner| {
                *owner
                    .hyphenated()
                    .encode_lower(&mut uuid::Uuid::encode_buffer())
                    == *session.as_str()
            })
        }),
        FileKey::Ui { directory, .. } => {
            directory.as_str() == "config"
                || directory.as_str().strip_prefix("session-") == Some(session.as_str())
                || directory.as_str().starts_with("task-")
        }
        FileKey::Input(path) => matches!(
            path.as_str(),
            "commands/catalog.json" | "commands/themes.json"
        ),
        _ => false,
    }
}

#[cfg(test)]
#[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
type Executions = Vec<((Option<Component>, u64), FileKey)>;
/// Completed executions in publication order and the unfinished inbox, per owner.
#[derive(Default)]
#[cfg_attr(acyclic_harness_loom, derive(Clone))]
pub(super) struct Commands {
    executions: BTreeMap<(Option<Component>, u64), FileKey>,
    pending: BTreeMap<Option<Component>, BTreeSet<FileKey>>,
}
/// A command file about to replace any stored version of its key.
pub(super) enum Command {
    Result(Option<Component>, FileKey),
    Pending(Option<Component>, FileKey),
}
impl Commands {
    /// Classifies before the index insert; a pending request checks its result once.
    pub(super) fn prepare(&self, files: &FileIndex, key: &FileKey) -> Option<Command> {
        if is_result(key) {
            Some(Command::Result(owner(key), key.clone()))
        } else {
            let result = related(key, TaskFile::ExecutionResult)?;
            (!files.contains_key(&result)).then(|| Command::Pending(owner(key), key.clone()))
        }
    }
    /// A first result completes its request and claim; replacements change nothing.
    pub(super) fn record(&mut self, command: Option<Command>, position: u64, replaced: bool) {
        match command {
            Some(Command::Result(owner, key)) if !replaced => {
                if let Some(pending) = self.pending.get_mut(&owner) {
                    for make in [TaskFile::Request, TaskFile::ExecutionClaim] {
                        if let Some(related) = related(&key, make) {
                            pending.remove(&related);
                        }
                    }
                    if pending.is_empty() {
                        self.pending.remove(&owner);
                    }
                }
                self.executions.insert((owner, position), key);
            }
            Some(Command::Pending(owner, key)) => {
                self.pending.entry(owner).or_default().insert(key);
            }
            _ => {}
        }
    }
    #[cfg(test)]
    #[cfg(all(feature = "test-support", not(target_arch = "wasm32")))]
    pub(super) fn parts(&self) -> (Executions, Vec<(Option<Component>, Vec<FileKey>)>) {
        let pending = self.pending.iter();
        (
            self.executions.clone().into_iter().collect(),
            pending
                .map(|(owner, keys)| (owner.clone(), keys.iter().cloned().collect()))
                .collect(),
        )
    }
}

// One disposable index, one locked snapshot cut. Completed history is kept in
// the volume; only the requested recent window and active inbox consume RAM.
pub(super) fn snapshot(
    index: &Index,
    session: &Component,
    recent: u32,
    maximum: usize,
) -> Result<Vec<FileValue>> {
    let mut selected = BTreeMap::new();
    let mut insert = |key: &FileKey| -> Result<()> {
        if !selected.contains_key(key) && selected.len() == maximum {
            return Err(Error::LimitExceeded);
        }
        if let Some(file) = index.files.get(key) {
            selected.insert(key.clone(), file.file.clone());
        }
        Ok(())
    };
    for directory in ["config".to_owned(), format!("session-{}", session.as_str())] {
        for range in Query::UiDirectory(Component::new(directory)?).ranges() {
            for key in index.files.range(range).map(|(key, _)| key) {
                insert(key)?;
            }
        }
    }
    for path in ["commands/catalog.json", "commands/themes.json"] {
        insert(&FileKey::parse(path)?)?;
    }
    let commands = &index.commands;
    for key in commands
        .pending
        .get(&None)
        .into_iter()
        .chain(commands.pending.get(&Some(session.clone())))
        .flat_map(|keys| keys.iter())
        .inspect(|_| super::item_examined())
    {
        insert(key)?;
    }
    let mut tasks = commands
        .executions
        .range((None, 0)..=(None, u64::MAX))
        .rev()
        .inspect(|_| super::item_examined())
        .peekable();
    let mut own = commands
        .executions
        .range((Some(session.clone()), 0)..=(Some(session.clone()), u64::MAX))
        .rev()
        .inspect(|_| super::item_examined())
        .peekable();
    for _ in 0..recent {
        let next = match (tasks.peek(), own.peek()) {
            (Some((left, _)), Some((right, _))) if left.1 >= right.1 => tasks.next(),
            (_, Some(_)) => own.next(),
            (Some(_), None) => tasks.next(),
            _ => break,
        };
        let (_, key) = next.ok_or(Error::InvalidSegment)?;
        insert(key)?;
        if let Some(request) = related(key, TaskFile::Request) {
            insert(&request)?;
        }
    }
    Ok(selected.into_values().collect())
}
