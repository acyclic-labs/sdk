use super::{Component, Error, FileKey, FileValue, Index, Query, Result, TaskFile};
use std::collections::BTreeMap;

pub(crate) fn identity(id: &Component) -> Option<(Option<uuid::Uuid>, crate::OperationId)> {
    let text = id.as_str();
    let (session, operation) = if let Some(owned) = text.strip_prefix("command-") {
        let (session, operation) = owned.split_once('.')?;
        (Some(uuid::Uuid::parse_str(session).ok()?), operation)
    } else {
        (None, text)
    };
    Some((session, crate::OperationId::parse(operation).ok()?))
}

pub(crate) fn related(key: &FileKey, make: fn(Component) -> TaskFile) -> Option<FileKey> {
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

pub(super) fn owner(key: &FileKey) -> Option<Component> {
    match key {
        FileKey::Session { session, .. } => Some(session.clone()),
        FileKey::Task {
            file:
                TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id),
            ..
        } => identity(id)
            .and_then(|(session, _)| session)
            .map(|session| {
                Component::new(session.to_string()).expect("UUID is a namespace component")
            }),
        _ => None,
    }
}

pub(crate) fn matches(session: &Component, key: &FileKey) -> bool {
    match key {
        FileKey::Session { session: owner, .. } => owner == session,
        FileKey::Task {
            file:
                TaskFile::Request(id) | TaskFile::ExecutionClaim(id) | TaskFile::ExecutionResult(id),
            ..
        } => identity(id).is_some_and(|(owner, _)| {
            owner.is_none_or(|owner| owner.to_string() == session.as_str())
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
    for key in index
        .pending_commands
        .get(&None)
        .into_iter()
        .chain(index.pending_commands.get(&Some(session.clone())))
        .flat_map(|keys| keys.iter())
        .inspect(|_| super::item_examined())
    {
        insert(key)?;
    }
    let mut tasks = index
        .executions
        .range((None, 0)..=(None, u64::MAX))
        .rev()
        .inspect(|_| super::item_examined())
        .peekable();
    let mut own = index
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
