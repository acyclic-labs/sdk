//! Bounded disposable display windows; archived files keep their identities.
use super::*;
use futures::StreamExt as _;

pub(super) fn kind(key: &FileKey) -> Option<u8> {
    let FileKey::Task { file, .. } = key else {
        return None;
    };
    Some(match file {
        TaskFile::Status => 0,
        TaskFile::TurnRequest(_) => 1,
        TaskFile::Chunk { .. }
        | TaskFile::TextHead {
            channel: Channel::Text,
            ..
        } => 2,
        TaskFile::TextHead {
            channel: Channel::Reasoning,
            ..
        } => 3,
        TaskFile::Tool {
            file: ToolFile::Stdout(_) | ToolFile::Stderr(_),
            ..
        } => 5,
        TaskFile::Tool { .. } => 4,
        TaskFile::ExecutionResult(_) => 6,
        TaskFile::Completion(_) => 7,
        _ => return None,
    })
}
fn file_kind(file: &FileValue) -> Option<u8> {
    if matches!(
        file.key,
        FileKey::Task {
            file: TaskFile::Chunk { .. },
            ..
        }
    ) && file.channel == Some(Channel::Reasoning)
    {
        Some(3)
    } else {
        kind(&file.key)
    }
}
pub(super) fn record(index: &mut Index, file: &FileValue, position: u64) {
    let (FileKey::Task { task, file: leaf }, Some(kind)) = (&file.key, file_kind(file)) else {
        return;
    };
    if let Some((old_kind, sequence)) = index
        .view_revisions
        .insert(file.key.clone(), (kind, position))
    {
        index.view_order.remove(&(task.clone(), old_kind, sequence));
    }
    index
        .view_order
        .insert((task.clone(), kind, position), file.key.clone());
    if *leaf == TaskFile::Status {
        index.overviews.insert(task.clone(), file.key.clone());
    }
}
pub(super) fn snapshot(index: &Index, query: &Query, maximum: usize) -> Result<Vec<FileValue>> {
    if let Query::TaskSession { task, session } = query {
        let display = Query::TaskView {
            task: task.clone(),
            recent: 128,
            reasoning: true,
            details: true,
            terminal: true,
        };
        let mut files = snapshot(index, &display, maximum.saturating_sub(3))?;
        for (_, stored) in index
            .files
            .range(Query::Session(session.clone()).ranges().remove(0))
        {
            if files.len() == maximum {
                return Err(Error::LimitExceeded);
            }
            files.push(stored.file.clone());
        }
        return Ok(files);
    }
    if let Query::TaskView { task, recent, .. } = query {
        if *recent == 0 {
            return Err(Error::LimitExceeded);
        }
        let mut files = Vec::new();
        for kind in 0..=7 {
            for ((_, _, sequence), key) in index
                .view_order
                .range((task.clone(), kind, 0)..=(task.clone(), kind, u64::MAX))
                .rev()
                .take((*recent as usize).min(maximum))
            {
                let file = index.files.get(key).ok_or(Error::InvalidSegment)?;
                if query.matches_file(&file.file) {
                    files.push((*sequence, file.file.clone()));
                }
            }
        }
        files.sort_by_key(|(sequence, _)| *sequence);
        let excess = files.len().saturating_sub(maximum);
        Ok(files
            .into_iter()
            .skip(excess)
            .map(|(_, file)| file)
            .collect())
    } else {
        Ok(index
            .overviews
            .values()
            .take(maximum)
            .filter_map(|key| index.files.get(key).map(|stored| stored.file.clone()))
            .collect())
    }
}
type Window = BTreeMap<FileKey, (u64, FileValue)>;
fn trim(files: &mut Window, maximum: usize, bytes: usize, recent: usize) {
    for kind in 0..=7 {
        while files
            .values()
            .filter(|(_, file)| file_kind(file) == Some(kind))
            .count()
            > recent
        {
            let key = files
                .iter()
                .filter(|(_, (_, file))| file_kind(file) == Some(kind))
                .min_by_key(|(_, (order, _))| order)
                .map(|(key, _)| key.clone())
                .unwrap();
            files.remove(&key);
        }
    }
    while files.len() > maximum
        || files
            .values()
            .map(|(_, file)| file.bytes.len())
            .sum::<usize>()
            > bytes
    {
        let Some(key) = files
            .iter()
            .min_by_key(|(_, (order, _))| order)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        files.remove(&key);
    }
}
pub(super) fn observe(
    mut observation: Observation,
    query: &Query,
    maximum: usize,
    bytes: usize,
) -> Result<Observation> {
    let recent = match query {
        Query::TaskView { recent, .. } => *recent as usize,
        _ => maximum,
    };
    let snapshot = observation.snapshot.as_mut().ok_or(Error::ResetRequired)?;
    let mut window: Window = snapshot
        .files
        .drain(..)
        .enumerate()
        .filter(|(_, file)| file.bytes.len() <= bytes)
        .map(|(order, file)| (file.key.clone(), (order as u64, file)))
        .collect();
    trim(&mut window, maximum, bytes, recent);
    snapshot.files = window.values().map(|(_, file)| file.clone()).collect();
    observation.changes = Box::pin(futures::stream::try_unfold(
        (observation.changes, window),
        move |(mut changes, mut window)| async move {
            let Some(change) = changes.next().await else {
                return Ok(None);
            };
            let mut change = change?;
            let before: BTreeSet<_> = window.keys().cloned().collect();
            for key in &change.removed {
                window.remove(key);
            }
            for file in change.file.iter().chain(&change.files) {
                window.remove(&file.key);
                if file.bytes.len() <= bytes {
                    window.insert(file.key.clone(), (change.cursor.position, file.clone()));
                }
            }
            trim(&mut window, maximum, bytes, recent);
            change.removed = before
                .into_iter()
                .filter(|key| !window.contains_key(key))
                .collect();
            change.files.retain(|file| window.contains_key(&file.key));
            if change
                .file
                .as_ref()
                .is_some_and(|file| !window.contains_key(&file.key))
            {
                change.file = None;
            }
            Ok(Some((change, (changes, window))))
        },
    ));
    Ok(observation)
}
