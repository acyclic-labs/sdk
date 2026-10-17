//! Bounded disposable display windows; archived files keep their identities.
use super::*;
use futures::StreamExt as _;
use window::Window;
mod window;

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
/// Recent display order per task, derived from index replacements alone.
#[derive(Default)]
pub(super) struct Views {
    order: BTreeMap<(Component, u8, u64), FileKey>,
    overviews: BTreeMap<Component, FileKey>,
}
/// One display file about to replace any stored version of its key.
pub(super) struct Display {
    task: Component,
    kind: u8,
    position: u64,
    key: FileKey,
}
impl Views {
    pub(super) fn prepare(file: &FileValue, position: u64) -> Option<Display> {
        let (FileKey::Task { task, .. }, Some(kind)) = (&file.key, file_kind(file)) else {
            return None;
        };
        Some(Display {
            task: task.clone(),
            kind,
            position,
            key: file.key.clone(),
        })
    }
    /// Every version of a display key is a display file, so the replaced stored
    /// file locates its old order entry.
    pub(super) fn record(&mut self, display: Option<Display>, previous: Option<&StoredFile>) {
        let Some(Display {
            task,
            kind,
            position,
            key,
        }) = display
        else {
            return;
        };
        if let Some(previous) = previous
            && let Some(old) = file_kind(&previous.file)
        {
            self.order.remove(&(task.clone(), old, previous.position));
        }
        if matches!(
            key,
            FileKey::Task {
                file: TaskFile::Status,
                ..
            }
        ) {
            self.overviews.insert(task.clone(), key.clone());
        }
        self.order.insert((task, kind, position), key);
    }
    #[cfg(test)]
    pub(super) fn parts(
        &self,
    ) -> (
        Vec<((Component, u8, u64), FileKey)>,
        Vec<(Component, FileKey)>,
    ) {
        let order = self.order.clone().into_iter().collect();
        (order, self.overviews.clone().into_iter().collect())
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
                .views
                .order
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
            .views
            .overviews
            .values()
            .take(maximum)
            .filter_map(|key| index.files.get(key).map(|stored| stored.file.clone()))
            .collect())
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
    let window = Window::new(&mut snapshot.files, maximum, bytes, recent);
    observation.changes = Box::pin(futures::stream::try_unfold(
        (observation.changes, window),
        |(mut changes, mut window)| async move {
            let Some(change) = changes.next().await else {
                return Ok(None);
            };
            let mut change = change?;
            window.apply(&mut change);
            Ok(Some((change, (changes, window))))
        },
    ));
    Ok(observation)
}
