use super::*;

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub(super) enum ChunkScope {
    Turn(Component, u64),
    Execution(Component, u64, crate::OperationId),
    Head(Component, u64, crate::OperationId, Channel),
}
impl Query {
    pub(super) fn chunk_scope(&self) -> Option<ChunkScope> {
        Some(match self {
            Self::Turn { task, turn } | Self::Chunks { task, turn } => {
                ChunkScope::Turn(task.clone(), *turn)
            }
            Self::ExecutionChunks {
                task,
                turn,
                execution,
            } => ChunkScope::Execution(task.clone(), *turn, *execution),
            Self::File(FileKey::Task {
                task,
                file:
                    TaskFile::TextHead {
                        turn,
                        execution,
                        channel,
                    },
            }) => ChunkScope::Head(task.clone(), *turn, *execution, *channel),
            _ => return None,
        })
    }
}

pub(super) fn record(index: &mut Index, file: &FileValue, position: u64) {
    let FileKey::Task {
        task,
        file: TaskFile::Chunk {
            turn, execution, ..
        },
    } = &file.key
    else {
        return;
    };
    let channel = file.channel.expect("validated chunk channel");
    for scope in [
        ChunkScope::Turn(task.clone(), *turn),
        ChunkScope::Execution(task.clone(), *turn, *execution),
        ChunkScope::Head(task.clone(), *turn, *execution, channel),
    ] {
        item_examined();
        index
            .chunk_positions
            .insert((scope, position), file.key.clone());
    }
}

impl Index {
    pub(super) fn visit<const SCOPED: bool>(
        &self,
        query: &Query,
        mut visit: impl FnMut(&StoredFile) -> Result<()>,
    ) -> Result<()> {
        let scope = query.chunk_scope();
        if SCOPED && let Some(scope) = &scope {
            for (_, key) in self
                .chunk_positions
                .range((scope.clone(), 0)..=(scope.clone(), u64::MAX))
            {
                item_examined();
                let stored = self.files.get(key).ok_or(Error::InvalidSegment)?;
                if query.matches_file(&stored.file) {
                    visit(stored)?;
                }
            }
        }
        let scan = if !SCOPED && scope.is_some() {
            let task = match scope.as_ref().unwrap() {
                ChunkScope::Turn(task, _)
                | ChunkScope::Execution(task, ..)
                | ChunkScope::Head(task, ..) => task,
            };
            Query::Task(task.clone()).ranges()
        } else {
            query.ranges()
        };
        for range in scan {
            if SCOPED
                && scope.is_some()
                && matches!(
                    &range.0,
                    std::ops::Bound::Included(FileKey::Task {
                        file: TaskFile::Chunk { .. },
                        ..
                    })
                )
            {
                continue;
            }
            for (_, stored) in self.files.range(range) {
                if query.matches_file(&stored.file) {
                    visit(stored)?;
                }
            }
        }
        Ok(())
    }

    pub(super) fn select<const SCOPED: bool>(
        &self,
        query: &Query,
        maximum: usize,
    ) -> Result<Vec<FileValue>> {
        let mut selected = Vec::new();
        self.visit::<SCOPED>(query, |stored| {
            if !query.snapshot_matches_file(&stored.file) {
                return Ok(());
            }
            if selected.len() == maximum {
                return Err(Error::LimitExceeded);
            }
            selected.push((stored.position, stored.file.clone()));
            Ok(())
        })?;
        selected.sort_by(|(pa, a), (pb, b)| match (&a.key, &b.key) {
            (
                FileKey::Task {
                    task: a,
                    file: TaskFile::Chunk { .. },
                },
                FileKey::Task {
                    task: b,
                    file: TaskFile::Chunk { .. },
                },
            ) if a == b => pa.cmp(pb),
            _ => a.key.cmp(&b.key),
        });
        Ok(selected.into_iter().map(|(_, file)| file).collect())
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
