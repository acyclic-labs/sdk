//! Active approval metadata is disposable; settled files remain canonically addressable.
use super::*;
use std::collections::BTreeSet;

#[derive(Default)]
#[cfg_attr(acyclic_harness_loom, derive(Clone))]
pub(super) struct Pending {
    tasks: BTreeMap<Component, BTreeMap<Component, BTreeSet<FileKey>>>,
}
impl Pending {
    pub(super) fn record(&mut self, files: &FileIndex, key: &FileKey) {
        let FileKey::Task { task, file } = key else {
            return;
        };
        match file {
            TaskFile::ApprovalRequest(request) => {
                let query = Query::Approval {
                    task: task.clone(),
                    request: request.clone(),
                };
                let settled = query.ranges().into_iter().any(|range| {
                    files.range(range).any(|(key, _)| {
                        matches!(
                            key,
                            FileKey::Task {
                                file: TaskFile::ApprovalDecision { .. },
                                ..
                            }
                        )
                    })
                });
                if !settled {
                    self.tasks
                        .entry(task.clone())
                        .or_default()
                        .entry(request.clone())
                        .or_default()
                        .insert(key.clone());
                }
            }
            TaskFile::ApprovalReview { request, .. } => {
                if let Some(keys) = self
                    .tasks
                    .get_mut(task)
                    .and_then(|requests| requests.get_mut(request))
                {
                    keys.insert(key.clone());
                }
            }
            TaskFile::ApprovalDecision { request, .. } => {
                if let Some(requests) = self.tasks.get_mut(task) {
                    requests.remove(request);
                    if requests.is_empty() {
                        self.tasks.remove(task);
                    }
                }
            }
            _ => {}
        }
    }
    pub(super) fn files(&self, index: &Index, task: &Component) -> Vec<FileValue> {
        self.tasks
            .get(task)
            .into_iter()
            .flat_map(|requests| requests.values())
            .flatten()
            .filter_map(|key| index.files.get(key))
            .map(|stored| stored.file.clone())
            .collect()
    }
    pub(super) fn keys(&self, task: &Component, maximum: usize) -> Result<Vec<FileKey>> {
        let mut keys = Vec::new();
        for request in self
            .tasks
            .get(task)
            .into_iter()
            .flat_map(|requests| requests.values())
        {
            for key in request {
                if !matches!(
                    key,
                    FileKey::Task {
                        file: TaskFile::ApprovalRequest(_),
                        ..
                    }
                ) {
                    continue;
                }
                if keys.len() == maximum {
                    return Err(Error::LimitExceeded);
                }
                keys.push(key.clone());
            }
        }
        Ok(keys)
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    pub(super) fn native_index_bytes(&self) -> usize {
        self.tasks.len()
            * std::mem::size_of::<(Component, BTreeMap<Component, BTreeSet<FileKey>>)>()
            + self
                .tasks
                .values()
                .map(|requests| {
                    requests.len() * std::mem::size_of::<(Component, BTreeSet<FileKey>)>()
                        + requests
                            .values()
                            .map(|keys| keys.len() * std::mem::size_of::<FileKey>())
                            .sum::<usize>()
                })
                .sum::<usize>()
    }
}

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Active requests and reviews at one cut, without copying settled history.
    /// The cursor follows all approval events, including decisions that retire them.
    #[doc(hidden)]
    pub async fn pending_approvals(&self, task: &Component) -> Result<Snapshot> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let files = index.approvals.files(&index, task);
        let generation = self.generation(&index);
        drop(index);
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        Ok(Snapshot {
            files,
            cursor: self.cursor(&Query::Approvals(task.clone()), generation, watermarks)?,
            generation,
        })
    }
}

#[cfg(test)]
mod tests;
