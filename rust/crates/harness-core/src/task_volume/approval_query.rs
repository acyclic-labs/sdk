//! Disposable open-request index, folded under the existing file-index lock.
use super::*;
#[derive(Default)]
#[cfg_attr(acyclic_harness_loom, derive(Clone))]
pub(super) struct Pending {
    keys: std::collections::BTreeSet<FileKey>,
}
impl Pending {
    pub(super) fn prepare(key: &FileKey) -> Option<(FileKey, bool)> {
        let FileKey::Task { task, file } = key else {
            return None;
        };
        match file {
            TaskFile::ApprovalRequest(_) => Some((key.clone(), true)),
            TaskFile::ApprovalDecision { request, .. } => Some((
                FileKey::Task {
                    task: task.clone(),
                    file: TaskFile::ApprovalRequest(request.clone()),
                },
                false,
            )),
            _ => None,
        }
    }
    pub(super) fn record(&mut self, change: Option<(FileKey, bool)>, existed: bool) {
        match change {
            Some((key, true)) if !existed => {
                self.keys.insert(key);
            }
            Some((key, false)) => {
                self.keys.remove(&key);
            }
            _ => {}
        }
    }
    pub(super) fn keys(&self, task: &Component, maximum: usize) -> Result<Vec<FileKey>> {
        let mut keys = Vec::new();
        for key in self
            .keys
            .range(Query::Approvals(task.clone()).ranges().remove(0))
        {
            if keys.len() == maximum {
                return Err(Error::LimitExceeded);
            }
            keys.push(key.clone());
        }
        Ok(keys)
    }
}
pub(super) fn snapshot(index: &Index, task: &Component, maximum: usize) -> Result<Vec<FileValue>> {
    index
        .approvals
        .keys(task, maximum)?
        .iter()
        .map(|key| {
            index
                .files
                .get(key)
                .map(|stored| stored.file.clone())
                .ok_or(Error::InvalidSegment)
        })
        .collect()
}

#[cfg(all(test, feature = "test-support"))]
mod tests {
    use super::*;
    fn key(request: u16, decision: bool) -> Result<FileKey> {
        Ok(FileKey::Task {
            task: Component::new("t")?,
            file: if decision {
                TaskFile::ApprovalDecision {
                    request: Component::new(request.to_string())?,
                    principal: Component::new("user")?,
                }
            } else {
                TaskFile::ApprovalRequest(Component::new(request.to_string())?)
            },
        })
    }
    #[test]
    fn open_and_exact_selections_examine_no_settled_history() -> Result<()> {
        for count in [129, 1024] {
            let mut index = Index::default();
            for id in 0..count {
                for decided in [false, true] {
                    apply(
                        &mut index,
                        FileValue::new(key(id, decided)?, b"{}".to_vec()),
                        u64::from(id) * 2 + u64::from(decided),
                    );
                }
            }
            apply(
                &mut index,
                FileValue::new(key(count, false)?, b"{}".to_vec()),
                u64::from(count) * 2,
            );
            let task = Component::new("t")?;
            let (selected, reads) =
                crate::test_support::measure_task_volume_work(|| snapshot(&index, &task, 1));
            let selected = selected?;
            assert_eq!(selected.len(), 1);
            let selected_key = &selected.first().ok_or(Error::InvalidSegment)?.key;
            assert_eq!(*selected_key, key(count, false)?);
            assert_eq!(reads, 1);
            let query = Query::Approval {
                task: task.clone(),
                request: Component::new((count - 1).to_string())?,
            };
            let (_, reads) = crate::test_support::measure_task_volume_work(|| {
                query
                    .ranges()
                    .into_iter()
                    .flat_map(|range| index.files.range(range))
                    .count()
            });
            assert_eq!(reads, 2);
            // Negative control: a filtered full-history range still pays for every entry.
            let (_, reads) = crate::test_support::measure_task_volume_work(|| {
                Query::Approvals(task)
                    .ranges()
                    .into_iter()
                    .flat_map(|range| index.files.range(range))
                    .filter(|(key, _)| **key == *selected_key)
                    .count()
            });
            assert!(reads > 2);
        }
        Ok(())
    }
    proptest::proptest! {
        #![proptest_config(proptest::test_runner::Config::with_cases(16))]
        #[test]
        fn pending_fold_never_retains_settled_requests(events in proptest::collection::vec((0u16..8, proptest::prelude::any::<bool>()), 0..32)) {
            let key = |request, decision| key(request, decision).map_err(|error| proptest::test_runner::TestCaseError::fail(error.to_string()));
            let mut index = Index::default();
            let mut seen = std::collections::BTreeSet::new();
            let mut expected = std::collections::BTreeSet::new();
            for (position, (id, decide)) in events.into_iter().enumerate() {
                // Decisions are admitted only after their immutable request exists.
                let decide = decide && seen.contains(&id);
                if decide { expected.remove(&key(id, false)?); }
                else if seen.insert(id) { expected.insert(key(id, false)?); }
                apply(&mut index, FileValue::new(key(id, decide)?, b"{}".to_vec()), position as u64);
                let actual = snapshot(&index, &Component::new("t").unwrap(), 8).unwrap().into_iter().map(|file| file.key).collect::<std::collections::BTreeSet<_>>();
                proptest::prop_assert_eq!(&actual, &expected);
            }
        }
    }
}
