use super::*;
use crate::filesystem::FilesystemWorkflowJournal;

pub(super) fn published(reference: &Component, values: &BTreeMap<FileKey, FileValue>) -> u64 {
    let key = |turn| FileKey::Child {
        reference: reference.clone(),
        file: ChildFile::Result(turn),
    };
    values
        .range(key(0)..=key(u64::MAX))
        .next_back()
        .map_or(0, |(key, _)| {
            let FileKey::Child {
                file: ChildFile::Result(turn),
                ..
            } = key
            else {
                unreachable!()
            };
            *turn
        })
}

impl LocalHarness {
    pub(super) async fn child_processed(
        &self,
        child: TaskId,
        journal: &FilesystemWorkflowJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>,
        reference: &Component,
        values: &BTreeMap<FileKey, FileValue>,
    ) -> Result<(u64, Vec<FileValue>)> {
        if self
            .runtime
            .task_host()
            .terminal_status(child)
            .await?
            .is_some()
        {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                child.into_bytes(),
            )));
        }
        // The worker has released its live lease at the receive checkpoint.
        // Bounded observation verifies the retained original reservation proof
        // rather than trying to publish under that released execution fence.
        let cancellation = acyclic_fs::CancellationToken::new();
        let io = crate::filesystem::PreparationIo::from_budget(
            acyclic_fs::performance::work_budget(65_536, self.limits.file_bytes),
            &cancellation,
        )?;
        let record = journal
            .observe_latest_record(Some(&io))
            .await?
            .ok_or_else(|| Error::Conflict("child checkpoint missing".into()))?;
        if record.transition.state["stage"].as_str() != Some("receive") {
            return Err(Error::Indeterminate(OperationId::from_bytes(
                child.into_bytes(),
            )));
        }
        let processed = record.transition.state["mail"]["sequence"]
            .as_u64()
            .unwrap_or(0);
        let published = published(reference, values);
        if published > processed {
            return Err(Error::Conflict(
                "child result cursor exceeds its checkpoint".into(),
            ));
        }
        let mut results = Vec::new();
        if published == processed {
            return Ok((processed, results));
        }
        for turn in (published + 1)..=processed {
            let earlier;
            let committed = if turn == processed {
                &record
            } else {
                let revision = turn
                    .checked_mul(2)
                    .and_then(|value| value.checked_add(1))
                    .ok_or_else(|| Error::Invalid("child result revision overflow".into()))?;
                earlier = journal.observe_transition(revision, Some(&io)).await?;
                &earlier
            };
            let result = crate::filesystem::child_agent::completed_turn(child, committed)?;
            if result.turn != turn {
                return Err(Error::Conflict(
                    "child result turn differs from its checkpoint".into(),
                ));
            }
            let bytes = serde_json::to_vec(&result).map_err(storage)?;
            if bytes.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid(
                    "child result exceeds the original file bound".into(),
                ));
            }
            results.push(FileValue::new(
                FileKey::Child {
                    reference: reference.clone(),
                    file: ChildFile::Result(turn),
                },
                bytes,
            ));
        }
        Ok((processed, results))
    }
}
