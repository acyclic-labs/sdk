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
    // Return at most one unpublished answer. The controller commits it before
    // observing the next turn, so interruption never retains or replays a batch
    // of answer bodies in memory.
    pub(super) async fn child_processed(
        &self,
        child: TaskId,
        journal: &FilesystemWorkflowJournal<LocalStream, LocalAuthorityBackend, LocalObjectBackend>,
        reference: &Component,
        values: &BTreeMap<FileKey, FileValue>,
    ) -> Result<(u64, Option<FileValue>)> {
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
        if published == processed {
            return Ok((processed, None));
        }
        let turn = published
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("child result cursor overflow".into()))?;
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
        let bytes = io.encode(&result, self.limits.file_bytes, false)?;
        let file = FileValue::new(
            FileKey::Child {
                reference: reference.clone(),
                file: ChildFile::Result(turn),
            },
            bytes,
        );
        Ok((processed, Some(file)))
    }
}
