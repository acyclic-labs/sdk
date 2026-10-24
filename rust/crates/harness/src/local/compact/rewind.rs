use super::*;
use crate::context::SummarySelection;
use crate::runtime::TaskStateProvider;
use crate::task_volume::{FileKey, TaskVolume};
use acyclic_stream::StreamProvider;

fn advances<const FENCE: bool>(
    current: Option<&SummarySelection>,
    next: &SummarySelection,
) -> bool {
    !FENCE || current.is_none_or(|current| current.selected_at < next.selected_at)
}

// One retained selection per request, followed by the ordinary selection file.
// Replays cannot move a newer selection backwards. Both publications use the
// task volume's generation CAS; no second lock or transaction domain exists.
async fn install<P: StreamProvider + ?Sized>(
    volume: &TaskVolume<P>,
    task: TaskId,
    next: &SummarySelection,
    maximum: u64,
) -> Result<()> {
    loop {
        let (cut, current, _) = checkpoints::capture_selection(volume, task, maximum).await?;
        if !advances::<true>(current.as_ref(), next) {
            return Ok(());
        }
        let value = FileValue::new(
            commands::compact_key(task)?,
            serde_json::to_vec(next).map_err(storage)?,
        );
        match volume.append(cut.generation, &value).await {
            Ok(_) => return Ok(()),
            Err(crate::task_volume::Error::Conflict(_)) => continue,
            Err(error) => return Err(storage(error)),
        }
    }
}

impl LocalHarness {
    pub(in crate::local) async fn rewind_checkpoint(
        &self,
        operation: OperationId,
        target: TaskId,
    ) -> Result<Value> {
        self.runtime.task_host().observe_admission(target).await?;
        let volume = &self.task_files.inner;
        let receipt = FileKey::Task {
            task: Component::new(target.to_string()).map_err(storage)?,
            file: TaskFile::Context(
                Component::new(format!("rewind-{operation}")).map_err(storage)?,
            ),
        };
        let next: SummarySelection = loop {
            let saved = volume
                .snapshot(&Query::File(receipt.clone()), 1)
                .await
                .map_err(storage)?;
            if let Some(file) = saved.files.first() {
                if file.bytes.len() as u64 > self.limits.file_bytes {
                    return Err(Error::Invalid("rewind selection exceeds file bound".into()));
                }
                break serde_json::from_slice(&file.bytes).map_err(storage)?;
            }
            let (cut, selected, _) =
                checkpoints::capture_selection(volume, target, self.limits.file_bytes).await?;
            let selected =
                selected.ok_or_else(|| Error::NotFound("conversation checkpoint".into()))?;
            let checkpoint = self.read_checkpoint(&selected.checkpoint).await?;
            let next = SummarySelection {
                continuation: None,
                checkpoint: selected.checkpoint,
                selected_at: cut.cursor.position,
                through: Some(checkpoint.boundary.through),
            };
            let bytes = serde_json::to_vec(&next).map_err(storage)?;
            if bytes.len() as u64 > self.limits.file_bytes {
                return Err(Error::Invalid("rewind selection exceeds file bound".into()));
            }
            match volume
                .create(cut.generation, &FileValue::new(receipt.clone(), bytes))
                .await
            {
                Ok(_) => break next,
                Err(crate::task_volume::Error::Conflict(_)) => continue,
                Err(error) => return Err(storage(error)),
            }
        };
        let checkpoint = self.read_checkpoint(&next.checkpoint).await?;
        let task = checkpoint.boundary.task;
        install(volume, task, &next, self.limits.file_bytes).await?;
        Ok(json!({"selected_task":task,"rewound":target,"checkpoint":next.checkpoint}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    proptest::proptest! {
        #[test]
        fn replay_never_overwrites_a_newer_selection(before in proptest::num::u64::ANY, after in proptest::num::u64::ANY) {
            let key = commands::compact_key(TaskId::new()).unwrap();
            let current = SummarySelection {continuation:None,checkpoint:key.clone(),selected_at:before,through:None};
            let next = SummarySelection {continuation:None,checkpoint:key,selected_at:after,through:None};
            proptest::prop_assert_eq!(advances::<true>(Some(&current),&next),before<after);
            proptest::prop_assert!(!advances::<true>(Some(&next),&next));
        }
    }
    #[test]
    #[should_panic(expected = "replay must preserve a later selection")]
    fn missing_selection_fence_negative_control() {
        let next = SummarySelection {
            continuation: None,
            checkpoint: commands::compact_key(TaskId::new()).unwrap(),
            selected_at: 7,
            through: None,
        };
        assert!(
            !advances::<false>(Some(&next), &next),
            "replay must preserve a later selection"
        );
    }
}
