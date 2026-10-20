//! Collision-free model turn identities allocated by immutable publication position.
use super::*;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    pub(crate) async fn model_turn(
        &self,
        task: Component,
        execution: Component,
        step: u32,
        work: NativeWork<'_>,
    ) -> Result<u64> {
        let marker = FileValue::new(
            FileKey::Task {
                task,
                file: TaskFile::ModelTurn { execution, step },
            },
            Vec::new(),
        );
        loop {
            self.refresh().await?;
            let index = self.index.lock().await;
            if let Some(stored) = index.files.get(&marker.key) {
                return Ok(stored.position);
            }
            let head = self.generation(&index);
            drop(index);
            match self.append_if(head, &marker, Vec::new(), None, work).await {
                Ok(accepted) => return Ok(accepted.position),
                Err(Error::Conflict(_)) => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
mod tests {
    use super::*;

    #[tokio::test]
    async fn model_turn_bindings_survive_competing_publishers_and_reopen() -> Result<()> {
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let path = StreamPath::new("turn-bindings")?;
        let first = TaskVolume::open(stream.clone(), path.clone()).await?;
        let second = TaskVolume::open(stream.clone(), path.clone()).await?;
        let task = Component::new("task")?;
        let execution = Component::new("execution")?;
        let (left, right) = futures::join!(
            first.model_turn(task.clone(), execution.clone(), 0, None),
            second.model_turn(task.clone(), execution.clone(), 0, None),
        );
        let turn = left?;
        assert_eq!(right?, turn);
        let next_step = second
            .model_turn(task.clone(), execution.clone(), 1, None)
            .await?;
        let next_execution = second
            .model_turn(task.clone(), Component::new("next")?, 0, None)
            .await?;
        assert_ne!(turn, next_step);
        assert_ne!(turn, next_execution);
        assert_ne!(next_step, next_execution);
        let before = first.head().await?;
        let reopened = TaskVolume::open(stream, path).await?;
        assert_eq!(reopened.model_turn(task, execution, 0, None).await?, turn);
        assert_eq!(reopened.head().await?, before);
        Ok(())
    }

    #[tokio::test]
    async fn model_turn_binding_uses_original_native_admission() -> Result<()> {
        use acyclic_fs::{CancellationToken, NativeMaterializationWork};
        let stream = Arc::new(acyclic_stream::MemoryStream::default());
        let volume =
            TaskVolume::open(stream.clone(), StreamPath::new("native-turn-bindings")?).await?;
        let task = Component::new("task")?;
        let execution = Component::new("execution")?;
        let work = NativeMaterializationWork::new(
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &CancellationToken::new(),
        )
        .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        let baseline = work.live_allocation_bytes();
        let turn = volume
            .model_turn(task.clone(), execution.clone(), 0, Some(&work))
            .await?;
        assert!(work.receipt().bytes_encoded > 0);
        assert!(work.receipt().allocation_operations > 0);
        assert_eq!(
            volume
                .model_turn(task.clone(), execution, 0, Some(&work))
                .await?,
            turn
        );
        let cancellation = CancellationToken::new();
        let cancelled =
            NativeMaterializationWork::new(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
                .map_err(|(cause, work)| crate::Error::NativeConstruction { cause, work })?;
        cancellation.cancel();
        let before = volume.head().await?;
        assert!(
            volume
                .model_turn(task, Component::new("cancelled")?, 0, Some(&cancelled))
                .await
                .is_err()
        );
        assert_eq!(volume.head().await?, before);
        drop(volume);
        drop(stream);
        assert_eq!(work.live_allocation_bytes(), baseline);
        Ok(())
    }
}
