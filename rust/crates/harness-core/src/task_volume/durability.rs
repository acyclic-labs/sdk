use super::{Error, FileValue, Result, TaskVolume};
use acyclic_stream::StreamProvider;

#[cfg(test)]
#[cfg(acyclic_harness_loom)]
#[path = "../../../harness/tests/task_volume/durability_model.rs"]
mod tests;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Confirms this exact file value's publication, independently of later writes.
    /// A concurrent replacement or lost accepted history requires revalidation.
    pub async fn file_is_durable(&self, value: &FileValue) -> Result<bool> {
        self.file_durability::<true, false>(value).await
    }

    async fn file_durability<const CHECK_ANCHOR: bool, const HOLD_INDEX: bool>(
        &self,
        value: &FileValue,
    ) -> Result<bool> {
        value.validate()?;
        self.refresh().await?;
        let (generation, position) = {
            let index = self.index.lock().await;
            let Some(stored) = index.files.get(&value.key) else {
                return Ok(false);
            };
            if &stored.file != value {
                return Err(Error::Conflict(self.generation(&index)));
            }
            (self.generation(&index), stored.position)
        };
        // HOLD_INDEX is the model's deliberately broken former implementation.
        let held = if HOLD_INDEX {
            Some(self.index.lock().await)
        } else {
            None
        };
        let watermarks = self.provider.watermarks(self.path.clone()).await?;
        drop(held);
        if watermarks.durable > watermarks.accepted || watermarks.accepted < generation.position {
            return Err(Error::ResetRequired);
        }
        if position > watermarks.durable {
            return Ok(false);
        }
        // Validate after observing durability. A matching immutable chain anchor
        // proves that the indexed file belongs to the now-irreversible prefix.
        // The index guard is released before revalidating provider history.
        if CHECK_ANCHOR {
            self.validate_anchor(generation.position, &generation.commit)
                .await?;
        }
        let index = self.index.lock().await;
        if self.generation(&index) != generation {
            return Err(Error::Conflict(self.generation(&index)));
        }
        Ok(true)
    }
}
