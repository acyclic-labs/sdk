use super::{Error, FileValue, Result, TaskVolume};
use acyclic_stream::StreamProvider;

#[cfg(all(test, acyclic_harness_loom))]
#[path = "../../tests/task_volume/durability_model.rs"]
mod tests;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    /// Confirms this exact file value's publication, independently of later writes.
    /// A concurrent replacement or lost accepted history requires revalidation.
    pub async fn file_is_durable(&self, value: &FileValue) -> Result<bool> {
        self.file_durability::<true>(value).await
    }

    async fn file_durability<const CHECK_ANCHOR: bool>(&self, value: &FileValue) -> Result<bool> {
        value.validate()?;
        let generation = {
            let mut index = self.index.lock().await;
            self.refresh(&mut index).await?;
            let Some(stored) = index.files.get(&value.key) else {
                return Ok(false);
            };
            if &stored.file != value {
                return Err(Error::Conflict(self.generation(&index)));
            }
            let watermarks = self.provider.watermarks(self.path.clone()).await?;
            if watermarks.durable > watermarks.accepted || watermarks.accepted < index.next {
                return Err(Error::ResetRequired);
            }
            if stored.position > watermarks.durable {
                return Ok(false);
            }
            self.generation(&index)
        };
        // Validate after observing durability. A matching immutable chain anchor
        // proves that the indexed file belongs to the now-irreversible prefix.
        // The index guard is released before revalidating provider history.
        if CHECK_ANCHOR {
            self.validate_anchor(generation.position, &generation.commit)
                .await?;
        }
        Ok(true)
    }
}
