use super::{Error, Generation, Result, TaskVolume, apply, item_examined, segment, validate_file};
use acyclic_stream::{ReadRequest, StreamProvider, StreamWatermarks};
use futures::StreamExt as _;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    // Lock order: index only. Provider calls and stream polls never hold it.
    // Every staged record is applied only to its captured generation.
    pub(super) async fn refresh(&self) -> Result<StreamWatermarks> {
        #[cfg(acyclic_harness_loom)]
        if super::model_checking::enabled(super::model_checking::Fault::ReturnOnPeerAdvance) {
            return self.refresh_to_tail::<false>().await;
        }
        self.refresh_to_tail::<true>().await
    }

    // RESUME=false is the model's deliberately broken early-success variant.
    pub(super) async fn refresh_to_tail<const RESUME: bool>(&self) -> Result<StreamWatermarks> {
        let mut expected = {
            let index = self.index.lock().await;
            self.generation(&index)
        };
        let mut watermarks = self.provider.watermarks(self.path.clone()).await?;
        if watermarks.durable > watermarks.accepted || watermarks.accepted < expected.position {
            return Err(Error::ResetRequired);
        }
        let tail = watermarks.accepted;
        loop {
            let mut records = self
                .provider
                .read(ReadRequest {
                    path: self.path.clone(),
                    from: expected.position - 1,
                    limit: u32::try_from(
                        (tail - expected.position + 1).min(acyclic_stream::MAX_ITEMS as u64),
                    )
                    .map_err(|_| Error::LimitExceeded)?,
                })
                .await?;
            let anchor = records.next().await.ok_or(Error::ResetRequired)??;
            item_examined();
            if anchor.sequence != expected.position - 1
                || anchor.commit_id.as_bytes() != &expected.commit
            {
                return Err(Error::ResetRequired);
            }
            let before = expected.position;
            let mut peer_advanced = false;
            while let Some(record) = records.next().await {
                item_examined();
                let record = record?;
                if record.sequence >= tail {
                    break;
                }
                if record.sequence != expected.position {
                    return Err(Error::ResetRequired);
                }
                let mut index = self.index.lock().await;
                let current = self.generation(&index);
                if current != expected {
                    expected = advanced(current, &expected)?;
                    peer_advanced = true;
                    break;
                }
                let file = segment::decode(&record.value)?;
                validate_file(&index, &file)?;
                let next = index.next.checked_add(1).ok_or(Error::LimitExceeded)?;
                apply(&mut index, file, next);
                index.commit = record.commit_id;
                index.next = next;
                expected = self.generation(&index);
            }
            let current = {
                let index = self.index.lock().await;
                self.generation(&index)
            };
            if current != expected {
                expected = advanced(current, &expected)?;
                peer_advanced = true;
            }
            if expected.position >= tail || (!RESUME && peer_advanced) {
                watermarks.accepted = watermarks.accepted.max(expected.position);
                return Ok(watermarks);
            }
            if expected.position == before {
                return Err(Error::ResetRequired);
            }
        }
    }
}

fn advanced(current: Generation, expected: &Generation) -> Result<Generation> {
    if current.volume != expected.volume
        || current.epoch != expected.epoch
        || current.position <= expected.position
    {
        return Err(Error::ResetRequired);
    }
    // Discard the staged suffix and resume at the validated shared generation.
    // A partial advance does not satisfy the accepted tail captured at entry.
    Ok(current)
}
