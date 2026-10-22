use super::*;

impl<P: StreamProvider> Journal<P> {
    pub(super) fn revision_path(&self, revision: u64) -> Result<StreamPath> {
        StreamPath::new(format!("{}/revisions/{revision}", self.path.as_str()))
            .map_err(|error| stream_error(&error))
    }

    async fn revision_position(&self, revision: u64) -> Result<u64> {
        let mut records = self
            .stream
            .read(ReadRequest {
                path: self.revision_path(revision)?,
                from: 0,
                limit: 1,
            })
            .await
            .map_err(|error| stream_error(&error))?;
        let record = records
            .try_next()
            .await
            .map_err(|error| stream_error(&error))?
            .ok_or_else(|| Error::Storage("actor revision index is empty".into()))?;
        serde_json::from_slice(&record.value).map_err(|error| codec_error(&error))
    }

    pub(super) async fn replay_records(
        &self,
        after: u64,
        maximum: u32,
    ) -> Result<Vec<WorkflowRecord>> {
        if maximum == 0 {
            return Err(Error::Invalid("invalid actor replay page size".into()));
        }
        let current = self.load().await?;
        if after >= current.last.snapshot.checkpoint.revision {
            return Ok(Vec::new());
        }
        // The position is published in the checkpoint's transaction, rather
        // than reconstructed by scanning all earlier pages on every request.
        let mut from = self.revision_position(after).await?;
        if from == 0 || from >= current.tail {
            return Err(Error::Storage(
                "actor revision index is out of range".into(),
            ));
        }
        let mut expected = after;
        let mut result = Vec::new();
        while from < current.tail && result.len() < maximum as usize {
            let page = self
                .page(
                    from,
                    PAGE.min(u32::try_from(current.tail - from).unwrap_or(PAGE)),
                )
                .await?;
            if page.is_empty() {
                return Err(Error::Storage("actor replay history is incomplete".into()));
            }
            from += page.len() as u64;
            for entry in page {
                if let Some(record) = entry.workflow {
                    if record.prior.revision != expected {
                        return Err(Error::Storage(
                            "actor replay revision is not contiguous".into(),
                        ));
                    }
                    expected += 1;
                    result.push(record);
                    if result.len() == maximum as usize {
                        break;
                    }
                }
            }
        }
        Ok(result)
    }
}
