use super::*;

#[derive(Default)]
pub(super) struct IntentTail {
    pub(super) index: intent_index::IntentIndex,
    pub(super) cursor: u64,
    pub(super) fallback: bool,
}

impl<P: StreamProvider> DistributedCoordinator<P> {
    pub(super) async fn refresh_intent_tail(
        &mut self,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if self.intent_tail.fallback {
            return Ok(());
        }
        // Applied records already belong to the coordinator's canonical index.
        // Retain only the separately inspected suffix until replay catches up.
        if self.intent_tail.cursor <= self.revision {
            self.intent_tail.index = intent_index::IntentIndex::default();
            self.intent_tail.cursor = self.revision;
        }
        let mut quota = CodecQuota::new(work, 0)?;
        let tail =
            operations::tail_optional_with_work(&self.client, self.stream.0.path(), quota.work())
                .await?
                .unwrap_or(0);
        while self.intent_tail.cursor < tail {
            let from = self.intent_tail.cursor;
            let maximum = u32::try_from(tail - from)
                .unwrap_or(READ_PAGE_SIZE)
                .min(READ_PAGE_SIZE);
            let mut source =
                read_records(&self.client, &self.stream.0, from, maximum, quota.work()).await?;
            let mut next = from;
            while let Some(record) = next_record(
                &mut source,
                &mut next,
                from + u64::from(maximum),
                &mut quota,
            )
            .await?
            {
                let raw_bytes = operations::sdk_record_retained_bytes(&record, quota.work());
                let inspected = inspect_envelope(&record.value, false, quota.work());
                let key = match inspected {
                    Ok((_, _, key)) => key,
                    // An anomalous history must retain the old reader's exact
                    // ordering of envelope, matching-payload and duplicate errors.
                    Err(error) if error.code() == wire::ErrorCode::Storage => {
                        self.intent_tail.fallback = true;
                        return Ok(());
                    }
                    Err(error) => return Err(error),
                };
                if let Some(key) = key {
                    if self.intents.get(key).is_some() || self.intent_tail.index.get(key).is_some()
                    {
                        self.intent_tail.fallback = true;
                        return Ok(());
                    }
                    self.intent_tail
                        .index
                        .insert_with(key, record.sequence, &mut quota, |_| Ok(()))?;
                }
                self.intent_tail.cursor = next;
                drop(record);
                quota.relinquish(raw_bytes)?;
            }
            if next == from {
                return Err(guarded_boundary(
                    quota.work(),
                    wire::ErrorCode::Storage,
                    "coordinator intent prefix is incomplete",
                ));
            }
        }
        Ok(())
    }
}
