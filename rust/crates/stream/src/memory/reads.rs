use super::*;

impl MemoryStream {
    pub(crate) async fn inspect_inner(
        &self,
        idempotency_key: IdempotencyKey,
        meter: WireMeter<'_>,
    ) -> MemoryResult<MemoryOutput<Option<IdempotencyObservation>>> {
        boundary(meter, false)?;
        let state = self.state.read().await;
        admit_item(meter)?;
        let replay = state.replays.get(idempotency_key.as_bytes());
        record_item(meter)?;
        let Some(replay) = replay else {
            return Ok(MemoryOutput { value: None, allocation: allocate(meter, 0)? });
        };
        let outcome = clone_idempotency_outcome(&replay.result, meter)?;
        Ok(MemoryOutput {
            value: Some(IdempotencyObservation {
                idempotency_key,
                request_digest: replay.digest,
                outcome: outcome.value,
            }),
            allocation: outcome.allocation,
        })
    }

    pub(crate) async fn bounds_inner(
        &self,
        path: StreamPath,
        meter: WireMeter<'_>,
    ) -> MemoryResult<MemoryOutput<StreamBounds>> {
        boundary(meter, false)?;
        let state = self.state.read().await;
        admit_item(meter)?;
        let stream = state.paths.get(&path);
        record_item(meter)?;
        let stream = stream.ok_or(StreamError::NotFound)?;
        Ok(MemoryOutput {
            value: StreamBounds { tail: stream.tail },
            allocation: allocate(meter, 0)?,
        })
    }

    pub(super) async fn read_inner(
        &self,
        request: ReadRequest,
        meter: WireMeter<'_>,
    ) -> MemoryResult<(MemoryOutput<Vec<Record>>, Option<std::io::Error>)> {
        boundary(meter, false)?;
        let state = self.state.read().await;
        admit_item(meter)?;
        let path = state.paths.get(&request.path);
        record_item(meter)?;
        let path = path.ok_or(StreamError::NotFound)?;
        if request.from > path.tail { return Err(StreamError::OutOfRange.into()); }
        read_history(
            path.history.as_ref(),
            path.tail,
            request.from,
            usize::try_from(request.limit).map_err(|_| StreamError::LimitExceeded)?,
            meter,
        )
    }

}
