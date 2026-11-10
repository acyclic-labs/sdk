use super::*;

impl CodecQuota<'_, '_> {
    pub(crate) fn reserve<T>(&mut self, capacity: usize, values: &mut Vec<T>) -> Result<()> {
        let old = values.capacity();
        if let Some(io) = self.work().as_deref_mut() {
            crate::store::history::reserve(values, capacity, io)?;
            self.adopt(((values.capacity() - old) * std::mem::size_of::<T>()) as u64)?;
        } else {
            values.try_reserve_exact(capacity.saturating_sub(values.len()))?;
        }
        Ok(())
    }
}

/// A request with its one hard effect, whose slot the quota funds and adopts
/// alongside the retained key, as `admit` and `commit` account for it.
pub(crate) fn commit_request<'a, 'b>(
    key: StreamIdempotencyKey,
    retained_key_bytes: u64,
    work: &'a mut Option<&'b mut dyn HistoryIo>,
) -> Result<(acyclic_stream::CommitRequest, CodecQuota<'a, 'b>)> {
    let mut quota = CodecQuota::from_retained(work, retained_key_bytes);
    let mut effects = Vec::new();
    quota.reserve(1, &mut effects)?;
    effects.push(acyclic_stream::CommitEffect::hard(Vec::new(), Vec::new()));
    Ok((
        acyclic_stream::CommitRequest {
            deadline_unix_millis: None,
            effects,
            idempotency_key: key,
        },
        quota,
    ))
}

pub(crate) fn add_condition(
    request: &mut acyclic_stream::CommitRequest,
    custody: &mut CodecQuota<'_, '_>,
    condition: acyclic_stream::CommitCondition,
) -> Result<()> {
    custody.reserve(
        request.effects[0].conditions.len() + 1,
        &mut request.effects[0].conditions,
    )?;
    request.effects[0].conditions.push(condition);
    Ok(())
}
