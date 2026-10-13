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

pub(crate) fn commit_request<'a, 'b>(
    key: StreamIdempotencyKey,
    retained_key_bytes: u64,
    work: &'a mut Option<&'b mut dyn HistoryIo>,
) -> (acyclic_stream::CommitRequest, CodecQuota<'a, 'b>) {
    (
        acyclic_stream::CommitRequest {
            conditions: Vec::new(),
            mutations: Vec::new(),
            idempotency_key: key,
        },
        CodecQuota::from_retained(work, retained_key_bytes),
    )
}

pub(crate) fn add_condition(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    condition: acyclic_stream::CommitCondition,
) -> Result<()> {
    custody.reserve(request.conditions.len() + 1, &mut request.conditions)?;
    request.conditions.push(condition);
    Ok(())
}

pub(crate) fn add_record(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    path: acyclic_stream::StreamPath,
    record: OwnedBytes,
) -> Result<()> {
    custody.reserve(request.mutations.len() + 1, &mut request.mutations)?;
    let mut records = Vec::new();
    custody.reserve(1, &mut records)?;
    records.push(record);
    request
        .mutations
        .push(acyclic_stream::CommitMutation::Append { path, records });
    Ok(())
}
