use super::*;

pub(crate) fn reserve_prepared_vector<T>(
    values: &mut Vec<T>,
    capacity: usize,
    custody: &mut CodecQuota<'_, '_>,
) -> Result<()> {
    if capacity <= values.capacity() {
        return Ok(());
    }
    let old = values.capacity();
    if let Some(work) = custody.work().as_deref_mut() {
        crate::store::history::reserve(values, capacity, work)?;
        custody.adopt(((values.capacity() - old) * std::mem::size_of::<T>()) as u64)?;
    } else {
        values.try_reserve_exact(capacity - values.len())?;
    }
    Ok(())
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
    let length = request.conditions.len() + 1;
    reserve_prepared_vector(&mut request.conditions, length, custody)?;
    request.conditions.push(condition);
    Ok(())
}

pub(crate) fn add_record(
    (request, custody): &mut (acyclic_stream::CommitRequest, CodecQuota<'_, '_>),
    path: acyclic_stream::StreamPath,
    record: OwnedBytes,
) -> Result<()> {
    let length = request.mutations.len() + 1;
    reserve_prepared_vector(&mut request.mutations, length, custody)?;
    let mut records = Vec::new();
    reserve_prepared_vector(&mut records, 1, custody)?;
    records.push(record);
    request
        .mutations
        .push(acyclic_stream::CommitMutation::Append { path, records });
    Ok(())
}
