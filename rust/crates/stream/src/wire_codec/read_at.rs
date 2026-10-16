pub(crate) fn read_response_at(
    value: wire::ReadResponse,
    next: u64,
    remaining: Option<u32>,
) -> Result<std::vec::IntoIter<Record>, StreamError> {
    let records: Vec<Record> = read_response_records(value)?
        .into_iter()
        .map(record)
        .collect::<Result<_, _>>()?;
    let first = records.first().map_or(next, |record| record.sequence);
    let contiguous = records
        .iter()
        .enumerate()
        .all(|(offset, record)| Some(record.sequence) == first.checked_add(offset as u64));
    let fresh = records
        .iter()
        .filter(|record| record.sequence >= next)
        .count();
    if first > next || !contiguous || remaining.is_some_and(|remaining| fresh > remaining as usize)
    {
        return Err(StreamError::Unavailable);
    }
    Ok(records.into_iter())
}
