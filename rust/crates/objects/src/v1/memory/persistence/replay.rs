use super::*;

pub(super) fn restore_body(
    records: Vec<BodyRecord>,
    root: &Arc<crate::physical::LocalRoot>,
    expected: u64,
) -> Result<StoredBody, LocalOpenError> {
    if records.is_empty() || records.len() > 10_000 {
        return Err(LocalOpenError::Corrupt);
    }
    let mut length = 0usize;
    let mut leaves = Vec::with_capacity(records.len());
    for value in records {
        let count = usize::try_from(value.length).map_err(corrupt)?;
        length = length.checked_add(count).ok_or(LocalOpenError::Corrupt)?;
        if value.offset < 68
            || value.journal && (count > inline::LIMIT || !value.segment.is_empty())
        {
            return Err(LocalOpenError::Corrupt);
        }
        leaves.push(StoredBody::Local {
            root: Arc::clone(root),
            digest: value.digest.as_slice().try_into().map_err(corrupt)?,
            length: count,
            location: if value.journal {
                LocalBodyLocation::Journal {
                    offset: value.offset,
                }
            } else {
                LocalBodyLocation::Segment {
                    id: value.segment.as_slice().try_into().map_err(corrupt)?,
                    offset: value.offset,
                }
            },
        });
    }
    if length as u64 != expected {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(StoredBody::Composite {
        parts: Arc::new(crate::v1::Guarded {
            value: leaves,
            _allocation: None,
        }),
        length,
    })
}

pub(super) fn apply(
    state: &mut State,
    delta: Delta,
    root: &Arc<crate::physical::LocalRoot>,
    options: MemoryOptions,
    limits: LocalObjectsLimits,
) -> Result<(), LocalOpenError> {
    super::super::durable::apply(
        state,
        delta,
        &|records, expected| {
            restore_body(records, root, expected).map_err(|_| super::super::durable::RestoreError)
        },
        options,
        limits.maximum_object_bytes,
    )
    .map_err(|_| LocalOpenError::Corrupt)
}
