use super::*;

pub(super) fn next_id(state: &mut State) -> Result<String, Error> {
    state.sequence = state
        .sequence
        .checked_add(1)
        .ok_or(Error::from(QuotaExceeded))?;
    // Opaque receipt identity; not a version selector or placement identifier.
    Ok(format!(
        "\"{}\"",
        blake3::hash(&state.sequence.to_le_bytes()).to_hex()
    ))
}

pub(super) fn check_condition(
    value: &Option<wire::Preconditions>,
    current: Option<&Stored>,
) -> Result<(), Error> {
    match value.as_ref().and_then(|value| value.condition.as_ref()) {
        Some(wire::preconditions::Condition::IfAbsent(true)) if current.is_some() => {
            Err(PreconditionFailed.into())
        }
        Some(wire::preconditions::Condition::IfMatch(etag))
            if current.is_none_or(|value| value.info.etag != *etag) =>
        {
            Err(PreconditionFailed.into())
        }
        _ => Ok(()),
    }
}

pub(super) fn read_condition(
    info: &wire::ObjectInfo,
    if_match: &str,
    if_none_match: &str,
) -> Result<(), Error> {
    if !if_match.is_empty() && if_match != "*" && if_match != info.etag {
        return Err(PreconditionFailed.into());
    }
    if !if_none_match.is_empty() && (if_none_match == "*" || if_none_match == info.etag) {
        return Err(NotModified.into());
    }
    Ok(())
}

pub(super) fn publish(
    state: &mut State,
    bucket: &str,
    key: &str,
    metadata: Option<wire::ObjectMetadata>,
    condition: &Option<wire::Preconditions>,
    body: StoredBody,
) -> Result<wire::ObjectInfo, Error> {
    publish_engine(
        state,
        bucket,
        key,
        condition,
        body,
        |state, size| {
            let info = wire::ObjectInfo {
                etag: next_id(state)?,
                size,
                metadata,
                last_modified: Some(timestamp()?),
            };
            Ok((info.clone().into(), info))
        },
        |key| Ok(key.to_owned().into()),
    )
}

pub(super) fn publish_engine<R, E: From<Error>>(
    state: &mut State,
    bucket: &str,
    key: &str,
    condition: &Option<wire::Preconditions>,
    body: StoredBody,
    info: impl FnOnce(&mut State, u64) -> Result<(SharedInfo, R), E>,
    copy_key: impl FnOnce(&str) -> Result<SharedInfo<String>, E>,
) -> Result<R, E> {
    let current = state
        .buckets
        .get(bucket)
        .ok_or(Error::from(NotFound))?
        .objects
        .get(key);
    check_condition(condition, current)?;
    let key = copy_key(key)?;
    let (info, response) = info(state, body.len() as u64)?;
    state
        .buckets
        .get_mut(bucket)
        .ok_or(Error::from(NotFound))?
        .objects
        .insert(key, Stored { info, body });
    Ok(response)
}

pub(super) fn upload<'a>(
    state: &'a State,
    bucket: &str,
    key: &str,
    id: &str,
) -> Result<&'a Upload, Error> {
    request::upload_id(id)?;
    let value = state.uploads.get(id).ok_or(Error::from(NotFound))?;
    if value.bucket.as_str() != bucket || value.key.as_str() != key {
        return Err(NotFound.into());
    }
    Ok(value)
}
