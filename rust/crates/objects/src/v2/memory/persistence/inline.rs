//! Small immutable bodies live in the authenticated protobuf frame suffix.
use super::*;
use crate::body::LocalBodyRelocations;

pub(super) const LIMIT: usize = 64 * 1024;

#[cfg(test)]
mod tests;

impl Journal {
    pub(super) fn prepare_inline(
        &self,
        body: &StoredBody,
        bytes: &mut Vec<u8>,
    ) -> Result<StoredBody, Error> {
        match body {
            StoredBody::Memory(value) if value.len() <= LIMIT => {
                let offset = u64::MAX
                    .checked_sub(bytes.len() as u64)
                    .ok_or(Error::from(QuotaExceeded))?;
                bytes.extend_from_slice(value);
                Ok(StoredBody::Local {
                    root: Arc::new(self.root.clone()),
                    digest: *blake3::hash(value).as_bytes(),
                    length: value.len(),
                    location: LocalBodyLocation::Journal { offset },
                })
            }
            StoredBody::Composite { parts, length } => Ok(StoredBody::Composite {
                parts: parts
                    .iter()
                    .map(|body| self.prepare_inline(body, bytes))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
                length: *length,
            }),
            _ => self.external(body),
        }
    }
    pub(super) fn materialize_inline(&self, state: &mut State) -> Result<(), LocalOpenError> {
        for bucket in state.buckets.values_mut() {
            for object in bucket.objects.values_mut() {
                object.body = self.materialized(&object.body)?;
            }
        }
        for upload in state.uploads.values_mut() {
            for (_, body) in upload.parts.values_mut() {
                *body = self.materialized(body)?;
            }
        }
        Ok(())
    }
    fn materialized(&self, body: &StoredBody) -> Result<StoredBody, LocalOpenError> {
        match body {
            StoredBody::Local {
                digest,
                length,
                location: LocalBodyLocation::Journal { offset },
                ..
            } => {
                let journal = File::open(self.root.join("mutations.log"))?;
                let bytes = crate::local::read_journal_body(&journal, *offset, digest, *length)
                    .map_err(corrupt)?;
                self.external(&StoredBody::Memory(bytes))
                    .map_err(|_| LocalOpenError::Unavailable)
            }
            StoredBody::Composite { parts, length } => Ok(StoredBody::Composite {
                parts: parts
                    .iter()
                    .map(|part| self.materialized(part))
                    .collect::<Result<Vec<_>, _>>()?
                    .into(),
                length: *length,
            }),
            _ => Ok(body.clone()),
        }
    }
}

fn records(delta: &Delta) -> impl Iterator<Item = &BodyRecord> {
    delta
        .objects
        .iter()
        .flat_map(|object| &object.bodies)
        .chain(delta.parts.iter().flat_map(|part| &part.bodies))
}

pub(super) fn place(delta: &mut Delta, state: &mut State, frame: u64) -> Result<(), Error> {
    let encoded = delta.encode_to_vec();
    if !encoded.ends_with(&delta.inline_data) {
        return Err(Unavailable.into());
    }
    let base = frame
        .checked_add(36 + (encoded.len() - delta.inline_data.len()) as u64)
        .ok_or(Error::from(QuotaExceeded))?;
    let mut moves = LocalBodyRelocations::new();
    for body in delta
        .objects
        .iter_mut()
        .flat_map(|object| &mut object.bodies)
        .chain(delta.parts.iter_mut().flat_map(|part| &mut part.bodies))
    {
        if body.journal && body.offset >= u64::MAX - delta.inline_data.len() as u64 {
            let offset = base
                .checked_add(u64::MAX - body.offset)
                .ok_or(Error::from(QuotaExceeded))?;
            moves.insert(
                (
                    LocalBodyLocation::Journal {
                        offset: body.offset,
                    },
                    body.digest
                        .as_slice()
                        .try_into()
                        .map_err(|_| Error::from(Unavailable))?,
                ),
                LocalBodyLocation::Journal { offset },
            );
            body.offset = offset;
        }
    }
    for bucket in state.buckets.values_mut() {
        for object in bucket.objects.values_mut() {
            if let Some(body) = object.body.relocated(&moves) {
                object.body = body;
            }
        }
    }
    for upload in state.uploads.values_mut() {
        for (_, body) in upload.parts.values_mut() {
            if let Some(moved) = body.relocated(&moves) {
                *body = moved;
            }
        }
    }
    if delta.encode_to_vec().len() != encoded.len() {
        return Err(Unavailable.into());
    }
    Ok(())
}

pub(super) fn authenticate(
    delta: &Delta,
    frame: u64,
    encoded: &[u8],
    known: &mut BTreeMap<(u64, [u8; 32]), u64>,
) -> Result<(), LocalOpenError> {
    if !encoded.ends_with(&delta.inline_data) {
        return Err(LocalOpenError::Corrupt);
    }
    let base = frame
        .checked_add(36 + (encoded.len() - delta.inline_data.len()) as u64)
        .ok_or(LocalOpenError::Corrupt)?;
    let mut ranges = BTreeSet::new();
    for body in records(delta).filter(|body| body.journal) {
        let digest: [u8; 32] = body.digest.as_slice().try_into().map_err(corrupt)?;
        if body.length > LIMIT as u64 || !body.segment.is_empty() {
            return Err(LocalOpenError::Corrupt);
        }
        if body.offset < base {
            if known.get(&(body.offset, digest)) != Some(&body.length) {
                return Err(LocalOpenError::Corrupt);
            }
            continue;
        }
        let start = usize::try_from(body.offset - base).map_err(corrupt)?;
        let end = start
            .checked_add(usize::try_from(body.length).map_err(corrupt)?)
            .ok_or(LocalOpenError::Corrupt)?;
        let bytes = delta
            .inline_data
            .get(start..end)
            .ok_or(LocalOpenError::Corrupt)?;
        if *blake3::hash(bytes).as_bytes() != digest {
            return Err(LocalOpenError::Corrupt);
        }
        known.insert((body.offset, digest), body.length);
        ranges.insert((start, end));
    }
    let mut end = 0;
    for (start, next) in ranges {
        if start != end {
            return Err(LocalOpenError::Corrupt);
        }
        end = next;
    }
    if end != delta.inline_data.len() {
        return Err(LocalOpenError::Corrupt);
    }
    Ok(())
}
