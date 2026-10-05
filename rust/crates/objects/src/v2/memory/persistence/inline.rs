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
    /// Moves every body the journal carries into segments, so a checkpoint can
    /// drop the frames that carried them. Bodies are packed into as few
    /// segments as the segment bounds allow, each synchronized as it is
    /// written, and their directory entries are made durable once, before the
    /// relocated state is returned for the checkpoint to record: a compaction
    /// costs a few synchronizations, not two for every small body.
    pub(super) fn materialize_inline(&self, state: &mut State) -> Result<(), LocalOpenError> {
        let mut carried = BTreeMap::new();
        for object in state
            .buckets
            .values()
            .flat_map(|bucket| bucket.objects.values())
        {
            journal_bodies(&object.body, &mut carried);
        }
        for upload in state.uploads.values() {
            for (_, body) in upload.parts.values() {
                journal_bodies(body, &mut carried);
            }
        }
        if carried.is_empty() {
            return Ok(());
        }
        let journal = File::open(self.root.join("mutations.log"))?;
        let mut moves = LocalBodyRelocations::new();
        let mut batch = Materialized::default();
        let mut entries = carried.into_iter().peekable();
        while let Some(((offset, digest), length)) = entries.next() {
            if !batch.holds(&digest) {
                let bytes = crate::physical::read_journal_body(&journal, offset, &digest, length)
                    .map_err(corrupt)?;
                batch.add(digest, bytes);
            }
            batch.carried.push((offset, digest));
            let full = entries.peek().is_none_or(|((_, next_digest), next)| {
                !batch.holds(next_digest)
                    && (batch.bodies.len() == crate::physical::MAXIMUM_SEGMENT_BODIES
                        || batch.bytes + next > crate::physical::MAXIMUM_SEGMENT_BYTES)
            });
            if full {
                self.write_materialized(&mut batch, &mut moves)?;
            }
        }
        crate::physical::sync_segment_directory(&self.root, self.limits.durability)
            .map_err(|_| LocalOpenError::Unavailable)?;
        for bucket in state.buckets.values_mut() {
            let relocated = bucket
                .objects
                .iter()
                .filter_map(|(key, object)| {
                    object
                        .body
                        .relocated(&moves)
                        .map(|body| (key.clone(), body))
                })
                .collect::<Vec<_>>();
            for (key, body) in relocated {
                bucket
                    .objects
                    .get_mut(&key)
                    .ok_or(LocalOpenError::Corrupt)?
                    .body = body;
            }
        }
        for upload in state.uploads.values_mut() {
            for (_, body) in upload.parts.values_mut() {
                if let Some(moved) = body.relocated(&moves) {
                    *body = moved;
                }
            }
        }
        Ok(())
    }
    /// Writes one segment of journal bodies and records where each moved.
    fn write_materialized(
        &self,
        batch: &mut Materialized,
        moves: &mut LocalBodyRelocations,
    ) -> Result<(), LocalOpenError> {
        let (id, offsets) =
            crate::physical::write_segment(&self.root, &batch.bodies, self.limits.durability)
                .map_err(|_| LocalOpenError::Unavailable)?;
        for (offset, digest) in batch.carried.drain(..) {
            let index = *batch.index.get(&digest).ok_or(LocalOpenError::Corrupt)?;
            moves.insert(
                (LocalBodyLocation::Journal { offset }, digest),
                LocalBodyLocation::Segment {
                    id,
                    offset: *offsets.get(index).ok_or(LocalOpenError::Corrupt)?,
                },
            );
        }
        *batch = Materialized::default();
        Ok(())
    }
}

/// One segment's worth of journal bodies being materialized. A segment holds
/// each distinct body once: the journal may carry equal bodies, such as empty
/// ones, at several offsets, and they all move to the same record.
#[derive(Default)]
struct Materialized {
    bodies: Vec<([u8; 32], bytes::Bytes)>,
    index: BTreeMap<[u8; 32], usize>,
    bytes: usize,
    /// Every journal location moving into this segment.
    carried: Vec<(u64, [u8; 32])>,
}

impl Materialized {
    fn holds(&self, digest: &[u8; 32]) -> bool {
        self.index.contains_key(digest)
    }
    fn add(&mut self, digest: [u8; 32], bytes: bytes::Bytes) {
        self.index.insert(digest, self.bodies.len());
        self.bytes += bytes.len();
        self.bodies.push((digest, bytes));
    }
}

/// Every distinct journal-carried leaf of `body`, with its length.
fn journal_bodies(body: &StoredBody, out: &mut BTreeMap<(u64, [u8; 32]), usize>) {
    match body {
        StoredBody::Local {
            digest,
            length,
            location: LocalBodyLocation::Journal { offset },
            ..
        } => {
            out.insert((*offset, *digest), *length);
        }
        StoredBody::Composite { parts, .. } => {
            for part in parts.iter() {
                journal_bodies(part, out);
            }
        }
        _ => {}
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
    // Only a body this record carries can sit at a placeholder offset, so only
    // the objects it adds or replaces can move.
    for change in delta.objects.iter().filter(|change| change.info.is_some()) {
        let object = state
            .buckets
            .get_mut(&change.bucket)
            .and_then(|bucket| bucket.objects.get_mut(&change.key))
            .ok_or(Error::from(Unavailable))?;
        if let Some(body) = object.body.relocated(&moves) {
            object.body = body;
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
