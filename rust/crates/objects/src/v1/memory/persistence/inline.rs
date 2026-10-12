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
        publication: &mut Publication,
    ) -> Result<StoredBody, NativeObjectsError> {
        if !unpersisted(body) { return Ok(body.clone()); }
        match body {
            StoredBody::Memory(value) if value.len() <= LIMIT => {
                let offset = u64::MAX
                    .checked_sub(bytes.len() as u64)
                    .ok_or(Error::from(QuotaExceeded))?;
                publication.reserve(bytes, value.len())?;
                publication.codec(0, value.len(), 0, || bytes.extend_from_slice(value))?;
                let digest = publication.codec(0, 0, value.len(), || *blake3::hash(value).as_bytes())?;
                Ok(StoredBody::Local {
                    root: Arc::clone(&self.root),
                    digest,
                    length: value.len(),
                    location: LocalBodyLocation::Journal { offset },
                })
            }
            StoredBody::Composite { parts, length } => {
                let mut output = Vec::new();
                publication.reserve(&mut output, parts.value.len())?;
                for body in parts.value.iter() {
                    output.push(self.prepare_inline(body, bytes, publication)?);
                }
                composite_publication(output, *length, publication)
            }
            _ => self.external(body, publication),
        }
    }
    pub(super) fn materialize_inline(
        &self,
        state: &mut State,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> Result<(), NativeObjectsError> {
        obs::scoped(
            &obs::span!(
                DEBUG,
                "acyclic.objects.journal.materialize",
                items = obs::Empty,
                bytes = obs::Empty,
                segments = obs::Empty
            ),
            || {
                let mut publication = Publication::new(meter);
                publication.check()?;
                let mut carried = BTreeMap::new();
                for object in state
                    .buckets
                    .values()
                    .flat_map(|bucket| bucket.objects.values())
                {
                    journal_bodies(&object.body, &mut carried, &mut publication)?;
                }
                for upload in state.uploads.values() {
                    for (_, body) in upload.parts.values() {
                        journal_bodies(body, &mut carried, &mut publication)?;
                    }
                }
                if carried.is_empty() {
                    return Ok(());
                }
                let (items, mut moved, mut segments) = (carried.len(), 0, 0_u64);
                let mut moves = LocalBodyRelocations::new();
                let mut batch = Materialized::default();
                let mut entries = carried.into_iter().peekable();
                while let Some(((offset, digest), length)) = entries.next() {
                    if !batch.holds(&digest) {
                        // Compaction closes this handle before journal replacement.
                        let bytes = match &publication.native {
                            Some((meter, _)) => futures::executor::block_on(
                                crate::physical::read_body_at(
                                    &self.root, &digest, length,
                                    &LocalBodyLocation::Journal { offset },
                                    0, length, BodyReadMode::Native(Arc::clone(meter)),
                                ),
                            )?,
                            None => self.root.read_journal_body(offset, &digest, length)
                                .map_err(|_| Error::from(Unavailable))?,
                        };
                        batch.add(digest, bytes, &mut publication)?;
                    }
                    publication.push(&mut batch.carried, (offset, digest))?;
                    let full = entries.peek().is_none_or(|((_, next_digest), next)| {
                        !batch.holds(next_digest)
                            && (batch.bodies.len() == crate::physical::MAXIMUM_SEGMENT_BODIES
                                || batch.bytes + next > crate::physical::MAXIMUM_SEGMENT_BYTES)
                    });
                    if full {
                        moved += batch.bytes;
                        segments += 1;
                        self.write_materialized(&mut batch, &mut moves, &mut publication)?;
                    }
                }
                obs::record!("items" = items, "bytes" = moved, "segments" = segments);
                crate::physical::sync_segment_directory(
                    &self.root.path, self.limits.durability, &mut publication,
                )?;
                for (name, _) in &state.buckets.clone() {
                    let bucket = state.buckets.get_mut(name).ok_or(Error::from(Unavailable))?;
                    let mut relocation = Publication::new(publication.native.as_ref().map(|(meter, _)| Arc::clone(meter)));
                    let mut relocated = Vec::new();
                    for (key, object) in bucket.objects.iter() {
                        if let Some(body) =
                            relocated_publication(&object.body, &moves, &publication)?
                        {
                            relocation.push(&mut relocated, (key.clone(), body))?;
                        }
                    }
                    for (key, body) in relocated {
                        bucket
                            .objects
                            .get_mut(&key)
                            .ok_or(Error::from(Unavailable))?
                            .body = body;
                    }
                }
                for (id, _) in &state.uploads.clone() {
                    let upload = state.uploads.get_mut(id).ok_or(Error::from(Unavailable))?;
                    for (number, _) in &upload.parts.clone() {
                        let (_, body) = upload.parts.get_mut(number).ok_or(Error::from(Unavailable))?;
                        if let Some(moved) = relocated_publication(body, &moves, &publication)? {
                            *body = moved;
                        }
                    }
                }
                Ok(())
            },
        )
    }
    /// Writes one segment of journal bodies and records where each moved.
    fn write_materialized(
        &self,
        batch: &mut Materialized,
        moves: &mut LocalBodyRelocations,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        let (id, offsets) = crate::physical::write_segment(
            &self.root.path, &batch.bodies, self.limits.durability, publication,
        )?;
        for (offset, digest) in batch.carried.drain(..) {
            let index = *batch.index.get(&digest).ok_or(Error::from(Unavailable))?;
            publication.retain(
                std::mem::size_of::<((LocalBodyLocation, [u8; 32]), LocalBodyLocation)>(),
            )?;
            moves.insert(
                (LocalBodyLocation::Journal { offset }, digest),
                LocalBodyLocation::Segment {
                    id,
                    offset: *offsets.get(index).ok_or(Error::from(Unavailable))?,
                },
            );
        }
        // Retain array capacity claims while clearing body owners.
        batch.bodies.clear();
        let index_bytes = batch.index.len()
            .checked_mul(std::mem::size_of::<([u8; 32], usize)>())
            .ok_or(Error::from(QuotaExceeded))?;
        batch.index.clear();
        publication.retire(index_bytes)?;
        batch.bytes = 0;
        Ok(())
    }
}

/// One segment's worth of journal bodies being materialized.
#[derive(Default)]
struct Materialized {
    bodies: Vec<([u8; 32], OwnedBytes)>,
    index: BTreeMap<[u8; 32], usize>,
    bytes: usize,
    /// Every journal location moving into this segment.
    carried: Vec<(u64, [u8; 32])>,
}

impl Materialized {
    fn holds(&self, digest: &[u8; 32]) -> bool {
        self.index.contains_key(digest)
    }
    fn add(
        &mut self,
        digest: [u8; 32],
        bytes: OwnedBytes,
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        publication.retain(std::mem::size_of::<([u8; 32], usize)>())?;
        if self.bodies.len() == self.bodies.capacity() {
            let additional = self.bodies.capacity().max(1);
            publication.reserve(&mut self.bodies, additional)?;
        }
        self.index.insert(digest, self.bodies.len());
        self.bytes += bytes.len();
        self.bodies.push((digest, bytes));
        Ok(())
    }
}

/// Every distinct journal-carried leaf of `body`, with its length.
fn journal_bodies(
    body: &StoredBody,
    out: &mut BTreeMap<(u64, [u8; 32]), usize>,
    publication: &mut Publication,
) -> Result<(), NativeObjectsError> {
    match body {
        StoredBody::Local {
            digest,
            length,
            location: LocalBodyLocation::Journal { offset },
            ..
        } => {
            if !out.contains_key(&(*offset, *digest)) {
                publication.retain(std::mem::size_of::<((u64, [u8; 32]), usize)>())?;
            }
            out.insert((*offset, *digest), *length);
        }
        StoredBody::Composite { parts, .. } => {
            for part in parts.value.iter() {
                journal_bodies(part, out, publication)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn records(delta: &Delta) -> impl Iterator<Item = &BodyRecord> {
    delta
        .objects
        .iter()
        .flat_map(|object| &object.bodies)
        .chain(delta.parts.iter().flat_map(|part| &part.bodies))
}

pub(super) fn place(delta: &mut Delta, state: &mut State, frame: u64, publication: &mut Publication) -> Result<(), NativeObjectsError> {
    let encoded = publication.encode(delta)?;
    if !encoded.ends_with(&delta.inline_data) {
        return Err(Error::from(Unavailable).into());
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
            let key = (
                LocalBodyLocation::Journal { offset: body.offset },
                body.digest.as_slice().try_into().map_err(|_| Error::from(Unavailable))?,
            );
            if !moves.contains_key(&key) {
                publication.retain(std::mem::size_of::<((LocalBodyLocation, [u8; 32]), LocalBodyLocation)>())?;
            }
            moves.insert(key, LocalBodyLocation::Journal { offset });
            body.offset = offset;
        }
    }
    // Only objects this record changes can have placeholder offsets.
    for change in delta.objects.iter().filter(|change| change.info.is_some()) {
        let object = state
            .buckets
            .get_mut(change.bucket.as_str())
            .and_then(|bucket| bucket.objects.get_mut(change.key.as_str()))
            .ok_or(Error::from(Unavailable))?;
        if let Some(body) = relocated_publication(&object.body, &moves, publication)? {
            object.body = body;
        }
    }
    for (id, _) in &state.uploads.clone() {
                    let upload = state.uploads.get_mut(id).ok_or(Error::from(Unavailable))?;
        for (number, _) in &upload.parts.clone() {
                        let (_, body) = upload.parts.get_mut(number).ok_or(Error::from(Unavailable))?;
            if let Some(moved) = relocated_publication(body, &moves, publication)? {
                *body = moved;
            }
        }
    }
    if delta.encoded_len() != encoded.len() {
        return Err(Error::from(Unavailable).into());
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
