//! Current-state persistence over immutable remote bodies, independent of HTTP.
use super::durable::{self, packing::Pending, protocol::*, snapshot};
use super::*;
use crate::body::{ImmutableBodies, RemoteBody};
use futures::future::BoxFuture;

pub(in crate::v1) use super::durable::RECORD_LIMIT as RECORD_BYTES;

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Unavailable.into()
}

fn externalize<'a>(
    body: &'a StoredBody,
    source: &'a Arc<dyn ImmutableBodies>,
) -> BoxFuture<'a, Result<StoredBody, Error>> {
    Box::pin(async move {
        match body {
            StoredBody::Memory(bytes) => {
                let id = *blake3::hash(bytes).as_bytes();
                source.put(id, bytes.clone()).await?;
                Ok(StoredBody::Remote(RemoteBody {
                    source: Arc::clone(source),
                    id,
                    length: bytes.len(),
                }))
            }
            StoredBody::Remote(_) => Ok(body.clone()),
            StoredBody::Composite { parts, length } => {
                let mut leaves = Vec::with_capacity(parts.value.len());
                for part in &parts.value {
                    leaves.push(externalize(part, source).await?);
                }
                Ok(StoredBody::Composite {
                    parts: Arc::new(Guarded {
                        value: leaves,
                        _allocation: None,
                    }),
                    length: *length,
                })
            }
            #[cfg(feature = "local")]
            StoredBody::Local { .. } => Err(Unavailable.into()),
        }
    })
}

fn restore(
    records: Vec<BodyRecord>,
    expected: u64,
    source: &Arc<dyn ImmutableBodies>,
) -> Result<StoredBody, durable::RestoreError> {
    if records.is_empty() || records.len() > 10_000 {
        return Err(durable::RestoreError);
    }
    let mut length = 0usize;
    let mut leaves = Vec::with_capacity(records.len());
    for record in records {
        let count = usize::try_from(record.length).map_err(|_| durable::RestoreError)?;
        let id: [u8; 32] = record
            .digest
            .as_slice()
            .try_into()
            .map_err(|_| durable::RestoreError)?;
        if record.segment != record.digest || record.offset != 0 || record.journal {
            return Err(durable::RestoreError);
        }
        length = length.checked_add(count).ok_or(durable::RestoreError)?;
        leaves.push(StoredBody::Remote(RemoteBody {
            source: Arc::clone(source),
            id,
            length: count,
        }));
    }
    if length as u64 != expected {
        return Err(durable::RestoreError);
    }
    Ok(StoredBody::Composite {
        parts: Arc::new(Guarded {
            value: leaves,
            _allocation: None,
        }),
        length,
    })
}

impl MemoryObjects {
    /// An isolated COW candidate. Cloning the public provider shares publication;
    /// staging instead shares only immutable persistent branches and cursor keys.
    pub(in crate::v1) fn stage(&self) -> Result<Self, Error> {
        Ok(Self {
            state: StateArc::new(Mutex::new(self.lock_state()?.clone())),
            options: self.options,
            token_key: self.token_key.clone(),
            #[cfg(feature = "local")]
            journal: None,
            #[cfg(feature = "local")]
            defer_local: false,
        })
    }

    /// Externalize newly written leaves and encode only retained state/receipts.
    /// Pages use the same records, ordering, bounds and funded codec as local
    /// checkpoints. Sequence is in the root so unchanged pages remain reusable.
    pub(in crate::v1) async fn snapshot(
        &self,
        source: Arc<dyn ImmutableBodies>,
    ) -> Result<(u64, Vec<Vec<u8>>), Error> {
        let mut state = self.lock_state()?.clone();
        for (name, bucket) in &state.buckets.clone() {
            for (key, object) in &bucket.objects {
                let body = externalize(&object.body, &source).await?;
                if body != object.body {
                    state
                        .buckets
                        .get_mut(name)
                        .ok_or(Error::from(Unavailable))?
                        .objects
                        .get_mut(key)
                        .ok_or(Error::from(Unavailable))?
                        .body = body;
                }
            }
        }
        for (id, upload) in &state.uploads.clone() {
            for (number, (_, part)) in &upload.parts {
                let body = externalize(part, &source).await?;
                if &body != part {
                    state
                        .uploads
                        .get_mut(id)
                        .ok_or(Error::from(Unavailable))?
                        .parts
                        .get_mut(number)
                        .ok_or(Error::from(Unavailable))?
                        .1 = body;
                }
            }
        }
        let mut pages = Vec::new();
        let mut pending = Pending::new(0, None);
        snapshot::walk(&state, |entry| -> Result<(), Error> {
            let mut custody = durable::Publication::new(None);
            let record = entry.record(&mut custody).map_err(unavailable)?;
            if pending.should_flush(record.encoded_len()) {
                pages.push(
                    pending
                        .publication
                        .encode(&pending.delta)
                        .map_err(unavailable)?,
                );
                pending = Pending::new(0, None);
            }
            pending.insert(record, custody).map_err(unavailable)?;
            if pending.encoded_len() > RECORD_BYTES {
                return Err(QuotaExceeded.into());
            }
            Ok(())
        })?;
        if pending.has_changes() {
            pages.push(
                pending
                    .publication
                    .encode(&pending.delta)
                    .map_err(unavailable)?,
            );
        }
        let sequence = state.sequence;
        *self.lock_state()? = state;
        Ok((sequence, pages))
    }

    pub(in crate::v1) fn load_snapshot(
        pages: Vec<Vec<u8>>,
        sequence: u64,
        cursor_key: [u8; 32],
        source: Arc<dyn ImmutableBodies>,
    ) -> Result<Self, Error> {
        let core = Self::new(MemoryOptions::default())?;
        let mut state = State::default();
        if pages.len() > core.options.maximum_entries {
            return Err(Unavailable.into());
        }
        for bytes in pages {
            if bytes.len() > RECORD_BYTES {
                return Err(Unavailable.into());
            }
            let delta = Delta::decode(bytes.as_slice()).map_err(unavailable)?;
            if delta.sequence != 0 || delta.ordinal != 0 || !delta.inline_data.is_empty() {
                return Err(Unavailable.into());
            }
            durable::apply(
                &mut state,
                delta,
                &|records, length| restore(records, length, &source),
                core.options,
                core.options.maximum_bytes as u64,
            )
            .map_err(unavailable)?;
        }
        state.sequence = sequence;
        *core.lock_state()? = state;
        core.replay_cursor_key(cursor_key)?;
        Ok(core)
    }
}
