use super::*;

impl Journal {
    pub(super) fn maintenance_due(&self) -> Result<bool, Error> {
        let tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
        Ok(tail.inline_bytes >= MINIMUM_MAINTENANCE_INLINE_BYTES
            && tail.inline_bytes >= tail.bytes.saturating_sub(tail.inline_bytes))
    }
    pub(in crate::v1::memory) fn validate_bodies(&self, state: &State) -> Result<(), Error> {
        let bodies = state
            .buckets
            .values()
            .flat_map(|bucket| bucket.objects.values())
            .map(|object| &object.body)
            .chain(
                state
                    .uploads
                    .values()
                    .flat_map(|upload| upload.parts.values())
                    .map(|(_, body)| body),
            );
        if bodies
            .into_iter()
            .any(|body| body.len() as u64 > self.limits.maximum_object_bytes)
        {
            return Err(QuotaExceeded.into());
        }
        Ok(())
    }
    pub(in crate::v1::memory) fn check(&self) -> Result<(), Error> {
        if self.poisoned.load(Ordering::Acquire) {
            Err(Unavailable.into())
        } else {
            Ok(())
        }
    }
    pub(in crate::v1::memory) fn commit(
        &self,
        before: &State,
        next: &mut State,
        meter: Option<Arc<dyn NativeReadMeter>>,
    ) -> Result<(), NativeObjectsError> {
        obs::scoped(
            &obs::span!(DEBUG, "acyclic.objects.journal.commit", bytes = obs::Empty),
            || {
                let mut publication = Publication::new(meter);
                publication.check()?;
                self.check()?;
                let mut tail = self.tail.lock().map_err(|_| Error::from(Unavailable))?;
                if tail.operations >= self.limits.maximum_journal_operations {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let mut inline_data = Vec::new();
                for (name, _) in &next.buckets.clone() {
                    let bucket = next.buckets.get_mut(name).ok_or(Error::from(Unavailable))?;
                    let keys = changed_keys(before.buckets.get(name), bucket, &mut publication)?;
                    for key in keys {
                        let object = bucket
                            .objects
                            .get_mut(&key)
                            .ok_or(Error::from(Unavailable))?;
                        if object.body.len() as u64 > self.limits.maximum_object_bytes {
                            return Err(Error::from(QuotaExceeded).into());
                        }
                        object.body =
                            self.prepare_inline(&object.body, &mut inline_data, &mut publication)?;
                    }
                }
                for (id, _) in &next.uploads.clone() {
                    let upload = next.uploads.get_mut(id).ok_or(Error::from(Unavailable))?;
                    for (number, _) in &upload.parts.clone() {
                        let (_, body) = upload
                            .parts
                            .get_mut(number)
                            .ok_or(Error::from(Unavailable))?;
                        *body = self.prepare_inline(body, &mut inline_data, &mut publication)?;
                    }
                }
                let mut delta = difference(before, next, tail.operations + 1, &mut publication)?;
                delta.inline_data = inline_data;
                inline::place(&mut delta, next, tail.bytes, &mut publication)?;
                let record = publication.encode(&delta)?;
                let end = tail
                    .bytes
                    .checked_add(36 + record.len() as u64)
                    .ok_or(Error::from(QuotaExceeded))?;
                if record.len() > RECORD_LIMIT || end > self.limits.maximum_journal_bytes {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let mut frame = Vec::new();
                publication.reserve(&mut frame, 36 + record.len())?;
                let length = u32::try_from(record.len()).map_err(|_| Error::from(QuotaExceeded))?;
                let digest =
                    publication.codec(0, 0, record.len(), || *blake3::hash(&record).as_bytes())?;
                publication.codec(0, 36 + record.len(), 0, || {
                    frame.extend_from_slice(&length.to_le_bytes());
                    frame.extend_from_slice(&digest);
                    frame.extend_from_slice(&record);
                })?;
                obs::record!("bytes" = frame.len());
                let start = tail.bytes;
                if let Err(error) = self.write(&mut tail.file, start, &frame, &mut publication) {
                    self.poisoned.store(true, Ordering::Release);
                    return Err(error);
                }
                tail.bytes = end;
                tail.operations += 1;
                tail.inline_bytes = tail
                    .inline_bytes
                    .saturating_add(delta.inline_data.len() as u64);
                Ok(())
            },
        )
    }
    pub(super) fn external(
        &self,
        body: &StoredBody,
        publication: &mut Publication,
    ) -> Result<StoredBody, NativeObjectsError> {
        if !unpersisted(body) {
            return Ok(body.clone());
        }
        match body {
            StoredBody::Memory(bytes) => {
                if bytes.len() as u64 > self.limits.maximum_object_bytes {
                    return Err(Error::from(QuotaExceeded).into());
                }
                let digest =
                    publication.codec(0, 0, bytes.len(), || *blake3::hash(bytes).as_bytes())?;
                let (id, offsets) = crate::physical::persist_segment(
                    &self.root.path,
                    &[(digest, bytes.as_slice())],
                    self.limits.durability,
                    publication,
                )?;
                Ok(StoredBody::Local {
                    root: Arc::clone(&self.root),
                    digest,
                    length: bytes.len(),
                    location: LocalBodyLocation::Segment {
                        id,
                        offset: *offsets.first().ok_or(Error::from(Unavailable))?,
                    },
                })
            }
            StoredBody::Composite { parts, length } => {
                let mut output = Vec::new();
                publication.reserve(&mut output, parts.value.len())?;
                for part in parts.value.iter() {
                    output.push(self.external(part, publication)?);
                }
                composite_publication(output, *length, publication)
            }
            StoredBody::Local { .. } => Ok(body.clone()),
        }
    }
    pub(super) fn write(
        &self,
        file: &mut File,
        offset: u64,
        frame: &[u8],
        publication: &mut Publication,
    ) -> Result<(), NativeObjectsError> {
        obs::scoped(
            &obs::span!(
                DEBUG,
                "acyclic.objects.journal.append",
                bytes = frame.len(),
                fsyncs = 1
            ),
            || {
                #[cfg(test)]
                {
                    let cut = self.fault_bytes.swap(u64::MAX, Ordering::AcqRel);
                    if cut != u64::MAX {
                        let count = usize::try_from(cut).unwrap_or(frame.len()).min(frame.len());
                        crate::physical::write_publication(
                            file,
                            offset,
                            frame
                                .get(..count)
                                .ok_or_else(|| std::io::Error::other("invalid fault point"))?,
                            publication,
                        )?;
                        crate::physical::sync_publication(
                            file,
                            self.limits.durability,
                            publication,
                        )?;
                        return Err(std::io::Error::other("injected journal write failure").into());
                    }
                }
                crate::physical::write_publication(file, offset, frame, publication)?;
                #[cfg(test)]
                if self.fault_sync.swap(false, Ordering::AcqRel) {
                    return Err(std::io::Error::other("injected journal sync failure").into());
                }
                crate::physical::sync_publication(file, self.limits.durability, publication)
            },
        )
    }
}
