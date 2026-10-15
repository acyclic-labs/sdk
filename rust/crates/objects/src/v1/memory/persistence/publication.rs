use super::*;

/// Current invocation authority and custody for temporary publication controls.
pub(crate) struct Publication {
    pub(crate) native: Option<(Arc<dyn NativeReadMeter>, ReadAllocation)>,
}
impl Publication {
    pub(crate) fn new(meter: Option<Arc<dyn NativeReadMeter>>) -> Self {
        Self {
            native: meter.map(|meter| {
                let retained = ReadAllocation::empty(&meter);
                (meter, retained)
            }),
        }
    }
    pub(crate) fn check(&self) -> Result<(), NativeObjectsError> {
        if let Some((meter, _)) = &self.native {
            meter.check()?;
        }
        Ok(())
    }
    pub(crate) fn allocate(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::new(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((meter, retained)) = &mut self.native {
            let mut claim = ReadAllocation::adopt(meter, bytes)?;
            retained.absorb(&mut claim)?;
        }
        Ok(())
    }
    pub(crate) fn retain_capacity(
        &mut self,
        requested: usize,
        actual: usize,
    ) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native {
            let total =
                usize::try_from(retained.bytes()).map_err(|_| Error::from(QuotaExceeded))?;
            retained.reconcile_capacity(
                total
                    .checked_sub(requested)
                    .and_then(|bytes| bytes.checked_add(actual))
                    .ok_or(Error::from(QuotaExceeded))?,
            )?;
        }
        Ok(())
    }
    pub(crate) fn allocation_failed(
        &self,
        error: std::collections::TryReserveError,
    ) -> NativeObjectsError {
        match &self.native {
            Some((meter, _)) => NativeObjectsError::from(NativeObjectsCause::Admission(
                meter.allocation_failed(error),
            )),
            None => NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into())),
        }
    }
    pub(crate) fn retire(&mut self, bytes: usize) -> Result<(), NativeObjectsError> {
        if let Some((_, retained)) = &mut self.native {
            retained.release(bytes)?;
        }
        Ok(())
    }
    pub(crate) fn reserve<T>(
        &mut self,
        value: &mut Vec<T>,
        additional: usize,
    ) -> Result<(), NativeObjectsError> {
        let needed = value
            .len()
            .checked_add(additional)
            .ok_or(Error::from(QuotaExceeded))?;
        if needed <= value.capacity() {
            return self.check();
        }
        let width = std::mem::size_of::<T>();
        let old = value
            .capacity()
            .checked_mul(width)
            .ok_or(Error::from(QuotaExceeded))?;
        let requested = needed
            .checked_mul(width)
            .ok_or(Error::from(QuotaExceeded))?;
        let mut claim = self
            .native
            .as_ref()
            .map(|(meter, _)| ReadAllocation::new(meter, requested))
            .transpose()?;
        if let Err(error) = value.try_reserve_exact(additional) {
            return Err(self.allocation_failed(error));
        }
        if let (Some(claim), Some((_, retained))) = (&mut claim, &mut self.native) {
            let reconciled = claim.reconcile_capacity(
                value
                    .capacity()
                    .checked_mul(width)
                    .ok_or(Error::from(QuotaExceeded))?,
            );
            retained.resize_retained(
                usize::try_from(retained.bytes())
                    .map_err(|_| Error::from(QuotaExceeded))?
                    .saturating_sub(old),
            )?;
            retained.absorb(claim)?;
            reconciled?;
        }
        Ok(())
    }
    pub(crate) fn push<T>(
        &mut self,
        value: &mut Vec<T>,
        item: T,
    ) -> Result<(), NativeObjectsError> {
        if value.len() == value.capacity() {
            self.reserve(value, value.capacity().max(1))?;
        } else {
            self.check()?;
        }
        value.push(item);
        Ok(())
    }
    pub(crate) fn codec<T>(
        &self,
        encoded: usize,
        copied: usize,
        hashed: usize,
        work: impl FnOnce() -> T,
    ) -> Result<T, NativeObjectsError> {
        if let Some((meter, _)) = &self.native {
            meter.admit_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        let value = work();
        if let Some((meter, _)) = &self.native {
            meter.record_codec(encoded as u64, copied as u64, hashed as u64)?;
        }
        Ok(value)
    }
    pub(crate) fn text(&mut self, value: &str) -> Result<String, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_text(value, meter, retained),
            _ => Ok(value.to_owned()),
        }
    }
    pub(super) fn info(
        &mut self,
        value: &wire::ObjectInfo,
    ) -> Result<wire::ObjectInfo, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_info(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    pub(super) fn metadata(
        &mut self,
        value: &wire::ObjectMetadata,
    ) -> Result<wire::ObjectMetadata, NativeObjectsError> {
        match &mut self.native {
            Some((meter, retained)) => clone_native_metadata(value, meter, retained),
            _ => Ok(value.clone()),
        }
    }
    pub(crate) fn bytes(&mut self, value: &[u8]) -> Result<Vec<u8>, NativeObjectsError> {
        let mut output = Vec::new();
        self.reserve(&mut output, value.len())?;
        self.codec(0, value.len(), 0, || output.extend_from_slice(value))?;
        Ok(output)
    }
    pub(crate) fn encode<M: prost::Message>(
        &mut self,
        value: &M,
    ) -> Result<Vec<u8>, NativeObjectsError> {
        let length = value.encoded_len();
        let mut output = Vec::new();
        self.reserve(&mut output, length)?;
        self.codec(length, 0, 0, || value.encode(&mut output))?
            .map_err(|_| {
                NativeObjectsError::from(NativeObjectsCause::Service(Unavailable.into()))
            })?;
        Ok(output)
    }

    pub(super) fn relocated(
        &mut self,
        body: &StoredBody,
        moves: &crate::body::LocalBodyRelocations,
    ) -> Result<Option<StoredBody>, NativeObjectsError> {
        self.check()?;
        match body {
            StoredBody::Memory(_) => Ok(None),
            StoredBody::Composite { parts, length } => {
                if !has_relocation(body, moves) {
                    return Ok(None);
                }
                let mut moved = Vec::new();
                self.reserve(&mut moved, parts.value.len())?;
                for part in &parts.value {
                    let relocated = self.relocated(part, moves)?;
                    self.codec(0, std::mem::size_of::<StoredBody>(), 0, || {
                        moved.push(relocated.unwrap_or_else(|| part.clone()));
                    })?;
                }
                composite_publication(moved, *length, self).map(Some)
            }
            StoredBody::Local {
                root,
                digest,
                length,
                location,
            } => Ok(moves
                .get(&(location.clone(), *digest))
                .map(|location| StoredBody::Local {
                    root: Arc::clone(root),
                    digest: *digest,
                    length: *length,
                    location: location.clone(),
                })),
        }
    }
}

pub(super) fn composite_publication(
    parts: Vec<StoredBody>,
    length: usize,
    publication: &mut Publication,
) -> Result<StoredBody, NativeObjectsError> {
    match &mut publication.native {
        Some((meter, retained)) => {
            let capacity = parts
                .capacity()
                .checked_mul(std::mem::size_of::<StoredBody>())
                .ok_or(Error::from(QuotaExceeded))?;
            let allocation = retained.split(capacity)?;
            StoredBody::composite_measured(parts, length, meter, allocation)
        }
        None => Ok(StoredBody::Composite {
            parts: Arc::new(crate::v1::Guarded {
                value: parts,
                _allocation: None,
            }),
            length,
        }),
    }
}

pub(super) fn has_relocation(body: &StoredBody, moves: &crate::body::LocalBodyRelocations) -> bool {
    match body {
        StoredBody::Memory(_) => false,
        StoredBody::Composite { parts, .. } => {
            parts.value.iter().any(|part| has_relocation(part, moves))
        }
        StoredBody::Local {
            location, digest, ..
        } => moves.contains_key(&(location.clone(), *digest)),
    }
}

pub(super) fn unpersisted(body: &StoredBody) -> bool {
    match body {
        StoredBody::Memory(_) => true,
        StoredBody::Composite { parts, .. } => parts.value.iter().any(unpersisted),
        StoredBody::Local { .. } => false,
    }
}
