use super::*;

impl Publication {
    pub(super) fn relocated(
        &mut self,
        body: &StoredBody,
        moves: &crate::body::LocalBodyRelocations,
    ) -> Result<Option<StoredBody>, NativeObjectsError> {
        #[cfg(test)]
        work::relocated();
        self.check()?;
        match body {
            StoredBody::Memory(_) => Ok(None),
            #[cfg(feature = "s3")]
            StoredBody::Remote(_) => Ok(None),
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
        #[cfg(feature = "s3")]
        StoredBody::Remote(_) => false,
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
        #[cfg(feature = "s3")]
        StoredBody::Remote(_) => true,
        StoredBody::Composite { parts, .. } => parts.value.iter().any(unpersisted),
        StoredBody::Local { .. } => false,
    }
}
