//! Private immutable body storage shared by logical and legacy recovery engines.
use futures::future::BoxFuture;
use std::sync::Arc;
#[cfg(feature = "local")]
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};

#[derive(Clone, Copy, Debug, thiserror::Error)]
pub(crate) enum BodyError {
    #[error("object body storage is unavailable")]
    Unavailable,
}

#[derive(Clone)]
pub(crate) enum StoredBody {
    Memory(bytes::Bytes),
    Composite {
        parts: Arc<[StoredBody]>,
        length: usize,
    },
    #[cfg(feature = "local")]
    Local {
        root: Arc<PathBuf>,
        digest: [u8; 32],
        length: usize,
        location: LocalBodyLocation,
    },
}

#[derive(Clone, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum BodyIdentity {
    Memory {
        address: usize,
        length: usize,
    },
    #[cfg(feature = "local")]
    Local([u8; 32]),
}

impl StoredBody {
    pub(crate) fn memory(body: bytes::Bytes) -> Self {
        Self::Memory(body)
    }

    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Memory(body) => body.len(),
            Self::Composite { length, .. } => *length,
            #[cfg(feature = "local")]
            Self::Local { length, .. } => *length,
        }
    }

    pub(crate) fn update_hash(&self, hasher: &mut blake3::Hasher) -> Result<(), BodyError> {
        match self {
            Self::Memory(body) => {
                hasher.update(body);
                Ok(())
            }
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.update_hash(hasher)?;
                }
                Ok(())
            }
            #[cfg(feature = "local")]
            Self::Local {
                root,
                digest,
                length,
                location,
            } => crate::local::hash_body_at(root, digest, *length, location, hasher)
                .map_err(|_| BodyError::Unavailable),
        }
    }

    pub(crate) fn leaves(&self, output: &mut Vec<(BodyIdentity, usize)>) {
        match self {
            Self::Memory(body) => output.push((
                BodyIdentity::Memory {
                    address: body.as_ptr() as usize,
                    length: body.len(),
                },
                body.len(),
            )),
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.leaves(output);
                }
            }
            #[cfg(feature = "local")]
            Self::Local { digest, length, .. } => {
                output.push((BodyIdentity::Local(*digest), *length));
            }
        }
    }

    #[cfg(feature = "local")]
    pub(crate) fn local_references(&self, output: &mut BTreeSet<LocalBodyReference>) {
        match self {
            Self::Memory(_) => {}
            Self::Composite { parts, .. } => {
                for part in parts.iter() {
                    part.local_references(output);
                }
            }
            Self::Local {
                digest,
                length,
                location,
                ..
            } => {
                output.insert(LocalBodyReference {
                    digest: *digest,
                    length: *length,
                    location: location.clone(),
                });
            }
        }
    }

    /// This body with every local leaf found in `relocations` moved, if any leaf moves.
    #[cfg(feature = "local")]
    pub(crate) fn relocated(&self, relocations: &LocalBodyRelocations) -> Option<Self> {
        match self {
            Self::Memory(_) => None,
            Self::Composite { parts, length } => {
                let moved = parts
                    .iter()
                    .map(|part| part.relocated(relocations))
                    .collect::<Vec<_>>();
                moved.iter().any(Option::is_some).then(|| Self::Composite {
                    parts: moved
                        .into_iter()
                        .zip(parts.iter())
                        .map(|(moved, part)| moved.unwrap_or_else(|| part.clone()))
                        .collect(),
                    length: *length,
                })
            }
            Self::Local {
                root,
                digest,
                length,
                location,
            } => relocations
                .get(&(location.clone(), *digest))
                .map(|destination| Self::Local {
                    root: Arc::clone(root),
                    digest: *digest,
                    length: *length,
                    location: destination.clone(),
                }),
        }
    }

    pub(crate) fn read_async(
        &self,
        start: usize,
        end: usize,
    ) -> BoxFuture<'_, Result<bytes::Bytes, BodyError>> {
        Box::pin(async move {
            if start > end || end > self.len() {
                return Err(BodyError::Unavailable);
            }
            match self {
                Self::Memory(body) => Ok(body.slice(start..end)),
                Self::Composite { parts, .. } => {
                    let mut output = Vec::with_capacity(end.saturating_sub(start));
                    let mut offset = 0usize;
                    for part in parts.iter() {
                        let part_end = offset
                            .checked_add(part.len())
                            .ok_or(BodyError::Unavailable)?;
                        if part_end > start && offset < end {
                            let selected_start = start.saturating_sub(offset).min(part.len());
                            let selected_end = end.saturating_sub(offset).min(part.len());
                            output.extend_from_slice(
                                &part.read_async(selected_start, selected_end).await?,
                            );
                        }
                        offset = part_end;
                    }
                    if output.len() != end.saturating_sub(start) {
                        return Err(BodyError::Unavailable);
                    }
                    Ok(output.into())
                }
                #[cfg(feature = "local")]
                Self::Local {
                    root,
                    digest,
                    length,
                    location,
                } => crate::local::read_body_at_async(root, digest, *length, location, start, end)
                    .await
                    .map_err(|_| BodyError::Unavailable),
            }
        })
    }
}

#[cfg(feature = "local")]
pub(crate) struct ExternalBody {
    pub(crate) root: PathBuf,
    pub(crate) digest: [u8; 32],
    pub(crate) length: usize,
    pub(crate) location: LocalBodyLocation,
}

#[cfg(feature = "local")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum LocalBodyLocation {
    /// A body record inside one immutable, content-addressed segment file.
    Segment { id: [u8; 32], offset: u64 },
    /// Bytes carried by the journal frame that committed the body.
    Journal { offset: u64 },
    /// Unreachable when journal compaction dropped its bytes; never readable.
    Reclaimed,
}

/// Physical moves of local bodies, keyed by current location and digest: an empty body
/// shares its journal offset with the inline body that follows it.
#[cfg(feature = "local")]
pub(crate) type LocalBodyRelocations = BTreeMap<(LocalBodyLocation, [u8; 32]), LocalBodyLocation>;

#[cfg(feature = "local")]
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct LocalBodyReference {
    pub(crate) digest: [u8; 32],
    pub(crate) length: usize,
    pub(crate) location: LocalBodyLocation,
}
