//! Incremental construction of the canonical nested manifest format.

use crate::{Blobs, Digest, Error, Manifest, Part, wire::ErrorCode};

// One page of ordered references, not a limit on the logical body.
const PAGE: usize = 128;

/// Constructs bounded manifest pages from already published logical parts.
///
/// The caller must retain each input until the final root has been retained.
/// A put alone is not a durable commitment; a page collected before it is
/// linked causes publication or final retention to fail.
/// Failed or cancelled pushes poison the builder: the original publication
/// outcome must be reconciled rather than repeated under a guessed state.
#[derive(Default)]
pub struct ManifestBuilder {
    levels: Vec<Vec<Part>>,
    length: u64,
    poisoned: bool,
}

impl ManifestBuilder {
    /// Appends a nonempty, already published part in logical order.
    pub async fn push<B: Blobs + ?Sized>(&mut self, blobs: &B, part: Part) -> Result<(), Error> {
        if self.poisoned {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        if part.length == 0 {
            return Err(ErrorCode::InvalidArgument.into());
        }
        self.poisoned = true;
        let length = self
            .length
            .checked_add(part.length)
            .ok_or(ErrorCode::QuotaExceeded)?;
        self.append(blobs, 0, part).await?;
        self.length = length;
        self.poisoned = false;
        Ok(())
    }

    async fn append<B: Blobs + ?Sized>(
        &mut self,
        blobs: &B,
        mut level: usize,
        mut part: Part,
    ) -> Result<(), Error> {
        loop {
            if level >= u64::BITS as usize {
                return Err(ErrorCode::QuotaExceeded.into());
            }
            if self.levels.len() <= level {
                self.levels
                    .try_reserve(1)
                    .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
                self.levels.push(Vec::new());
            }
            let entries = &mut self.levels[level];
            entries
                .try_reserve(1)
                .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
            entries.push(part);
            if entries.len() < PAGE {
                return Ok(());
            }
            part = publish(blobs, std::mem::take(entries)).await?;
            level += 1;
        }
    }

    /// Publishes the final manifest and returns its canonical BLAKE3 identity.
    /// The caller commits its own retention before releasing input custody.
    pub async fn finish<B: Blobs + ?Sized>(mut self, blobs: &B) -> Result<Part, Error> {
        if self.poisoned {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        if self.levels.is_empty() {
            self.levels.push(Vec::new());
        }
        let mut level = 0;
        loop {
            let entries = std::mem::take(&mut self.levels[level]);
            if entries.is_empty() && level + 1 < self.levels.len() {
                level += 1;
                continue;
            }
            let page = publish(blobs, entries).await?;
            if self.levels.iter().skip(level + 1).all(Vec::is_empty) {
                if page.length != self.length {
                    return Err(ErrorCode::Corrupt.into());
                }
                return Ok(page);
            }
            self.append(blobs, level + 1, page).await?;
            level += 1;
        }
    }
}

async fn publish<B: Blobs + ?Sized>(blobs: &B, parts: Vec<Part>) -> Result<Part, Error> {
    let mut manifest = Manifest::default();
    manifest.parts = parts;
    let length = manifest.length()?;
    let digest = Digest::of(&manifest.encode()?);
    let put = blobs.put_manifest(manifest).await?;
    let verified = put.verified().ok_or(ErrorCode::OutcomeUnknown)?;
    if verified.digest != digest || verified.length != length {
        return Err(ErrorCode::Corrupt.into());
    }
    Ok(Part { digest, length })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ByteRange, MemoryObjects, RootId};

    #[tokio::test]
    async fn nested_pages_preserve_order_across_page_boundaries()
    -> Result<(), Box<dyn std::error::Error>> {
        let blobs = MemoryObjects::memory();
        let mut parts = Vec::new();
        let mut expected = Vec::new();
        for value in 0_u16..257 {
            let bytes = value.to_le_bytes();
            let put = blobs.put(bytes.to_vec().into(), Vec::new()).await?;
            let digest = put.verified().ok_or("unknown put")?.digest;
            expected.extend_from_slice(&bytes);
            parts.push(Part { digest, length: 2 });
        }
        let root = RootId::new(b"manifest-builder-inputs".to_vec())?;
        blobs
            .retain(&root, parts.iter().map(|part| part.digest).collect())
            .await?;
        let mut builder = ManifestBuilder::default();
        for part in parts {
            builder.push(&blobs, part).await?;
        }
        let result = builder.finish(&blobs).await?;
        assert_eq!(result.length, expected.len() as u64);
        blobs.retain(&root, vec![result.digest]).await?;
        let selected = blobs
            .get(result.digest, ByteRange::new(250, 270), 20)
            .await?;
        assert_eq!(selected.body().as_ref(), &expected[250..270]);
        let selected = blobs
            .get(result.digest, ByteRange::FULL, result.length)
            .await?;
        assert_eq!(selected.body().as_ref(), expected);
        Ok(())
    }
}
