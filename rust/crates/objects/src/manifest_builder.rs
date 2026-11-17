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
            let entries = self.levels.get_mut(level).ok_or(ErrorCode::Corrupt)?;
            entries
                .try_reserve(1)
                .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
            entries.push(part);
            if entries.len() < PAGE {
                return Ok(());
            }
            part = publish(
                blobs,
                Manifest {
                    parts: std::mem::take(entries),
                    ..Manifest::default()
                },
            )
            .await?;
            level += 1;
        }
    }

    /// Publishes the final manifest and returns its canonical BLAKE3 identity.
    /// `root` supplies the final manifest's metadata and edges; its parts must
    /// be empty. Commit root retention before releasing the input custody.
    pub async fn finish<B: Blobs + ?Sized>(
        mut self,
        blobs: &B,
        mut root: Manifest,
    ) -> Result<Part, Error> {
        if self.poisoned {
            return Err(ErrorCode::OutcomeUnknown.into());
        }
        if !root.parts.is_empty() {
            return Err(ErrorCode::InvalidArgument.into());
        }
        if self.levels.is_empty() {
            let put = blobs.put(Vec::new().into(), Vec::new()).await?;
            let verified = put.verified().ok_or(ErrorCode::OutcomeUnknown)?;
            if verified.digest != Digest::of(&[]) || verified.length != 0 {
                return Err(ErrorCode::Corrupt.into());
            }
            root.parts
                .try_reserve(1)
                .map_err(|_| Error::from(ErrorCode::QuotaExceeded))?;
            root.parts.push(Part {
                digest: verified.digest,
                length: 0,
            });
            return publish(blobs, root).await;
        }
        let mut level = 0;
        loop {
            let entries = std::mem::take(self.levels.get_mut(level).ok_or(ErrorCode::Corrupt)?);
            if entries.is_empty() && level + 1 < self.levels.len() {
                level += 1;
                continue;
            }
            if self.levels.iter().skip(level + 1).all(Vec::is_empty) {
                root.parts = entries;
                let page = publish(blobs, root).await?;
                if page.length != self.length {
                    return Err(ErrorCode::Corrupt.into());
                }
                return Ok(page);
            }
            let page = publish(
                blobs,
                Manifest {
                    parts: entries,
                    ..Manifest::default()
                },
            )
            .await?;
            self.append(blobs, level + 1, page).await?;
            level += 1;
        }
    }
}

async fn publish<B: Blobs + ?Sized>(blobs: &B, manifest: Manifest) -> Result<Part, Error> {
    let length = manifest.length()?;
    let put = blobs.put_manifest(manifest).await?;
    let verified = put.verified().ok_or(ErrorCode::OutcomeUnknown)?;
    if verified.length != length {
        return Err(ErrorCode::Corrupt.into());
    }
    Ok(Part {
        digest: verified.digest,
        length,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ByteRange, MemoryObjects, RootId, Store, tests::Shared};

    #[tokio::test]
    async fn empty_body_preserves_root_metadata() -> Result<(), Box<dyn std::error::Error>> {
        let blobs = MemoryObjects::memory();
        let edge = blobs.put(b"edge".to_vec().into(), Vec::new()).await?;
        let edge = edge.verified().ok_or("unknown put")?.digest;
        let metadata = Manifest {
            edges: vec![edge],
            ..Manifest::default()
        };
        let root = ManifestBuilder::default().finish(&blobs, metadata).await?;
        assert_eq!(root.length, 0);
        assert!(blobs.head(root.digest).await?.references.contains(&edge));
        assert!(
            blobs
                .get(root.digest, ByteRange::FULL, 0)
                .await?
                .body()
                .is_empty()
        );
        Ok(())
    }

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
        let result = builder.finish(&blobs, Manifest::default()).await?;
        assert_eq!(result.length, expected.len() as u64);
        blobs.retain(&root, vec![result.digest]).await?;
        let selected = blobs
            .get(result.digest, ByteRange::new(250, 270), 20)
            .await?;
        assert_eq!(
            selected.body().as_ref(),
            expected.get(250..270).ok_or("range")?
        );
        let selected = blobs
            .get(result.digest, ByteRange::FULL, result.length)
            .await?;
        assert_eq!(selected.body().as_ref(), expected);
        Ok(())
    }

    #[tokio::test]
    async fn upper_page_carries_keep_ranges_in_order() -> Result<(), Box<dyn std::error::Error>> {
        let blobs = MemoryObjects::memory();
        let mut values = Vec::new();
        for value in 0_u8..=250 {
            let put = blobs.put(vec![value].into(), Vec::new()).await?;
            values.push(Part {
                digest: put.verified().ok_or("unknown put")?.digest,
                length: 1,
            });
        }
        let hold = RootId::new(b"carry-inputs".to_vec())?;
        blobs
            .retain(&hold, values.iter().map(|part| part.digest).collect())
            .await?;
        for count in [16_257_usize, 16_384, 16_385] {
            let mut builder = ManifestBuilder::default();
            let mut expected = Vec::new();
            for index in 0..count {
                let value = index % values.len();
                expected.push(u8::try_from(value)?);
                builder
                    .push(&blobs, *values.get(value).ok_or("missing part")?)
                    .await?;
            }
            let root = builder.finish(&blobs, Manifest::default()).await?;
            for start in [16_252_usize, count - 4] {
                let end = (start + 8).min(count);
                let object = blobs
                    .get(
                        root.digest,
                        ByteRange::new(start as u64, end as u64),
                        (end - start) as u64,
                    )
                    .await?;
                assert_eq!(
                    object.body().as_ref(),
                    expected.get(start..end).ok_or("range")?
                );
            }
        }
        Ok(())
    }

    #[tokio::test]
    async fn interrupted_page_publication_poisoned_the_builder()
    -> Result<(), Box<dyn std::error::Error>> {
        for outcome in ["failure", "unknown", "cancelled"] {
            let medium = Shared::default();
            let blobs = Store::new(medium.clone()).account(b"builder-interruption")?;
            let put = blobs.put(b"part".to_vec().into(), Vec::new()).await?;
            let valid = Part {
                digest: put.verified().ok_or("unknown input")?.digest,
                length: 4,
            };
            let mut builder = ManifestBuilder::default();
            for _ in 0..PAGE - 1 {
                builder.push(&blobs, valid).await?;
            }
            match outcome {
                "failure" => {
                    let absent = Part {
                        digest: Digest::of(b"missing"),
                        length: 7,
                    };
                    assert_eq!(
                        builder.push(&blobs, absent).await.unwrap_err().code,
                        ErrorCode::MissingReference
                    );
                }
                "unknown" => {
                    *medium.ambiguous.lock().map_err(|_| "poisoned fault lock")? = Some("/e/");
                    assert_eq!(
                        builder.push(&blobs, valid).await.unwrap_err().code,
                        ErrorCode::OutcomeUnknown
                    );
                }
                "cancelled" => {
                    let (reached, release) = medium.park("create", "/e/");
                    let mut push = Box::pin(builder.push(&blobs, valid));
                    tokio::select! {
                        reached = reached => reached?,
                        _ = &mut push => return Err("page did not reach its publication cut".into()),
                    }
                    drop(push);
                    drop(release);
                }
                _ => return Err("unexpected outcome".into()),
            }
            assert_eq!(
                builder.push(&blobs, valid).await.unwrap_err().code,
                ErrorCode::OutcomeUnknown
            );
            assert_eq!(
                builder
                    .finish(&blobs, Manifest::default())
                    .await
                    .unwrap_err()
                    .code,
                ErrorCode::OutcomeUnknown
            );
        }
        Ok(())
    }
}
