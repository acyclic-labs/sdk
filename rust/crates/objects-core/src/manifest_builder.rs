//! Incremental construction of the canonical nested manifest format.

use crate::{Blobs, Digest, Error, Manifest, Part, wire::ErrorCode};

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
    /// The most parts one published page holds before it is linked upward.
    /// This is a page size, not a limit on the logical body.
    pub const PAGE: usize = 128;

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
            if entries.len() < Self::PAGE {
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
