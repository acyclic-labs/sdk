//! Exact-path fork diff publications between two authorized volume generations.
use super::directory::DirectoryListing;
use super::*;
use crate::context::{ContextRoot, DiffPath, DiffPublication, PinnedContextPath};
use acyclic_fs::kernel::FileRecord;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// The complete exact-path publication of `volume`'s changes from the fork
    /// `base` to `current`: every regular file whose content differs, with
    /// each existing endpoint pinned and its largest length. Equal Merkle
    /// subtrees are never traversed, and equal content is certified
    /// independently of chunk layout, so a file restored to its base bytes or
    /// changed only in metadata is not published. A path is disclosed only
    /// when `grant` and each owner's current policy let the caller list it at
    /// the generation that holds it; internal paths never are.
    pub async fn diff_publication(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        base: &GenerationRef,
        current: &GenerationRef,
    ) -> Result<DiffPublication> {
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let mut ready = || std::future::ready(Ok(()));
        let before = self
            .select_directory(&root_listing(volume, grant, base), &io, &mut ready)
            .await?;
        let after = self
            .select_directory(&root_listing(volume, grant, current), &io, &mut ready)
            .await?;
        let (changes, _changes) = io.sdk(
            before
                .generation
                .diff_to(&after.generation, u32::MAX, &io, io.cancellation())
                .await,
        )?;
        let (changed, _changed) = io.sdk(
            changes
                .changed_paths(u32::MAX, &io, io.cancellation())
                .await,
        )?;
        let root = ContextRoot {
            volume: volume.clone(),
            directory: String::new(),
        };
        let now = super::super::access::access_now()?;
        let mut paths = Vec::new();
        for change in changed {
            let regular = |record: Option<FileRecord>| {
                record.filter(|record| record.kind == acyclic_fs::kernel::FileKind::Regular)
            };
            let (was, is) = (regular(change.before), regular(change.after));
            if was.is_none() && is.is_none() {
                continue;
            }
            let Some(path) = content_path(&change.path, &io)? else {
                continue;
            };
            if crate::conversation::is_internal_path(&path)
                || was.is_some() && !before.lists(&path, now)
                || is.is_some() && !after.lists(&path, now)
            {
                continue;
            }
            if was.is_some()
                && is.is_some()
                && io.sdk(
                    after
                        .generation
                        .matches_content(
                            &before.generation,
                            &format!("/{path}"),
                            &io,
                            io.cancellation(),
                        )
                        .await,
                )?
            {
                continue;
            }
            let pin = |generation: &GenerationRef| PinnedContextPath {
                root: root.clone(),
                generation: generation.clone(),
                path: path.clone(),
            };
            paths.push(DiffPath {
                base: was.map(|_| pin(base)),
                current: is.map(|_| pin(current)),
                largest_file_bytes: was
                    .iter()
                    .chain(&is)
                    .map(FileRecord::logical_file_bytes)
                    .max(),
                matches_base: false,
                path,
            });
        }
        before.require_current(&io).await?;
        after.require_current(&io).await?;
        grant.recheck(Some(&io))?;
        Ok(DiffPublication {
            previous: base.clone(),
            generation: current.clone(),
            paths,
        })
    }
}

/// The volume root at one exact generation: selecting it requires the
/// caller's whole-volume list authority under that generation's policy.
fn root_listing<'a>(
    volume: &'a VolumeRef,
    grant: &'a ContentGrant,
    generation: &'a GenerationRef,
) -> DirectoryListing<'a> {
    DirectoryListing {
        volume,
        grant,
        ceiling: None,
        path: "",
        generation: Some(generation),
        after: None,
        maximum_entries: 1,
    }
}
