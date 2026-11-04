//! Exact-path fork diff publications between two authorized volume generations.
use super::directory::DirectoryListing;
use super::*;
use crate::context::{ContextRoot, DiffKind, DiffPath, DiffPublication, PinnedContextPath};
use acyclic_fs::{access::Action, kernel::FileRecord};

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// The complete exact-path publication of `volume`'s changes from the fork
    /// `base` to `current`: every regular file whose payload may differ, with
    /// each existing endpoint pinned and its largest length. Equal Merkle
    /// subtrees are never traversed. Equal payload identities exclude metadata
    /// changes without reading content. Different layouts may hold equal bytes;
    /// the consumer compares those only within its content allowance, retaining
    /// an explicit omission when it cannot establish equality. A path is disclosed only
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
        let (before, after) = self
            .diff_endpoints(volume, grant, (base, current), &io)
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
            if was
                .zip(is)
                .is_some_and(|(was, is)| was.payload == is.payload)
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

    /// Whether `grant` and each owner's current policy still let the caller
    /// list and read each of `paths` at the endpoints that hold it, as
    /// publishing and rendering it required: a retained projection discloses
    /// a path or its patch again only after this holds for it. Work is the two
    /// endpoint selections plus two decisions per endpoint of each path.
    pub async fn diff_disclosure<'path>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        (base, current): (&GenerationRef, &GenerationRef),
        paths: impl IntoIterator<Item = (&'path str, DiffKind)>,
    ) -> Result<bool> {
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let (before, after) = self
            .diff_endpoints(volume, grant, (base, current), &io)
            .await?;
        let now = super::super::access::access_now()?;
        let discloses = |selection: &super::directory::DirectorySelection<'_, A, O>, path: &str| {
            [Action::List, Action::Read]
                .into_iter()
                .all(|action| selection.allows(action, path, now))
        };
        let listed = paths.into_iter().all(|(path, kind)| {
            !crate::conversation::is_internal_path(path)
                && (kind == DiffKind::Added || discloses(&before, path))
                && (kind == DiffKind::Removed || discloses(&after, path))
        });
        before.require_current(&io).await?;
        after.require_current(&io).await?;
        grant.recheck(Some(&io))?;
        Ok(listed)
    }

    /// Both endpoint roots, each selected under the caller's whole-volume
    /// list authority and that generation's policy.
    async fn diff_endpoints<'io>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        (base, current): (&GenerationRef, &GenerationRef),
        io: &'io PreparationIo,
    ) -> Result<(
        super::directory::DirectorySelection<'io, A, O>,
        super::directory::DirectorySelection<'io, A, O>,
    )> {
        let mut ready = || std::future::ready(Ok(()));
        let before = self
            .select_directory(&root_listing(volume, grant, base), io, &mut ready)
            .await?;
        let after = self
            .select_directory(&root_listing(volume, grant, current), io, &mut ready)
            .await?;
        Ok((before, after))
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
