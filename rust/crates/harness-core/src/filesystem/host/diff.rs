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
        let root = ContextRoot {
            volume: volume.clone(),
            directory: String::new(),
        };
        self.diff_private_publications(&[(&root, grant)], (base, current), &io)
            .await?
            .pop()
            .ok_or_else(|| io.invalid("comparison lost its root"))
    }

    /// One comparison shared by every originally authorized root. Unrelated
    /// changes are traversed once, not once for each skill root, and are never
    /// copied into a root's disclosed result.
    pub(crate) async fn diff_private_publications(
        &self,
        roots: &[(&ContextRoot, &ContentGrant)],
        (base, current): (&GenerationRef, &GenerationRef),
        io: &PreparationIo,
    ) -> Result<Vec<DiffPublication>> {
        let mut selections = Vec::new();
        for (root, grant) in roots {
            selections.push(
                self.diff_endpoints(&root.volume, grant, (base, current), &root.directory, io)
                    .await?,
            );
        }
        let (before, after) = selections
            .first()
            .ok_or_else(|| io.invalid("comparison has no authorized roots"))?;
        let (changes, _changes) = io.sdk(
            before
                .generation
                .diff_to(&after.generation, u32::MAX, io, io.cancellation())
                .await,
        )?;
        let (changed, _changed) =
            io.sdk(changes.changed_paths(u32::MAX, io, io.cancellation()).await)?;
        let mut paths = vec![Vec::new(); roots.len()];
        let now = super::super::access::access_now()?;
        for change in changed {
            let regular = |record: Option<FileRecord>| {
                record.filter(|record| record.kind == acyclic_fs::kernel::FileKind::Regular)
            };
            let (was, is) = (regular(change.before), regular(change.after));
            if was.is_none() && is.is_none()
                || was
                    .zip(is)
                    .is_some_and(|(was, is)| was.payload == is.payload)
            {
                continue;
            }
            let Some(path) = content_path(&change.path, io)? else {
                continue;
            };
            if crate::conversation::is_internal_path(&path) {
                continue;
            }
            for (((root, _), (before, after)), paths) in
                roots.iter().zip(&selections).zip(&mut paths)
            {
                if !(root.directory.is_empty()
                    || path == root.directory
                    || path
                        .strip_prefix(&root.directory)
                        .is_some_and(|tail| tail.starts_with('/')))
                    || was.is_some() && !before.lists(&path, now)
                    || is.is_some() && !after.lists(&path, now)
                {
                    continue;
                }
                let pin = |generation: &GenerationRef| PinnedContextPath {
                    root: (*root).clone(),
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
                    path: path.clone(),
                });
            }
        }
        for ((_, grant), (before, after)) in roots.iter().zip(&selections) {
            before.require_current(io).await?;
            after.require_current(io).await?;
            grant.recheck(Some(io))?;
        }
        Ok(paths
            .into_iter()
            .map(|paths| DiffPublication {
                previous: base.clone(),
                generation: current.clone(),
                paths,
            })
            .collect())
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
            .diff_endpoints(volume, grant, (base, current), "", &io)
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

    /// Both endpoints selected under the caller's directory authority and
    /// that generation's policy, even when the directory is absent.
    async fn diff_endpoints<'io>(
        &self,
        volume: &VolumeRef,
        grant: &ContentGrant,
        (base, current): (&GenerationRef, &GenerationRef),
        directory: &str,
        io: &'io PreparationIo,
    ) -> Result<(
        super::directory::DirectorySelection<'io, A, O>,
        super::directory::DirectorySelection<'io, A, O>,
    )> {
        let mut ready = || std::future::ready(Ok(()));
        let before = self
            .select_directory(
                &root_listing(volume, grant, base, directory),
                io,
                &mut ready,
            )
            .await?;
        let after = self
            .select_directory(
                &root_listing(volume, grant, current, directory),
                io,
                &mut ready,
            )
            .await?;
        Ok((before, after))
    }
}

/// One authorized directory at an exact generation; no entries are scanned.
fn root_listing<'a>(
    volume: &'a VolumeRef,
    grant: &'a ContentGrant,
    generation: &'a GenerationRef,
    directory: &'a str,
) -> DirectoryListing<'a> {
    DirectoryListing {
        volume,
        grant,
        ceiling: None,
        path: directory,
        generation: Some(generation),
        after: None,
        maximum_entries: 1,
    }
}
