//! Original immutable filesystem pins and their consuming commit conditions.
use acyclic_fs::{
    AsyncObjectStore, FilesystemCommit, Fs, Generation, StreamAuthorityStore, WorkspaceError,
};

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// One complete configuration cut. Every position is an original retained
/// generation, including an empty configuration volume. A digest or decoded
/// manifest alone cannot construct a generation or supply its read authority.
///
/// The caller owns the pins; this view borrows them without allocating, copying
/// their closures or starting followers. Reads and evaluation use these exact
/// generations until the consuming commit conditions all of their heads.
pub struct Manifest<'a, A: 'static, O: 'static> {
    generations: [&'a Generation<A, O>; 6],
    digest: [u8; 32],
}

impl<'a, A, O> Manifest<'a, A, O> {
    /// Pins every configuration position in [`super::ORDER`]. The host selects
    /// these original sources; a subject cannot replace an issuer's source by
    /// writing a similarly named file in its own home.
    #[must_use]
    pub fn new(generations: [&'a Generation<A, O>; 6]) -> Self {
        let workspaces = generations.map(|generation| generation.workspace_id().into_bytes());
        let identities = generations.map(|generation| generation.id().to_bytes());
        let digest = super::stack_digest(
            super::ORDER
                .into_iter()
                .zip(workspaces.iter().zip(&identities))
                .flat_map(|(name, (workspace, generation))| {
                    [(name, workspace.as_slice()), (name, generation.as_slice())]
                }),
        );
        Self {
            generations,
            digest,
        }
    }

    /// Cached identity of the complete ordered cut. Comparing an unchanged
    /// manifest reads one fixed digest and does not reopen or parse any file.
    #[must_use]
    pub const fn digest(&self) -> &[u8; 32] {
        &self.digest
    }

    /// Exact original sources, lowest first. Every settings fold, policy meet
    /// and import resolution must use this same cut.
    pub fn sources(&self) -> impl DoubleEndedIterator<Item = (&'static str, &'a Generation<A, O>)> {
        super::ORDER.into_iter().zip(self.generations)
    }

    /// Evaluates a staged home against the original issuer sources. This does
    /// not publish it or change any of the other five layer positions.
    ///
    /// # Errors
    /// A candidate from another workspace cannot replace the subject's home.
    pub fn with_home<'m>(
        &'m self,
        home: &'m Generation<A, O>,
    ) -> Result<Manifest<'m, A, O>, WorkspaceError> {
        if home.workspace_id() != self.generations[5].workspace_id() {
            return Err(WorkspaceError::ForeignGeneration);
        }
        let mut generations: [&'m Generation<A, O>; 6] = self.generations;
        generations[5] = home;
        Ok(Manifest::new(generations))
    }

    /// Resolves evaluator imports against this exact cut, using the caller's
    /// original operation and byte bound. Native evaluation and typed result
    /// decoding use that operation's allowance. Cached backing retains its
    /// producer's credit. No reader, follower or work root is created.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn tree<'m, 'io>(
        &'m self,
        io: &'io crate::filesystem::PreparationIo,
        maximum_bytes: u64,
    ) -> ManifestTree<'m, 'a, 'io, A, O> {
        ManifestTree {
            manifest: self,
            io,
            maximum_bytes,
        }
    }
}

/// The evaluator's import adapter over one retained manifest. The same view is
/// shared by settings, policies and enabled plugin modules. `sys/` comes only
/// from the seed; other paths replace from the highest layer, including `off`.
#[cfg(not(target_arch = "wasm32"))]
pub struct ManifestTree<'m, 'g, 'io, A: 'static, O: 'static> {
    manifest: &'m Manifest<'g, A, O>,
    io: &'io crate::filesystem::PreparationIo,
    maximum_bytes: u64,
}

#[cfg(not(target_arch = "wasm32"))]
impl<A: acyclic_fs::AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    crate::config::Tree for ManifestTree<'_, '_, '_, A, O>
{
    fn preparation_io(&self) -> Option<&crate::filesystem::PreparationIo> {
        Some(self.io)
    }

    fn read<'a>(
        &'a self,
        path: &'a str,
    ) -> futures::future::BoxFuture<'a, crate::Result<Option<acyclic_stream::OwnedBytes>>> {
        Box::pin(async move {
            use crate::store::history::HistoryIo as _;
            // The evaluator uses relative module names; the filesystem uses
            // absolute portable paths. Fund and copy that one boundary spelling
            // once, then borrow it for every layer lookup.
            let absolute = {
                let mut text = String::new();
                let bytes = path
                    .len()
                    .checked_add(1)
                    .ok_or_else(|| self.io.invalid("module path size overflows"))?;
                let mut work = self.io.history_io();
                work.admit_copy(bytes as u64)?;
                work.reserve_text(&mut text, bytes)?;
                text.push('/');
                text.push_str(path);
                work.copy_bytes(bytes as u64)?;
                let hold = self.io.guard_owned(text.capacity() as u64)?;
                (text, hold)
            };
            for (name, generation) in self.manifest.sources().rev() {
                if !super::layered(path) && name != super::SEED {
                    continue;
                }
                let (result, recorded) = generation
                    .read(
                        &absolute.0,
                        self.maximum_bytes,
                        self.io,
                        self.io.cancellation(),
                    )
                    .await;
                let bytes = match result {
                    // Only absence of this path permits a lower layer. A
                    // missing authority source can share the wire NotFound
                    // code, but must retain its original terminal failure.
                    Err(source) if source.is_absent() => {
                        self.io.sdk::<(), WorkspaceError>((Ok(()), recorded))?;
                        continue;
                    }
                    result => self.io.sdk((result, recorded))?,
                };
                if super::is_off(&bytes) {
                    return Ok(None);
                }
                return self.io.retain_read_bytes(bytes).map(Some);
            }
            Ok(None)
        })
    }
}

impl<
    P: acyclic_fs::distributed::AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
> Manifest<'_, StreamAuthorityStore<P>, O>
{
    /// Retains each original generation's publication condition together with
    /// the caller's original root cuts. This never resolves a replacement head.
    /// The caller enters this one complete group only around publication.
    pub async fn policy_group(
        &self,
        additional: &[acyclic_fs::PolicyCut],
        io: &crate::filesystem::PreparationIo,
    ) -> crate::Result<acyclic_fs::PolicyGroup> {
        let first = io.sdk(
            self.generations[0]
                .capture_policy_cut(io, io.cancellation())
                .await,
        )?;
        let mut cuts = [first; 6];
        for (cut, source) in cuts.iter_mut().zip(self.generations).skip(1) {
            *cut = io.sdk(source.capture_policy_cut(io, io.cancellation()).await)?;
        }
        io.sdk(acyclic_fs::PolicyGroup::new(
            &[&cuts, additional],
            io,
            io.cancellation(),
        ))
    }
    /// Checks an admitted candidate's actual changed paths against the
    /// original permissions, admits that exact candidate against the retained
    /// sources, then contributes its publication to the caller's commit.
    /// Decoded candidate policy never supplies its own issuer authority.
    /// `admit` receives the candidate view of the same six sources and the
    /// original preparation IO. Its refusal contributes no publication or
    /// source conditions to the caller's commit.
    ///
    /// No head or caller record is published during preparation. All six
    /// original source heads fence the same commit as the home publication.
    /// A denial leaves the commit untouched. A later preparation failure may
    /// retain source conditions, but never a partial home publication. Unknown
    /// submission outcomes retain the original sealed commit for exact retry.
    ///
    /// # Errors
    /// Foreign candidates, denied changes and original filesystem failures
    /// retain their typed cause. Work uses only the supplied preparation IO.
    pub async fn prepare_apply<'m, F, Admission>(
        &'m self,
        filesystem: &Fs<StreamAuthorityStore<P>, O>,
        apply: HomeApply<'m, StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        io: &'m crate::filesystem::PreparationIo,
        admit: F,
    ) -> crate::Result<()>
    where
        F: FnOnce(
            Manifest<'m, StreamAuthorityStore<P>, O>,
            &'m crate::filesystem::PreparationIo,
        ) -> Admission,
        Admission: std::future::Future<Output = crate::Result<()>>,
    {
        let expected = self.generations[5];
        let candidate = self.with_home(apply.candidate)?;
        let mut spent = acyclic_fs::WorkCounters::UNCHARGED;
        let mut changes = io.cursor_receipt(
            expected
                .changed_paths_to(apply.candidate, io, io.cancellation())
                .await,
            &mut spent,
        )?;
        while let Some(change) = io.cursor_receipt(changes.next().await, &mut spent)? {
            if !apply.permissions.allows_change(&change, apply.now) {
                return Err(crate::executor::stock_boundary(
                    Some(io),
                    crate::wire::ErrorCode::Unauthorized,
                    "configuration change is denied by original permissions",
                ));
            }
        }
        drop(changes);
        admit(candidate, io).await?;
        io.sdk(
            self.condition(filesystem, commit, io, io.cancellation())
                .await,
        )?;
        io.sdk(
            filesystem
                .prepare_publish_many(
                    &[acyclic_fs::PublishSpec {
                        expected,
                        generation: apply.candidate.id(),
                        operation: apply.operation,
                        permit: apply.permit,
                    }],
                    &[],
                    commit,
                    io,
                    io.cancellation(),
                )
                .await,
        )
    }

    /// Adds every source-head condition to the SAME caller commit, spending
    /// one original work source. Nothing is published here. The caller submits
    /// its commit once through the original filesystem submission path;
    /// its Rejected/Unknown outcomes and exact retry ownership stay unchanged.
    /// A source changed between reading and submission rejects the whole
    /// consuming commit, even when all changed data paths are unrelated.
    ///
    /// # Errors
    /// The exact source failure; the caller keeps its original commit and all
    /// preparation, publication and allocation custody. No retry or polling.
    pub async fn condition<W: acyclic_fs::facade::FsWork>(
        &self,
        filesystem: &Fs<StreamAuthorityStore<P>, O>,
        commit: &mut FilesystemCommit,
        work: W,
        cancellation: &acyclic_fs::CancellationToken,
    ) -> W::Output<(), WorkspaceError> {
        filesystem
            .require_heads(&self.generations, commit, work, cancellation)
            .await
    }
}

/// A staged subject-home apply under the host's original policy and filesystem
/// publication authority. The caller retains the candidate through submission.
pub struct HomeApply<'a, A: 'static, O: 'static> {
    /// Immutable, unpublished candidate admitted by the supplied settings fold.
    pub candidate: &'a Generation<A, O>,
    /// Original issuer/advisory meet and signed caller ceiling.
    pub permissions: &'a crate::config::policy::Permissions,
    /// Host clock observation used for permission expiry.
    pub now: u64,
    /// Stable filesystem publication identity, unchanged on exact retry.
    pub operation: acyclic_fs::OperationId,
    /// Original filesystem admission; the shared publication core checks it.
    pub permit: acyclic_fs::PublicationPermit,
}
