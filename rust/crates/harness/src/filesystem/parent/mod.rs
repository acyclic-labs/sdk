use super::*;

#[cfg(not(target_arch = "wasm32"))]
pub(in crate::filesystem) type MergeAllocation = acyclic_fs::NativeAllocation;
#[cfg(target_arch = "wasm32")]
pub(in crate::filesystem) type MergeAllocation = ();

/// Parent-owned project operations.
pub struct ParentProjectController<'a, A, O> {
    pub(in crate::filesystem) host: &'a FilesystemHost<A, O>,
    pub(in crate::filesystem) project: VolumeRef,
    pub(in crate::filesystem) verifier: AuthorityVerifier,
    pub(in crate::filesystem) scope: Scope,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Inspected project changes whose publication remains bound to the parent controller.
pub struct ParentMergePlan<A, O> {
    pub(in crate::filesystem) plan: JoinPlan<A, O>,
    pub(in crate::filesystem) child_project: VolumeRef,
    pub(in crate::filesystem) parent_project: VolumeRef,
    pub(in crate::filesystem) parent_scope: Scope,
    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) native_custody: Option<acyclic_fs::NativeAllocation>,
}

/// Destination-authorized volume import.
pub struct VolumeImportPlan<A, O> {
    pub(in crate::filesystem) plan: JoinPlan<A, O>,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> VolumeImportPlan<A, O> {
    /// Publishes only to the authorized destination using a stable retry key.
    pub async fn apply(&self, key: &IdempotencyKey) -> Result<JoinOutcome<A, O>> {
        self.plan
            .apply(
                ApplyOptions {
                    if_target: self.plan.target_head(),
                    idempotency_key: filesystem_key(key),
                }, std::collections::BTreeMap::new(),
                acyclic_fs::PublicationPermit::Unrestricted,
            )
            .await
            .map_err(map_error)
    }
}

/// Prepared child-owned prefix and the exact read grants needed by every attached reader.
pub struct InheritedContextCapture {
    /// Child-owned inherited context file.
    pub file: FileRef,
    /// Pinned child-private generation containing the file.
    pub generation: GenerationRef,
    /// Exact attached-reader grants for referenced files.
    pub reference_grants: Vec<ReferenceGrant>,
    /// Pinned attachment-list manifests admitted with the context.
    pub attachment_manifests: Vec<FileRef>,
}

impl<A, O> ParentMergePlan<A, O> {
    /// Exact parent generation observed while planning; publication uses this CAS target.
    #[must_use]
    pub const fn target_head(&self) -> GenerationId {
        self.plan.target_head()
    }

    /// Exact child generation whose changes were inspected.
    #[must_use]
    pub const fn source_head(&self) -> GenerationId {
        self.plan.source_head()
    }
}

impl<'a, A, O> ParentProjectController<'a, A, O> {
    /// Binds the authenticated parent and project using optional original preparation IO.
    pub fn new(
        host: &'a FilesystemHost<A, O>,
        parent: &Reducer,
        verifier: &AuthorityVerifier,
        scope: &Scope,
        project: &VolumeRef,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        if parent.authority().kind != crate::core::AggregateKind::Conversation {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "project controller requires a parent conversation",
            ));
        }
        let mut observer = io.map(PreparationIo::history_io);
        let mut work = observer
            .as_mut()
            .map(|observer| observer as &mut dyn crate::store::history::HistoryIo);
        verifier.verify_audience(parent.authority(), &mut work)?;
        verifier.verify(scope, &mut work)?;
        if parent.conversation().and_then(|state| state.agent) != scope.agent()
            || scope.agent().is_none()
        {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "project controller requires the bound parent agent",
            ));
        }
        project.validate(&mut work)?;
        if project.provider() != &host.provider {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "parent project belongs to another provider",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let verifier = verifier.copy(&mut work)?;
        let scope = scope.copy(&mut work)?;
        let project = project.copy(&mut work)?;
        #[cfg(not(target_arch = "wasm32"))]
        let native_custody =
            io.filter(|io| io.original_native_work().is_some())
                .map(|io| {
                    io.take_native_custody(io.owned_bytes()?.checked_sub(before).ok_or_else(
                        || io.invalid("parent project controller custody underflows"),
                    )?)
                })
                .transpose()?;
        Ok(Self {
            host,
            project,
            verifier,
            scope,
            #[cfg(not(target_arch = "wasm32"))]
            native_custody,
        })
    }

    pub(in crate::filesystem) fn require(
        &self,
        capability: &str,
        operation: VolumeOperation,
    ) -> Result<()> {
        ContentGrant::verify(
            &self.verifier,
            &self.scope,
            &self.project,
            operation,
            u64::MAX,
            &mut None,
        )?;
        if !self.scope.capabilities().contains(capability) {
            return Err(Error::Unauthorized(
                "parent project operation is not granted".into(),
            ));
        }
        Ok(())
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub(in crate::filesystem) fn require_with_native_work(
        &self,
        capability: &str,
        operation: VolumeOperation,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<PreparationIo> {
        if !self
            .native_custody
            .as_ref()
            .is_some_and(|guard| guard.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project controller belongs to another original resource",
            });
        }
        self.verifier.verify_audience(
            self.verifier.audience(),
            &mut Some(
                &mut crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?,
            ),
        )?;
        self.verifier.verify(
            &self.scope,
            &mut Some(&mut crate::filesystem::PreparationIo::from_native_work(work).map(crate::store::HistoryWork::new)?),
        )?;
        let io = PreparationIo::from_native_work(work)?;
        let prefix = match operation {
            VolumeOperation::Read => "volume:read:",
            VolumeOperation::Write => "volume:write:",
        };
        if !self.scope.capabilities().contains(capability)
            || !io.contains_capability(self.scope.capabilities(), &self.project, prefix)?
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
                reason: "parent project operation is not currently granted",
            });
        }
        Ok(io)
    }
}

mod inherited;
mod project;

use crate::executor::{stock_boundary};
