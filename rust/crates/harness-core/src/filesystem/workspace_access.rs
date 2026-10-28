//! Authenticated raw workspace access shared by every filesystem host.
use crate::{Error, Result, conversation::{VolumeOperation, VolumeRef}, core::{AuthorityVerifier, Scope}};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Workspace};
use super::PreparationIo;
/// An authenticated scope and invocation ceiling for a raw workspace surface.
/// Construction is mandatory: raw facades cannot omit the grant admission.
#[derive(Clone)]
pub struct WorkspaceAccess {
    verifier: AuthorityVerifier,
    scope: Scope,
    ceiling: crate::Capabilities,
    volume: Option<VolumeRef>,
}

impl WorkspaceAccess {
    /// Bind an authenticated workload to its original logical volume.
    pub fn new(
        verifier: &AuthorityVerifier,
        scope: &Scope,
        ceiling: &crate::Capabilities,
        volume: Option<&VolumeRef>,
    ) -> Result<Self> {
        verifier.verify(scope, &mut None)?;
        Ok(Self {
            verifier: verifier.clone(),
            scope: scope.copy(&mut None)?,
            ceiling: ceiling.clone(),
            volume: volume.cloned(),
        })
    }

    /// Recheck revocation, the current policy revision, and both grant ceilings.
    pub async fn require<A: AsyncAuthorityStore, O: AsyncObjectStore>(
        &self,
        workspace: &Workspace<A, O>,
    ) -> Result<()> {
        self.verifier.verify(&self.scope, &mut None)?;
        require_raw_workspace_access(
            workspace,
            self.volume.as_ref(),
            &self.scope,
            &self.ceiling,
            "raw workspace",
        )
        .await?;
        self.verifier.verify(&self.scope, &mut None)
    }
}

/// Check a verified scope and caller ceiling before exposing a raw workspace.
/// The caller must authenticate the scope before invoking this admission check.
pub async fn require_unrestricted_volume_access<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &acyclic_fs::Workspace<A, O>,
    volume: &VolumeRef,
    signed: &Scope,
    ceiling: &crate::Capabilities,
    surface: &'static str,
) -> Result<()> {
    require_raw_workspace_access(workspace, Some(volume), signed, ceiling, surface).await
}

async fn require_raw_workspace_access<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    workspace: &Workspace<A, O>,
    volume: Option<&VolumeRef>,
    signed: &Scope,
    ceiling: &crate::Capabilities,
    surface: &'static str,
) -> Result<()> {
    use crate::filesystem::{PreparationIo, access};
    let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
    for grants in [ceiling, signed.capabilities()] {
        if crate::core::filesystem_scope_label(grants, &mut None)?.is_none() {
            let volume = volume.ok_or(Error::FilesystemAccessConfiguration(surface))?;
            if !grants.contains(&volume.capability(VolumeOperation::Read, &mut None)?)
                || !grants.contains(&volume.capability(VolumeOperation::Write, &mut None)?)
            {
                return Err(Error::FilesystemAccessConfiguration(surface));
            }
        }
    }
    let label = crate::core::filesystem_scope_label(signed.capabilities(), &mut None)?;
    let payload = match label {
        Some(_) => access::capability_payload(label, VolumeOperation::Read, &io)?,
        None => access::whole_volume_payload(acyclic_fs::access::ActionSet::Owner)?,
    };
    let compiled =
        access::access_view(workspace, signed, None, payload, Some(ceiling), &io).await?;
    if compiled.0.generation_sensitive() || compiled.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted(&compiled.0, &acyclic_fs::access::Action::ALL, surface)
}

/// Verify a scope before exposing read-only raw checkout context.
pub async fn require_unrestricted_workspace_read<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &acyclic_fs::Workspace<A, O>,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    io: &PreparationIo,
    surface: &'static str,
) -> Result<()> {
    use crate::filesystem::access;
    verifier.verify(scope, &mut Some(&mut io.history_io()))?;
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    let view = access::access_view(
        workspace,
        scope,
        None,
        access::capability_payload(label, VolumeOperation::Read, io)?,
        None,
        io,
    )
    .await?;
    if view.0.generation_sensitive() || view.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted(
        &view.0,
        &[
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::List,
        ],
        surface,
    )
}

