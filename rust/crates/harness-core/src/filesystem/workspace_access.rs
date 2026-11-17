//! Authenticated raw workspace access shared by every filesystem host.
use super::PreparationIo;
use crate::{
    Error, Result,
    conversation::{VolumeOperation, VolumeRef},
    core::{AuthorityVerifier, Scope},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, Workspace};
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

/// The explicit whole-volume labels that, held together on a volume, admit
/// its raw workspace surface without a structured `filesystem:` label.
pub(crate) const RAW_WORKSPACE_LABELS: [VolumeOperation; 2] =
    [VolumeOperation::Read, VolumeOperation::Write];

/// What [`RAW_WORKSPACE_LABELS`] grant together. A raw surface exposes the
/// whole volume to one admission check, so it is ownership. Whatever derives
/// explicit labels from a narrower grant must withhold one of them.
pub(crate) const RAW_WORKSPACE_ACTIONS: acyclic_fs::access::ActionSet =
    acyclic_fs::access::ActionSet::Owner;

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
            require_volume_operations(grants, volume, &RAW_WORKSPACE_LABELS, &io, surface)?;
        }
    }
    let label = crate::core::filesystem_scope_label(signed.capabilities(), &mut None)?;
    let payload = match label {
        Some(_) => access::capability_payload(label, VolumeOperation::Read, &io)?,
        // Both grant sets hold every raw workspace label, checked above.
        None => access::whole_volume_payload(RAW_WORKSPACE_ACTIONS)?,
    };
    let reference = volume
        .map(|volume| volume.storage_name(&mut Some(&mut io.history_io())))
        .transpose()?;
    let compiled = access::bound_access_view(
        workspace,
        reference.as_deref(),
        signed,
        None,
        payload,
        Some(ceiling),
        &io,
        access::access_now()?,
    )
    .await?;
    if compiled.0.generation_sensitive() || compiled.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    access::require_unrestricted_at(
        &compiled.0,
        &acyclic_fs::access::Action::ALL,
        surface,
        access::access_now()?,
    )
}

fn require_volume_operations(
    grants: &crate::Capabilities,
    volume: Option<&VolumeRef>,
    operations: &[VolumeOperation],
    io: &PreparationIo,
    surface: &'static str,
) -> Result<()> {
    use crate::store::history::HistoryIo as _;
    let volume = volume.ok_or(Error::FilesystemAccessConfiguration(surface))?;
    let mut history = io.history_io();
    let digest = volume.identity_digest(
        crate::store::default_projection_read_limits().maximum_bytes,
        &mut Some(&mut history),
    )?;
    for operation in operations {
        let prefix = operation.capability_prefix();
        history.codec_charge(
            crate::executor::json::JsonCodecCharge::Inspect { items: 1 },
            true,
        )?;
        history.copy_bytes((prefix.len() + 64) as u64)?;
        if !crate::conversation::contains_content_label(grants, prefix, digest) {
            return Err(Error::FilesystemAccessConfiguration(surface));
        }
    }
    Ok(())
}

/// Verify complete read access to the selected workspace subtree at `generation`.
/// The original signed grant and persisted policy must admit Read and List
/// throughout `root`. This grants no access outside that canonical source root.
/// The decision is fenced to `generation`, the cut the caller reads: a head that
/// moves after capture can neither widen nor narrow what that cut discloses.
pub async fn require_workspace_subtree_read<
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
>(
    workspace: &acyclic_fs::Workspace<A, O>,
    volume: &VolumeRef,
    root: &str,
    generation: acyclic_fs::GenerationId,
    verifier: &AuthorityVerifier,
    scope: &Scope,
    io: &PreparationIo,
    surface: &'static str,
) -> Result<()> {
    use crate::filesystem::access;
    if !root.is_empty() {
        crate::conversation::validate_content_path(root, &mut Some(&mut io.history_io()))?;
    }
    verifier.verify(scope, &mut Some(&mut io.history_io()))?;
    let label =
        crate::core::filesystem_scope_label(scope.capabilities(), &mut Some(&mut io.history_io()))?;
    if label.is_none()
        && !crate::core::directory_read_granted_with_work(
            scope.capabilities(),
            volume,
            root,
            &mut Some(&mut io.history_io()),
        )?
    {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    let reference = volume.storage_name(&mut Some(&mut io.history_io()))?;
    let view = access::bound_access_view(
        workspace,
        Some(&reference),
        scope,
        Some(generation),
        access::capability_payload(label, VolumeOperation::Read, io)?,
        None,
        io,
        access::access_now()?,
    )
    .await?;
    if view.0.valid_until().is_some() {
        return Err(Error::FilesystemAccessConfiguration(surface));
    }
    if view.0.allows_subtree(
        root,
        &[
            acyclic_fs::access::Action::Read,
            acyclic_fs::access::Action::List,
        ],
        access::access_now()?,
    ) {
        Ok(())
    } else {
        Err(Error::FilesystemAccessConfiguration(surface))
    }
}
