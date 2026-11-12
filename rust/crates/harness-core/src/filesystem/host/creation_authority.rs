use super::*;

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// A retained private volume must be writable at its actual policy cut
    /// before a new root can adopt it. Reading that policy grants no content IO.
    pub(in crate::filesystem) async fn authorize_retained_writer(
        &self,
        scope: &Scope,
        volume: &VolumeRef,
    ) -> Result<()> {
        let Some(label) = crate::core::filesystem_scope_label(scope.capabilities(), &mut None)?
        else {
            return Ok(());
        };
        let io = PreparationIo::for_unmeasured(&acyclic_fs::CancellationToken::new())?;
        let reference = bounded_io::content_workspace(&self.provider, volume, Some(&io))?;
        let workspace = self.open(&reference, Some(&io)).await?;
        let payload = access::capability_payload(Some(label), VolumeOperation::Write, &io)?;
        let view = access::access_view(&workspace, scope, None, payload, None, &io).await?;
        if !view.0.allows_every_path(
            &[
                acyclic_fs::access::Action::Create,
                acyclic_fs::access::Action::Write,
            ],
            access::access_now()?,
        ) {
            return Err(Error::Unauthorized(
                "admission does not grant the retained private writer".into(),
            ));
        }
        Ok(())
    }

    /// Checks a verified caller's allocation ceiling before creating storage.
    /// Zero is the initial generation, never a substitute for an existing head.
    /// Subsequent content operations bind the persisted policy at their real cut.
    pub(in crate::filesystem) fn authorize_volume_creation(
        &self,
        scope: &Scope,
        volume: &VolumeRef,
    ) -> Result<()> {
        use acyclic_fs::access::{
            Capability, Context, PathTrie, Policy, Principal, PrincipalKind, Resource,
        };
        let Some(label) = crate::core::filesystem_scope_label(scope.capabilities(), &mut None)?
        else {
            return Ok(());
        };
        if volume.provider() != &self.provider {
            return Err(Error::Unauthorized(
                "volume belongs to another filesystem".into(),
            ));
        }
        let denied =
            || Error::Unauthorized("admission does not grant fresh volume creation".into());
        let capability = Capability::from_scope_label(label)
            .map_err(|_| denied())?
            .ok_or_else(denied)?;
        let identity = self
            .filesystem
            .workspace_id(volume.storage_name(&mut None)?)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let identity = uuid::Uuid::from_bytes(identity.volume_id().into_bytes()).to_string();
        let policy: Policy = serde_json::from_slice(acyclic_fs::access::INITIAL_POLICY)
            .map_err(|_| Error::FilesystemAccessConfiguration("initial policy is invalid"))?;
        let principal = Principal {
            kind: PrincipalKind::Agent,
            id: scope
                .agent()
                .map_or_else(|| scope.issuer().to_string(), |agent| agent.to_string()),
            lineage: Vec::new(),
            groups: Default::default(),
            roles: Default::default(),
            tags: Default::default(),
            attributes: Default::default(),
        };
        let now = access::access_now()?;
        let view = PathTrie::compile(
            &policy,
            &principal,
            Resource {
                config: self.volume_initialization().config,
                lineage: &[],
                volume: &identity,
                generation: 0,
                head: 0,
                path: "",
            },
            Context { now, approvals: 0 },
            &capability,
        )
        .map_err(|_| denied())?;
        let actions: &[_] = if volume.is_agent_journal() {
            &[
                acyclic_fs::access::Action::Read,
                acyclic_fs::access::Action::Create,
                acyclic_fs::access::Action::Write,
            ]
        } else {
            &[
                acyclic_fs::access::Action::Create,
                acyclic_fs::access::Action::Write,
            ]
        };
        if !view.allows_every_path(actions, now) {
            return Err(denied());
        }
        Ok(())
    }
}
