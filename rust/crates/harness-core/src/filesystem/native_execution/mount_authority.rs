//! Scoped native admission port. Only the runtime retains signing custody.
use super::*;

/// A verified workload and a weak runtime admission port, never a root signer.
#[derive(Clone)]
pub struct NativeMountAuthority {
    verifier: AuthorityVerifier,
    scope: Scope,
}
impl NativeMountAuthority {
    /// Called by the runtime composition while it still owns the bootstrap issuer.
    pub fn new(issuer: &crate::core::AuthorityIssuer, agent: crate::AgentId) -> Result<Self> {
        Ok(Self {
            verifier: issuer.verifier(&mut None)?,
            scope: issuer.issue(
                "native-checkout",
                &crate::Capabilities::default(),
                Some(agent),
                None,
                &mut None,
            )?,
        })
    }

    /// Retains an already authenticated task workload without acquiring root custody.
    pub fn for_scope(&self, scope: &Scope) -> Result<Self> {
        self.verifier.verify(scope, &mut None)?;
        Ok(Self {
            verifier: self.verifier.clone(),
            scope: scope.copy(&mut None)?,
        })
    }

    /// Created resources are delegated only from the original live task admission.
    pub async fn created_scope(&self, context: &crate::runtime::ToolContext) -> Result<Scope> {
        let owner = context
            .task()
            .owned_mailbox
            .as_ref()
            .ok_or(Error::FilesystemAccessConfiguration("child mount owner"))?;
        owner.validate_scope(context.effective_scope(), None)?;
        owner.verify_owner(None)?.await?;
        let (task, fence) = owner.task_binding();
        if !context.task().original_task_binding_matches(task, fence) {
            return Err(Error::Unauthorized("child mount owner differs".into()));
        }
        let access = context.task().scope().created_access.as_ref().ok_or(
            Error::FilesystemAccessConfiguration("child mount allocation"),
        )?;
        if access.operation.into_bytes() != task.into_bytes() {
            return Err(Error::Unauthorized("child mount allocation differs".into()));
        }
        let mut private = access.content.iter().filter(|binding| {
            binding.volume.class() == crate::conversation::VolumeClass::AgentPrivate
        });
        let binding = private
            .next()
            .ok_or(Error::FilesystemAccessConfiguration("child private mount"))?;
        if private.next().is_some() {
            return Err(Error::Unauthorized(
                "child private mount is ambiguous".into(),
            ));
        }
        let crate::conversation::VolumeOwner::Agent(agent) = binding.volume.owner() else {
            return Err(Error::Unauthorized(
                "child private mount has no agent".into(),
            ));
        };
        let scope = self.verifier.admit_native_descendant(
            &self.scope,
            "child-checkout",
            context.effective_scope().grants(),
            Some(*agent),
            &mut None,
        )?;
        owner.verify_owner(None)?.await?;
        Ok(scope)
    }

    /// The runtime issues only the exact selected native volume's filesystem grant.
    pub async fn mount_options<A, O>(
        &self,
        workspace: &acyclic_fs::Workspace<A, O>,
        writable: bool,
        work: &NativeMaterializationWork,
    ) -> Result<acyclic_fs::MountOptions>
    where
        A: AsyncAuthorityStore + Send + Sync + 'static,
        O: AsyncObjectStore + Send + Sync + 'static,
    {
        let io = PreparationIo::from_native_work(work)?;
        let exact = super::access::mount_capabilities(workspace, writable)?;
        let grants = intersect_mount_ceiling(self.scope.capabilities(), exact, &io)?;
        let scope = self.verifier.admit_native_descendant(
            &self.scope,
            "native-volume",
            &grants,
            self.scope.agent(),
            &mut Some(&mut io.history_io()),
        )?;
        let generation = workspace.head(work, work.cancellation()).await?.id();
        authenticated_mount_options(
            workspace,
            generation,
            NativeAccessAuthority {
                verifier: &self.verifier,
                scope: &scope,
                ceiling: &grants,
            },
            writable,
            work,
        )
        .await
        .map(|options| options.native_context(work, None, None))
    }
}

fn intersect_mount_ceiling(
    parent: &crate::Capabilities,
    exact: crate::Capabilities,
    io: &PreparationIo,
) -> Result<crate::Capabilities> {
    use acyclic_fs::access::Capability;
    let Some(label) = crate::core::filesystem_scope_label(parent, &mut Some(&mut io.history_io()))?
    else {
        return Ok(exact);
    };
    let parent = Capability::from_scope_label(label)
        .map_err(|_| io.invalid("invalid native filesystem ceiling"))?
        .ok_or_else(|| io.invalid("native parent has no filesystem ceiling"))?;
    let exact = exact
        .iter()
        .next()
        .ok_or_else(|| io.invalid("native volume has no grant"))?;
    let exact = Capability::from_scope_label(exact)
        .map_err(|_| io.invalid("invalid native filesystem ceiling"))?
        .ok_or_else(|| io.invalid("native volume has no filesystem ceiling"))?;
    let layer = exact
        .layers
        .into_iter()
        .next()
        .ok_or_else(|| io.invalid("native volume has no ceiling layer"))?;
    let grant = parent.attenuate(layer, exact.expires_at);
    Ok(crate::Capabilities::new([grant.to_scope_label().map_err(
        |_| io.invalid("invalid native filesystem ceiling"),
    )?]))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn mount_port_cannot_outlive_runtime_or_revoked_parent() -> Result<()> {
        let cancellation = acyclic_fs::CancellationToken::new();
        let work = NativeMaterializationWork::new(acyclic_fs::WorkBudget::UNBOUNDED, &cancellation)
            .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
        let filesystem = acyclic_fs::Fs::memory();
        let workspace = filesystem
            .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
                "mount-port",
                acyclic_fs::VolumeInitialization {
                    config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                    access_policy: Some(acyclic_fs::access::INITIAL_POLICY),
                },
                acyclic_fs::OperationId::new(),
                &work,
                &cancellation,
                None,
            )
            .await?
            .workspace();
        let audience = crate::core::Authority {
            kind: crate::core::AggregateKind::Task,
            id: "mount-port".into(),
        };
        let issuer = crate::core::AuthorityIssuer::new("mount-port", [63; 32], audience.clone());
        let port = NativeMountAuthority::new(&issuer, crate::AgentId::from_bytes([4; 16]))?;
        port.mount_options(&workspace, true, &work).await?;
        let limited = crate::Capabilities::new([acyclic_fs::access::Capability {
            layers: vec![vec![acyclic_fs::access::Grant {
                actions: acyclic_fs::access::Action::ALL.into_iter().collect(),
                resource: acyclic_fs::access::ResourcePattern {
                    lineage_root: None,
                    volume: None,
                    generations: acyclic_fs::access::Generations::All,
                    path: "public/**".into(),
                },
            }]],
            expires_at: None,
        }
        .to_scope_label()
        .map_err(|error| Error::Invalid(error.to_string()))?]);
        let narrow = issuer.issue(
            "narrow-mount",
            &limited,
            port.scope.agent(),
            None,
            &mut None,
        )?;
        let options = port
            .for_scope(&narrow)?
            .mount_options(&workspace, true, &work)
            .await?;
        let view = options
            .require_access_view()
            .map_err(|error| Error::Invalid(error.to_string()))?
            .current()
            .map_err(|error| Error::Invalid(error.to_string()))?;
        assert!(
            view.decide(acyclic_fs::access::Action::Read, "public/file", 0)
                .allowed
        );
        assert!(
            !view
                .decide(acyclic_fs::access::Action::Read, "private/key", 0)
                .allowed
        );
        issuer.revoke_workload(&port.scope)?;
        assert!(port.mount_options(&workspace, true, &work).await.is_err());
        let foreign = crate::core::AuthorityIssuer::new("mount-port", [64; 32], audience.clone());
        let forged = foreign.issue(
            "forged",
            &crate::Capabilities::default(),
            None,
            None,
            &mut None,
        )?;
        assert!(port.for_scope(&forged).is_err());
        let live = NativeMountAuthority::new(&foreign, crate::AgentId::from_bytes([4; 16]))?;
        live.mount_options(&workspace, true, &work).await?;
        drop(foreign);
        assert!(live.mount_options(&workspace, true, &work).await.is_err());
        Ok(())
    }
}
