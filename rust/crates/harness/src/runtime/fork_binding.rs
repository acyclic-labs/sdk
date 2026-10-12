use super::*;

impl AgentHarness {
/// Binds parent-controlled project operations independently of content.

pub(super) fn bind_workspaces(
        self: &Arc<Self>,
        workspaces: Arc<dyn crate::merge::ProjectWorkspaceProvider>,
    ) -> Result<Arc<Self>> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH)
            && !self.scope.grants().contains(capability::PROJECT_MERGE)
        {
            return Err(Error::Unauthorized(
                "project workspaces require fork:publish or project:merge".into(),
            ));
        }
        workspaces.provider().validate(&mut None)?;
        workspaces.project().validate(&mut None)?;
        if workspaces.project().class() != crate::conversation::VolumeClass::Project
            || workspaces.project().provider() != workspaces.provider()
        {
            return Err(Error::Invalid("workspace binding is not a project".into()));
        }
        let mut bound = self.as_ref().clone();
        bound.workspaces = Some(workspaces);
        Ok(Arc::new(bound))
    }

/// Returns the authorized workspace boundary.

pub fn workspaces(&self) -> Result<Arc<dyn crate::merge::ProjectWorkspaceProvider>> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH)
            && !self.scope.grants().contains(capability::PROJECT_MERGE)
        {
            return Err(Error::Unauthorized(
                "runtime scope lacks project authority".into(),
            ));
        }
        self.workspaces
            .as_ref()
            .cloned()
            .ok_or_else(|| Error::Unsupported("project workspace provider is not bound".into()))
    }

/// Selects a newly captured parent revision for future work.

pub fn at_parent_snapshot(
        self: &Arc<Self>,
        preparer: Arc<dyn crate::fork::ForkPreparer>,
    ) -> Result<Arc<Self>> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized(
                "runtime scope lacks fork:publish".into(),
            ));
        }
        if let Some(current) = &self.fork_preparer {
            let (parent, revision) = preparer.parent_snapshot();
            if parent != &current.parent {
                return Err(Error::Unauthorized(
                    "fork snapshot belongs to another parent conversation".into(),
                ));
            }
            if revision <= current.revision {
                return Err(Error::Conflict(
                    "fork snapshot must advance the parent revision".into(),
                ));
            }
        } else {
            return Err(Error::Unauthorized(
                "parent fork control is not bound".into(),
            ));
        }
        self.bind_fork_preparer(preparer)
    }

/// Seals the parent-authorized fork provider during binding construction.

pub(super) fn bind_fork_preparer(
        self: &Arc<Self>,
        preparer: Arc<dyn crate::fork::ForkPreparer>,
    ) -> Result<Arc<Self>> {
        let (parent, revision) = preparer.parent_snapshot();
        if parent.kind != crate::core::AggregateKind::Conversation || revision == 0 {
            return Err(Error::Invalid(
                "fork preparer needs a bound parent conversation revision".into(),
            ));
        }
        parent.stream_path(&mut None)?;
        let mut bound = self.as_ref().clone();
        bound.fork_preparer = Some(ForkBinding {
            parent: parent.clone(),
            revision,
            preparer,
        });
        Ok(Arc::new(bound))
    }

pub(super) fn fork_binding(&self, request: &crate::fork::ForkRequest) -> Result<&ForkBinding> {
        if !self.scope.grants().contains(capability::FORK_PUBLISH) {
            return Err(Error::Unauthorized(
                "runtime scope lacks fork:publish".into(),
            ));
        }
        request.validate(&mut None)?;
        let binding = self
            .fork_preparer
            .as_ref()
            .ok_or_else(|| Error::Unsupported("fork preparer is not bound".into()))?;
        if request.parent != binding.parent
            || request.parent_revision != binding.revision
            || binding.preparer.parent_snapshot() != (&binding.parent, binding.revision)
        {
            return Err(Error::Conflict(
                "fork request is outside the bound parent revision".into(),
            ));
        }
        Ok(binding)
    }

/// Captures the exact requested resources without publishing a child.

pub async fn prepare_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<crate::fork::ForkReport> {
        let binding = self.fork_binding(&request)?;
        let report = binding.preparer.prepare(request.clone()).await?;
        report.validate(&mut None)?;
        if report.request != request {
            return Err(Error::Conflict("fork preparer changed the request".into()));
        }
        Ok(report)
    }

/// Observes an uncertain preparation without redispatching captures.

pub async fn reconcile_fork(
        &self,
        request: crate::fork::ForkRequest,
    ) -> Result<Option<crate::fork::ForkReport>> {
        let binding = self.fork_binding(&request)?;
        let report = binding.preparer.reconcile(request.clone()).await?;
        if let Some(report) = &report {
            report.validate(&mut None)?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork changed the request".into(),
                ));
            }
        }
        Ok(report)
    }

/// Binds a model only for scoped task/tool model requests.

pub fn bind_model(
        self: &Arc<Self>,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        model.validate()?;
        let mut bound = self.as_ref().clone();
        bound.model = Some(ModelBinding { model, provider });
        Ok(Arc::new(bound))
    }

/// Replaces the ordered context stages for future local task work.

pub fn bind_context(self: &Arc<Self>, context: ContextPipeline) -> Arc<Self> {
        let mut bound = self.as_ref().clone();
        bound.context = context;
        Arc::new(bound)
    }

/// Selects a different model for future local child work without changing the parent's pinned task, tool, policy, or authority bindings.

pub fn scoped_model(
        self: &Arc<Self>,
        grants: Capabilities,
        limits: Limits,
        model: Model,
        provider: Arc<dyn ModelProvider>,
    ) -> Result<Arc<Self>> {
        self.scoped(grants, limits)?.bind_model(model, provider)
    }
}
