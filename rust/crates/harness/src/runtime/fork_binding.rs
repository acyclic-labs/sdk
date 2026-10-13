use super::*;

impl AgentHarness {
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
        let mut bound = self.as_ref().clone();
        bound.fork_preparer = Some(ForkBinding::new(preparer, None)?);
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
}
