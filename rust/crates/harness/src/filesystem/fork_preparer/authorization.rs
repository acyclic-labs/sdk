use super::*;
impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemForkPreparer<A, O> {
    pub(super) fn authorize_request(&self, request: &ForkRequest) -> Result<()> {
        request.validate(&mut None)?;
        if request.parent != *self.parent.authority()
            || request.preparation.child_project_volume.provider() != &self.host.provider
        {
            return Err(Error::Conflict(
                "fork request does not match this parent/provider boundary".into(),
            ));
        }
        if request.boundary.is_some() {
            return Err(Error::Unsupported(
                "this preparer cannot attest a cross-provider boundary".into(),
            ));
        }
        if self.parent.conversation().and_then(|state| state.agent) == Some(request.child_agent) {
            return Err(Error::Invalid(
                "fork child must have a distinct agent identity".into(),
            ));
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference)
                    if reference.as_resource().provider() != &self.stream_provider =>
                {
                    return Err(Error::Invalid(
                        "fork history belongs to another Stream provider".into(),
                    ));
                }
                ResourceRevision::Project { volume, .. } if volume != &self.project => {
                    return Err(Error::Invalid(
                        "fork project is not the bound parent project".into(),
                    ));
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    if volume.owner()
                        != &crate::conversation::VolumeOwner::Agent(self.scope.agent().ok_or_else(
                            || Error::Unauthorized("scratch fork requires a parent agent".into()),
                        )?)
                    {
                        return Err(Error::Unauthorized(
                            "scratch fork is not from the direct parent owner".into(),
                        ));
                    }
                    ContentGrant::verify(
                        &self.verifier,
                        &self.scope,
                        volume,
                        VolumeOperation::Read,
                    )?;
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(super) fn check_request(&self, request: &ForkRequest) -> Result<()> {
        self.authorize_request(request)?;
        if let Some(selection) = &request.preparation.summary {
            let summary = self.summary.as_ref().ok_or_else(|| {
                Error::Unsupported(
                    "summary fork owner-bound checkpoint provider is not installed".into(),
                )
            })?;
            if summary.prepared.selection() != selection
                || summary.prepared.through_sequence()
                    != request.preparation.inherited_through_sequence
            {
                return Err(Error::Conflict(
                    "summary fork request differs from its original capture".into(),
                ));
            }
        }
        if request.parent_revision != self.parent.revision() {
            return Err(Error::Conflict(
                "fork preparation is not at the bound parent revision".into(),
            ));
        }
        Ok(())
    }
}
