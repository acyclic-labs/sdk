use super::*;
impl Reducer {
    pub(in crate::core) fn require_fresh_fork_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        let mut owned = crate::fork::CodecQuota::observed(work)?;
        let mut child_project = None;
        for resource in &seed.resources {
            owned.checkpoint()?;
            if let crate::fork::ResourceRevision::Project { volume, .. } = &resource.revision {
                child_project = Some(volume);
                break;
            }
        }
        owned.checkpoint()?;
        if seed.child == self.authority {
            return Err(Error::Conflict("invalid fork publication".into()));
        }
        owned.checkpoint()?;
        if self.projection.forks.contains_key(&seed.child) {
            return Err(Error::Conflict("invalid fork publication".into()));
        }
        for prior in self.projection.forks.values() {
            owned.checkpoint()?;
            if prior.child_agent == seed.child_agent {
                return Err(Error::Conflict("invalid fork publication".into()));
            }
            owned.checkpoint()?;
            if prior.child_private_volume == seed.child_private_volume {
                return Err(Error::Conflict("invalid fork publication".into()));
            }
            for resource in &prior.resources {
                owned.checkpoint()?;
                if matches!((&resource.revision, child_project),
                    (crate::fork::ResourceRevision::Project { volume, .. }, Some(child_project)) if volume == child_project)
                {
                    return Err(Error::Conflict("invalid fork publication".into()));
                }
            }
        }
        Ok(())
    }

    #[cfg(feature = "filesystem")]
    pub(crate) fn attest_fork_history_with_work(
        &self,
        seed: &mut ForkSeed,
        history: &ConversationState,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        if seed.parent != self.authority
            || seed.parent_revision != self.revision
            || history.agent != self.projection.conversation.agent
            || history.resident_after_sequence() != 0
            || history.logical_revision() != seed.inherited_through_sequence
        {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "fork archive differs from its original parent cut",
            ));
        }
        self.validate_fork_reference_ownership(seed, Some(history), work)?;
        seed.history_witness = Some(self.authority_verifier.fork_history_proof(seed, work)?);
        Ok(())
    }

    #[allow(
        clippy::too_many_lines,
        reason = "fork publication checks one complete seed"
    )]
    pub(in crate::core) fn validate_fork_reference_ownership(
        &self,
        seed: &ForkSeed,
        history: Option<&ConversationState>,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if history.is_none()
            && let Some(proof) = seed.history_witness
        {
            if proof != self.authority_verifier.fork_history_proof(seed, work)? {
                return Err(crate::fork::boundary_error(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork archive witness differs from its exact seed",
                ));
            }
            return Ok(());
        }
        let history = history.unwrap_or(&self.projection.conversation);
        let mut quota = crate::fork::CodecQuota::observed(work)?;
        let count = usize::try_from(seed.inherited_through_sequence)
            .map_err(|_| quota.invalid("inherited conversation prefix is too large"))?;
        let parent = history
            .agent
            .ok_or_else(|| Error::Conflict("fork parent conversation is unbound".into()))?;
        if seed.child_agent == parent {
            return Err(quota.invalid("fork child must have a distinct agent identity"));
        }
        if seed.inherited_through_sequence > history.logical_revision() {
            return Err(quota.invalid("fork inherited prefix exceeds parent conversation"));
        }
        let prefix = seed
            .inherited_context
            .iter()
            .find(|file| file.path() == ".system/inherited-conversation/prefix.json");
        if count > 0 && prefix.is_none() {
            return Err(quota.invalid("fork omitted its inherited conversation prefix"));
        }
        if let Some(file) = prefix {
            let authority = copying::copy_authority(&self.authority, &mut quota)?;
            let prefix = InheritedConversationPrefix::select(
                authority,
                self.revision,
                seed.inherited_through_sequence,
                &seed.attached_agents,
                history,
                quota.work(),
            )?;
            quota.adopt(
                prefix.attached_agents.capacity() as u64 * std::mem::size_of::<AgentId>() as u64,
            )?;
            let bytes = quota.canonical(&prefix)?;
            if file.descriptor().media_type()
                != "application/vnd.acyclic.harness.inherited-conversation+json"
                || file.display_name() != "inherited-conversation.json"
            {
                return Err(
                    quota.invalid("inherited conversation metadata differs from its parent")
                );
            }
            file.descriptor().verify(&bytes, quota.work())?;
            let paid = ownership::owned_sum([
                bytes.capacity() as u64,
                prefix.parent.id.capacity() as u64,
                ownership::owned_buffer::<AgentId>(prefix.attached_agents.capacity())?,
            ])?;
            drop((bytes, prefix));
            quota.release(paid)?;
        }
        if history.resident_after_sequence() > 0 && count > 0 {
            return Err(quota.invalid("fork grant capture requires authenticated archived history"));
        }
        let manifests =
            || {
                history.messages().iter().take(count).filter_map(|message| {
                    match &message.attachments {
                        crate::conversation::ReferencedAttachments::Manifest {
                            manifest, ..
                        } => Some(manifest),
                        _ => None,
                    }
                })
            };
        if seed
            .attachment_manifests
            .iter()
            .any(|file| !manifests().any(|published| published == file))
            || manifests().any(|file| !seed.attachment_manifests.contains(file))
        {
            return Err(
                quota.invalid("fork attachment manifests do not match the parent transcript")
            );
        }
        let published = || {
            history
                .messages()
                .iter()
                .take(count)
                .flat_map(|message| {
                    let items = match &message.attachments {
                        crate::conversation::ReferencedAttachments::Inline { items } => {
                            items.as_slice()
                        }
                        _ => &[],
                    };
                    let manifest = match &message.attachments {
                        crate::conversation::ReferencedAttachments::Manifest {
                            manifest, ..
                        } => Some(manifest),
                        _ => None,
                    };
                    std::iter::once(&message.content)
                        .chain(items.iter().map(|item| &item.file))
                        .chain(manifest)
                        .chain(message.extensions.values())
                })
                .chain(
                    seed.summary
                        .iter()
                        .flat_map(|summary| summary.references.iter()),
                )
        };
        for reader in std::iter::once(seed.child_agent).chain(seed.attached_agents.iter().copied())
        {
            for file in published() {
                quota.checkpoint()?;
                if file.volume().owner() != &VolumeOwner::Agent(reader)
                    && !seed
                        .reference_grants
                        .iter()
                        .any(|grant| grant.reader == reader && grant.file == *file)
                {
                    return Err(quota.invalid("fork omits a published reference grant"));
                }
            }
        }
        for grant in &seed.reference_grants {
            quota.checkpoint()?;
            let file = &grant.file;
            if file.volume() == &seed.child_private_volume {
                if grant.attachment_manifest.is_some() || !seed.inherited_context.contains(file) {
                    return Err(quota.invalid("fork reference is not inherited context"));
                }
            } else if file.volume().owner() == &VolumeOwner::Agent(parent)
                || seed.resources.iter().any(|capture| match &capture.source {
                    crate::fork::ResourceRevision::Project { volume, .. }
                    | crate::fork::ResourceRevision::SharedVolume(volume) => {
                        volume == file.volume()
                    }
                    _ => false,
                })
            {
                match &grant.attachment_manifest {
                    None if !published().any(|published| published == file) => {
                        return Err(quota.invalid("fork reference was not published by parent"));
                    }
                    Some(manifest) if !manifests().any(|published| published == manifest) => {
                        return Err(quota.invalid(
                            "fork reference manifest is not a published attachment list",
                        ));
                    }
                    _ => {}
                }
            } else {
                return Err(Error::Unauthorized(
                    "fork cannot delegate another owner's file".into(),
                ));
            }
        }
        Ok(())
    }

    pub(in crate::core) fn validate_extension_fork_with_work(
        &self,
        seed: &ForkSeed,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        for capture in &seed.resources {
            if let crate::fork::ResourceRevision::Extension {
                name,
                version,
                implementation_digest,
                ..
            } = &capture.revision
            {
                let binding = self.schemas.pinned_binding(name, *version, None, work)?;
                if binding.implementation_digest != *implementation_digest {
                    return Err(Error::Conflict(
                        "fork extension implementation mismatch".into(),
                    ));
                }
                match binding.fork_policy {
                    ExtensionForkPolicy::Inherit if capture.source == capture.revision => {}
                    ExtensionForkPolicy::Reset if capture.source != capture.revision => {}
                    ExtensionForkPolicy::Reject => {
                        return Err(Error::Unsupported(format!(
                            "extension {name}@{version} cannot be forked"
                        )));
                    }
                    _ => {
                        return Err(Error::Conflict(format!(
                            "extension {name}@{version} capture violates its fork policy"
                        )));
                    }
                }
            }
        }
        Ok(())
    }
}
