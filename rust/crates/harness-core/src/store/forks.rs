use super::*;

pub(super) fn fork_child_binding_with_work(
    seed: &ForkSeed,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<(OperationId, EventReference)> {
    if let Some(observer) = work.as_deref_mut() {
        observer.check_cancellation()?;
    }
    let revision = seed
        .parent_revision
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("fork parent revision overflow".into()))?;
    if let Some(observer) = work.as_deref_mut() {
        observer.hashed_bytes(b"harness/v2/fork-child-bind\0".len() as u64 + 16)?;
    }
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/fork-child-bind\0");
    hash.update(&seed.operation_id.into_bytes());
    if let Some(observer) = work.as_deref_mut() {
        observer.copy_bytes(16)?;
    }
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((
        OperationId::from_bytes(identity),
        EventReference {
            authority: match work.as_deref_mut() {
                Some(observer) => observer.clone_authority(&seed.parent)?,
                None => seed.parent.clone(),
            },
            revision,
        },
    ))
}

pub(super) fn fork_command_key_with_work(
    prefix: &str,
    operation: OperationId,
    work: &mut Option<&mut dyn history::HistoryIo>,
) -> Result<(IdempotencyKey, u64)> {
    use std::fmt::Write as _;
    let capacity = prefix
        .len()
        .checked_add(36)
        .ok_or_else(|| Error::Invalid("fork command identity length overflows".into()))?;
    let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
    let mut text = String::new();
    if let Some(observer) = quota.work().as_deref_mut() {
        observer.reserve_text(&mut text, capacity)?;
        quota.adopt(text.capacity() as u64)?;
        quota.observer()?.copy_bytes(capacity as u64)?;
    } else {
        text.try_reserve_exact(capacity)
            .map_err(|_| Error::Invalid("fork command identity allocation failed".into()))?;
    }
    write!(&mut text, "{prefix}{operation}")
        .map_err(|_| Error::Invalid("fork command identity formatting failed".into()))?;
    let key = IdempotencyKey::new(text)?;
    let held = quota.retained();
    quota.transfer();
    Ok((key, held))
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Prepares the selected resources at the captured parent revision.
    pub async fn prepare_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<ForkReport> {
        request.validate(&mut None)?;
        if request.parent != *self.reducer.authority()
            || request.parent_revision != self.reducer.revision()
        {
            return Err(Error::Conflict(
                "fork preparation is not at the parent revision".into(),
            ));
        }
        let report = provider.prepare(request.clone()).await?;
        report.validate(&mut None)?;
        if report.request != request {
            return Err(Error::Conflict(
                "fork provider returned a different request".into(),
            ));
        }
        Ok(report)
    }

    /// Reconciles the original preparation identity after an unknown reply.
    pub async fn reconcile_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<Option<ForkReport>> {
        request.validate(&mut None)?;
        if request.parent != *self.reducer.authority() {
            return Err(Error::Unauthorized(
                "fork request belongs to another parent".into(),
            ));
        }
        let report = provider.reconcile(request.clone()).await?;
        if let Some(report) = &report {
            report.validate(&mut None)?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork belongs to a different request".into(),
                ));
            }
        }
        Ok(report)
    }

    /// Publishes a captured fork through the parent append.
    pub async fn publish_fork_report(
        &mut self,
        report: ForkReport,
        scope: Scope,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<ApplyResult> {
        if let Some(observer) = work.as_deref_mut() {
            observer.check_cancellation()?;
        }
        if report.request.parent != *self.reducer.authority() {
            return Err(guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "fork report belongs to another parent",
            ));
        }
        #[cfg(feature = "filesystem")]
        if let Some(owner) = owner {
            owner.authenticate_fork_parent(self.reducer.authority(), work)?;
            if scope.agent() != Some(owner.authenticated_fork_agent(work)?) {
                return Err(guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork publication scope differs from its admitted caller",
                ));
            }
        }
        let mut custody = crate::wire_codec::CodecQuota::new(work, 0)?;
        let seed = report.into_seed(custody.work())?;
        self.publish_fork_seed(
            seed,
            scope,
            #[cfg(feature = "filesystem")]
            owner,
            &mut custody,
        )
        .await
    }

    // Both public report routes validate and consume the report exactly once.
    // Borrow its quota through publication so every early return drops the
    // owned seed before the caller releases its allocation claims.
    async fn publish_fork_seed(
        &mut self,
        seed: ForkSeed,
        scope: Scope,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        custody: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<ApplyResult> {
        let vector_slots = seed.resources.capacity() as u64
            * std::mem::size_of::<crate::fork::CapturedResource>() as u64
            + seed.omissions.capacity() as u64
                * std::mem::size_of::<crate::fork::ForkOmission>() as u64;
        custody.adopt(vector_slots)?;
        let (key, key_slots) =
            fork_command_key_with_work("fork-publication:", seed.operation_id, custody.work())?;
        custody.adopt(key_slots)?;
        let box_slots = std::mem::size_of::<ForkSeed>() as u64;
        if let Some(observer) = custody.work().as_deref_mut() {
            observer.admit_allocation(box_slots)?;
            observer.retain_owned(box_slots)?;
        }
        custody.adopt(box_slots)?;
        let seed = Box::new(seed);
        let command = Command {
            operation_id: seed.operation_id,
            idempotency_key: key,
            expected_revision: seed.parent_revision,
            scope,
            causal_parent: None,
            action: Action::PublishFork { seed },
        };
        #[cfg(feature = "filesystem")]
        if owner.is_some() {
            return self
                .publish(
                    command,
                    owner,
                    crate::distributed::JournalWrite::Fresh,
                    false,
                    custody.work(),
                )
                .await
                .map(|(result, _)| result);
        }
        self.execute(
            command,
            #[cfg(feature = "filesystem")]
            None,
            custody.work(),
        )
        .await
    }

    /// Admits a prepared child through the parent, then binds its fresh conversation to the published seed.
    pub async fn spawn_from_report(
        &mut self,
        parent: &mut Self,
        report: ForkReport,
        parent_scope: Scope,
        child_scope: Scope,
    ) -> Result<ForkSeed> {
        if report.request.child != *self.reducer.authority()
            || report.request.parent != *parent.reducer.authority()
        {
            return Err(Error::Unauthorized(
                "fork report does not match parent and child aggregates".into(),
            ));
        }
        let seed = report.into_seed(&mut None)?;
        if child_scope.agent() != Some(seed.child_agent)
            || (self.reducer.revision() != 0 && parent.reducer.fork(&seed.child) != Some(&seed))
        {
            return Err(Error::Conflict(
                "child binding is not fresh or previously published by this parent".into(),
            ));
        }
        let mut publication_work = None;
        let mut custody = crate::wire_codec::CodecQuota::new(&mut publication_work, 0)?;
        parent
            .publish_fork_seed(
                seed.clone(),
                parent_scope,
                #[cfg(feature = "filesystem")]
                None,
                &mut custody,
            )
            .await?;
        self.bind_published_child(
            parent,
            &seed,
            child_scope,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await?;
        Ok(seed)
    }

    /// Binds a child to its exact published parent seed.
    pub async fn bind_published_child(
        &mut self,
        parent: &Self,
        seed: &ForkSeed,
        scope: Scope,
        #[cfg(feature = "filesystem")] owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        work: &mut Option<&mut dyn history::HistoryIo>,
    ) -> Result<()> {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        seed.validate(quota.work())?;
        #[cfg(feature = "filesystem")]
        let parent_agent = if let Some(owner) = owner {
            owner.authenticate_fork_parent(&seed.parent, quota.work())?;
            Some(owner.authenticated_fork_agent(quota.work())?)
        } else {
            None
        };
        #[cfg(not(feature = "filesystem"))]
        let parent_agent: Option<crate::AgentId> = None;
        if parent.reducer.authority() != &seed.parent
            || parent.reducer.fork(&seed.child) != Some(seed)
            || self.reducer.authority() != &seed.child
            || scope.agent() != Some(seed.child_agent)
        {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Unauthorized,
                "child binding has no published parent fork or matching scope",
            ));
        }
        parent
            .verify_parent_fork(seed, parent_agent, &mut quota)
            .await?;
        let (operation, causal) = fork_child_binding_with_work(seed, quota.work())?;
        quota.adopt(causal.authority.id.capacity() as u64)?;
        let conversation = self.reducer.conversation().ok_or_else(|| {
            guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Invalid,
                "fork child is not a conversation",
            )
        })?;
        if conversation.agent == Some(seed.child_agent) {
            let (binding, owned) = self.read_event_at(1, quota.work()).await?;
            quota.adopt(owned)?;
            if binding.revision != 1
                || binding.operation_id != operation
                || binding.causal_parent.as_ref() != Some(&causal)
                || !matches!(binding.payload, EventPayload::ConversationBound { agent } if agent == seed.child_agent)
            {
                return Err(guarded_boundary(
                    quota.work(),
                    crate::wire::ErrorCode::Conflict,
                    "child conversation was bound outside its published fork",
                ));
            }
            #[cfg(feature = "filesystem")]
            if let Some(owner) = owner {
                owner.verify(false, quota.work()).await?;
            }
            return Ok(());
        }
        if self.reducer.revision() != 0 || conversation.agent.is_some() {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Conflict,
                "fork child already has a different history",
            ));
        }
        let (key, owned) =
            fork_command_key_with_work("fork-child-bind:", seed.operation_id, quota.work())?;
        quota.adopt(owned)?;
        let command = Command {
            operation_id: operation,
            idempotency_key: key,
            expected_revision: 0,
            scope,
            causal_parent: Some(causal),
            action: Action::BindConversation {
                agent: seed.child_agent,
            },
        };
        #[cfg(feature = "filesystem")]
        let (result, owned) = self
            .publish(
                command,
                owner,
                crate::distributed::JournalWrite::Fresh,
                false,
                quota.work(),
            )
            .await?;
        #[cfg(not(feature = "filesystem"))]
        let (result, owned) = (self.execute(command, quota.work()).await?, 0);
        quota.adopt(owned)?;
        drop(result);
        Ok(())
    }

    pub(super) async fn release_replayed_fork_fence(
        &self,
        seed: &crate::fork::ForkSeed,
    ) -> Result<()> {
        let verifier = self
            .fork_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?;
        verifier
            .release_private_fence(seed, &mut None)
            .await
            .map_err(|_| Error::Indeterminate(seed.operation_id))
    }

    async fn verify_parent_fork(
        &self,
        seed: &ForkSeed,
        parent_agent: Option<crate::AgentId>,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<()> {
        let revision = crate::contract::next_revision(seed.parent_revision)?;
        let (publication, owned) = self.read_event_at(revision, quota.work()).await?;
        quota.adopt(owned)?;
        if publication.revision != revision
            || publication.operation_id != seed.operation_id
            || parent_agent.is_some_and(|agent| publication.scope.agent() != Some(agent))
            || !matches!(&publication.payload, EventPayload::ForkPublished { seed: published } if published.as_ref() == seed)
        {
            return Err(guarded_boundary(
                quota.work(),
                crate::wire::ErrorCode::Conflict,
                "parent fork event does not match the child seed",
            ));
        }
        Ok(())
    }
}
