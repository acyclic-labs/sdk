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
    let mut text = String::new();
    if let Some(observer) = work.as_deref_mut() {
        observer.reserve_text(&mut text, capacity)?;
        if let Err(error) = observer.copy_bytes(capacity as u64) {
            let held = text.capacity() as u64;
            drop(text);
            observer.release_owned(held)?;
            return Err(error);
        }
    } else {
        text.try_reserve_exact(capacity)
            .map_err(|_| Error::Invalid("fork command identity allocation failed".into()))?;
    }
    let held = if work.is_some() {
        text.capacity() as u64
    } else {
        0
    };
    let result = write!(&mut text, "{prefix}{operation}")
        .map_err(|_| Error::Invalid("fork command identity formatting failed".into()))
        .and_then(|()| IdempotencyKey::new(text));
    if result.is_err()
        && let Some(observer) = work.as_deref_mut()
    {
        observer.release_owned(held)?;
    }
    result.map(|key| (key, held))
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
        let mut custody = crate::wire_codec::CodecQuota::observed(work)?;
        let seed = report.into_seed(custody.work())?;
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
        }
        custody.retain(box_slots)?;
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
        report.validate(&mut None)?;
        if report.request.child != *self.reducer.authority()
            || report.request.parent != *parent.reducer.authority()
        {
            return Err(Error::Unauthorized(
                "fork report does not match parent and child aggregates".into(),
            ));
        }
        let seed = report.clone().into_seed(&mut None)?;
        if child_scope.agent() != Some(seed.child_agent)
            || (self.reducer.revision() != 0 && parent.reducer.fork(&seed.child) != Some(&seed))
        {
            return Err(Error::Conflict(
                "child binding is not fresh or previously published by this parent".into(),
            ));
        }
        parent
            .publish_fork_report(
                report,
                parent_scope,
                #[cfg(feature = "filesystem")]
                None,
                &mut None,
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
        let mut custody = crate::wire_codec::CodecQuota::observed(work)?;
        seed.validate(custody.work())?;
        #[cfg(feature = "filesystem")]
        let parent_agent = if let Some(owner) = owner {
            owner.authenticate_fork_parent(&seed.parent, custody.work())?;
            Some(owner.authenticated_fork_agent(custody.work())?)
        } else {
            None
        };
        #[cfg(not(feature = "filesystem"))]
        let parent_agent: Option<crate::AgentId> = None;
        if parent.reducer.authority() != &seed.parent
            || parent.reducer.fork(&seed.child) != Some(seed)
            || self.reducer.authority() != &seed.child
        {
            return Err(guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Unauthorized,
                "child binding has no published parent fork",
            ));
        }
        if scope.agent() != Some(seed.child_agent) {
            return Err(guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Unauthorized,
                "child binding scope belongs to another agent",
            ));
        }
        let fork_revision = seed.parent_revision.checked_add(1).ok_or_else(|| {
            guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Invalid,
                "fork parent revision overflow",
            )
        })?;
        let (fork_event, fork_event_owned) =
            parent.read_event_at(fork_revision, custody.work()).await?;
        custody.adopt(fork_event_owned)?;
        if fork_event.revision != fork_revision
            || fork_event.operation_id != seed.operation_id
            || parent_agent.is_some_and(|agent| fork_event.scope.agent() != Some(agent))
            || !matches!(&fork_event.payload, crate::core::EventPayload::ForkPublished { seed: published } if published.as_ref() == seed)
        {
            return Err(guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Conflict,
                "parent fork event does not match the child seed",
            ));
        }
        let (bind_operation, causal_parent) = fork_child_binding_with_work(seed, custody.work())?;
        custody.adopt(causal_parent.authority.id.capacity() as u64)?;
        let conversation = self.reducer.conversation().ok_or_else(|| {
            guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Invalid,
                "fork child is not a conversation",
            )
        })?;
        if conversation.agent == Some(seed.child_agent) {
            let (event, event_owned) = self.read_event_at(1, custody.work()).await?;
            custody.adopt(event_owned)?;
            if event.revision == 1
                && event.operation_id == bind_operation
                && event.causal_parent.as_ref() == Some(&causal_parent)
                && matches!(event.payload, crate::core::EventPayload::ConversationBound { agent } if agent == seed.child_agent)
            {
                #[cfg(feature = "filesystem")]
                if let Some(owner) = owner {
                    owner.verify(false, custody.work()).await?;
                }
                drop(event);
                custody.release(event_owned)?;
                drop(fork_event);
                custody.release(fork_event_owned)?;
                return Ok(());
            }
            return Err(guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Conflict,
                "child conversation was bound outside its published fork",
            ));
        }
        if self.reducer.revision() != 0 || conversation.agent.is_some() {
            return Err(guarded_boundary(
                custody.work(),
                crate::wire::ErrorCode::Conflict,
                "fork child already has a different history",
            ));
        }
        let (key, key_slots) =
            fork_command_key_with_work("fork-child-bind:", seed.operation_id, custody.work())?;
        custody.adopt(key_slots)?;
        let command = Command {
            operation_id: bind_operation,
            idempotency_key: key,
            expected_revision: 0,
            scope,
            causal_parent: Some(causal_parent),
            action: Action::BindConversation {
                agent: seed.child_agent,
            },
        };
        #[cfg(feature = "filesystem")]
        let (result, returned_owned) = if owner.is_some() || custody.work().is_some() {
            self.publish(
                command,
                owner,
                crate::distributed::JournalWrite::Fresh,
                false,
                custody.work(),
            )
            .await?
        } else {
            (self.execute(command, None, custody.work()).await?, 0)
        };
        #[cfg(not(feature = "filesystem"))]
        let (result, returned_owned) = (self.execute(command, custody.work()).await?, 0);
        custody.adopt(returned_owned)?;
        drop(result);
        custody.release(returned_owned)?;
        drop(fork_event);
        custody.release(fork_event_owned)?;
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
}
