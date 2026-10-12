use super::*;

impl<P: StreamProvider> HistoryReader<P> {
    /// Hydrates a bounded logical prefix for fork grant capture.
    pub async fn fork_history(
        &self,
        parent: &crate::core::Reducer,
        through_sequence: u64,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<crate::fork::PreparedForkHistory> {
        self.verifier.verify_audience(parent.authority(), work)?;
        let source = parent.conversation().ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork archive parent is unbound",
            )
        })?;
        if through_sequence > source.logical_revision() {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "fork archive exceeds the logical parent cut",
            ));
        }
        let cursor_authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(parent.authority())?,
            None => parent.authority().clone(),
        };
        let cursor = HistoryCursor {
            authority: cursor_authority,
            after_revision: 0,
            through_revision: parent.revision(),
        };
        let cursor_bytes = cursor.authority.id.capacity() as u64;
        let messages = self
            .conversation_range(&cursor, 0, through_sequence, limits, work)
            .await;
        drop(cursor);
        if let Some(work) = work.as_deref_mut() {
            let released = work.release_owned(cursor_bytes);
            if messages.is_ok() {
                released?;
            }
        }
        let (mut conversation, _) = crate::conversation::ConversationState::from_wire(
            crate::conversation::ConversationStateWire {
                agent: None,
                messages: messages?,
                archived_prefix: None,
            },
            acyclic_stream::MAX_RECORD_BYTES as u64,
            work,
        )?;
        conversation.agent = source.agent;
        if through_sequence >= source.resident_after_sequence()
            && conversation.history_prefix_digest(through_sequence, work)?
                != source.history_prefix_digest(through_sequence, work)?
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Storage,
                "fork archive differs from the authenticated parent prefix",
            ));
        }
        #[cfg(feature = "filesystem")]
        {
            let proof = self.verifier.fork_history_selection_proof_with_work(
                parent.revision(),
                through_sequence,
                conversation.agent,
                conversation.history_prefix_digest(through_sequence, work)?,
                work,
            )?;
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(parent.authority())?,
                None => parent.authority().clone(),
            };
            Ok(crate::fork::PreparedForkHistory::from_archive(
                authority,
                parent.revision(),
                conversation,
                proof,
            ))
        }
        #[cfg(not(feature = "filesystem"))]
        {
            let authority = match work.as_deref_mut() {
                Some(work) => work.clone_authority(parent.authority())?,
                None => parent.authority().clone(),
            };
            Ok(crate::fork::PreparedForkHistory::from_archive(
                authority,
                parent.revision(),
                conversation,
            ))
        }
    }

    /// Captures an admitted Summary fork through cold authenticated indexes.
    pub async fn summary_fork_stage(
        &self,
        parent: &Self,
        seed: &crate::fork::ForkSeed,
        scope: &crate::core::Scope,
        reader: std::sync::Arc<dyn crate::conversation::ContentResidencyVerifier>,
        limits: crate::conversation::Limits,
        history_limits: HistoryReadLimits,
    ) -> Result<crate::context::PinnedContextStage> {
        if history_limits.maximum_events < 2 || history_limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "summary fork history allowance is insufficient".into(),
            ));
        }
        crate::contract::json_byte_length(seed, history_limits.maximum_bytes, &mut None)
            .map(|_| ())?;
        self.verifier.verify(scope, &mut None)?;
        if parent.verifier.audience() != &seed.parent
            || self.verifier.audience() != &seed.child
            || scope.agent() != Some(seed.child_agent)
        {
            return Err(Error::Unauthorized(
                "summary source is not this admitted child fork".into(),
            ));
        }
        let capture = seed
            .summary
            .as_ref()
            .ok_or_else(|| Error::Invalid("fork has no Summary projection".into()))?;
        let limits =
            crate::context::restrict_context_limits(capture.selection.limits, limits, None)?;
        for file in capture.inherited_files().chain(&capture.references) {
            limits.validate_file(file, &mut None)?;
            crate::conversation::ContentGrant::verify_read(&self.verifier, scope, file)?;
        }
        self.verify_published_child_with_work(parent, seed, history_limits, &mut None)
            .await?;
        crate::context::PinnedContextStage::capture(
            "fork-summary",
            "1",
            capture.context.clone(),
            reader,
            crate::context::ContextPlacement::Prepend,
            limits,
        )
        .await
    }

    /// Authenticates the original publication and initial child binding only.
    pub async fn verify_published_child_with_work(
        &self,
        parent: &Self,
        seed: &crate::fork::ForkSeed,
        limits: HistoryReadLimits,
        work: &mut Option<&mut dyn HistoryIo>,
    ) -> Result<()> {
        if limits.maximum_events < 2 || limits.maximum_bytes == 0 {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "published child verification requires two bounded canonical events",
            ));
        }
        seed.validate(work)?;
        if parent.verifier.audience() != &seed.parent || self.verifier.audience() != &seed.child {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "published child differs from the original parent or child audience",
            ));
        }
        let child_cut = self
            .pin(
                0,
                work.as_mut().map(|work| &mut **work as &mut dyn HistoryIo),
            )
            .await?;
        if child_cut.through_revision == 0 {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Unauthorized,
                "published child is not bound",
            ));
        }
        let child_cursor_bytes = child_cut.authority.id.capacity() as u64;
        let cut = child_cut.through_revision;
        drop(child_cut);
        if let Some(work) = work.as_deref_mut() {
            work.release_owned(child_cursor_bytes)?;
        }
        let (operation, causal) = crate::store::fork_child_binding_with_work(seed, work)?;
        let (publication, consumed) =
            crate::store::operations::bounded_operation_with_retained_quota(
                &parent.client,
                parent.verifier.audience(),
                &parent.verifier,
                seed.operation_id,
                limits.maximum_bytes,
                false,
                work,
            )
            .await?;
        let (publication, publication_owned) = publication.ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "child has no original parent publication",
            )
        })?;
        let publication_matches = publication.revision == causal.revision
            && matches!(&publication.payload, crate::core::EventPayload::ForkPublished { seed: published } if published.as_ref() == seed);
        drop(publication);
        let released_publication = match work.as_deref_mut() {
            Some(work) => work.release_owned(publication_owned),
            None => Ok(()),
        };
        if !publication_matches {
            let _ = released_publication;
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "child differs from the original parent publication",
            ));
        }
        released_publication?;
        let remaining = limits
            .maximum_bytes
            .checked_sub(consumed)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| {
                crate::store::guarded_boundary(
                    work,
                    crate::wire::ErrorCode::Invalid,
                    "published child verification exceeds byte allowance",
                )
            })?;
        let (binding, _) = crate::store::operations::bounded_operation_with_retained_quota(
            &self.client,
            self.verifier.audience(),
            &self.verifier,
            operation,
            remaining,
            false,
            work,
        )
        .await?;
        let (binding, binding_owned) = binding.ok_or_else(|| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "child lacks its original fork binding",
            )
        })?;
        let binding_matches = binding.revision == 1
            && binding.revision <= cut
            && binding.scope.agent() == Some(seed.child_agent)
            && binding.causal_parent.as_ref() == Some(&causal)
            && matches!(&binding.payload, crate::core::EventPayload::ConversationBound { agent } if *agent == seed.child_agent);
        let released_owned = if work.is_some() {
            binding_owned
                .checked_add(causal.authority.id.capacity() as u64)
                .ok_or_else(|| {
                    crate::store::guarded_boundary(
                        work,
                        crate::wire::ErrorCode::Invalid,
                        "child verification custody overflows",
                    )
                })?
        } else {
            0
        };
        drop(binding);
        drop(causal);
        let released_binding = match work.as_deref_mut() {
            Some(work) => work.release_owned(released_owned),
            None => Ok(()),
        };
        if !binding_matches {
            let _ = released_binding;
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Conflict,
                "child lacks its original fork binding",
            ));
        }
        released_binding
    }

    /// Reads one explicit page of a child's published parent-history cut.
    pub async fn inherited_conversation_page(
        &self,
        seed: &crate::fork::ForkSeed,
        child: crate::AgentId,
        after_sequence: u64,
        limits: HistoryReadLimits,
    ) -> Result<Vec<crate::conversation::ConversationMessage>> {
        self.verifier.verify_audience(&seed.parent, &mut None)?;
        if child != seed.child_agent {
            return Err(Error::Unauthorized(
                "inherited history belongs to another child".into(),
            ));
        }
        if limits.maximum_bytes == 0
            || limits.maximum_events == 0
            || after_sequence > seed.inherited_through_sequence
            || (after_sequence < seed.inherited_through_sequence && limits.maximum_events < 2)
        {
            return Err(Error::Invalid(
                "inherited history page bounds are invalid".into(),
            ));
        }
        crate::contract::json_byte_length(seed, limits.maximum_bytes, &mut None).map(|_| ())?;
        seed.validate(&mut None)?;
        let publication_revision = crate::contract::next_revision(seed.parent_revision)?;
        let (publication, consumed) = self
            .operation_event(seed.operation_id, limits.maximum_bytes, &mut None)
            .await?;
        let publication = publication
            .ok_or_else(|| Error::Conflict("inherited history has no published fork".into()))?;
        if publication.revision != publication_revision
            || !matches!(&publication.payload,
                crate::core::EventPayload::ForkPublished { seed: published }
                if published.as_ref() == seed)
        {
            return Err(Error::Conflict(
                "inherited history differs from the published fork".into(),
            ));
        }
        if after_sequence == seed.inherited_through_sequence {
            return Ok(Vec::new());
        }
        let remaining_bytes = limits
            .maximum_bytes
            .checked_sub(consumed)
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| Error::Invalid("inherited history page exceeds byte bound".into()))?;
        let count = (seed.inherited_through_sequence - after_sequence)
            .min(u64::from(limits.maximum_events - 1));
        let through_sequence = after_sequence + count;
        self.conversation_range(
            &HistoryCursor {
                authority: seed.parent.clone(),
                after_revision: 0,
                through_revision: seed.parent_revision,
            },
            after_sequence,
            through_sequence,
            HistoryReadLimits {
                maximum_events: limits.maximum_events - 1,
                maximum_bytes: remaining_bytes,
            },
            &mut None,
        )
        .await
    }
}
