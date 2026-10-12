use super::*;

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits one open event under the optional original task lease.
    pub async fn open(
        &self,
        operation_id: OperationId,
        scope: Option<&Scope>,
        ticket: &InteractionTicket,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        loop {
            let ticket_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let copied = crate::executor::with_history_io(io, |work| {
                crate::wire_codec::CodecQuota::produce(work, |quota| {
                    crate::core::copy_ticket(ticket, quota)
                })
                .map(|(ticket, _)| ticket)
            })?;
            ticket_hold = io
                .map(|io| {
                    io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        interaction_boundary(
                            Some(io),
                            ErrorCode::Invalid,
                            "interaction ticket custody underflows",
                        )
                    })?)
                })
                .transpose()?;
            let result = self
                .execute(
                    operation_id,
                    scope.unwrap_or(&self.owner_scope),
                    Action::OpenInteraction { ticket: copied },
                    owner,
                    io,
                )
                .await;
            drop(ticket_hold);
            match result {
                Err(error) if error.code() == ErrorCode::Conflict => {
                    let request_hold;
                    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                    let request = self
                        .read_request(InteractionId::from_bytes(ticket.id.into_bytes()), io)
                        .await?;
                    request_hold = io
                        .map(|io| {
                            io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(
                                || {
                                    interaction_boundary(
                                        Some(io),
                                        ErrorCode::Invalid,
                                        "interaction request custody underflows",
                                    )
                                },
                            )?)
                        })
                        .transpose()?;
                    let opened = request.is_some();
                    drop(request);
                    drop(request_hold);
                    if opened {
                        return Err(error);
                    }
                }
                result => return result,
            }
        }
    }

    pub(super) async fn execute(
        &self,
        operation_id: OperationId,
        scope: &Scope,
        action: Action,
        owner: Option<&crate::durable_host::TaskJournalOwner<P>>,
        io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        let command_hold;
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some(owner) = owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(false, &mut work).await?;
        }
        let mut aggregate = self.aggregate(io).await?;
        let committed_hold;
        let (committed, _) = crate::store::operations::bounded_operation_with_retained_quota(
            &self.stream,
            self.verifier.audience(),
            &self.verifier,
            operation_id,
            2 * acyclic_stream::MAX_RECORD_BYTES as u64,
            true,
            &mut work,
        )
        .await?;
        committed_hold = io
            .map(|io| io.guard_owned(committed.as_ref().map_or(0, |(_, bytes)| *bytes)))
            .transpose()?;
        let committed = (committed, committed_hold);
        let expected_revision = match &committed.0 {
            Some((event, _)) => event.revision.checked_sub(1).ok_or_else(|| {
                interaction_boundary(
                    io,
                    ErrorCode::Invalid,
                    "committed interaction revision is zero",
                )
            })?,
            None => aggregate.0.reducer().revision(),
        };
        drop(committed);
        let after_aggregate = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let scope = scope.copy(&mut work)?;
        let idempotency_key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("interaction:{operation_id}"),
            io,
        )?)?;
        command_hold = io
            .map(|io| {
                io.guard_owned(
                    io.owned_bytes()?
                        .checked_sub(after_aggregate)
                        .ok_or_else(|| {
                            interaction_boundary(
                                Some(io),
                                ErrorCode::Invalid,
                                "interaction command custody underflows",
                            )
                        })?,
                )
            })
            .transpose()?;
        let command = Command {
            operation_id,
            idempotency_key,
            expected_revision,
            scope,
            causal_parent: None,
            action,
        };
        let result = aggregate.0.execute(command, owner, &mut work).await;
        drop(aggregate);
        drop(command_hold);
        result
    }

    pub(super) async fn aggregate<'a>(
        &self,
        io: Option<&'a PreparationIo>,
    ) -> Result<(StreamAggregate<P>, Option<DecodeReservation<'a>>)> {
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        let authority = match work.as_deref_mut() {
            Some(work) => work.clone_authority(self.verifier.audience())?,
            None => self.verifier.audience().clone(),
        };
        let verifier = self.verifier.copy(&mut work)?;
        let schemas = self.schemas.copy(&mut work)?;
        let content = FilesystemContentVerifier::new(
            Arc::clone(&self.host),
            &self.verifier,
            &self.owner_scope,
            self.maximum_bytes,
            io,
        )?;
        let aggregate = StreamAggregate::open(
            &self.stream,
            authority,
            verifier,
            schemas,
            None,
            crate::store::default_projection_read_limits(),
            crate::store::DEFAULT_TERMINAL_EFFECTS,
            None,
            &mut work,
        )
        .await?
        .with_content_verifier(content);
        let agent = aggregate
            .reducer()
            .conversation()
            .and_then(|conversation| conversation.agent)
            .ok_or_else(|| {
                interaction_boundary(
                    io,
                    ErrorCode::Conflict,
                    "interaction conversation is not bound to an agent",
                )
            })?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "interaction private volume belongs to another agent",
            ));
        }
        let guard = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    interaction_boundary(
                        Some(io),
                        ErrorCode::Invalid,
                        "interaction aggregate custody underflows",
                    )
                })?)
            })
            .transpose()?;
        Ok((aggregate, guard))
    }
}
