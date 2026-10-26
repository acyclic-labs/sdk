use super::*;

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Resolves an interaction with an authenticated responder grant.
    pub async fn resolve_interaction(
        &self,
        id: InteractionId,
        response: InteractionResponse,
        responder: &Scope,
    ) -> Result<()> {
        let span = obs_span!(INFO, "acyclic.harness.journal.resolve_interaction");
        traced(span, async {
            let host = self.interactions()?;
            let approval = matches!(response, InteractionResponse::Approval { .. });
            let resolution = host.resolve_latest(id, response, responder);
            if approval {
                // Only the complete approval publication can certify a
                // verdict. Its explicit receipts still flush every staged
                // write before any caller receives the decision.
                #[cfg(feature = "filesystem-local")]
                let resolution =
                    acyclic_stream::deferring_provider_durability(&*self.stream, resolution);
                resolution.await?;
                self.published_decision(id, None).await?;
            } else {
                resolution.await?;
            }
            Ok(())
        })
        .await
    }

    pub(super) async fn open_interaction_with_io(
        &self,
        id: InteractionId,
        interaction: &Interaction,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(approvals) = &self.agent_approvals {
            return approvals.open(id, interaction).await;
        }
        let reject = |code, reason| {
            crate::filesystem::interaction_host::interaction_boundary(io, code, reason)
        };
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        interaction.validate(&mut work)?;
        let host = self.interactions.as_ref().ok_or_else(|| {
            reject(
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        let mut transaction = host.transaction(io).await?;
        {
            let (request_hold, original) = {
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let original = host
                    .read_request_at(&transaction.aggregate.0, id, io)
                    .await?;
                let request_hold = io
                    .map(|io| io.guard_owned(io.owned_since(before)?))
                    .transpose()?;
                (request_hold, original)
            };
            if let Some(original) = &original {
                return if original.matches_reopened(interaction) {
                    Ok(())
                } else {
                    Err(reject(
                        crate::wire::ErrorCode::Conflict,
                        "interaction identity reused",
                    ))
                };
            }
            drop(original);
            drop(request_hold);
        }
        let operation = interaction_open_operation(id, io)?;
        let (ticket_hold, ticket) = {
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let ticket = host.stage_request(id, interaction, None, io).await?;
            let ticket_hold = io
                .map(|io| io.guard_owned(io.owned_since(before)?))
                .transpose()?;
            (ticket_hold, ticket)
        };
        let publication = host.open_at(operation, None, &ticket, io, &mut transaction);
        let publication = publication.await;
        drop(ticket);
        drop(ticket_hold);
        match publication {
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.code(),
                    crate::wire::ErrorCode::Conflict | crate::wire::ErrorCode::Indeterminate
                ) =>
            {
                // A cross-owner conflict or lost acknowledgement needs one fresh
                // read to confirm the exact committed request, never a resubmit.
                transaction.aggregate = host.aggregate(io).await?;
                let (request_hold, original) = {
                    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                    let original = host
                        .read_request_at(&transaction.aggregate.0, id, io)
                        .await?;
                    let request_hold = io
                        .map(|io| io.guard_owned(io.owned_since(before)?))
                        .transpose()?;
                    (request_hold, original)
                };
                let result = match &original {
                    Some(original) if original.matches_reopened(interaction) => Ok(()),
                    Some(_) => Err(reject(
                        crate::wire::ErrorCode::Conflict,
                        "interaction identity reused",
                    )),
                    None => Err(Error::Indeterminate(operation)),
                };
                drop(original);
                drop(request_hold);
                result
            }
            Err(error) => Err(error),
        }
    }

    pub(super) async fn interaction_outcome_with_io(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        if let Some(approvals) = &self.agent_approvals {
            return approvals.outcome(id).await;
        }
        let host = self.interactions.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        let aggregate = host.aggregate(io).await?;
        let outcome = match host.read_outcome(&aggregate.0, id, io)? {
            // A verdict reaches execution only as a published decision.
            Some(InteractionOutcome::Approved {} | InteractionOutcome::Declined {}) => Some(
                self.published_decision_from(id, None, aggregate, io)
                    .await?
                    .outcome(),
            ),
            outcome => {
                drop(aggregate);
                outcome
            }
        };
        Ok(outcome)
    }
}

pub(super) fn interaction_open_operation(
    id: InteractionId,
    io: Option<&PreparationIo>,
) -> Result<OperationId> {
    let mut source = [0_u8; 53];
    source[..12].copy_from_slice(b"interaction:");
    let mut encoded = [0_u8; 36];
    source[12..48].copy_from_slice(
        uuid::Uuid::from_bytes(id.into_bytes())
            .hyphenated()
            .encode_lower(&mut encoded)
            .as_bytes(),
    );
    source[48..].copy_from_slice(b":open");
    let hashed = acyclic_fs::WorkCounters {
        bytes_hashed: source.len() as u64,
        ..Default::default()
    };
    if let Some(io) = io {
        io.preflight(hashed)?;
    }
    let digest = blake3::hash(&source);
    if let Some(io) = io {
        io.charge(hashed)?;
    }
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest.as_bytes()[..16]);
    Ok(OperationId::from_bytes(bytes))
}
