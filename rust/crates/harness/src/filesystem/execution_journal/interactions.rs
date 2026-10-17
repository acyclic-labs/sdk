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
        let host = self.interactions()?;
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        if let Some(owner) = owner {
            owner.require_interaction_grant(&mut None)?;
            owner.verify(true, &mut None).await?;
        }
        let approval = matches!(response, InteractionResponse::Approval { .. });
        host.resolve_latest(id, response, responder, owner).await?;
        if approval {
            self.published_decision(id, None).await?;
        }
        if let Some(owner) = owner {
            owner.verify(true, &mut None).await?;
        }
        Ok(())
    }

    pub(super) async fn open_interaction_with_io(
        &self,
        id: InteractionId,
        interaction: &Interaction,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let reject = |code, reason| {
            crate::filesystem::interaction_host::interaction_boundary(io, code, reason)
        };
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        interaction.validate(&mut work)?;
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(false, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| {
            reject(
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        {
            let (request_hold, original) = {
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let original = host.read_request(id, io).await?;
                let request_hold = io
                    .map(|io| io.guard_owned(io.owned_since(before)?))
                    .transpose()?;
                (request_hold, original)
            };
            if let Some(original) = &original {
                if let Some((owner, _)) = &self.owner {
                    owner.verify(false, &mut work).await?;
                }
                return if original == interaction {
                    self.publish_approval_request(id, io).await
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
        let publication = host
            .open(
                operation,
                None,
                &ticket,
                self.owner.as_ref().map(|(owner, _)| owner),
                io,
            )
            .await;
        drop(ticket);
        drop(ticket_hold);
        if let Some((owner, _)) = &self.owner {
            owner.verify(false, &mut work).await?;
        }
        let result = match publication {
            Ok(_) => self.publish_approval_request(id, io).await,
            Err(error)
                if matches!(
                    error.code(),
                    crate::wire::ErrorCode::Conflict | crate::wire::ErrorCode::Indeterminate
                ) =>
            {
                let (request_hold, original) = {
                    let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                    let original = host.read_request(id, io).await?;
                    let request_hold = io
                        .map(|io| io.guard_owned(io.owned_since(before)?))
                        .transpose()?;
                    (request_hold, original)
                };
                let result = match &original {
                    Some(original) if original == interaction => {
                        self.publish_approval_request(id, io).await
                    }
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
        };
        if let Some((owner, _)) = &self.owner {
            owner.verify(false, &mut work).await?;
        }
        result
    }

    pub(super) async fn interaction_outcome_with_io(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(&mut work)?;
            owner.verify(true, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        let outcome = match host.read_outcome(id, io).await? {
            // A verdict reaches execution only as a published decision.
            Some(InteractionOutcome::Approved {} | InteractionOutcome::Declined {}) => {
                Some(self.published_decision(id, io).await?.outcome())
            }
            outcome => outcome,
        };
        if let Some((owner, _)) = &self.owner {
            owner.verify(true, &mut work).await?;
        }
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
