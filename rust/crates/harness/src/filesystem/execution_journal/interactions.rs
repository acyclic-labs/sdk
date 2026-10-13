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
            host.verify_responder_scope_with_io(id, responder, None)?;
            if let Some((owner, _)) = &self.owner {
                owner.require_interaction_grant(None)?;
                owner.verify(true, &mut None).await?;
            }
            let (ticket, prior) = host
                .read(id, None)
                .await?
                .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
            let request = host
                .read_request(id, None)
                .await?
                .ok_or_else(|| Error::Storage("admitted interaction has no request".into()))?;
            request.validate_response(&response, &mut None)?;
            if let Some(previous) = &prior
                && previous.outcome.is_terminal()
            {
                let stored = match (&previous.outcome, &response) {
                    (
                        InteractionOutcome::Approved,
                        InteractionResponse::Approval { approved: true, .. },
                    )
                    | (
                        InteractionOutcome::Declined,
                        InteractionResponse::Approval {
                            approved: false, ..
                        },
                    ) => Some(
                        host.read_resolution_file(id, true, None)
                            .await?
                            .ok_or_else(|| {
                                Error::Storage("admitted approval detail is missing".into())
                            })?,
                    ),
                    (InteractionOutcome::Answered { .. }, _) => Some(
                        host.read_resolution_file(id, false, None)
                            .await?
                            .ok_or_else(|| Error::Storage("admitted answer is missing".into()))?,
                    ),
                    _ => None,
                };
                let same = match stored {
                    Some(bytes) => {
                        crate::contract::json_from_slice::<InteractionResponse>(&bytes)
                            .map_err(|error| Error::Storage(error.to_string()))?
                            == response
                    }
                    None => false,
                };
                if let Some((owner, _)) = &self.owner {
                    owner.verify(true, &mut None).await?;
                }
                return if same {
                    Ok(())
                } else {
                    Err(Error::Conflict("interaction was already resolved".into()))
                };
            }
            let expected_version = prior.as_ref().map_or(Ok(1_u64), |value| {
                value
                    .expected_version
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("interaction version exhausted".into()))
            })?;
            let digest = blake3::hash(
                format!(
                    "interaction:{}:resolve-{expected_version}",
                    uuid::Uuid::from_bytes(id.into_bytes()).hyphenated()
                )
                .as_bytes(),
            );
            let mut bytes = [0_u8; 16];
            bytes.copy_from_slice(&digest.as_bytes()[..16]);
            let operation = OperationId::from_bytes(bytes);
            if let Some((owner, _)) = &self.owner {
                owner.verify(true, &mut None).await?;
            }
            let staged = host
                .stage_answer(id, expected_version, operation, &response, None)
                .await?;
            let (outcome, detail) = match response {
                InteractionResponse::Approval { approved: true, .. } => {
                    (InteractionOutcome::Approved, Some(staged))
                }
                InteractionResponse::Approval {
                    approved: false, ..
                } => (InteractionOutcome::Declined, Some(staged)),
                _ => (
                    InteractionOutcome::Answered {
                        answer: Box::new(staged),
                    },
                    None,
                ),
            };
            host.resolve(
                operation,
                responder,
                InteractionResolution {
                    id: ticket.id,
                    expected_version,
                    outcome,
                    detail,
                },
                None,
            )
            .await?;
            if let Some((owner, _)) = &self.owner {
                owner.verify(true, &mut None).await?;
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
        let mut borrowed = io.map(PreparationIo::history_io);
        let mut work = borrowed
            .as_mut()
            .map(|work| work as &mut dyn crate::store::history::HistoryIo);
        interaction.validate(&mut work)?;
        if let Some((owner, _)) = &self.owner {
            owner.require_interaction_grant(io)?;
            owner.verify(false, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        {
            let request_hold;
            let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
            let original = host.read_request(id, io).await?;
            request_hold = io
                .map(|io| {
                    io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                        crate::filesystem::interaction_host::interaction_boundary(
                            Some(io),
                            crate::wire::ErrorCode::Invalid,
                            "interaction request custody underflows",
                        )
                    })?)
                })
                .transpose()?;
            if let Some(original) = &original {
                if let Some((owner, _)) = &self.owner {
                    owner.verify(false, &mut work).await?;
                }
                return if original == interaction {
                    Ok(())
                } else {
                    Err(crate::filesystem::interaction_host::interaction_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "interaction identity reused",
                    ))
                };
            }
            drop(original);
            drop(request_hold);
        }
        let operation = interaction_open_operation(id, io)?;
        let ticket_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let ticket = host
            .stage_request_with_io(id, interaction, None, io)
            .await?;
        ticket_hold = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    crate::filesystem::interaction_host::interaction_boundary(
                        Some(io),
                        crate::wire::ErrorCode::Invalid,
                        "interaction ticket custody underflows",
                    )
                })?)
            })
            .transpose()?;
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
            Ok(_) => Ok(()),
            Err(error)
                if matches!(
                    error.code(),
                    crate::wire::ErrorCode::Conflict | crate::wire::ErrorCode::Indeterminate
                ) =>
            {
                let request_hold;
                let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
                let original = host.read_request(id, io).await?;
                request_hold = io
                    .map(|io| {
                        io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                            crate::filesystem::interaction_host::interaction_boundary(
                                Some(io),
                                crate::wire::ErrorCode::Invalid,
                                "interaction request custody underflows",
                            )
                        })?)
                    })
                    .transpose()?;
                let result = match &original {
                    Some(original) if original == interaction => Ok(()),
                    Some(_) => Err(crate::filesystem::interaction_host::interaction_boundary(
                        io,
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
            owner.require_interaction_grant(io)?;
            owner.verify(true, &mut work).await?;
        }
        let host = self.interactions.as_ref().ok_or_else(|| {
            crate::filesystem::interaction_host::interaction_boundary(
                io,
                crate::wire::ErrorCode::Unsupported,
                "task execution has no conversation interaction owner",
            )
        })?;
        let outcome = host.read_outcome_with_io(id, io).await?;
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
