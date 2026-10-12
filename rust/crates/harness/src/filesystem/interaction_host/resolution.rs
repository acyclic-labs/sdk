use super::*;

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) fn verify_responder_scope_with_io(
        &self,
        id: InteractionId,
        scope: &Scope,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| self.verifier.verify(scope, work))?;
        let grant_hold;
        let grant =
            crate::contract::preparation_format(format_args!("interaction:respond:{id}"), io)?;
        grant_hold = io
            .map(|io| io.guard_owned(grant.capacity() as u64))
            .transpose()?;
        let examined = acyclic_fs::WorkCounters {
            items_examined: 2,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(examined)?;
        }
        let allowed = scope
            .capabilities()
            .contains(capability::INTERACTION_RESOLVE)
            && scope.capabilities().contains(&grant);
        if let Some(io) = io {
            io.charge(examined)?;
        }
        drop(grant);
        drop(grant_hold);
        if !allowed {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "responder lacks the exact interaction grant",
            ));
        }
        Ok(())
    }

    /// Resolves with the exact responder grant and pinned version.
    pub async fn resolve(
        &self,
        operation_id: OperationId,
        scope: &Scope,
        resolution: InteractionResolution,
        io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        self.execute(
            operation_id,
            scope,
            Action::ResolveInteraction { resolution },
            None,
            io,
        )
        .await
    }
    /// Authenticates a responder, validates one typed answer against the
    pub async fn resolve_answer(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        response: InteractionResponse,
    ) -> Result<ResolutionReceipt> {
        let ticket = self.open_resolution(&scope, id, expected_version).await?;
        if ticket.kind == InteractionKind::Approval {
            return Err(Error::Invalid("approval requires a decision".into()));
        }
        let request = self
            .read_request(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        request.validate_response(&response, &mut None)?;
        let answer = self
            .stage_answer(id, expected_version, operation_id, &response, None)
            .await?;
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(answer),
            },
            detail: None,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(
            self.resolve(operation_id, &scope, resolution, None).await?,
        )
    }

    /// Resolves an approval of the ticket's exact operation and argument digest.
    pub async fn resolve_approval(
        &self,
        operation_id: OperationId,
        scope: Scope,
        id: InteractionId,
        expected_version: u64,
        approved: bool,
        reason: Option<String>,
    ) -> Result<ResolutionReceipt> {
        let ticket = self.open_resolution(&scope, id, expected_version).await?;
        if ticket.kind != InteractionKind::Approval {
            return Err(Error::Invalid("interaction is not an approval".into()));
        }
        let outcome = if approved {
            InteractionOutcome::Approved
        } else {
            InteractionOutcome::Declined
        };
        let response = InteractionResponse::Approval { approved, reason };
        let detail = Some(
            self.stage_answer(id, expected_version, operation_id, &response, None)
                .await?,
        );
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version,
            outcome,
            detail,
        };
        resolution.validate(&ticket)?;
        ResolutionReceipt::from_apply_result(
            self.resolve(operation_id, &scope, resolution, None).await?,
        )
    }

    pub(super) async fn open_resolution(
        &self,
        scope: &Scope,
        id: InteractionId,
        expected_version: u64,
    ) -> Result<InteractionTicket> {
        self.verifier.verify(scope, &mut None)?;
        if !scope
            .capabilities()
            .contains(capability::INTERACTION_RESOLVE)
        {
            return Err(Error::Unauthorized(
                "scope lacks interaction:resolve".into(),
            ));
        }
        let (ticket, prior) = self
            .read(id, None)
            .await?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?;
        if !scope.capabilities().contains(&ticket.responder_grant()) {
            return Err(Error::Unauthorized(
                "scope lacks interaction responder grant".into(),
            ));
        }
        let next = match &prior {
            Some(prior) if prior.outcome.is_terminal() => prior.expected_version,
            Some(prior) => next_revision(prior.expected_version)?,
            None => 1,
        };
        if expected_version != next {
            return Err(Error::Conflict(
                "interaction resolution version mismatch".into(),
            ));
        }
        Ok(ticket)
    }

    pub(in crate::filesystem) async fn verify_approval_with_io(
        &self,
        id: InteractionId,
        operation: OperationId,
        digest: [u8; 32],
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let aggregate = self.aggregate(io).await?;
        let Some((ticket, Some(resolution))) = aggregate
            .0
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "tool installation lacks a resolved approval",
            ));
        };
        if !matches!(&resolution.outcome, InteractionOutcome::Approved)
            || !ticket.approval.as_ref().is_some_and(|binding| {
                binding.operation_id == operation && binding.action_digest == digest
            })
        {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "approval does not authorize this tool definition",
            ));
        }
        let bytes_hold;
        let bytes = self.read_file(&ticket.request, io).await?;
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let request = match io {
            Some(io) => {
                let request = io.decode(
                    std::marker::PhantomData::<Interaction>,
                    &bytes,
                    ErrorCode::Storage,
                )?;
                crate::executor::with_history_io(Some(io), |work| {
                    ticket.validate_decoded_request_with_work(&request.0, work)
                })?;
                (request.0, Some(request.1))
            }
            None => (ticket.validate_request_bytes(&bytes, &mut None)?.0, None),
        };
        drop(bytes);
        drop(bytes_hold);
        if let Some(detail) = &resolution.detail {
            if detail.descriptor().media_type() != "application/json" {
                return Err(interaction_boundary(
                    io,
                    ErrorCode::Invalid,
                    "approval decision artifact is invalid",
                ));
            }
            let bytes_hold;
            let bytes = self.read_file(detail, io).await?;
            bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
            match io {
                Some(io) => {
                    let response = io.decode(
                        std::marker::PhantomData::<InteractionResponse>,
                        &bytes,
                        ErrorCode::Storage,
                    )?;
                    crate::executor::with_history_io(Some(io), |work| {
                        ticket.validate_decoded_decision_with_work(
                            &request.0,
                            &resolution.outcome,
                            detail,
                            &response.0,
                            work,
                        )
                    })?;
                }
                None => ticket.validate_decision_bytes(
                    &request.0,
                    &resolution.outcome,
                    detail,
                    &bytes,
                    &mut None,
                )?,
            }
            drop(bytes);
            drop(bytes_hold);
        }
        Ok(())
    }
}
