use super::*;

const DIAGNOSTIC_LABELS: &[&str] = &[
    "Interaction",
    "InteractionResponse",
    "ChoiceOption",
    "type",
    "question",
    "choice",
    "form",
    "approval",
    "prompt",
    "response_schema",
    "options",
    "min_selections",
    "max_selections",
    "title",
    "schema",
    "operation_id",
    "action_digest",
    "id",
    "label",
    "description",
    "value",
    "option_ids",
    "approved",
    "reason",
];
impl PreparationRecord for Interaction {
    fn maximum_slot_bytes() -> u64 {
        preparation_slot_bytes::<Self>()
            .max(std::mem::size_of::<crate::interaction::ChoiceOption>() as u64)
    }
    fn diagnostic_label_bytes() -> u64 {
        crate::contract::preparation_serde_diagnostic_label_bytes()
            + DIAGNOSTIC_LABELS
                .iter()
                .map(|label| label.len() as u64 + 4)
                .sum::<u64>()
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::Interaction
    }
}
impl PreparationRecord for InteractionResponse {
    fn diagnostic_label_bytes() -> u64 {
        <Interaction as PreparationRecord>::diagnostic_label_bytes()
    }
    fn json_value_schema() -> crate::contract::JsonValueSchema {
        crate::contract::JsonValueSchema::InteractionResponse
    }
}
impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(crate) async fn stage_request_with_io(
        &self,
        id: InteractionId,
        request: &Interaction,
        deadline_unix_ms: Option<u64>,
        io: Option<&PreparationIo>,
    ) -> Result<InteractionTicket> {
        if id.into_bytes() == [0; 16] || deadline_unix_ms == Some(0) {
            return Err(interaction_boundary(
                io,
                ErrorCode::Invalid,
                "interaction ticket identity or deadline is invalid",
            ));
        }
        crate::executor::with_history_io(io, |work| request.validate(work))?;
        let approval = match request {
            Interaction::Approval {
                operation_id,
                action_digest,
                ..
            } => Some(ApprovalBinding {
                operation_id: *operation_id,
                action_digest: *action_digest,
            }),
            _ => None,
        };
        let ticket = InteractionTicket {
            id: Uuid::from_bytes(id.into_bytes()),
            kind: request.kind(),
            request: self.stage(id, "request", request, io).await?,
            deadline_unix_ms,
            approval,
        };
        crate::executor::with_history_io(io, |work| ticket.validate(work))?;
        Ok(ticket)
    }

    /// Stages an answer against its pinned interaction version.
    pub async fn stage_answer(
        &self,
        id: InteractionId,
        expected_version: u64,
        operation_id: OperationId,
        response: &InteractionResponse,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        if expected_version == 0 {
            return Err(interaction_boundary(
                io,
                ErrorCode::Invalid,
                "interaction answer version must be positive",
            ));
        }
        let kind_hold;
        let kind = crate::contract::preparation_format(
            format_args!("answer-{expected_version}-{operation_id}"),
            io,
        )?;
        kind_hold = io
            .map(|io| io.guard_owned(kind.capacity() as u64))
            .transpose()?;
        let result = self.stage(id, &kind, response, io).await;
        drop(kind);
        drop(kind_hold);
        result
    }

    /// Reads the authoritative ticket and latest resolution.
    pub async fn read(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let aggregate = self.aggregate(io).await?;
        crate::executor::with_history_io(io, |work| {
            aggregate
                .0
                .reducer()
                .interaction(&Uuid::from_bytes(id.into_bytes()))
                .map(|(ticket, resolution)| {
                    Ok((
                        crate::wire_codec::CodecQuota::produce(work, |quota| {
                            crate::core::copy_ticket(ticket, quota)
                        })?
                        .0,
                        resolution
                            .as_ref()
                            .map(|resolution| {
                                crate::wire_codec::CodecQuota::produce(work, |quota| {
                                    crate::core::copy_resolution(resolution, quota)
                                })
                                .map(|(value, _)| value)
                            })
                            .transpose()?,
                    ))
                })
                .transpose()
        })
    }

    pub(crate) async fn read_outcome_with_io(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let aggregate = self.aggregate(io).await?;
        crate::executor::with_history_io(io, |work| {
            aggregate
                .0
                .reducer()
                .interaction(&Uuid::from_bytes(id.into_bytes()))
                .and_then(|(_, resolution)| resolution.as_ref())
                .map(|resolution| {
                    crate::wire_codec::CodecQuota::produce(work, |quota| {
                        crate::core::copy_interaction_outcome(&resolution.outcome, quota)
                    })
                    .map(|(value, _)| value)
                })
                .transpose()
        })
    }

    /// Loads the pinned request under the owner read grant.
    pub async fn read_request(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Interaction>> {
        let aggregate = self.aggregate(io).await?;
        let Some((ticket, _)) = aggregate
            .0
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        let bytes_hold;
        let bytes = self.read_file(&ticket.request, io).await?;
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let request = match io {
            Some(io) => {
                let decoded = io.decode(
                    std::marker::PhantomData::<Interaction>,
                    &bytes,
                    ErrorCode::Storage,
                )?;
                crate::executor::with_history_io(Some(io), |work| {
                    ticket.validate_decoded_request_with_work(&decoded.0, work)
                })?;
                crate::filesystem::retain_decoded(decoded)
            }
            None => ticket.validate_request_bytes(&bytes, &mut None)?.0,
        };
        drop(bytes);
        drop(bytes_hold);
        Ok(Some(request))
    }

    pub(crate) async fn read_resolution_file(
        &self,
        id: InteractionId,
        detail: bool,
        io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let aggregate = self.aggregate(io).await?;
        let Some((_, Some(resolution))) = aggregate
            .0
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        let reference = if detail {
            resolution.detail.as_ref()
        } else {
            match &resolution.outcome {
                InteractionOutcome::Answered { answer } => Some(answer.as_ref()),
                _ => None,
            }
        };
        match reference {
            Some(reference) => self.read_file(reference, io).await.map(Some),
            None => Ok(None),
        }
    }

    pub(super) async fn read_file(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        let maximum = io
            .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        crate::executor::with_history_io(io, |work| {
            reference.validate(work)?;
            ContentGrant::authorize_volume(
                &self.verifier,
                &self.owner_scope,
                reference.volume(),
                VolumeOperation::Read,
                maximum,
                work,
            )
        })?;
        let bytes = self
            .host
            .read_pinned_io(reference, self.maximum_bytes, io)
            .await?;
        if io.is_none() {
            self.host.retain_file_generation(reference).await?;
        }
        Ok(bytes)
    }

    pub(super) async fn stage<T: serde::Serialize>(
        &self,
        id: InteractionId,
        kind: &str,
        value: &T,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        let bytes_hold;
        let bytes = match io {
            Some(io) => io.encode(value, self.maximum_bytes, false)?,
            None => serde_json::to_vec(value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into(),
        };
        bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if bytes.len() as u64 > self.maximum_bytes {
            return Err(interaction_boundary(
                io,
                ErrorCode::Invalid,
                "interaction payload exceeds limit",
            ));
        }
        let names_hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let path = crate::contract::preparation_format(
            format_args!(".system/interactions/{id}/{kind}.json"),
            io,
        )?;
        let name = crate::contract::preparation_format(format_args!("{kind}.json"), io)?;
        let key = IdempotencyKey::new(crate::contract::preparation_format(
            format_args!("interaction:{id}:{kind}"),
            io,
        )?)?;
        names_hold = io
            .map(|io| {
                io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| {
                    interaction_boundary(
                        Some(io),
                        ErrorCode::Invalid,
                        "interaction name custody underflows",
                    )
                })?)
            })
            .transpose()?;
        let result = match io {
            Some(io) => {
                self.host
                    .put_internal_content_original_with_io(
                        &self.private_volume,
                        &self.verifier,
                        &self.owner_scope,
                        &path,
                        &bytes,
                        "application/json",
                        &name,
                        self.maximum_bytes,
                        &key,
                        InternalContentClass::Interaction,
                        io,
                    )
                    .await
            }
            None => {
                let grant = ContentGrant::verify(
                    &self.verifier,
                    &self.owner_scope,
                    &self.private_volume,
                    VolumeOperation::Write,
                    u64::MAX,
                    &mut None,
                )?;
                self.host
                    .put_internal_content(
                        &self.private_volume,
                        &grant,
                        &path,
                        &bytes,
                        "application/json",
                        &name,
                        self.maximum_bytes,
                        &key,
                        InternalContentClass::Interaction,
                    )
                    .await
            }
        };
        drop(key);
        drop(name);
        drop(path);
        drop(names_hold);
        drop(bytes);
        drop(bytes_hold);
        result
    }
}
