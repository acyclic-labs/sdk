use super::*;
use crate::executor::with_history_io;
use crate::wire_codec::CodecQuota;

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(crate) async fn stage_request(
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
                "invalid interaction identity or deadline",
            ));
        }
        with_history_io(io, |work| request.validate(work))?;
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
        with_history_io(io, |work| ticket.validate(work))?;
        Ok(ticket)
    }

    /// Stages an answer or approval decision against its pinned interaction version.
    pub async fn stage_answer<T: serde::Serialize>(
        &self,
        id: InteractionId,
        expected_version: u64,
        operation_id: OperationId,
        response: &T,
        io: Option<&PreparationIo>,
    ) -> Result<FileRef> {
        if expected_version == 0 {
            return Err(interaction_boundary(
                io,
                ErrorCode::Invalid,
                "interaction version must be positive",
            ));
        }
        let kind = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!("answer-{expected_version}-{operation_id}"),
                work,
            )
        })?;
        let hold = io
            .map(|io| io.guard_owned(kind.capacity() as u64))
            .transpose()?;
        let result = self.stage(id, &kind, response, io).await;
        drop((kind, hold));
        result
    }

    /// Reads the authoritative ticket and latest resolution.
    pub async fn read(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let aggregate = self.aggregate(io).await?;
        Self::admitted(&aggregate.0, id, io)
    }

    /// Reads the ticket and latest resolution together with the provider's
    /// receipt that the conversation events they were reduced from are durable.
    /// `None` for the receipt while any of them is only accepted.
    pub(crate) async fn read_durable<'io>(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        io: Option<&'io PreparationIo>,
    ) -> Result<(
        Option<(InteractionTicket, Option<InteractionResolution>)>,
        Option<acyclic_stream::DurableReceipt>,
        Option<DecodeReservation<'io>>,
    )> {
        let aggregate = self.aggregate(io).await?;
        if let Some((operation, digest)) = binding {
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
            if resolution.outcome != (InteractionOutcome::Approved {})
                || !ticket.approval.as_ref().is_some_and(|approved| {
                    approved.operation_id == operation && approved.action_digest == digest
                })
            {
                return Err(interaction_boundary(
                    io,
                    ErrorCode::Unauthorized,
                    "approval does not authorize this definition",
                ));
            }
        }
        // Invalid bindings cannot force a device flush merely by asking to
        // install an unrelated tool.
        let hold;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let read = Self::admitted(&aggregate.0, id, io)?;
        hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        #[cfg(test)]
        acyclic_test_support::differential::record(("approval-receipt", 1_u64));
        let receipt = aggregate.0.durable_receipt().await?;
        Ok((read, receipt, hold))
    }

    fn admitted(
        aggregate: &crate::store::StreamAggregate<P>,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        let Some((ticket, resolution)) = aggregate
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        with_history_io(io, |work| {
            CodecQuota::produce(work, |quota| {
                Ok((
                    crate::core::copy(ticket, quota, |value, _| crate::core::owned_bytes(value))?,
                    resolution
                        .as_ref()
                        .map(|resolution| {
                            crate::core::copy(resolution, quota, |value, _| {
                                crate::core::owned_bytes(value)
                            })
                        })
                        .transpose()?,
                ))
            })
        })
        .map(|(value, _)| Some(value))
    }

    pub(crate) async fn read_outcome(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let aggregate = self.aggregate(io).await?;
        let Some((_, Some(resolution))) = aggregate
            .0
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        with_history_io(io, |work| {
            CodecQuota::produce(work, |quota| {
                crate::core::copy(&resolution.outcome, quota, |value, _| {
                    crate::core::owned_bytes(value)
                })
            })
        })
        .map(|(value, _)| Some(value))
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
        let bytes = self.read_file(&ticket.request, io).await?;
        let _bytes = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        admit_interaction(&bytes, io)?;
        let (request, _, custody) =
            with_history_io(io, |work| ticket.validate_request_bytes(&bytes, work))?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(custody) = custody {
            io.ok_or_else(|| {
                crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "native request observer",
                )
            })?
            .absorb_native_custody(custody)?;
        }
        #[cfg(target_arch = "wasm32")]
        let _ = custody;
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

    pub(in crate::filesystem) async fn read_file(
        &self,
        reference: &FileRef,
        io: Option<&PreparationIo>,
    ) -> Result<acyclic_stream::OwnedBytes> {
        #[cfg(test)]
        acyclic_test_support::differential::record((
            "interaction-file-read",
            reference.descriptor().byte_length(),
        ));
        let maximum = io
            .map(|io| io.remaining().map(|budget| budget.bytes_encoded))
            .transpose()?
            .unwrap_or(u64::MAX);
        with_history_io(io, |work| {
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
        let bytes = match io {
            Some(io) => io.encode(value, self.maximum_bytes, false)?,
            None => serde_json::to_vec(value)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into(),
        };
        let bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if bytes.len() as u64 > self.maximum_bytes {
            return Err(interaction_boundary(
                io,
                ErrorCode::Invalid,
                "interaction payload exceeds limit",
            ));
        }
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let path = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(
                format_args!(".system/interactions/{id}/{kind}.json"),
                work,
            )
        })?;
        let name = crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("{kind}.json"), work)
        })?;
        let key = IdempotencyKey::new(crate::executor::with_history_io(io, |work| {
            crate::executor::preparation_format(format_args!("interaction:{id}:{kind}"), work)
        })?)?;
        let names_hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        let grant = with_history_io(io, |work| {
            ContentGrant::verify(
                &self.verifier,
                &self.owner_scope,
                &self.private_volume,
                VolumeOperation::Write,
                io.map_or(Ok(u64::MAX), |io| {
                    io.remaining().map(|budget| budget.bytes_encoded)
                })?,
                work,
            )
        })?;
        let grant_hold = io
            .map(|io| io.guard_owned(grant.logical_owned_bytes()?))
            .transpose()?;
        let result = self
            .host
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
                io,
            )
            .await;
        drop((
            grant, grant_hold, key, name, path, names_hold, bytes, bytes_hold,
        ));
        result
    }
}

pub(in crate::filesystem) fn admit_interaction(
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<()> {
    let Some(io) = io else {
        return Ok(());
    };
    let _diagnostic = io.reserve_decode(crate::executor::json::lexical_json_diagnostic_bytes())?;
    let bounds = crate::executor::json::JsonBounds {
        maximum_bytes: bytes.len() as u64,
        maximum_items: io.remaining()?.items_examined,
        maximum_depth: 128,
    };
    match crate::executor::json::admit_json(bytes, bounds, &mut |charge| {
        io.codec_charge(charge, false)
    }) {
        Err(Error::Invalid(_)) => Err(interaction_boundary(
            Some(io),
            ErrorCode::Storage,
            "interaction JSON exceeds bounds",
        )),
        result => result,
    }
}
