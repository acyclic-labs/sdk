use super::*;
use crate::core::OwnedBytes;
use crate::executor::with_history_io;
use crate::wire_codec::CodecQuota;

/// A canonical read keeps its original decoded allocation alive until publication ends.
pub(in crate::filesystem) struct PublicationRead<'io> {
    pub value: Option<(InteractionTicket, Option<InteractionResolution>)>,
    pub reservation: Option<DecodeReservation<'io>>,
}

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
        self.read_at(&aggregate.0, id, io)
    }

    /// Copies a ticket from the transaction's already authenticated cut.
    pub(in crate::filesystem) fn read_at(
        &self,
        aggregate: &StreamAggregate<P>,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<(InteractionTicket, Option<InteractionResolution>)>> {
        Self::admitted(aggregate, id, io)
    }

    /// Validates the exact installation binding and copies its authoritative
    /// ticket and resolution. The publication owner obtains the canonical
    /// durable receipt after publishing the mandatory task files.
    pub(in crate::filesystem) fn read_for_publication<'io>(
        &self,
        aggregate: &StreamAggregate<P>,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        io: Option<&'io PreparationIo>,
    ) -> Result<PublicationRead<'io>> {
        if let Some((operation, digest)) = binding {
            let Some((ticket, Some(resolution))) = aggregate
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
        // Invalid bindings cannot publish a file or force a device flush merely
        // by asking to install an unrelated tool.
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let read = Self::admitted(aggregate, id, io)?;
        let hold = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        Ok(PublicationRead {
            value: read,
            reservation: hold,
        })
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
                    crate::core::copy(ticket, quota, |value, _| value.owned_heap_bytes())?,
                    resolution
                        .as_ref()
                        .map(|resolution| {
                            crate::core::copy(resolution, quota, |value, _| {
                                value.owned_heap_bytes()
                            })
                        })
                        .transpose()?,
                ))
            })
        })
        .map(|(value, _)| Some(value))
    }

    pub(crate) fn read_outcome(
        &self,
        aggregate: &StreamAggregate<P>,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<InteractionOutcome>> {
        let Some((_, Some(resolution))) = aggregate
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        with_history_io(io, |work| {
            CodecQuota::produce(work, |quota| {
                crate::core::copy(&resolution.outcome, quota, |value, _| {
                    value.owned_heap_bytes()
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
        self.read_request_at(&aggregate.0, id, io).await
    }

    pub(in crate::filesystem) async fn read_request_at(
        &self,
        aggregate: &StreamAggregate<P>,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<Option<Interaction>> {
        let Some((ticket, _)) = aggregate
            .reducer()
            .interaction(&Uuid::from_bytes(id.into_bytes()))
        else {
            return Ok(None);
        };
        self.read_ticket_request(ticket, io).await.map(Some)
    }

    pub(super) async fn read_ticket_request(
        &self,
        ticket: &InteractionTicket,
        io: Option<&PreparationIo>,
    ) -> Result<Interaction> {
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
        Ok(request)
    }

    pub(crate) async fn read_resolution_file(
        &self,
        aggregate: &StreamAggregate<P>,
        id: InteractionId,
        detail: bool,
        io: Option<&PreparationIo>,
    ) -> Result<Option<acyclic_stream::OwnedBytes>> {
        let Some((_, Some(resolution))) = aggregate
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
        self.host
            .read_pinned_receipt_io(reference, self.maximum_bytes, io)
            .await?
            .retain()
            .await
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
        let ((path, name, key), retained) = with_history_io(io, |work| {
            CodecQuota::produce(work, |quota| {
                Ok((
                    quota.format(format_args!(".system/interactions/{id}/{kind}.json"))?,
                    quota.format(format_args!("{kind}.json"))?,
                    IdempotencyKey::new(quota.format(format_args!("interaction:{id}:{kind}"))?)?,
                ))
            })
        })?;
        let names_hold = io.map(|io| io.guard_owned(retained)).transpose()?;
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
