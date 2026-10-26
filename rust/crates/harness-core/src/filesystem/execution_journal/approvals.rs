//! Approval decisions published by the conversation interaction owner.
//!
//! The interaction owner is the only settlement authority. Execution observes a
//! verdict only as a [`PublishedDecision`], which exists only after the
//! decision's mandatory file validated against its ticket and the provider
//! issued a durable receipt for the conversation events that committed it. An
//! acknowledgement never stands in for a receipt, so nothing written under
//! deferred durability reaches execution until the provider's barrier covers it.
use super::*;
use crate::{executor::with_history_io, interaction::InteractionKind};

fn admitted<T>(read: Option<T>, io: Option<&PreparationIo>) -> Result<T> {
    read.ok_or_else(|| {
        canonical_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "published approval was not admitted",
        )
    })
}

/// Execution's only view of an approval verdict. Its single constructor is
/// [`FilesystemExecutionJournal::published_decision`].
pub(super) struct PublishedDecision {
    approved: bool,
    /// The conversation events that admitted the request and committed this
    /// resolution are in the provider's durable prefix.
    _resolution: acyclic_stream::DurableReceipt,
}

impl PublishedDecision {
    pub(super) fn outcome(&self) -> InteractionOutcome {
        if self.approved {
            InteractionOutcome::Approved {}
        } else {
            InteractionOutcome::Declined {}
        }
    }

    pub(super) fn approved(&self) -> bool {
        self.approved
    }
}

impl<P, A, O> FilesystemExecutionJournal<P, A, O>
where
    P: StreamProvider,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Validates the committed decision's mandatory file and proves its
    /// resolution durable before any verdict reaches execution.
    pub(super) async fn published_decision(
        &self,
        id: InteractionId,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        self.published_decision_for(id, None, io).await
    }

    pub(super) async fn published_decision_for(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        let host = self.interactions()?;
        let aggregate = host.aggregate(io).await?;
        self.published_decision_from(id, binding, aggregate, io)
            .await
    }

    pub(super) async fn published_decision_from(
        &self,
        id: InteractionId,
        binding: Option<(OperationId, [u8; 32])>,
        aggregate: crate::filesystem::interaction_host::InteractionProjection<P>,
        io: Option<&PreparationIo>,
    ) -> Result<PublishedDecision> {
        let host = self.interactions()?;
        let read = host.read_for_publication(&aggregate.0, id, binding, io)?;
        let _read = read.reservation;
        let read = read.value;
        let (ticket, resolution) = admitted(read, io)?;
        let (Some(resolution), InteractionKind::Approval) = (&resolution, ticket.kind) else {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval has no committed decision",
            ));
        };
        let detail = resolution.detail.as_ref().ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval decision has no decision file",
            )
        })?;
        let request_bytes = Some(host.read_file(&ticket.request, io).await?);
        let _request_bytes = request_bytes
            .as_ref()
            .map(|bytes| io.map(|io| io.guard_bytes(bytes)).transpose())
            .transpose()?
            .flatten()
            .flatten();
        let request = {
            let bytes = request_bytes.as_ref().ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "approval request is missing",
                )
            })?;
            crate::filesystem::interaction_host::admit_interaction(bytes, io)?;
            let decoded = with_history_io(io, |work| ticket.validate_request_bytes(bytes, work))?;
            let hold = io.map(|io| io.guard_owned(decoded.1)).transpose()?;
            (decoded, hold)
        };
        let legacy_request = request.0.0.is_legacy_approval();
        let bytes = host.read_file(detail, io).await?;
        let _detail = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        if binding.is_some() {
            crate::filesystem::interaction_host::admit_interaction(&bytes, io)?;
        }
        with_history_io(io, |work| {
            ticket.validate_published_decision_bytes(
                resolution,
                detail,
                &bytes,
                legacy_request,
                work,
            )
        })?;
        // The validation above bound the file's verdict to this outcome.
        let approved = matches!(resolution.outcome, InteractionOutcome::Approved {});
        #[cfg(test)]
        acyclic_test_support::differential::record(("approval-receipt", 1_u64));
        let resolution = aggregate.0.durable_receipt().await?.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval decision awaits durability",
            )
        })?;
        drop(aggregate);
        Ok(PublishedDecision {
            approved,
            _resolution: resolution,
        })
    }
}
