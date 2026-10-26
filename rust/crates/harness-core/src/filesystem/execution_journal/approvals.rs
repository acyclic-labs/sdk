//! Approval requests and decisions followed as task files.
//!
//! The interaction owner is the only settlement authority. These files are
//! verbatim copies of its admitted request and staged decision. Execution
//! observes a verdict only as a [`PublishedDecision`], which exists only after
//! the decision's mandatory file validated against its ticket, the provider
//! issued a durable receipt for the conversation events that committed it and,
//! for a task journal, for that exact followed file. An acknowledgement never
//! stands in for a receipt, so nothing written under deferred durability
//! reaches execution until the provider's barrier covers it.
use super::*;
use crate::{
    executor::with_history_io,
    interaction::{ApprovalDecisionFile, InteractionKind, principal_hex},
    task_volume::{Component, DurableFile, FileKey, FileValue, TaskFile},
};

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
    /// For a task journal, its followed decision file is durable too. A
    /// conversation journal is itself the interaction owner and has no
    /// task-file surface.
    _file: Option<DurableFile>,
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
    /// Admits `approvals/<id>/decisions/<principal>.json` through the owner's
    /// authenticated compare-and-set resolve of exactly the submitted binding,
    /// then publishes the owner's copy durably.
    pub async fn write_approval_decision(&self, file: &FileValue, responder: &Scope) -> Result<()> {
        let task_id = self.approval_task_id().ok_or_else(|| {
            Error::Unsupported("approval decisions require a task journal".into())
        })?;
        let FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision { request, principal },
        } = &file.key
        else {
            return Err(Error::Unauthorized(
                "only approval decisions are approver files".into(),
            ));
        };
        if task.as_str() != task_id.to_string()
            || principal.as_str() != ApprovalDecisionFile::principal_file(responder.id())
            || file.channel.is_some()
            || file.requester.is_some()
            || file.publication.is_some()
        {
            return Err(Error::Unauthorized(
                "approval decision principal or task differs".into(),
            ));
        }
        let id: uuid::Uuid = request
            .as_str()
            .parse()
            .map_err(|_| Error::Invalid("invalid approval request identity".into()))?;
        if id.hyphenated().to_string() != request.as_str() {
            return Err(Error::Invalid(
                "noncanonical approval request identity".into(),
            ));
        }
        let id = InteractionId::from_bytes(id.into_bytes());
        let decision = ApprovalDecisionFile::decode(&file.bytes, self.maximum_payload_bytes)?;
        let host = self.interactions()?;
        // The passive route can submit only an already published request in
        // this task. Its bytes must be the interaction owner's exact request.
        let mut transaction = host.transaction(None).await?;
        let ticket = host
            .read_at(&transaction.aggregate.0, id, None)?
            .ok_or_else(|| Error::NotFound(format!("interaction {id}")))?
            .0;
        let key = FileKey::Task {
            task: task.clone(),
            file: TaskFile::ApprovalRequest(request.clone()),
        };
        let published = self
            .approval_files()?
            .snapshot(&crate::task_volume::Query::File(key), 1)
            .await?;
        let admitted = host.read_file(&ticket.request, None).await?;
        if !matches!(published.files.as_slice(), [file] if file.bytes == admitted) {
            return Err(Error::Unauthorized(
                "approval request is not owned by this task".into(),
            ));
        }
        let owner = self.owner.as_ref().map(|(owner, _)| owner);
        if let Some(owner) = owner {
            owner.require_interaction_grant(&mut None)?;
            owner.verify(true, &mut None).await?;
        }
        // The resolution's filesystem staging and canonical append are one
        // publication. Its explicit receipt below remains the durability gate;
        // intermediate appends must not each submit a separate device flush.
        let resolution = host.resolve_decision(id, decision, responder, owner, &mut transaction);
        // Only this provider owner may share its receipt. A separately stored
        // filesystem authority must retain its own publication barrier.
        #[cfg(feature = "filesystem-local")]
        let resolution = acyclic_stream::deferring_provider_durability(&*self.stream, resolution);
        resolution.await?;
        self.published_decision_from(id, None, transaction.aggregate, None)
            .await?;
        if let Some(owner) = owner {
            owner.verify(true, &mut None).await?;
        }
        Ok(())
    }

    /// Publishes the owner's admitted approval request into the task's files.
    pub(super) async fn publish_approval_request(
        &self,
        id: InteractionId,
        aggregate: &StreamAggregate<P>,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let Some((owner, _)) = &self.owner else {
            return Ok(());
        };
        let host = self.interactions()?;
        let before = io.map(PreparationIo::owned_bytes).transpose()?.unwrap_or(0);
        let (ticket, _) = admitted(host.read_at(aggregate, id, io)?, io)?;
        let _read = io
            .map(|io| io.guard_owned(io.owned_since(before)?))
            .transpose()?;
        if ticket.kind != InteractionKind::Approval {
            return Ok(());
        }
        let files = self.approval_files()?;
        let bytes = host.read_file(&ticket.request, io).await?;
        let _request = io.map(|io| io.guard_bytes(&bytes)).transpose()?.flatten();
        let value = FileValue::new(
            FileKey::Task {
                task: Component::new(owner.task_id().to_string())?,
                file: TaskFile::ApprovalRequest(Component::new(ticket.id.to_string())?),
            },
            bytes,
        );
        let _file = files.publish_durable(&value, native_work(io)).await?;
        // The file and this canonical journal are bound to the same provider
        // by with_task_files. Its eager publication covers the earlier open
        // event too; require that provider's explicit canonical proof as well.
        let _request = aggregate.durable_receipt().await?.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "approval request awaits durability",
            )
        })?;
        Ok(())
    }

    /// Validates the committed decision's mandatory file and proves its
    /// resolution and, for a task journal, the request and decision files
    /// durable before any verdict reaches execution.
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
        let principal = with_history_io(io, |work| {
            ticket.validate_published_decision_bytes(resolution, detail, &bytes, legacy_request, work)
        })?;
        // The validation above bound the file's verdict to this outcome.
        let approved = matches!(resolution.outcome, InteractionOutcome::Approved {});
        let file = if let Some(task_id) = self.approval_task_id() {
            Some(
                self.publish_decision_files(task_id, id, &principal, request_bytes, bytes, io)
                    .await?,
            )
        } else {
            None
        };
        // The exact task-file publication shares this provider's device
        // barrier with the preceding canonical resolution. Obtain the proof
        // afterwards; neither accepted records nor a file alone release a verdict.
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
            _file: file,
        })
    }

    async fn publish_decision_files(
        &self,
        task_id: crate::TaskId,
        id: InteractionId,
        principal: &[u8; 16],
        request_bytes: Option<acyclic_stream::OwnedBytes>,
        bytes: acyclic_stream::OwnedBytes,
        io: Option<&PreparationIo>,
    ) -> Result<DurableFile> {
        let files = self.approval_files()?;
        let task = Component::new(task_id.to_string())?;
        let request = Component::new(id.to_string())?;
        let request_bytes = request_bytes.ok_or_else(|| {
            canonical_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "approval request is missing",
            )
        })?;
        let work = native_work(io);
        let request_file = FileValue::new(
            FileKey::Task {
                task: task.clone(),
                file: TaskFile::ApprovalRequest(request.clone()),
            },
            request_bytes,
        );
        // Stage this exact owner's request, then let the decision's mandatory
        // device barrier cover both files. A fresh binding must not pay a
        // preliminary barrier just to rediscover the request's durable prefix.
        let staging = files.publish_owned(&request_file, work);
        #[cfg(feature = "filesystem-local")]
        let staging = acyclic_stream::deferring_provider_durability(&*self.stream, staging);
        staging.await?;
        let key = FileKey::Task {
            task,
            file: TaskFile::ApprovalDecision {
                request,
                principal: Component::new(principal_hex(principal))?,
            },
        };
        let proof = files
            .publish_durable(&FileValue::new(key.clone(), bytes), work)
            .await?;
        if proof.key() != &key {
            return Err(Error::Storage("durable proof names another file".into()));
        }
        let _request = files.publish_durable(&request_file, work).await?;
        Ok(proof)
    }

    fn approval_task_id(&self) -> Option<crate::TaskId> {
        self.owner
            .as_ref()
            .map(|(owner, _)| owner.task_id())
            .or(self.approval_task)
    }

    /// A task journal bound to an interaction owner always carries its files.
    fn approval_files(&self) -> Result<&crate::task_volume::TaskFiles<P>> {
        self.task_files
            .as_deref()
            .ok_or_else(|| Error::Invalid("task approvals require private task files".into()))
    }
}

fn native_work(io: Option<&PreparationIo>) -> crate::NativeWork<'_> {
    #[cfg(not(target_arch = "wasm32"))]
    return io.and_then(PreparationIo::original_native_work);
    #[cfg(target_arch = "wasm32")]
    io.and(None)
}
