use super::*;
use crate::core::OwnedBytes;
use crate::{executor::with_history_io, store::history::HistoryIo, wire_codec::CodecQuota};

impl<P, A, O> FilesystemInteractionHost<P, A, O>
where
    P: StreamProvider + ?Sized,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Admits an open event.
    pub async fn open(
        &self,
        operation_id: OperationId,
        scope: Option<&Scope>,
        ticket: &InteractionTicket,
        io: Option<&PreparationIo>,
    ) -> Result<ApplyResult> {
        let mut transaction = self.transaction(io).await?;
        self.open_at(operation_id, scope, ticket, io, &mut transaction)
            .await
    }

    pub(in crate::filesystem) async fn open_at(
        &self,
        operation_id: OperationId,
        scope: Option<&Scope>,
        ticket: &InteractionTicket,
        io: Option<&PreparationIo>,
        transaction: &mut InteractionTransaction<'_, P>,
    ) -> Result<ApplyResult> {
        let (copied, retained) = with_history_io(io, |work| {
            CodecQuota::produce(work, |quota| {
                crate::core::copy(
                    ticket,
                    quota,
                    |value: &crate::interaction::InteractionTicket, work| {
                        value.request.validate(work)?;
                        value.owned_heap_bytes()
                    },
                )
            })
        })?;
        let hold = io.map(|io| io.guard_owned(retained)).transpose()?;
        let result = self
            .execute(
                operation_id,
                scope.unwrap_or(&self.owner_scope),
                Action::OpenInteraction {
                    ticket: (copied).into(),
                },
                io,
                transaction,
            )
            .await;
        drop(hold);
        result
    }

    pub(in crate::filesystem) async fn execute(
        &self,
        operation_id: OperationId,
        scope: &Scope,
        action: Action,
        io: Option<&PreparationIo>,
        transaction: &mut InteractionTransaction<'_, P>,
    ) -> Result<ApplyResult> {
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|work| work as &mut dyn HistoryIo);
        let aggregate = &mut transaction.aggregate.0;
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
        let hold = io
            .map(|io| io.guard_owned(committed.as_ref().map_or(0, |(_, bytes)| *bytes)))
            .transpose()?;
        let expected_revision = match &committed {
            Some((event, _)) => event.revision.checked_sub(1).ok_or_else(|| {
                interaction_boundary(
                    io,
                    ErrorCode::Invalid,
                    "committed interaction revision is zero",
                )
            })?,
            None => aggregate.reducer().revision(),
        };
        drop((committed, hold));
        let ((scope, idempotency_key), retained) = CodecQuota::produce(&mut work, |quota| {
            Ok((
                scope.copy_in_quota(quota)?,
                IdempotencyKey::new(quota.format(format_args!("interaction:{operation_id}"))?)?,
            ))
        })?;
        let hold = io.map(|io| io.guard_owned(retained)).transpose()?;
        let command = Command {
            operation_id,
            idempotency_key,
            expected_revision,
            scope,
            causal_parent: None,
            action,
        };
        let result = aggregate.execute(command, &mut work).await;
        drop(hold);
        result
    }

    /// Serializes every interaction writer of this provider/conversation before
    /// reconstructing its cut. The provider owns the registry, so newly composed
    /// passive and leased hosts cannot accidentally acquire different locks.
    pub(in crate::filesystem) async fn transaction<'a>(
        &'a self,
        io: Option<&'a PreparationIo>,
    ) -> Result<InteractionTransaction<'a, P>> {
        let digest = with_history_io(io, |work| {
            crate::executor::json::canonical_json_digest(
                &("harness/v2/interaction-writer", self.verifier.audience()),
                acyclic_stream::MAX_RECORD_BYTES as u64,
                work,
            )
        })?;
        let mut key = [0; 48];
        key[..32].copy_from_slice(&digest);
        key[32..].copy_from_slice(b"interaction-lock");
        let claims = self.stream.operation_claims();
        #[cfg(test)]
        acyclic_test_support::differential::record(("interaction-writer-attempt", 1_u64));
        let writer = loop {
            if let Some(writer) = claims.claim(
                key,
                || crate::model::charge_items(io, 1),
                |bytes| {
                    io.map(|io| {
                        let hold = io.reserve_decode(bytes)?;
                        io.allocation_attempt(bytes)?;
                        Ok::<_, Error>(hold)
                    })
                    .transpose()
                },
            )? {
                break writer;
            }
            // Waiting acquires ownership; no failed publication is resubmitted.
            claims.wait_until_released(key).await;
        };
        #[cfg(test)]
        acyclic_test_support::differential::record(("interaction-writer-acquired", 1_u64));
        Ok(InteractionTransaction {
            aggregate: self.aggregate(io).await?,
            _writer: writer,
        })
    }

    pub(in crate::filesystem) async fn aggregate(
        &self,
        io: Option<&PreparationIo>,
    ) -> Result<InteractionProjection<P>> {
        #[cfg(test)]
        acyclic_test_support::differential::record(("interaction-reconstruction", 1_u64));
        let custody = io.map(PreparationIo::scoped).transpose()?;
        let io = custody.as_ref();
        let mut history = io.map(PreparationIo::history_io);
        let mut work = history.as_mut().map(|work| work as &mut dyn HistoryIo);
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
            (authority, verifier, schemas),
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
                    "interaction conversation has no agent",
                )
            })?;
        if self.private_volume.owner() != &VolumeOwner::Agent(agent) {
            return Err(interaction_boundary(
                io,
                ErrorCode::Unauthorized,
                "interaction private volume belongs to another agent",
            ));
        }
        Ok((aggregate, custody))
    }
}
