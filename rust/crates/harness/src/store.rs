//! Direct Stream persistence for durable aggregate histories.

use crate::{
    Error, IdempotencyKey, Result,
    core::{ApplyResult, Authority, AuthorityVerifier, Command, Reducer, SchemaRegistry, Snapshot},
    wire_codec::{decode_event, encode_event},
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;

const READ_PAGE_SIZE: u32 = 1_024;

/// One authoritative reducer whose canonical history is stored directly in Stream.
pub struct StreamAggregate<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    reducer: Reducer,
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Opens and replays an aggregate, treating an absent path as empty.
    pub async fn open(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Result<Self> {
        Self::open_inner(client, authority, authority_verifier, schemas, None).await
    }

    /// Restores an integrity-checked snapshot, then replays its retained suffix.
    ///
    /// Snapshot storage belongs to the Filesystem integration; Stream remains
    /// the canonical event history and the snapshot is only an accelerator.
    pub async fn open_from_snapshot(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Snapshot,
    ) -> Result<Self> {
        Self::open_inner(
            client,
            authority,
            authority_verifier,
            schemas,
            Some(snapshot),
        )
        .await
    }

    async fn open_inner(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Option<Snapshot>,
    ) -> Result<Self> {
        authority_verifier.verify_audience(&authority)?;
        let path = authority.stream_path()?;
        let stream = client
            .stream(path)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let mut reducer = if let Some(snapshot) = snapshot {
            if snapshot.authority != authority {
                return Err(Error::Invalid(
                    "snapshot authority does not match requested aggregate".into(),
                ));
            }
            Reducer::restore(snapshot, authority_verifier, schemas)?
        } else {
            Reducer::new(authority, authority_verifier, schemas)
        };
        let mut from = reducer.revision();
        loop {
            let records = match stream.read(from, READ_PAGE_SIZE).await {
                Ok(records) => records,
                Err(StreamError::NotFound) if from == 0 => break,
                Err(error) => return Err(Error::Storage(error.to_string())),
            };
            let page = records
                .try_collect::<Vec<_>>()
                .await
                .map_err(|error| Error::Storage(error.to_string()))?;
            if page.is_empty() {
                break;
            }
            for record in &page {
                let (event_authority, event) = decode_event(&record.value)?;
                if &event_authority != reducer.authority() {
                    return Err(Error::Storage(
                        "event authority does not match its Stream aggregate".into(),
                    ));
                }
                if event.revision != record.sequence.saturating_add(1) {
                    return Err(Error::Storage(
                        "event revision does not match its Stream sequence".into(),
                    ));
                }
                reducer.apply_committed(event)?;
            }
            from = from
                .checked_add(page.len() as u64)
                .ok_or_else(|| Error::Storage("Stream cursor exhausted".into()))?;
        }
        Ok(Self {
            client: client.clone(),
            stream,
            reducer,
        })
    }

    /// Returns the current deterministic projection.
    #[must_use]
    pub const fn reducer(&self) -> &Reducer {
        &self.reducer
    }

    /// Plans, CAS-appends, and only then applies one command.
    pub async fn execute(&mut self, command: Command) -> Result<ApplyResult> {
        let idempotency_key =
            stream_idempotency_key(self.stream.path().as_str(), &command.idempotency_key)?;
        let planned = self.reducer.plan(&command)?;
        let ApplyResult::Applied { event } = planned else {
            return Ok(planned);
        };
        self.validate_causal_reference(event.causal_parent.as_ref())
            .await?;
        let bytes = encode_event(self.reducer.authority(), &event)?;
        let append = self
            .stream
            .append_batch(
                vec![Bytes::from(bytes)],
                Some(command.expected_revision),
                Some(idempotency_key.clone()),
            )
            .await;
        let outcome = match append {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                return match self.reconcile(&command).await {
                    Ok(Some(result)) => Ok(result),
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(command.operation_id))
                    }
                    Err(error) => Err(error),
                };
            }
            Err(StreamError::IdempotencyMismatch) => {
                return Err(Error::Conflict(
                    "retry identity is already bound to another append".into(),
                ));
            }
            Err(error) => return Err(Error::Storage(error.to_string())),
        };
        match outcome {
            AppendOutcome::Committed(receipt)
                if receipt.start == command.expected_revision
                    && receipt.end == event.revision
                    && receipt.tail == event.revision =>
            {
                self.reducer.apply_committed(event)
            }
            AppendOutcome::Committed(_) => Err(Error::Storage(
                "Stream returned an invalid append receipt".into(),
            )),
            AppendOutcome::TailConflict { actual_tail } => Err(Error::Conflict(format!(
                "expected revision {}, found {actual_tail}",
                command.expected_revision
            ))),
        }
    }

    /// Reconciles a possibly committed append without issuing another append.
    ///
    /// `None` means the provider has no durable observation yet; callers must
    /// retain the original command and operation identity until it resolves.
    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        let planned = self.reducer.plan(command)?;
        if matches!(planned, ApplyResult::Replayed { .. }) {
            return Ok(Some(planned));
        }
        let key = stream_idempotency_key(self.stream.path().as_str(), &command.idempotency_key)?;
        let Some(observation) = self
            .client
            .inspect_idempotency(key)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
        else {
            return Ok(None);
        };
        let IdempotencyOutcome::Append(outcome) = observation.outcome else {
            return Err(Error::Conflict(
                "retry identity is bound to a non-append operation".into(),
            ));
        };
        let receipt = match outcome {
            AppendOutcome::Committed(receipt) => receipt,
            AppendOutcome::TailConflict { actual_tail } => {
                return Err(Error::Conflict(format!(
                    "expected revision {}, found {actual_tail}",
                    command.expected_revision
                )));
            }
        };
        if receipt.start != command.expected_revision
            || receipt.end != command.expected_revision.saturating_add(1)
            || receipt.tail < receipt.end
        {
            return Err(Error::Storage(
                "Stream returned an invalid reconciled append receipt".into(),
            ));
        }
        let records = self
            .stream
            .read(receipt.start, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .first()
            .ok_or_else(|| Error::Storage("reconciled append record is unavailable".into()))?;
        let (authority, event) = decode_event(&record.value)?;
        let ApplyResult::Applied { event: expected } = planned else {
            unreachable!("replayed commands returned above")
        };
        if authority != *self.reducer.authority()
            || event != expected
            || record.sequence != receipt.start
        {
            return Err(Error::Storage(
                "reconciled append does not match the original command".into(),
            ));
        }
        self.reducer.apply_committed(event).map(Some)
    }

    async fn validate_causal_reference(
        &self,
        reference: Option<&crate::core::EventReference>,
    ) -> Result<()> {
        let Some(reference) = reference else {
            return Ok(());
        };
        if reference.authority == *self.reducer.authority() {
            return Ok(());
        }
        let stream = self
            .client
            .stream(reference.authority.stream_path()?)
            .map_err(|error| Error::Storage(error.to_string()))?;
        let records = stream
            .read(reference.revision.saturating_sub(1), 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        let record = records
            .first()
            .ok_or_else(|| Error::NotFound("causal event".into()))?;
        let (authority, event) = decode_event(&record.value)?;
        if authority != reference.authority || event.revision != reference.revision {
            return Err(Error::Invalid(
                "causal event reference does not resolve exactly".into(),
            ));
        }
        Ok(())
    }
}

fn stream_idempotency_key(path: &str, value: &IdempotencyKey) -> Result<StreamIdempotencyKey> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(b"acyclic-harness-stream-append-v1");
    hasher.update(&(path.len() as u64).to_le_bytes());
    hasher.update(path.as_bytes());
    hasher.update(&(value.0.len() as u64).to_le_bytes());
    hasher.update(value.0.as_bytes());
    StreamIdempotencyKey::new(Bytes::copy_from_slice(hasher.finalize().as_bytes()))
        .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        Capabilities, OperationId,
        core::{Action, AggregateKind, AuthorityIssuer},
    };
    use acyclic_stream::MemoryStream;
    use serde_json::json;
    use std::sync::Arc;

    fn authority() -> Authority {
        Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        }
    }

    fn command(identity: u8) -> Command {
        Command {
            operation_id: OperationId::from_bytes([identity; 16]),
            idempotency_key: IdempotencyKey(format!("append-conversation-1-{identity}")),
            expected_revision: 0,
            scope: issuer().root("root", Capabilities::new(["event:append"])),
            causal_parent: None,
            action: Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                value: json!({"text": "hello"}),
            },
        }
    }

    fn issuer() -> AuthorityIssuer {
        AuthorityIssuer::new("test", [7; 32], authority())
    }

    fn schemas() -> SchemaRegistry {
        let mut schemas = SchemaRegistry::new();
        assert!(
            schemas
                .register("example.message", 1, json!({"type": "object"}))
                .is_ok()
        );
        schemas
    }

    #[tokio::test]
    async fn commit_then_reopen_replays_the_same_event() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(Arc::clone(&provider));
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        let first = aggregate.execute(command(1)).await?;
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert_eq!(reopened.reducer().revision(), 1);
        assert_eq!(
            reopened.reducer().events_after(0, 1)?,
            vec![match first {
                ApplyResult::Applied { event } | ApplyResult::Replayed { event } => event,
            }]
        );
        Ok(())
    }

    #[tokio::test]
    async fn verifier_must_match_the_exact_aggregate_audience() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let foreign = AuthorityIssuer::new(
            "test",
            [7; 32],
            Authority {
                kind: AggregateKind::Task,
                id: authority().id.clone(),
            },
        );
        assert!(matches!(
            StreamAggregate::open(&client, authority(), foreign.verifier(), schemas()).await,
            Err(Error::Unauthorized(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn stale_writer_does_not_advance_its_projection() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(provider);
        let mut first =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        let mut stale =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        first.execute(command(1)).await?;
        assert!(matches!(
            stale.execute(command(2)).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(stale.reducer().revision(), 0);
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_observes_a_commit_without_redispatch() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(provider);
        let mut writer =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        let mut uncertain =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        let submitted = command(1);
        writer.execute(submitted.clone()).await?;
        assert!(matches!(
            uncertain.reconcile(&submitted).await?,
            Some(ApplyResult::Applied { .. })
        ));
        assert!(matches!(
            uncertain.reconcile(&submitted).await?,
            Some(ApplyResult::Replayed { .. })
        ));
        assert_eq!(uncertain.reducer().revision(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn canonical_paths_bind_aggregate_authority() -> Result<()> {
        assert_eq!(
            authority().stream_path()?,
            "harness/conversations/conversation-1"
        );
        assert_ne!(
            authority().stream_path()?,
            Authority {
                kind: AggregateKind::Task,
                id: "conversation-1".into(),
            }
            .stream_path()?
        );
        assert!(
            Authority {
                kind: AggregateKind::Task,
                id: "../escape".into(),
            }
            .stream_path()
            .is_err()
        );
        Ok(())
    }

    #[tokio::test]
    async fn snapshot_reopens_a_trimmed_stream_suffix() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(provider);
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        aggregate.execute(command(1)).await?;
        let mut second = command(2);
        second.expected_revision = 1;
        aggregate.execute(second).await?;
        let snapshot = aggregate.reducer().snapshot()?;
        let mut third = command(3);
        third.expected_revision = 2;
        aggregate.execute(third).await?;
        client
            .stream(authority().stream_path()?)
            .map_err(|error| Error::Storage(error.to_string()))?
            .trim(2, None)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;

        let reopened = StreamAggregate::open_from_snapshot(
            &client,
            authority(),
            issuer().verifier(),
            schemas(),
            snapshot,
        )
        .await?;
        assert_eq!(reopened.reducer().revision(), 3);
        Ok(())
    }
}
