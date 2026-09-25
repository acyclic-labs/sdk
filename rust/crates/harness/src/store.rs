//! Direct Stream persistence for durable aggregate histories.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{ContentResidencyVerifier, Limits, ReferencedAttachments},
    core::{
        Action, ApplyResult, Authority, AuthorityVerifier, Command, EffectStatus, Reducer,
        SchemaRegistry, Scope, Snapshot,
    },
    effects::{validate_result_bytes, validate_schema_bytes},
    fork::{CompositeForkVerifier, ForkSeed},
    interaction::InteractionOutcome,
    merge::ProjectMergeVerifier,
    wire_codec::{decode_event, encode_event},
};
use acyclic_stream::{
    AppendOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use std::sync::Arc;

const READ_PAGE_SIZE: u32 = 1_024;

/// One authoritative reducer whose canonical history is stored directly in Stream.
pub struct StreamAggregate<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    reducer: Reducer,
    content_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    fork_verifier: Option<Arc<CompositeForkVerifier>>,
    merge_verifier: Option<Arc<dyn ProjectMergeVerifier>>,
    limits: Limits,
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Binds a child conversation only after its exact parent seed is already
    /// committed. A retry observes the same immutable agent binding.
    pub async fn bind_published_child(
        &mut self,
        parent: &Self,
        seed: &ForkSeed,
        scope: Scope,
    ) -> Result<()> {
        seed.validate()?;
        if parent.reducer.authority() != &seed.parent
            || parent.reducer.fork(&seed.child) != Some(seed)
            || self.reducer.authority() != &seed.child
        {
            return Err(Error::Unauthorized(
                "child binding has no published parent fork".into(),
            ));
        }
        if scope.agent() != Some(seed.child_agent) {
            return Err(Error::Unauthorized(
                "child binding scope belongs to another agent".into(),
            ));
        }
        let fork_revision = seed
            .parent_revision
            .checked_add(1)
            .ok_or_else(|| Error::Invalid("fork parent revision overflow".into()))?;
        let fork_event = parent
            .reducer
            .events_after(seed.parent_revision, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::NotFound("published parent fork".into()))?;
        if fork_event.revision != fork_revision
            || fork_event.operation_id != seed.operation_id
            || !matches!(&fork_event.payload,
                crate::core::EventPayload::ForkPublished { seed: published }
                if published.as_ref() == seed)
        {
            return Err(Error::Conflict(
                "parent fork event does not match the child seed".into(),
            ));
        }
        let causal_parent = crate::core::EventReference {
            authority: seed.parent.clone(),
            revision: fork_revision,
        };
        let mut hash = blake3::Hasher::new();
        hash.update(b"harness/v2/fork-child-bind\0");
        hash.update(&seed.operation_id.into_bytes());
        let mut identity = [0; 16];
        identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
        let bind_operation = OperationId::from_bytes(identity);
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| Error::Invalid("fork child is not a conversation".into()))?;
        if conversation.agent == Some(seed.child_agent) {
            let first = self.reducer.events_after(0, 1)?.into_iter().next();
            if first.is_some_and(|event| {
                event.revision == 1
                    && event.operation_id == bind_operation
                    && event.causal_parent == Some(causal_parent.clone())
                    && matches!(event.payload,
                    crate::core::EventPayload::ConversationBound { agent }
                    if agent == seed.child_agent)
            }) {
                return Ok(());
            }
            return Err(Error::Conflict(
                "child conversation was bound outside its published fork".into(),
            ));
        }
        if self.reducer.revision() != 0 || conversation.agent.is_some() {
            return Err(Error::Conflict(
                "fork child already has a different history".into(),
            ));
        }
        self.execute(Command {
            operation_id: bind_operation,
            idempotency_key: IdempotencyKey::new(format!("fork-child-bind:{}", seed.operation_id))?,
            expected_revision: 0,
            scope,
            causal_parent: Some(causal_parent),
            action: Action::BindConversation {
                agent: seed.child_agent,
            },
        })
        .await?;
        Ok(())
    }

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
            content_verifier: None,
            fork_verifier: None,
            merge_verifier: None,
            limits: Limits::default(),
        })
    }

    /// Installs the provider boundary that verifies every message file before admission.
    #[must_use]
    pub fn with_content_verifier(mut self, verifier: Arc<dyn ContentResidencyVerifier>) -> Self {
        self.content_verifier = Some(verifier);
        self
    }

    /// Installs the provider boundary that checks a prepared child before publication.
    #[must_use]
    pub fn with_fork_verifier(mut self, verifier: Arc<CompositeForkVerifier>) -> Self {
        self.fork_verifier = Some(verifier);
        self
    }

    /// Installs the provider proof for parent-controlled project joins.
    #[must_use]
    pub fn with_merge_verifier(mut self, verifier: Arc<dyn ProjectMergeVerifier>) -> Self {
        self.merge_verifier = Some(verifier);
        self
    }

    /// Configures content and projection bounds for future admissions.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
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
            if let crate::core::Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
            return Ok(planned);
        };
        self.validate_admission_content(&command.action).await?;
        self.validate_causal_reference(event.causal_parent.as_ref())
            .await?;
        let bytes = encode_event(self.reducer.authority(), &event)?;
        // The private-volume reservation spans the final scan and the Stream
        // append. An unknown append result retains the durable reservation.
        let fork_guard = if let crate::core::Action::PublishFork { seed } = &command.action {
            let verifier = self
                .fork_verifier
                .as_ref()
                .ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?;
            Some(verifier.activate(seed).await?)
        } else {
            None
        };
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
                return match self.reconcile_inner(&command).await {
                    Ok(Some(result)) => {
                        if let Some(guard) = fork_guard {
                            guard
                                .release()
                                .await
                                .map_err(|_| Error::Indeterminate(command.operation_id))?;
                        }
                        Ok(result)
                    }
                    Ok(None) | Err(Error::Storage(_)) => {
                        Err(Error::Indeterminate(command.operation_id))
                    }
                    Err(error) => Err(error),
                };
            }
            Err(StreamError::IdempotencyMismatch) => {
                if let Some(guard) = fork_guard {
                    guard
                        .release()
                        .await
                        .map_err(|_| Error::Indeterminate(command.operation_id))?;
                }
                return Err(Error::Conflict(
                    "retry identity is already bound to another append".into(),
                ));
            }
            Err(error) => {
                if let Some(guard) = fork_guard {
                    guard
                        .release()
                        .await
                        .map_err(|_| Error::Indeterminate(command.operation_id))?;
                }
                return Err(Error::Storage(error.to_string()));
            }
        };
        let result = match outcome {
            AppendOutcome::Committed(receipt)
                if receipt.start == command.expected_revision
                    && receipt.end == event.revision
                    && receipt.tail == event.revision =>
            {
                match self.reducer.apply_committed(event) {
                    Ok(result) => Ok(result),
                    Err(_) => return Err(Error::Indeterminate(command.operation_id)),
                }
            }
            // A provider that claims a commit but returns a contradictory
            // receipt has not proved non-publication. Keep the private gate
            // held for reconciliation rather than exposing unselected state.
            AppendOutcome::Committed(_) => return Err(Error::Indeterminate(command.operation_id)),
            AppendOutcome::TailConflict { actual_tail } => Err(Error::Conflict(format!(
                "expected revision {}, found {actual_tail}",
                command.expected_revision
            ))),
        };
        if let Some(guard) = fork_guard {
            guard
                .release()
                .await
                .map_err(|_| Error::Indeterminate(command.operation_id))?;
        }
        result
    }

    async fn validate_admission_content(&self, action: &crate::core::Action) -> Result<()> {
        self.validate_interaction_admission(action).await?;
        if let crate::core::Action::AppendCustom {
            schema,
            version,
            content,
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let bytes = verifier.read(content).await?;
            self.reducer
                .validate_custom_bytes(schema, *version, content, &bytes)?;
        }
        let message = match action {
            crate::core::Action::AppendConversationMessage { message } => Some(message.as_ref()),
            crate::core::Action::PublishProjectMerge { receipt } => Some(&receipt.notice),
            _ => None,
        };
        if let Some(message) = message {
            self.limits.validate_message(message)?;
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("conversation content verifier is not bound".into())
            })?;
            verifier.verify(&message.content).await?;
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for attachment in items {
                        verifier.verify(&attachment.file).await?;
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    verifier
                        .verify_manifest(manifest, *item_count, &self.limits)
                        .await?;
                }
            }
            for reference in message.extensions.values() {
                verifier.verify(reference).await?;
            }
        }
        if let crate::core::Action::PlanEffect {
            request,
            result_schema,
            ..
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("effect request content verifier is not bound".into())
            })?;
            verifier.verify(request).await?;
            let schema_bytes = verifier.read(result_schema).await?;
            validate_schema_bytes(result_schema, &schema_bytes)?;
        }
        if let crate::core::Action::ResolveEffect { observation } = action
            && let EffectStatus::Succeeded { result } = &observation.status
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("effect result content verifier is not bound".into())
            })?;
            let effect = self
                .reducer
                .effect(observation.effect_id)
                .ok_or_else(|| Error::NotFound(format!("effect {}", observation.effect_id)))?;
            let bytes = verifier.read(result).await?;
            let schema_bytes = verifier.read(&effect.result_schema).await?;
            let schema = validate_schema_bytes(&effect.result_schema, &schema_bytes)?;
            validate_result_bytes(&schema, result, &bytes)?;
        }
        if let crate::core::Action::PublishProjectMerge { receipt } = action {
            let verifier = self
                .merge_verifier
                .as_ref()
                .ok_or_else(|| Error::Unsupported("project merge verifier is not bound".into()))?;
            verifier.verify(receipt).await?;
        }
        Ok(())
    }

    async fn validate_interaction_admission(&self, action: &crate::core::Action) -> Result<()> {
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && matches!(resolution.outcome, InteractionOutcome::Expired)
        {
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let deadline = ticket
                .deadline_unix_ms
                .ok_or_else(|| Error::Invalid("interaction has no expiry deadline".into()))?;
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(|error| Error::Storage(error.to_string()))?
                .as_millis();
            if now < u128::from(deadline) {
                return Err(Error::Conflict(
                    "interaction deadline has not elapsed".into(),
                ));
            }
        }
        if let crate::core::Action::OpenInteraction { ticket } = action {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let bytes = verifier.read(&ticket.request).await?;
            ticket.validate_request_bytes(&bytes)?;
        }
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && let InteractionOutcome::Answered { answer } = &resolution.outcome
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let request_bytes = verifier.read(&ticket.request).await?;
            let request = ticket.validate_request_bytes(&request_bytes)?;
            let answer_bytes = verifier.read(answer).await?;
            ticket.validate_answer_bytes(&request, answer, &answer_bytes)?;
        }
        if let crate::core::Action::ResolveInteraction { resolution } = action
            && let Some(detail) = &resolution.detail
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("interaction content verifier is not bound".into())
            })?;
            let (ticket, _) = self
                .reducer
                .interaction(&resolution.id)
                .ok_or_else(|| Error::NotFound(format!("interaction {}", resolution.id)))?;
            let request_bytes = verifier.read(&ticket.request).await?;
            let request = ticket.validate_request_bytes(&request_bytes)?;
            let detail_bytes = verifier.read(detail).await?;
            ticket.validate_decision_bytes(&request, &resolution.outcome, detail, &detail_bytes)?;
        }
        Ok(())
    }

    /// Reconciles a possibly committed append without issuing another append.
    ///
    /// `None` means the provider has no durable observation yet; callers must
    /// retain the original command and operation identity until it resolves.
    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        let result = self.reconcile_inner(command).await?;
        if result.is_some() {
            if let crate::core::Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
        }
        Ok(result)
    }

    async fn release_replayed_fork_fence(&self, seed: &crate::fork::ForkSeed) -> Result<()> {
        let verifier = self
            .fork_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("fork seed verifier is not bound".into()))?;
        verifier
            .release_private_fence(seed)
            .await
            .map_err(|_| Error::Indeterminate(seed.operation_id))
    }

    async fn reconcile_inner(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        let planned = self.reducer.plan(command);
        if matches!(&planned, Ok(ApplyResult::Replayed { .. })) {
            return planned.map(Some);
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
            if let crate::core::Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
            return Err(Error::Conflict(
                "retry identity is bound to a non-append operation".into(),
            ));
        };
        let receipt = match outcome {
            AppendOutcome::Committed(receipt) => receipt,
            AppendOutcome::TailConflict { actual_tail } => {
                // The Stream has durably rejected this append. A previously
                // ambiguous response may have left the private gate held.
                if let crate::core::Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                return Err(Error::Conflict(format!(
                    "expected revision {}, found {actual_tail}",
                    command.expected_revision
                )));
            }
        };
        // A stale local reducer can no longer plan the original command after
        // another writer wins the tail. Inspecting the durable retry outcome
        // first lets the terminal-conflict branch release a held fork fence.
        let planned = planned?;
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
    hasher.update(b"acyclic-harness-stream-append-v2");
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
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        core::{Action, AggregateKind, AuthorityIssuer},
        interaction::{
            Interaction, InteractionKind, InteractionOutcome, InteractionResolution,
            InteractionTicket,
        },
        resources::ProviderRef,
    };
    use acyclic_stream::MemoryStream;
    use serde_json::json;
    use std::sync::Arc;

    struct InteractionContent(std::collections::BTreeMap<String, Vec<u8>>);

    impl ContentResidencyVerifier for InteractionContent {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { self.read(reference).await.map(|_| ()) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>>
        {
            Box::pin(async move {
                let bytes = self
                    .0
                    .get(reference.path())
                    .ok_or_else(|| Error::NotFound(reference.path().into()))?;
                reference.descriptor().verify(bytes)?;
                Ok(bytes.clone())
            })
        }
    }

    fn interaction_file(path: &str, bytes: &[u8]) -> Result<FileRef> {
        FileRef::new(
            content()?.volume().clone(),
            path,
            "generation-1",
            FileDescriptor::from_bytes(bytes, "application/json")?,
            "interaction.json",
        )
    }

    fn authority() -> Authority {
        Authority {
            kind: AggregateKind::Conversation,
            id: "conversation-1".into(),
        }
    }

    fn command(identity: u8) -> Result<Command> {
        Ok(Command {
            operation_id: OperationId::from_bytes([identity; 16]),
            idempotency_key: IdempotencyKey(format!("append-conversation-1-{identity}")),
            expected_revision: 0,
            scope: issuer().root("root", Capabilities::new(["event:append"])),
            causal_parent: None,
            action: Action::AppendCustom {
                schema: "example.message".into(),
                version: 1,
                content: content()?,
            },
        })
    }

    const EXTENSION_BYTES: &[u8] = br#"{"text":"hello"}"#;

    fn content() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "extension",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([8; 16])),
            )?,
            "extension/event.json",
            "generation-1",
            FileDescriptor::from_bytes(EXTENSION_BYTES, "application/json")?,
            "event.json",
        )
    }

    struct TestContent;

    impl ContentResidencyVerifier for TestContent {
        fn verify<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send + 'a>> {
            Box::pin(async move { reference.descriptor().verify(EXTENSION_BYTES) })
        }

        fn read<'a>(
            &'a self,
            reference: &'a FileRef,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send + 'a>>
        {
            Box::pin(async move {
                reference.descriptor().verify(EXTENSION_BYTES)?;
                Ok(EXTENSION_BYTES.to_vec())
            })
        }
    }

    fn with_content<P: StreamProvider>(aggregate: StreamAggregate<P>) -> StreamAggregate<P> {
        aggregate.with_content_verifier(Arc::new(TestContent))
    }

    fn issuer() -> AuthorityIssuer {
        AuthorityIssuer::new("test", [7; 32], authority())
    }

    fn schemas() -> SchemaRegistry {
        let mut schemas = SchemaRegistry::new();
        assert!(
            schemas
                .register(
                    "example.message",
                    1,
                    json!({"type": "object"}),
                    [9; 32],
                    crate::core::ExtensionForkPolicy::Inherit
                )
                .is_ok()
        );
        schemas
    }

    #[tokio::test]
    async fn commit_then_reopen_replays_the_same_event() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(Arc::clone(&provider));
        let mut aggregate = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        let first = aggregate.execute(command(1)?).await?;
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
    async fn extension_bytes_are_validated_before_ref_only_publication() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(provider);
        let mut unbound =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert!(matches!(
            unbound.execute(command(1)?).await,
            Err(Error::Unsupported(_))
        ));
        let mut wrong_schemas = SchemaRegistry::new();
        wrong_schemas.register(
            "example.message",
            1,
            json!({
                "type": "object", "properties": {"text": {"type": "integer"}},
                "required": ["text"]
            }),
            [9; 32],
            crate::core::ExtensionForkPolicy::Inherit,
        )?;
        let mut invalid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), wrong_schemas).await?,
        );
        assert!(matches!(
            invalid.execute(command(1)?).await,
            Err(Error::Invalid(_))
        ));
        let mut valid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        valid.execute(command(1)?).await?;
        let records = client
            .stream(authority().stream_path()?)
            .map_err(|error| Error::Storage(error.to_string()))?
            .read(0, 1)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(records.len(), 1);
        assert!(
            !records[0]
                .value
                .windows(b"hello".len())
                .any(|window| window == b"hello")
        );
        assert!(matches!(
            decode_event(&records[0].value)?.1.payload,
            crate::core::EventPayload::Custom { record } if record.content == content()?
        ));
        Ok(())
    }

    #[tokio::test]
    async fn interaction_request_and_answer_commit_only_after_exact_content_validation()
    -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let request =
            Interaction::question("secret prompt", json!({"type": "string", "minLength": 2}))?;
        let request_bytes =
            serde_json::to_vec(&request).map_err(|error| Error::Invalid(error.to_string()))?;
        let answer_bytes = br#"{"type":"question","value":"yes"}"#;
        let ticket = InteractionTicket {
            id: uuid::Uuid::from_bytes([3; 16]),
            kind: InteractionKind::Question,
            request: interaction_file("interactions/request.json", &request_bytes)?,
            deadline_unix_ms: None,
            approval: None,
        };
        let answer = interaction_file("interactions/answer.json", answer_bytes)?;
        let mut bytes = std::collections::BTreeMap::new();
        bytes.insert(ticket.request.path().to_owned(), request_bytes.clone());
        bytes.insert(answer.path().to_owned(), answer_bytes.to_vec());
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                .await?
                .with_content_verifier(Arc::new(InteractionContent(bytes)));
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([9; 16]),
                idempotency_key: IdempotencyKey("bind-interaction-owner".into()),
                expected_revision: 0,
                scope: issuer().root("owner", Capabilities::new(["conversation:bind"])),
                causal_parent: None,
                action: Action::BindConversation {
                    agent: crate::AgentId::from_bytes([8; 16]),
                },
            })
            .await?;
        let open = Command {
            operation_id: OperationId::from_bytes([10; 16]),
            idempotency_key: IdempotencyKey("open-interaction".into()),
            expected_revision: 1,
            scope: issuer().root("opener", Capabilities::new(["interaction:open"])),
            causal_parent: None,
            action: Action::OpenInteraction {
                ticket: ticket.clone(),
            },
        };
        aggregate.execute(open).await?;
        let resolution = InteractionResolution {
            id: ticket.id,
            expected_version: 1,
            outcome: InteractionOutcome::Answered {
                answer: Box::new(answer),
            },
            detail: None,
        };
        let resolve = Command {
            operation_id: OperationId::from_bytes([11; 16]),
            idempotency_key: IdempotencyKey("resolve-interaction".into()),
            expected_revision: 2,
            scope: issuer().root(
                "responder",
                Capabilities::new(["interaction:resolve", ticket.responder_grant().as_str()]),
            ),
            causal_parent: None,
            action: Action::ResolveInteraction {
                resolution: resolution.clone(),
            },
        };
        aggregate.execute(resolve).await?;
        let records = client
            .stream(authority().stream_path()?)
            .map_err(|error| Error::Storage(error.to_string()))?
            .read(0, 3)
            .await
            .map_err(|error| Error::Storage(error.to_string()))?
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| Error::Storage(error.to_string()))?;
        assert_eq!(records.len(), 3);
        assert!(records.iter().all(|record| {
            !record
                .value
                .windows(b"secret prompt".len())
                .any(|window| window == b"secret prompt")
        }));
        assert!(records.iter().all(|record| {
            !record
                .value
                .windows(answer_bytes.len())
                .any(|window| window == answer_bytes)
        }));
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert_eq!(
            reopened
                .reducer()
                .interaction(&ticket.id)
                .and_then(|(_, outcome)| outcome.as_ref()),
            Some(&resolution)
        );
        let future = InteractionTicket {
            id: uuid::Uuid::from_bytes([4; 16]),
            deadline_unix_ms: Some(u64::MAX),
            ..ticket
        };
        aggregate
            .execute(Command {
                operation_id: OperationId::from_bytes([12; 16]),
                idempotency_key: IdempotencyKey("open-future-interaction".into()),
                expected_revision: 3,
                scope: issuer().root("opener", Capabilities::new(["interaction:open"])),
                causal_parent: None,
                action: Action::OpenInteraction {
                    ticket: future.clone(),
                },
            })
            .await?;
        let premature = Command {
            operation_id: OperationId::from_bytes([13; 16]),
            idempotency_key: IdempotencyKey("premature-expiry".into()),
            expected_revision: 4,
            scope: issuer().root(
                "responder",
                Capabilities::new(["interaction:resolve", future.responder_grant().as_str()]),
            ),
            causal_parent: None,
            action: Action::ResolveInteraction {
                resolution: InteractionResolution {
                    id: future.id,
                    expected_version: 1,
                    outcome: InteractionOutcome::Expired,
                    detail: None,
                },
            },
        };
        assert!(matches!(
            aggregate.execute(premature).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(aggregate.reducer().revision(), 4);
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
                id: authority().id,
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
        let mut first = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        let mut stale = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        first.execute(command(1)?).await?;
        let rejected = command(2)?;
        assert!(matches!(
            stale.execute(rejected.clone()).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(stale.reducer().revision(), 0);
        let mut reopened = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        assert!(matches!(
            reopened.reconcile(&rejected).await,
            Err(Error::Conflict(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn reconciliation_observes_a_commit_without_redispatch() -> Result<()> {
        let provider = Arc::new(MemoryStream::default());
        let client = StreamClient::new(provider);
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        let mut uncertain = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        let submitted = command(1)?;
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
            "harness/v2/conversations/conversation-1"
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
        let mut aggregate = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        aggregate.execute(command(1)?).await?;
        let mut second = command(2)?;
        second.expected_revision = 1;
        aggregate.execute(second).await?;
        let snapshot = aggregate.reducer().snapshot()?;
        let mut third = command(3)?;
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
