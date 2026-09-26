//! Direct Stream persistence for durable aggregate histories.

use crate::{
    Error, IdempotencyKey, OperationId, Result,
    conversation::{
        ContentPublisher, ContentResidencyVerifier, Limits, ReferencedAttachments,
        verified_attachment_manifest, verified_content_bytes,
    },
    core::{
        Action, ApplyResult, Authority, AuthorityVerifier, Command, EffectStatus, EventPayload,
        EventReference, ExtensionDependency, Reducer, SchemaRegistry, Scope, Snapshot,
    },
    effects::{validate_result_bytes, validate_schema_bytes},
    fork::{CompositeForkVerifier, ForkPreparer, ForkReport, ForkRequest, ForkSeed},
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
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

const READ_PAGE_SIZE: u32 = 1_024;

/// Native, deterministic migration implementation selected by the exact
/// installed target digest. External-effect migrations belong in an effect
/// journal, not this pre-publication transformation boundary.
pub trait ExtensionMigrationProvider: Send + Sync {
    /// Transforms verified prior state into the candidate target state.
    fn migrate(
        &self,
        name: &str,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        previous: &Value,
    ) -> Result<Value>;
}

/// Deterministic, side-effect-free native migration callable.
pub type ExtensionMigrationFn = dyn Fn(&Value) -> Result<Value> + Send + Sync;

/// Exact-version native migration registry for local and customer-hosted runs.
#[derive(Default)]
pub struct NativeExtensionMigrations {
    implementations: BTreeMap<(String, u32, u32, [u8; 32]), Arc<ExtensionMigrationFn>>,
}

impl NativeExtensionMigrations {
    /// Starts an empty registry that rejects unsupported migrations.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            implementations: BTreeMap::new(),
        }
    }

    /// Pins one installed target implementation to a pure state transformation.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        migration: Arc<ExtensionMigrationFn>,
    ) -> Result<()> {
        let name = name.into();
        ExtensionDependency {
            name: name.clone(),
            version: from_version,
        }
        .validate()?;
        ExtensionDependency {
            name: name.clone(),
            version: to_version,
        }
        .validate()?;
        if from_version == to_version || target_implementation_digest == [0; 32] {
            return Err(Error::Invalid(
                "extension migration identity is invalid".into(),
            ));
        }
        let key = (name, from_version, to_version, target_implementation_digest);
        if self.implementations.contains_key(&key) {
            return Err(Error::Conflict(
                "extension migration is already registered".into(),
            ));
        }
        self.implementations.insert(key, migration);
        Ok(())
    }
}

impl ExtensionMigrationProvider for NativeExtensionMigrations {
    fn migrate(
        &self,
        name: &str,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        previous: &Value,
    ) -> Result<Value> {
        self.implementations
            .get(&(
                name.to_owned(),
                from_version,
                to_version,
                target_implementation_digest,
            ))
            .ok_or_else(|| {
                Error::Unsupported("exact extension migration is not installed".into())
            })?(previous)
    }
}

/// Caller-retained identity of one exact state migration. Keeping the source
/// and aggregate CAS revision permits deterministic retry after a lost reply.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExtensionMigrationRequest {
    /// Stable migration operation.
    pub operation_id: OperationId,
    /// Aggregate revision observed when migration was planned.
    pub expected_revision: u64,
    /// Exact prior state event, independent of later extension changes.
    pub previous: EventReference,
    /// Namespaced extension state identity.
    pub name: String,
    /// Installed target implementation version.
    pub to_version: u32,
}

fn extension_migration_command(
    request: ExtensionMigrationRequest,
    scope: Scope,
    content: crate::conversation::FileRef,
) -> Result<Command> {
    Ok(Command {
        operation_id: request.operation_id,
        idempotency_key: IdempotencyKey::new(format!(
            "extension-migration:{}",
            request.operation_id
        ))?,
        expected_revision: request.expected_revision,
        scope,
        causal_parent: None,
        action: Action::MigrateExtensionState {
            name: request.name,
            previous: request.previous,
            to_version: request.to_version,
            content,
        },
    })
}

/// One authoritative reducer whose canonical history is stored directly in Stream.
pub struct StreamAggregate<P> {
    client: StreamClient<P>,
    stream: Stream<P>,
    reducer: Reducer,
    content_verifier: Option<Arc<dyn ContentResidencyVerifier>>,
    fork_verifier: Option<Arc<CompositeForkVerifier>>,
    merge_verifier: Option<Arc<dyn ProjectMergeVerifier>>,
    extension_migrations: Option<Arc<dyn ExtensionMigrationProvider>>,
    limits: Limits,
}

impl<P: StreamProvider> StreamAggregate<P> {
    /// Executes a pinned pure migration, stages its bytes through the owning
    /// provider, then publishes only the resulting ref. The admission path
    /// executes the same migration again before CAS, so a caller cannot
    /// substitute arbitrary staged JSON for the registered implementation.
    pub async fn migrate_extension_state(
        &mut self,
        request: ExtensionMigrationRequest,
        scope: Scope,
        publisher: &dyn ContentPublisher,
    ) -> Result<ApplyResult> {
        if request.previous.authority != *self.reducer.authority()
            || request.previous.revision == 0
            || request.previous.revision > request.expected_revision
            || request.to_version == 0
        {
            return Err(Error::Invalid(
                "extension migration request is invalid".into(),
            ));
        }
        if let Some(revision) = self.reducer.operation_revision(request.operation_id) {
            return self
                .replay_extension_migration(request, scope, publisher, revision)
                .await;
        }
        self.reducer.preflight_extension_migration(
            &scope,
            request.expected_revision,
            &request.previous,
            &request.name,
            request.to_version,
            publisher.volume(),
        )?;
        let output = self.extension_migration_output(&request).await?;
        let path = format!("extensions/migration-{}.json", request.operation_id);
        let content = publisher
            .stage(
                request.operation_id,
                &path,
                &output,
                "application/json",
                "state.json",
            )
            .await?;
        if content.volume() != publisher.volume() {
            return Err(Error::Unauthorized(
                "extension publisher returned another volume".into(),
            ));
        }
        self.execute(extension_migration_command(request, scope, content)?)
            .await
    }

    async fn replay_extension_migration(
        &mut self,
        request: ExtensionMigrationRequest,
        scope: Scope,
        publisher: &dyn ContentPublisher,
        revision: u64,
    ) -> Result<ApplyResult> {
        let event = self
            .reducer
            .events_after(revision - 1, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::Storage("retained migration event is missing".into()))?;
        let EventPayload::ExtensionStateMigrated { migration } = &event.payload else {
            return Err(Error::Conflict(
                "operation belongs to another action".into(),
            ));
        };
        if migration.previous != request.previous
            || migration.record.name != request.name
            || migration.record.version != request.to_version
        {
            return Err(Error::Conflict(
                "operation belongs to another migration".into(),
            ));
        }
        if publisher.volume() != migration.record.content.volume() {
            return Err(Error::Unauthorized(
                "extension migration publisher owns another volume".into(),
            ));
        }
        self.execute(extension_migration_command(
            request,
            scope,
            migration.record.content.clone(),
        )?)
        .await
    }

    async fn extension_migration_output(
        &self,
        request: &ExtensionMigrationRequest,
    ) -> Result<Vec<u8>> {
        let prior_event = self
            .reducer
            .events_after(request.previous.revision - 1, 1)?
            .into_iter()
            .next()
            .ok_or_else(|| Error::NotFound("prior extension state event".into()))?;
        if prior_event.revision != request.previous.revision {
            return Err(Error::Conflict(
                "extension migration source event changed".into(),
            ));
        }
        let prior = match &prior_event.payload {
            EventPayload::Custom { record } => record,
            EventPayload::ExtensionStateMigrated { migration } => &migration.record,
            _ => {
                return Err(Error::Invalid(
                    "migration source is not an extension state".into(),
                ));
            }
        };
        if prior.name != request.name {
            return Err(Error::Conflict(
                "migration source belongs to another extension".into(),
            ));
        }
        if self
            .reducer
            .extension_state(&request.name)
            .map(|(source, _)| source)
            != Some(&request.previous)
        {
            return Err(Error::Conflict("extension migration source changed".into()));
        }
        let verifier = self
            .content_verifier
            .as_ref()
            .ok_or_else(|| Error::Unsupported("extension content verifier is not bound".into()))?;
        let migrations = self.extension_migrations.as_ref().ok_or_else(|| {
            Error::Unsupported("extension migration executor is not bound".into())
        })?;
        self.limits.validate_file(&prior.content)?;
        let bytes = verified_content_bytes(verifier.as_ref(), &prior.content).await?;
        self.reducer
            .validate_custom_bytes(&request.name, prior.version, &prior.content, &bytes)?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|error| {
            Error::Invalid(format!("previous extension state is invalid: {error}"))
        })?;
        let target_digest = self
            .reducer
            .extension_implementation_digest(&request.name, request.to_version)?;
        let migrated = migrations.migrate(
            &request.name,
            prior.version,
            request.to_version,
            target_digest,
            &value,
        )?;
        let output = crate::contract::canonical_json_bytes(&migrated)?;
        if output.len() as u64 > self.limits.file_bytes {
            return Err(Error::Invalid(
                "extension migration exceeds file limit".into(),
            ));
        }
        Ok(output)
    }

    /// Asks an explicit provider to prepare each selected resource at the
    /// parent's exact current revision. No child is published by preparation.
    pub async fn prepare_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<ForkReport> {
        request.validate()?;
        if request.parent != *self.reducer.authority()
            || request.parent_revision != self.reducer.revision()
        {
            return Err(Error::Conflict(
                "fork preparation is not at the parent revision".into(),
            ));
        }
        let report = provider.prepare(request.clone()).await?;
        report.validate()?;
        if report.request != request {
            return Err(Error::Conflict(
                "fork provider returned a different request".into(),
            ));
        }
        Ok(report)
    }

    /// Reconciles the original preparation identity after an unknown reply.
    pub async fn reconcile_fork(
        &self,
        provider: &dyn ForkPreparer,
        request: ForkRequest,
    ) -> Result<Option<ForkReport>> {
        request.validate()?;
        if request.parent != *self.reducer.authority() {
            return Err(Error::Unauthorized(
                "fork request belongs to another parent".into(),
            ));
        }
        let report = provider.reconcile(request.clone()).await?;
        if let Some(report) = &report {
            report.validate()?;
            if report.request != request {
                return Err(Error::Conflict(
                    "reconciled fork belongs to a different request".into(),
                ));
            }
        }
        Ok(report)
    }

    /// Converts a fully captured report to a seed and publishes it only
    /// through the parent's authenticated, conflict-checked Stream append.
    /// Replaying the same operation reconciles a lost publication reply.
    pub async fn publish_fork_report(
        &mut self,
        report: ForkReport,
        scope: Scope,
    ) -> Result<ApplyResult> {
        report.validate()?;
        if report.request.parent != *self.reducer.authority() {
            return Err(Error::Unauthorized(
                "fork report belongs to another parent".into(),
            ));
        }
        let seed = report.into_seed()?;
        self.execute(Command {
            operation_id: seed.operation_id,
            idempotency_key: IdempotencyKey::new(format!(
                "fork-publication:{}",
                seed.operation_id
            ))?,
            expected_revision: seed.parent_revision,
            scope,
            causal_parent: None,
            action: Action::PublishFork {
                seed: Box::new(seed),
            },
        })
        .await
    }

    /// Admits a prepared child through the parent, then binds its fresh
    /// conversation to the published seed. A failed child bind does not undo
    /// parent publication: retry this exact report and scopes to reconcile it.
    /// The child aggregate must have no independent conversation binding.
    pub async fn spawn_from_report(
        &mut self,
        parent: &mut Self,
        report: ForkReport,
        parent_scope: Scope,
        child_scope: Scope,
    ) -> Result<ForkSeed> {
        report.validate()?;
        if report.request.child != *self.reducer.authority()
            || report.request.parent != *parent.reducer.authority()
        {
            return Err(Error::Unauthorized(
                "fork report does not match parent and child aggregates".into(),
            ));
        }
        let seed = report.clone().into_seed()?;
        if child_scope.agent() != Some(seed.child_agent)
            || (self.reducer.revision() != 0 && parent.reducer.fork(&seed.child) != Some(&seed))
        {
            return Err(Error::Conflict(
                "child binding is not fresh or previously published by this parent".into(),
            ));
        }
        parent.publish_fork_report(report, parent_scope).await?;
        self.bind_published_child(parent, &seed, child_scope)
            .await?;
        Ok(seed)
    }

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
            extension_migrations: None,
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

    /// Requires an installed, exact-digest migration implementation before
    /// accepting any `MigrateExtensionState` event.
    #[must_use]
    pub fn with_extension_migrations(
        mut self,
        migrations: Arc<dyn ExtensionMigrationProvider>,
    ) -> Self {
        self.extension_migrations = Some(migrations);
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
        let fresh_migration = matches!(&command.action, Action::MigrateExtensionState { .. })
            && self
                .reducer
                .operation_revision(command.operation_id)
                .is_none();
        let planned = self.plan_command(&command, fresh_migration).await?;
        let ApplyResult::Applied { event } = planned else {
            if let crate::core::Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
            return Ok(planned);
        };
        if !fresh_migration {
            self.validate_admission_content(&command.action).await?;
        }
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
        self.append_planned_command(&command, event, bytes, idempotency_key, fork_guard)
            .await
    }

    async fn plan_command(&self, command: &Command, fresh_migration: bool) -> Result<ApplyResult> {
        if !fresh_migration {
            return self.reducer.plan(command);
        }
        if let Action::MigrateExtensionState {
            name,
            previous,
            to_version,
            content,
        } = &command.action
        {
            self.reducer.preflight_extension_migration(
                &command.scope,
                command.expected_revision,
                previous,
                name,
                *to_version,
                content.volume(),
            )?;
        }
        self.validate_admission_content(&command.action).await?;
        self.reducer.plan_verified_migration(command)
    }

    async fn append_planned_command(
        &mut self,
        command: &Command,
        event: crate::core::Event,
        bytes: Vec<u8>,
        idempotency_key: StreamIdempotencyKey,
        fork_guard: Option<Box<dyn crate::fork::ForkPublicationGuard>>,
    ) -> Result<ApplyResult> {
        let append = self
            .stream
            .append_batch(
                vec![Bytes::from(bytes)],
                Some(command.expected_revision),
                Some(idempotency_key),
            )
            .await;
        let outcome = match append {
            Ok(outcome) => outcome,
            Err(StreamError::Unavailable) => {
                return match self.reconcile_inner(command).await {
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
        self.validate_extension_admission_content(action).await?;
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
            verified_content_bytes(verifier.as_ref(), &message.content).await?;
            match &message.attachments {
                ReferencedAttachments::Inline { items } => {
                    for attachment in items {
                        verified_content_bytes(verifier.as_ref(), &attachment.file).await?;
                    }
                }
                ReferencedAttachments::Manifest {
                    manifest,
                    item_count,
                } => {
                    verified_attachment_manifest(
                        verifier.as_ref(),
                        manifest,
                        *item_count,
                        &self.limits,
                    )
                    .await?;
                }
            }
            for reference in message.extensions.values() {
                verified_content_bytes(verifier.as_ref(), reference).await?;
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
            verified_content_bytes(verifier.as_ref(), request).await?;
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

    async fn validate_extension_admission_content(
        &self,
        action: &crate::core::Action,
    ) -> Result<()> {
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
        if let crate::core::Action::MigrateExtensionState {
            name,
            previous,
            to_version,
            content,
        } = action
        {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let migrations = self.extension_migrations.as_ref().ok_or_else(|| {
                Error::Unsupported("extension migration executor is not bound".into())
            })?;
            let (source, prior) = self
                .reducer
                .extension_state(name)
                .ok_or_else(|| Error::NotFound(format!("extension state {name}")))?;
            if source != previous {
                return Err(Error::Conflict("extension migration source changed".into()));
            }
            self.limits.validate_file(&prior.content)?;
            self.limits.validate_file(content)?;
            let old_bytes = verified_content_bytes(verifier.as_ref(), &prior.content).await?;
            self.reducer
                .validate_custom_bytes(name, prior.version, &prior.content, &old_bytes)?;
            let previous_value: Value = serde_json::from_slice(&old_bytes).map_err(|error| {
                Error::Invalid(format!("previous extension state is invalid: {error}"))
            })?;
            let target_digest = self
                .reducer
                .extension_implementation_digest(name, *to_version)?;
            let expected = migrations.migrate(
                name,
                prior.version,
                *to_version,
                target_digest,
                &previous_value,
            )?;
            let bytes = verified_content_bytes(verifier.as_ref(), content).await?;
            self.reducer
                .validate_custom_bytes(name, *to_version, content, &bytes)?;
            // A direct admission must publish the executor's exact canonical
            // bytes, not merely a JSON value that compares equal after parse.
            // This makes content identity deterministic across host paths.
            if bytes != crate::contract::canonical_json_bytes(&expected)? {
                return Err(Error::Conflict(
                    "staged extension migration differs from its pinned implementation".into(),
                ));
            }
        }
        if let crate::core::Action::ConfigureExtension { extension, content } = action {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let bytes = verifier.read(content).await?;
            self.reducer
                .validate_configuration_bytes(extension, content, &bytes)?;
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
        if result.is_some()
            && let crate::core::Action::PublishFork { seed } = &command.action
        {
            self.release_replayed_fork_fence(seed).await?;
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
        core::{Action, AggregateKind, AuthorityIssuer, ExtensionDependency, ExtensionForkPolicy},
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

    struct TestPublisher {
        volume: VolumeRef,
        stages: std::sync::atomic::AtomicUsize,
        files: MigrationContentStore,
    }

    #[derive(Clone, Default)]
    struct MigrationContentStore(Arc<std::sync::Mutex<BTreeMap<String, Vec<u8>>>>);

    impl MigrationContentStore {
        fn key(reference: &FileRef) -> Result<String> {
            Ok(format!(
                "{}:{}",
                serde_json::to_string(reference.volume())
                    .map_err(|error| Error::Invalid(error.to_string()))?,
                reference.path()
            ))
        }

        fn insert(&self, reference: &FileRef, bytes: &[u8]) -> Result<()> {
            reference.descriptor().verify(bytes)?;
            self.0
                .lock()
                .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
                .insert(Self::key(reference)?, bytes.to_vec());
            Ok(())
        }
    }

    impl ContentResidencyVerifier for MigrationContentStore {
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
                    .lock()
                    .map_err(|_| Error::Storage("migration content lock poisoned".into()))?
                    .get(&Self::key(reference)?)
                    .cloned()
                    .ok_or_else(|| Error::NotFound("migration content is absent".into()))?;
                reference.descriptor().verify(&bytes)?;
                Ok(bytes)
            })
        }
    }

    impl ContentPublisher for TestPublisher {
        fn volume(&self) -> &VolumeRef {
            &self.volume
        }

        fn stage<'a>(
            &'a self,
            _operation_id: OperationId,
            path: &'a str,
            bytes: &'a [u8],
            media_type: &'a str,
            display_name: &'a str,
        ) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<FileRef>> + Send + 'a>>
        {
            Box::pin(async move {
                if bytes != EXTENSION_BYTES || media_type != "application/json" {
                    return Err(Error::Invalid("unexpected migration output".into()));
                }
                self.stages
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                let reference = FileRef::new(
                    self.volume.clone(),
                    path,
                    "generation-1",
                    FileDescriptor::from_bytes(bytes, media_type)?,
                    display_name,
                )?;
                self.files.insert(&reference, bytes)?;
                Ok(reference)
            })
        }
    }

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
    async fn migrated_extension_state_is_validated_before_stream_publication() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut valid_schemas = schemas();
        valid_schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset,
        )?;
        let mut migrations = NativeExtensionMigrations::new();
        migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|value| Ok(value.clone())),
        )?;
        assert!(matches!(
            migrations.register(
                "example.message",
                1,
                2,
                [10; 32],
                Arc::new(|value| Ok(value.clone()))
            ),
            Err(Error::Conflict(_))
        ));
        let migrations = Arc::new(migrations);
        let mut original = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(),
                valid_schemas.clone(),
            )
            .await?,
        );
        original.execute(command(1)?).await?;
        let migration = Command {
            operation_id: OperationId::from_bytes([2; 16]),
            idempotency_key: IdempotencyKey::new("migrate-extension-state")?,
            expected_revision: 1,
            scope: issuer().root_for_agent(
                crate::AgentId::from_bytes([8; 16]),
                "owner",
                Capabilities::new([
                    "extension:migrate".to_owned(),
                    content()?
                        .volume()
                        .capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            ),
            causal_parent: None,
            action: Action::MigrateExtensionState {
                name: "example.message".into(),
                previous: crate::core::EventReference {
                    authority: authority(),
                    revision: 1,
                },
                to_version: 2,
                content: content()?,
            },
        };
        let mut invalid_schemas = schemas();
        invalid_schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"integer"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset,
        )?;
        let mut invalid = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), invalid_schemas)
                .await?,
        )
        .with_extension_migrations(migrations.clone());
        assert!(matches!(
            invalid.execute(migration.clone()).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(invalid.reducer().revision(), 1);
        let mut missing_executor = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(),
                valid_schemas.clone(),
            )
            .await?,
        );
        assert!(matches!(
            missing_executor.execute(migration.clone()).await,
            Err(Error::Unsupported(_))
        ));
        assert_eq!(missing_executor.reducer().revision(), 1);
        let mut wrong_migrations = NativeExtensionMigrations::new();
        wrong_migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|_| Ok(json!({"text":"substituted"}))),
        )?;
        let mut substituted = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(),
                valid_schemas.clone(),
            )
            .await?,
        )
        .with_extension_migrations(Arc::new(wrong_migrations));
        assert!(matches!(
            substituted.execute(migration.clone()).await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(substituted.reducer().revision(), 1);
        let mut valid = with_content(
            StreamAggregate::open(
                &client,
                authority(),
                issuer().verifier(),
                valid_schemas.clone(),
            )
            .await?,
        )
        .with_extension_migrations(migrations);
        valid.execute(migration.clone()).await?;
        assert!(matches!(
            valid.execute(migration).await?,
            ApplyResult::Replayed { .. }
        ));
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(), valid_schemas).await?;
        assert_eq!(
            reopened
                .reducer()
                .extension_state("example.message")
                .map(|(_, state)| state.version),
            Some(2)
        );
        Ok(())
    }

    #[tokio::test]
    async fn extension_migration_retries_without_republishing_content() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut schemas = schemas();
        schemas.register(
            "example.message",
            2,
            json!({"type":"object","required":["text"],"properties":{"text":{"type":"string"}}}),
            [10; 32],
            ExtensionForkPolicy::Reset,
        )?;
        let mut migrations = NativeExtensionMigrations::new();
        migrations.register(
            "example.message",
            1,
            2,
            [10; 32],
            Arc::new(|value| Ok(value.clone())),
        )?;
        let files = MigrationContentStore::default();
        files.insert(&content()?, EXTENSION_BYTES)?;
        let mut aggregate =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas)
                .await?
                .with_content_verifier(Arc::new(files.clone()))
                .with_extension_migrations(Arc::new(migrations));
        aggregate.execute(command(1)?).await?;
        let request = ExtensionMigrationRequest {
            operation_id: OperationId::from_bytes([3; 16]),
            expected_revision: 1,
            previous: EventReference {
                authority: authority(),
                revision: 1,
            },
            name: "example.message".into(),
            to_version: 2,
        };
        let publisher = TestPublisher {
            volume: VolumeRef::new(
                ProviderRef::new("test", "filesystem", "2")?,
                "extension-child",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(crate::AgentId::from_bytes([9; 16])),
            )?,
            stages: std::sync::atomic::AtomicUsize::new(0),
            files: files.clone(),
        };
        let scope = issuer().root_for_agent(
            crate::AgentId::from_bytes([9; 16]),
            "child-owner",
            Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write)?,
            ]),
        );
        let unauthorized = issuer().root_for_agent(
            crate::AgentId::from_bytes([9; 16]),
            "child-unauthorized",
            Capabilities::new([publisher
                .volume
                .capability(crate::conversation::VolumeOperation::Write)?]),
        );
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), unauthorized, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let no_destination_write = issuer().root_for_agent(
            crate::AgentId::from_bytes([9; 16]),
            "child-no-write",
            Capabilities::new(["extension:migrate"]),
        );
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), no_destination_write, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let foreign_writer = issuer().root_for_agent(
            crate::AgentId::from_bytes([8; 16]),
            "foreign-writer",
            Capabilities::new([
                "extension:migrate".to_owned(),
                publisher
                    .volume
                    .capability(crate::conversation::VolumeOperation::Write)?,
            ]),
        );
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), foreign_writer, &publisher)
                .await,
            Err(Error::Unauthorized(_))
        ));
        let mut stale = request.clone();
        stale.expected_revision = 2;
        assert!(matches!(
            aggregate
                .migrate_extension_state(stale, scope.clone(), &publisher)
                .await,
            Err(Error::Conflict(_))
        ));
        assert_eq!(
            publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
            0
        );
        assert!(matches!(
            aggregate
                .migrate_extension_state(request.clone(), scope.clone(), &publisher)
                .await?,
            ApplyResult::Applied { .. }
        ));
        assert!(matches!(
            aggregate
                .migrate_extension_state(request, scope, &publisher)
                .await?,
            ApplyResult::Replayed { .. }
        ));
        assert_eq!(
            publisher.stages.load(std::sync::atomic::Ordering::SeqCst),
            1
        );
        let (_, migrated) = aggregate
            .reducer()
            .extension_state("example.message")
            .ok_or_else(|| Error::NotFound("migrated extension state".into()))?;
        assert_eq!(migrated.content.volume(), &publisher.volume);
        assert_eq!(
            files.read(&migrated.content).await?.as_slice(),
            EXTENSION_BYTES
        );
        Ok(())
    }

    #[tokio::test]
    async fn configured_extension_cannot_publish_a_dangling_or_invalid_reference() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let authority = Authority {
            kind: AggregateKind::Agent,
            id: "configured-agent".into(),
        };
        let issuer = AuthorityIssuer::new("test", [7; 32], authority.clone());
        let extension = ExtensionDependency {
            name: "example.configured".into(),
            version: 1,
        };
        let mut schemas = SchemaRegistry::new();
        schemas.register_configured(
            extension.name.clone(),
            extension.version,
            json!({"type":"object"}),
            [9; 32],
            ExtensionForkPolicy::Inherit,
            [],
            Some(json!({"type":"object","required":["enabled"],"properties":{"enabled":{"type":"boolean"}}})),
        )?;
        let bytes = br#"{"enabled":true}"#;
        let reference = interaction_file("extensions/configuration.json", bytes)?;
        let command = Command {
            operation_id: OperationId::from_bytes([32; 16]),
            idempotency_key: IdempotencyKey("configure-extension".into()),
            expected_revision: 0,
            scope: issuer.root("owner", Capabilities::new(["extension:configure"])),
            causal_parent: None,
            action: Action::ConfigureExtension {
                extension,
                content: reference.clone(),
            },
        };
        let mut aggregate = StreamAggregate::open(
            &client,
            authority.clone(),
            issuer.verifier(),
            schemas.clone(),
        )
        .await?;
        assert!(matches!(
            aggregate.execute(command.clone()).await,
            Err(Error::Unsupported(_))
        ));
        let mut invalid = std::collections::BTreeMap::new();
        invalid.insert(
            reference.path().to_owned(),
            br#"{"enabled":"wrong"}"#.to_vec(),
        );
        let mut aggregate = aggregate.with_content_verifier(Arc::new(InteractionContent(invalid)));
        assert!(aggregate.execute(command.clone()).await.is_err());
        assert_eq!(aggregate.reducer().revision(), 0);
        let mut valid = std::collections::BTreeMap::new();
        valid.insert(reference.path().to_owned(), bytes.to_vec());
        let mut aggregate = aggregate.with_content_verifier(Arc::new(InteractionContent(valid)));
        aggregate.execute(command).await?;
        let reopened =
            StreamAggregate::open(&client, authority, issuer.verifier(), schemas).await?;
        assert_eq!(reopened.reducer().revision(), 1);
        assert!(matches!(
            &reopened.reducer().events_after(0, 1)?[0].payload,
            crate::core::EventPayload::ExtensionConfigured { record, .. }
                if record.content == reference
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
