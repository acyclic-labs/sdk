//! Direct Stream persistence for durable aggregate histories.

mod checkpoints;
mod effects;
mod history;
pub use checkpoints::{DEFAULT_PROJECTION_EVENTS, default_projection_read_limits};
pub(crate) mod operations;
pub use history::*;

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
    wire_codec::decode_event,
};
use acyclic_stream::{
    CommitOutcome, IdempotencyKey as StreamIdempotencyKey, IdempotencyOutcome, Stream,
    StreamClient, StreamError, StreamProvider,
};
use bytes::Bytes;
use futures::TryStreamExt as _;
use serde_json::Value;
use std::{collections::BTreeMap, sync::Arc};

fn fork_child_binding(seed: &ForkSeed) -> Result<(OperationId, EventReference)> {
    let revision = seed
        .parent_revision
        .checked_add(1)
        .ok_or_else(|| Error::Invalid("fork parent revision overflow".into()))?;
    let mut hash = blake3::Hasher::new();
    hash.update(b"harness/v2/fork-child-bind\0");
    hash.update(&seed.operation_id.into_bytes());
    let mut identity = [0; 16];
    identity.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    Ok((
        OperationId::from_bytes(identity),
        EventReference {
            authority: seed.parent.clone(),
            revision,
        },
    ))
}

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
        if let Some(revision) = self.operation_revision(request.operation_id).await? {
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
        let event = self.read_event_at(revision).await?;
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
        let prior_event = self.read_event_at(request.previous.revision).await?;
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
        let value: Value = crate::contract::json_from_slice(&bytes).map_err(|error| {
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
        let fork_event = parent.read_event_at(fork_revision).await?;
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
        let (bind_operation, causal_parent) = fork_child_binding(seed)?;
        let conversation = self
            .reducer
            .conversation()
            .ok_or_else(|| Error::Invalid("fork child is not a conversation".into()))?;
        if conversation.agent == Some(seed.child_agent) {
            let first = Some(self.read_event_at(1).await?);
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
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.open",
            level = "info",
            skip_all,
            fields(rev = crate::obs::Empty, items = crate::obs::Empty, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn open(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
    ) -> Result<Self> {
        crate::obs::outcome(
            Self::open_with_read_limits(
                client,
                authority,
                authority_verifier,
                schemas,
                default_projection_read_limits(),
            )
            .await,
        )
    }

    /// Opens the latest authenticated durable projection and a finite canonical suffix.
    /// The allowance includes checkpoint pointer, payload, canonical anchor and suffix.
    /// Missing checkpoints for established histories fail rather than replaying from zero.
    pub async fn open_with_read_limits(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        limits: HistoryReadLimits,
    ) -> Result<Self> {
        Self::open_inner(client, authority, authority_verifier, schemas, None, limits).await
    }

    /// Restores an integrity-checked snapshot, then replays its retained suffix.
    ///
    /// Caller-supplied storage is supported alongside default Stream checkpoints.
    /// Snapshot bytes, cached events and suffix share the ordinary finite default allowance.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.restore",
            level = "info",
            skip_all,
            fields(rev = crate::obs::Empty, items = crate::obs::Empty, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn open_from_snapshot(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Snapshot,
    ) -> Result<Self> {
        crate::obs::outcome(
            Self::open_from_snapshot_with_read_limits(
                client,
                authority,
                authority_verifier,
                schemas,
                snapshot,
                default_projection_read_limits(),
            )
            .await,
        )
    }

    /// Restores caller-supplied authenticated state under one cumulative input
    /// allowance for serialized snapshot bytes, cached events and canonical suffix.
    pub async fn open_from_snapshot_with_read_limits(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Snapshot,
        limits: HistoryReadLimits,
    ) -> Result<Self> {
        Self::open_inner(
            client,
            authority,
            authority_verifier,
            schemas,
            Some(snapshot),
            limits,
        )
        .await
    }

    async fn open_inner(
        client: &StreamClient<P>,
        authority: Authority,
        authority_verifier: AuthorityVerifier,
        schemas: SchemaRegistry,
        snapshot: Option<Snapshot>,
        limits: HistoryReadLimits,
    ) -> Result<Self> {
        authority_verifier.verify_audience(&authority)?;
        if limits.maximum_events == 0 || limits.maximum_bytes == 0 {
            return Err(Error::Invalid(
                "projection read bounds must be positive".into(),
            ));
        }
        let reader = HistoryReader::new(client, &authority, authority_verifier.clone())?;
        let loaded = match snapshot {
            Some(snapshot) => checkpoints::supplied(client, &authority, snapshot, limits).await?,
            None => checkpoints::load(client, &authority, &authority_verifier, limits).await?,
        };
        let path = authority.stream_path()?;
        let stream = client.stream(path)?;
        let mut reducer = if let Some(snapshot) = loaded.snapshot {
            if snapshot.authority != authority {
                return Err(Error::Invalid(
                    "snapshot authority does not match requested aggregate".into(),
                ));
            }
            Reducer::restore(snapshot, authority_verifier, schemas)?
        } else {
            Reducer::new(authority, authority_verifier, schemas)
        };
        reducer.set_resident_event_limit(1024)?;
        let start = reducer.revision();
        let remaining_events = limits.maximum_events - loaded.consumed_events;
        if loaded.through_revision < start
            || loaded.through_revision - start > u64::from(remaining_events)
        {
            return Err(Error::Invalid(
                "projection suffix exceeds event allowance".into(),
            ));
        }
        if start < loaded.through_revision {
            let remaining_bytes = limits.maximum_bytes.saturating_sub(loaded.consumed_bytes);
            if remaining_bytes == 0 {
                return Err(Error::Invalid(
                    "projection suffix exceeds byte allowance".into(),
                ));
            }
            let page = reader
                .read_page(
                    &HistoryCursor {
                        authority: reducer.authority().clone(),
                        after_revision: start,
                        through_revision: loaded.through_revision,
                    },
                    HistoryReadLimits {
                        maximum_events: remaining_events,
                        maximum_bytes: remaining_bytes,
                    },
                )
                .await?;
            if page.cursor.after_revision != loaded.through_revision {
                return Err(Error::Invalid(
                    "projection suffix exceeds byte allowance".into(),
                ));
            }
            for event in page.events {
                reducer.apply_committed(event)?;
            }
        }
        crate::obs::obs_record!(
            "rev" = reducer.revision(),
            "items" = reducer.revision().saturating_sub(start)
        );
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

    /// Configures the event/retry cache; canonical Stream history and live projections remain authoritative.
    /// The default is 1024 events. Older identities are resolved through the atomic operation index.
    pub fn with_resident_event_limit(mut self, maximum: usize) -> Result<Self> {
        self.set_resident_event_limit(maximum)?;
        Ok(self)
    }

    /// Reconfigures the bounded event/retry cache without moving a borrowed aggregate.
    /// Canonical history and live projections remain authoritative; older retry
    /// identities and selections are resolved through the existing atomic index.
    pub fn set_resident_event_limit(&mut self, maximum: usize) -> Result<()> {
        self.reducer.set_resident_event_limit(maximum)
    }

    /// Retires only closed, checkpoint-covered conversation records from memory.
    /// Canonical Stream events and atomic identity indexes remain unchanged.
    /// Old selections and replies must be resolved through the history reader;
    /// this cache operation admits no events and grants no authority.
    pub fn compact_conversation_projection(&mut self) -> Result<()> {
        self.reducer.compact_conversation_projection()
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
        self.set_limits(limits)?;
        Ok(self)
    }

    /// Updates admission bounds without discarding the verified projection.
    pub fn set_limits(&mut self, limits: Limits) -> Result<()> {
        limits.validate()?;
        self.limits = limits;
        Ok(())
    }

    /// Returns the current deterministic projection.
    #[must_use]
    pub const fn reducer(&self) -> &Reducer {
        &self.reducer
    }

    /// Resolves one archived operation through its atomically published canonical location.
    pub async fn operation_event(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::core::Event>> {
        operations::find_operation(
            &self.client,
            self.reducer.authority(),
            &self.reducer.event_verifier(),
            operation,
        )
        .await
    }

    /// Exact historical operation revision, independent of the resident retry index.
    pub async fn operation_revision(&self, operation: OperationId) -> Result<Option<u64>> {
        Ok(self
            .operation_event(operation)
            .await?
            .map(|event| event.revision))
    }

    /// Exact committed model selection, including operations archived from resident state.
    pub async fn context_selection_for_operation(
        &self,
        operation: OperationId,
    ) -> Result<Option<crate::conversation::ModelContextSelection>> {
        Ok(self
            .operation_event(operation)
            .await?
            .and_then(|event| match event.payload {
                EventPayload::ModelContextSelected { selection } => Some(selection),
                _ => None,
            }))
    }

    async fn replay_indexed(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        self.reducer.verify_command_scope(command)?;
        let Some(event) = self.operation_event(command.operation_id).await? else {
            if self
                .reducer
                .operation_revision(command.operation_id)
                .is_some()
            {
                return Err(Error::Storage(
                    "resident operation has no atomic canonical location".into(),
                ));
            }
            return Ok(None);
        };
        self.reducer.verify_retained_command(command, &event)?;
        if event.revision > self.reducer.revision() {
            if event.revision
                != self
                    .reducer
                    .revision()
                    .checked_add(1)
                    .ok_or_else(|| Error::Invalid("aggregate revision exhausted".into()))?
            {
                return Err(Error::Conflict(
                    "aggregate must refresh to the indexed operation boundary".into(),
                ));
            }
            return self.reducer.apply_committed(event).map(Some);
        }
        Ok(Some(ApplyResult::Replayed { event }))
    }

    /// Opens an archival reader without replaying or cloning the resident event history.
    pub fn history_reader(&self) -> Result<HistoryReader<P>> {
        HistoryReader::new(
            &self.client,
            self.reducer.authority(),
            self.reducer.event_verifier(),
        )
    }

    /// Resolves one attested event within this projection's committed boundary.
    pub async fn read_event_at(&self, revision: u64) -> Result<crate::core::Event> {
        let after_revision = revision
            .checked_sub(1)
            .ok_or_else(|| Error::Invalid("event revision must be positive".into()))?;
        let cursor = HistoryCursor {
            authority: self.reducer.authority().clone(),
            after_revision,
            through_revision: self.reducer.revision(),
        };
        let page = self
            .history_reader()?
            .read_page(
                &cursor,
                HistoryReadLimits {
                    maximum_events: 1,
                    maximum_bytes: acyclic_stream::MAX_RECORD_BYTES as u64,
                },
            )
            .await?;
        page.events
            .into_iter()
            .next()
            .ok_or_else(|| Error::NotFound("committed event is unavailable".into()))
    }

    /// Captures the authoritative tail for a finite incremental refresh.
    pub async fn tail_revision(&self) -> Result<u64> {
        Ok(self.stream.tail().await?)
    }

    /// Advances the existing verified projection by at most one caller-sized page.
    /// Returns whether the captured `through_revision` has been reached. Later
    /// appends are excluded; callers may continue paging without a total-work cap.
    /// On failure, any successfully applied prefix remains authoritative.
    pub async fn refresh_through(
        &mut self,
        through_revision: u64,
        maximum_events: u32,
    ) -> Result<bool> {
        let current = self.reducer.revision();
        if maximum_events == 0 || through_revision < current {
            return Err(Error::Invalid("aggregate refresh cursor is invalid".into()));
        }
        if through_revision == current {
            return Ok(true);
        }
        if through_revision > self.stream.tail().await? {
            return Err(Error::Invalid(
                "aggregate refresh exceeds committed tail".into(),
            ));
        }
        let count = u32::try_from((through_revision - current).min(u64::from(maximum_events)))
            .map_err(|_| Error::Invalid("aggregate refresh page exceeds platform size".into()))?;
        let mut records = self.stream.read(current, count).await?;
        let mut consumed = 0;
        while let Some(record) = records.try_next().await? {
            if consumed >= count || record.sequence != self.reducer.revision() {
                return Err(Error::Storage(
                    "aggregate refresh page is not contiguous".into(),
                ));
            }
            let (authority, event) = decode_event(&record.value)?;
            if &authority != self.reducer.authority()
                || event.revision
                    != record.sequence.checked_add(1).ok_or_else(|| {
                        Error::Storage("aggregate refresh revision overflows".into())
                    })?
            {
                return Err(Error::Storage(
                    "aggregate refresh event binding is invalid".into(),
                ));
            }
            self.reducer.apply_committed(event)?;
            consumed += 1;
        }
        if consumed == 0 {
            return Err(Error::Storage(
                "aggregate refresh stopped before captured tail".into(),
            ));
        }
        Ok(self.reducer.revision() == through_revision)
    }

    /// Plans, CAS-appends, and only then applies one command.
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.execute",
            level = "info",
            skip_all,
            fields(rev = command.expected_revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn execute(&mut self, command: Command) -> Result<ApplyResult> {
        crate::obs::outcome(self.execute_untraced(command).await)
    }

    async fn execute_untraced(&mut self, command: Command) -> Result<ApplyResult> {
        if let Some(result) = self.replay_indexed(&command).await? {
            if let Action::PublishFork { seed } = &command.action {
                self.release_replayed_fork_fence(seed).await?;
            }
            return Ok(result);
        }
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
        let publication = checkpoints::publication(&self.client, &self.reducer, &event).await?;
        let request = publication.request(&self.client, idempotency_key).await?;
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
        self.append_planned_command(&command, event, publication, request, fork_guard)
            .await
    }

    /// Uses the existing conversation reducer and content validation, while the
    /// task owner compares its lease and this conversation tail in one Stream
    /// transaction. Fresh admission rejects cancellation; effect settlement
    /// remains allowed for that exact owner. Responders remain conversation-owned.
    #[cfg(feature = "filesystem")]
    pub(crate) async fn execute_task_command(
        &mut self,
        command: Command,
        owner: &crate::durable_host::TaskJournalOwner<P>,
    ) -> Result<ApplyResult> {
        use crate::distributed::JournalWrite;
        let write = match &command.action {
            Action::OpenInteraction { .. } => {
                owner.require_interaction_grant()?;
                JournalWrite::Fresh
            }
            Action::PlanEffect {
                provider,
                request,
                result_schema,
                ..
            } => {
                owner.require_effect_grants(provider, true)?;
                owner.validate_input_file(request)?;
                owner.validate_input_file(result_schema)?;
                JournalWrite::Fresh
            }
            Action::MarkEffectDispatched { effect_id, .. }
            | Action::ResolveEffect {
                observation: crate::core::EffectAttestation { effect_id, .. },
            } => {
                let effect = self
                    .reducer
                    .effect(*effect_id)
                    .ok_or_else(|| Error::NotFound(format!("effect {effect_id}")))?;
                owner.require_effect_grants(&effect.provider, false)?;
                if matches!(command.action, Action::ResolveEffect { .. }) {
                    JournalWrite::Settlement
                } else {
                    JournalWrite::Fresh
                }
            }
            _ => {
                return Err(Error::Invalid(
                    "task journal action is not an admitted request or effect".into(),
                ));
            }
        };
        owner.verify(write.settlement()).await?;
        if let Some(result) = self.replay_indexed(&command).await? {
            return Ok(result);
        }
        let planned = self.plan_command(&command, false).await?;
        let ApplyResult::Applied { event } = planned else {
            return Ok(planned);
        };
        self.validate_admission_content(&command.action).await?;
        self.validate_causal_reference(event.causal_parent.as_ref())
            .await?;
        let publication = checkpoints::publication(&self.client, &self.reducer, &event).await?;
        let key = stream_idempotency_key(self.stream.path().as_str(), &command.idempotency_key)?;
        if !owner.append_conversation(&publication, &key, write).await? {
            return Err(Error::Conflict(
                "task publication lost its owner or conversation tail".into(),
            ));
        }
        self.reducer
            .apply_committed(event)
            .map_err(|_| Error::Indeterminate(command.operation_id))
    }

    async fn plan_command(&self, command: &Command, fresh_migration: bool) -> Result<ApplyResult> {
        if !fresh_migration {
            return self.reducer.plan_indexed(command, false);
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
        self.reducer.plan_indexed(command, true)
    }

    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.append",
            level = "debug",
            skip_all,
            fields(rev = command.expected_revision, bytes = publication.bytes.len() as u64)
        )
    )]
    async fn append_planned_command(
        &mut self,
        command: &Command,
        event: crate::core::Event,
        publication: operations::IndexedPublication,
        request: acyclic_stream::CommitRequest,
        fork_guard: Option<Box<dyn crate::fork::ForkPublicationGuard>>,
    ) -> Result<ApplyResult> {
        let append = self.client.commit(request).await;
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
            CommitOutcome::Committed(envelope) => {
                if publication.verify(&envelope).is_err() {
                    return Err(Error::Indeterminate(publication.operation_id));
                }
                match self.reducer.apply_committed(event) {
                    Ok(result) => Ok(result),
                    Err(_) => return Err(Error::Indeterminate(command.operation_id)),
                }
            }
            CommitOutcome::Conflict(_) => Err(Error::Conflict(
                "aggregate tail or operation identity changed".into(),
            )),
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
            let previous_value: Value =
                crate::contract::json_from_slice(&old_bytes).map_err(|error| {
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
    #[cfg_attr(
        not(target_arch = "wasm32"),
        tracing::instrument(
            name = "acyclic.harness.store.reconcile",
            level = "info",
            skip_all,
            fields(rev = command.expected_revision, outcome = crate::obs::Empty, error.kind = crate::obs::Empty)
        )
    )]
    pub async fn reconcile(&mut self, command: &Command) -> Result<Option<ApplyResult>> {
        crate::obs::outcome(
            async {
                let result = self.reconcile_inner(command).await?;
                if result.is_some()
                    && let crate::core::Action::PublishFork { seed } = &command.action
                {
                    self.release_replayed_fork_fence(seed).await?;
                }
                Ok(result)
            }
            .await,
        )
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
        if let Some(result) = self.replay_indexed(command).await? {
            return Ok(Some(result));
        }
        let key = stream_idempotency_key(self.stream.path().as_str(), &command.idempotency_key)?;
        let Some(observation) = self.client.inspect_idempotency(key).await? else {
            return Ok(None);
        };
        match observation.outcome {
            IdempotencyOutcome::Commit(CommitOutcome::Conflict(_)) => {
                if let Action::PublishFork { seed } = &command.action {
                    self.release_replayed_fork_fence(seed).await?;
                }
                Err(Error::Conflict("aggregate publication was rejected".into()))
            }
            IdempotencyOutcome::Commit(CommitOutcome::Committed(_)) => Err(Error::Storage(
                "committed aggregate publication has no atomic operation location".into(),
            )),
            _ => Err(Error::Conflict(
                "retry identity is bound to another operation contract".into(),
            )),
        }
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
        let stream = self.client.stream(reference.authority.stream_path()?)?;
        let records = stream
            .read(reference.revision.saturating_sub(1), 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
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
    use crate::wire_codec::encode_event;
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
    async fn empty_history_cursor_stays_terminal_after_first_publication() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let reader = HistoryReader::new(&client, &authority(), issuer().verifier())?;
        let cursor = reader.pin(0).await?;
        let limits = HistoryReadLimits {
            maximum_events: 1,
            maximum_bytes: 65_536,
        };
        assert_eq!(cursor.through_revision, 0);
        assert!(reader.read_page(&cursor, limits).await?.events.is_empty());
        assert!(matches!(reader.pin(1).await, Err(Error::Invalid(_))));
        let future = HistoryCursor {
            through_revision: 1,
            ..cursor.clone()
        };
        assert!(matches!(
            reader.read_page(&future, limits).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        writer.execute(command(1)?).await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        assert!(reader.read_page(&cursor, limits).await?.events.is_empty());
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        let current = reader.pin(0).await?;
        let page = reader.read_page(&current, limits).await?;
        assert_eq!(page.events.len(), 1);
        assert_eq!(page.cursor.after_revision, 1);
        Ok(())
    }

    #[tokio::test]
    async fn operation_lookup_counts_atomic_records_under_one_byte_budget() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                .await?
                .with_resident_event_limit(1)?,
        );
        let original = command(1)?;
        let first = writer.execute(original.clone()).await?;
        let ApplyResult::Applied { event } = first else {
            return Err(Error::Invalid("fixture event was not published".into()));
        };
        let mut later = command(2)?;
        later.expected_revision = 1;
        writer.execute(later).await?;
        assert!(
            writer
                .reducer()
                .operation_revision(original.operation_id)
                .is_none()
        );
        let index = client
            .stream(operations::operation_path(&authority(), original.operation_id)?.as_str())?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let canonical = client
            .stream(authority().stream_path()?)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let index_bytes = index
            .first()
            .ok_or_else(|| Error::Storage("fixture index missing".into()))?
            .value
            .len() as u64;
        let total_bytes = index_bytes
            + canonical
                .first()
                .ok_or_else(|| Error::Storage("fixture canonical event missing".into()))?
                .value
                .len() as u64;
        let reader = writer.history_reader()?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .operation_event_bounded(original.operation_id, 0)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        for maximum in [index_bytes - 1, index_bytes] {
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert!(matches!(
                reader
                    .operation_event_bounded(original.operation_id, maximum)
                    .await,
                Err(Error::Invalid(_))
            ));
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 1);
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .operation_event_bounded(original.operation_id, total_bytes - 1)
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader
                .operation_event_bounded(original.operation_id, total_bytes)
                .await?,
            (Some(event.clone()), total_bytes)
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        assert_eq!(
            reader.operation_event(original.operation_id).await?,
            Some(event)
        );
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert_eq!(
            reader
                .operation_event_bounded(OperationId::from_bytes([254; 16]), total_bytes)
                .await?,
            (None, 0)
        );
        Ok(())
    }

    #[tokio::test]
    async fn conversation_head_pins_racing_append_and_rejects_corrupt_atomic_proof() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        let agent = crate::AgentId::from_bytes([8; 16]);
        let file = content()?;
        let scope = issuer().root_for_agent(
            agent,
            "head-writer",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                file.read_capability()?,
            ]),
        );
        let bind = Command {
            operation_id: OperationId::from_bytes([101; 16]),
            idempotency_key: IdempotencyKey::new("head-bind")?,
            expected_revision: 0,
            scope: scope.clone(),
            causal_parent: None,
            action: Action::BindConversation { agent },
        };
        writer.execute(bind.clone()).await?;
        let reader = writer.history_reader()?;
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        let message = crate::conversation::ConversationMessage {
            id: uuid::Uuid::from_u128(1),
            sequence: 1,
            kind: crate::conversation::MessageKind::User,
            content: file,
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        let command = Command {
            operation_id: OperationId::from_bytes([102; 16]),
            idempotency_key: IdempotencyKey::new("head-racing-user")?,
            expected_revision: 1,
            scope,
            causal_parent: None,
            action: Action::AppendConversationMessage {
                message: Box::new(message.clone()),
            },
        };
        let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
            writer.reducer().plan(&command)?;
        let publication = operations::IndexedPublication::new(
            &authority(),
            &event,
            encode_event(&authority(), &event)?,
        )?;
        *provider
            .message_head_race
            .lock()
            .map_err(|_| Error::Storage("head race lock".into()))? = Some(
            publication
                .request(&client, StreamIdempotencyKey::new("head-racing-commit")?)
                .await?,
        );
        // The first lookup captured the empty head before this actual atomic
        // append. The next lookup sees the newly committed message.
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        assert_eq!(
            reader.latest_conversation_message(65_536).await?,
            Some(message)
        );
        drop(writer);
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert_eq!(
            reopened
                .reducer()
                .conversation()
                .map(|state| state.messages().len()),
            Some(1)
        );
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(reader.latest_conversation_message(65_536).await.is_err());
        }
        let head = client.stream("harness/v2/conversation-heads/conversations/conversation-1")?;
        let records = head.read(1, 1).await?.try_collect::<Vec<_>>().await?;
        let mut forged: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("head record missing".into()))?
                .value,
        )?;
        forged["sequence"] = json!(2);
        head.append(crate::contract::canonical_json_bytes(&forged)?)
            .await?;
        assert!(matches!(reader.latest_conversation_message(65_536).await,
            Err(Error::Storage(detail)) if detail.contains("atomic canonical event")));

        // A separately appended canonical binding has no atomic head proof.
        // Missing derived state must not be reported as an empty conversation.
        let incomplete = StreamClient::new(Arc::new(MemoryStream::default()));
        let reducer = Reducer::new(authority(), issuer().verifier(), schemas());
        let (ApplyResult::Applied { event } | ApplyResult::Replayed { event }) =
            reducer.plan(&bind)?;
        incomplete
            .stream(authority().stream_path()?)?
            .append(encode_event(&authority(), &event)?)
            .await?;
        let incomplete_reader = HistoryReader::new(&incomplete, &authority(), issuer().verifier())?;
        assert!(matches!(
            incomplete_reader.latest_conversation_message(65_536).await,
            Err(Error::Storage(_))
        ));
        Ok(())
    }

    #[tokio::test]
    async fn indexed_conversation_lookup_is_atomic_bounded_and_pinned_after_eviction() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                .await?
                .with_resident_event_limit(1)?,
        );
        let agent = crate::AgentId::from_bytes([8; 16]);
        let file = content()?;
        let scope = issuer().root_for_agent(
            agent,
            "message-writer",
            Capabilities::new([
                "conversation:bind".to_owned(),
                "conversation:append".to_owned(),
                "conversation:select_context".to_owned(),
                file.read_capability()?,
            ]),
        );
        writer
            .execute(Command {
                operation_id: OperationId::from_bytes([101; 16]),
                idempotency_key: IdempotencyKey::new("message-index-bind")?,
                expected_revision: 0,
                scope: scope.clone(),
                causal_parent: None,
                action: Action::BindConversation { agent },
            })
            .await?;
        let reader = writer.history_reader()?;
        let empty = reader.pin(0).await?;
        assert!(reader.latest_conversation_message(65_536).await?.is_none());
        let message = |sequence| crate::conversation::ConversationMessage {
            id: uuid::Uuid::from_u128(u128::from(sequence)),
            sequence,
            kind: crate::conversation::MessageKind::User,
            content: file.clone(),
            attachments: Vec::new().into(),
            reply_to: None,
            tool_call_id: None,
            extensions: Default::default(),
        };
        let mut first_command = None;
        for sequence in 1..=10_000u64 {
            let next = Command {
                operation_id: OperationId::from_bytes(
                    uuid::Uuid::from_u128(u128::from(sequence) + 100_000).into_bytes(),
                ),
                idempotency_key: IdempotencyKey::new(format!("message-index-{sequence}"))?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(message(sequence)),
                },
            };
            if sequence == 1 {
                first_command = Some(next.clone());
            }
            writer.execute(next).await?;
            if matches!(sequence, 1 | 1_000 | 10_000) {
                // A fresh reader has no reducer or cached history. The same
                // two reads suffice while retained history grows by 10,000x.
                let cold = HistoryReader::new(&client, &authority(), issuer().verifier())?;
                provider.forbid_writes.store(true, Ordering::SeqCst);
                provider.observation_reads.store(0, Ordering::SeqCst);
                assert_eq!(
                    cold.latest_conversation_message(65_536).await?,
                    Some(message(sequence))
                );
                assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
                assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
                provider.forbid_writes.store(false, Ordering::SeqCst);
            }
        }
        // The large checkpoint spans reused immutable chunks. Restore through
        // the real default constructor with writes prohibited, then compare the
        // complete logical-prefix identity with the original live reducer.
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let restored =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert_eq!(restored.reducer().revision(), writer.reducer().revision());
        let restored_history = restored
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("restored conversation missing".into()))?;
        let live_history = writer
            .reducer()
            .conversation()
            .ok_or_else(|| Error::Invalid("live conversation missing".into()))?;
        assert_eq!(restored_history.logical_revision(), 10_000);
        assert_eq!(
            restored_history.history_prefix_digest(10_000)?,
            live_history.history_prefix_digest(10_000)?
        );
        assert!(
            restored
                .reducer()
                .events_after(restored.reducer().archived_through_revision(), 64)?
                .len()
                <= 64
        );
        assert!(provider.observation_maximum.load(Ordering::SeqCst) <= 64);
        // Subsequent one-record index observations have their own measurement
        // window; the cold canonical suffix above legitimately reads 17 records.
        provider.observation_maximum.store(0, Ordering::SeqCst);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        let archived = writer.reducer().archived_through_revision();
        assert_eq!(archived, writer.reducer().revision() - 1);
        assert_eq!(writer.reducer().events_after(archived, 2)?.len(), 1);
        assert!(matches!(
            writer.reducer().events_after(0, 2),
            Err(Error::Unsupported(_))
        ));
        let selection_operation = OperationId::from_bytes([98; 16]);
        let selection = crate::conversation::ModelContextSelection {
            conversation_revision: 10_000,
            message_ids: [1, 5_000, 10_000]
                .into_iter()
                .map(|sequence| message(sequence).id)
                .collect(),
            checkpoint: None,
        };
        writer
            .execute(Command {
                operation_id: selection_operation,
                idempotency_key: IdempotencyKey::new("indexed-context-selection")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::SelectModelContext {
                    selection: selection.clone(),
                },
            })
            .await?;
        let pinned = reader.pin(0).await?;
        let mut future = message(10_001);
        future.kind = crate::conversation::MessageKind::Interaction;
        writer
            .execute(Command {
                operation_id: OperationId::from_bytes([99; 16]),
                idempotency_key: IdempotencyKey::new("message-after-pin")?,
                expected_revision: writer.reducer().revision(),
                scope: scope.clone(),
                causal_parent: None,
                action: Action::AppendConversationMessage {
                    message: Box::new(future.clone()),
                },
            })
            .await?;
        assert!(matches!(
            writer
                .execute(
                    first_command.ok_or_else(|| Error::Invalid("missing first command".into()))?
                )
                .await?,
            ApplyResult::Replayed { .. }
        ));
        // Forge a separate-commit locator pointing at the signed first message.
        let first_path = format!(
            "harness/v2/conversation-messages/conversations/conversation-1/{}",
            message(1).id
        );
        let records = client
            .stream(&first_path)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut forged: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("message index missing".into()))?
                .value,
        )?;
        let forged_id = uuid::Uuid::from_u128(50_000);
        forged["message_id"] = json!(forged_id);
        client
            .stream(format!(
                "harness/v2/conversation-messages/conversations/conversation-1/{forged_id}"
            ))?
            .append(crate::contract::canonical_json_bytes(&forged)?)
            .await?;
        let mut forged_sequence: Value = crate::executor::decode_json(
            &records
                .first()
                .ok_or_else(|| Error::Storage("message index missing".into()))?
                .value,
        )?;
        forged_sequence["sequence"] = json!(20_001);
        client
            .stream("harness/v2/conversation-sequences/conversations/conversation-1/20001")?
            .append(crate::contract::canonical_json_bytes(&forged_sequence)?)
            .await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader.latest_conversation_message(65_536).await?,
            Some(future.clone())
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader.latest_conversation_message(0).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            reader.latest_conversation_message(1).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 1);
        let range_limits = HistoryReadLimits {
            maximum_events: 3,
            maximum_bytes: 65_536,
        };
        provider.observation_reads.store(0, Ordering::SeqCst);
        let selected = reader
            .selected_conversation(&pinned, &selection, range_limits)
            .await?;
        assert_eq!(
            selected
                .messages()
                .iter()
                .map(|message| message.id)
                .collect::<Vec<_>>(),
            selection.message_ids
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 6);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        assert!(selected.agent.is_none());
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .selected_conversation(
                    &pinned,
                    &selection,
                    HistoryReadLimits {
                        maximum_events: 2,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert_eq!(
            reader
                .conversation_range(&pinned, 9_997, 10_000, range_limits)
                .await?,
            [9_998, 9_999, 10_000]
                .into_iter()
                .map(message)
                .collect::<Vec<_>>()
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 6);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(
                    &pinned,
                    9_997,
                    10_000,
                    HistoryReadLimits {
                        maximum_events: 2,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 10_000, 10_001, range_limits)
                .await,
            Err(Error::Storage(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        // Exact aggregate byte exhaustion stops before the third locator read.
        let mut two_record_bytes = 0u64;
        for sequence in [9_998u64, 9_999] {
            let index = client
                .stream(format!(
                    "harness/v2/conversation-sequences/conversations/conversation-1/{sequence}"
                ))?
                .read(0, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let event = client
                .stream(authority().stream_path()?)?
                .read(sequence, 1)
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            two_record_bytes += index
                .first()
                .ok_or_else(|| Error::Storage("sequence index missing".into()))?
                .value
                .len() as u64
                + event
                    .first()
                    .ok_or_else(|| Error::Storage("message event missing".into()))?
                    .value
                    .len() as u64;
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(
                    &pinned,
                    9_997,
                    10_000,
                    HistoryReadLimits {
                        maximum_bytes: two_record_bytes,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 4);
        let last_selection = crate::conversation::ModelContextSelection {
            conversation_revision: 10_000,
            message_ids: [9_998, 9_999, 10_000]
                .into_iter()
                .map(|sequence| message(sequence).id)
                .collect(),
            checkpoint: None,
        };
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .selected_conversation(
                    &pinned,
                    &last_selection,
                    HistoryReadLimits {
                        maximum_bytes: two_record_bytes,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 4);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 19_999, 20_000, range_limits)
                .await,
            Err(Error::Storage(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 1);
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader
                .conversation_range(&pinned, 20_000, 20_001, range_limits)
                .await,
            Err(Error::Storage(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        let mixed_cut = reader.pin(0).await?;
        assert_eq!(
            reader
                .conversation_range(&mixed_cut, 10_000, 10_001, range_limits)
                .await?,
            vec![future.clone()]
        );
        assert!(matches!(
            reader
                .conversation_range(
                    &mixed_cut,
                    10_000,
                    10_001,
                    HistoryReadLimits {
                        maximum_events: 0,
                        ..range_limits
                    }
                )
                .await,
            Err(Error::Invalid(_))
        ));
        for sequence in [1, 5_000, 10_000] {
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert_eq!(
                reader
                    .conversation_message(&pinned, message(sequence).id, 65_536)
                    .await?,
                Some(message(sequence))
            );
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
            assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        }
        assert!(
            reader
                .conversation_message(&pinned, future.id, 65_536)
                .await?
                .is_none()
        );
        assert!(
            reader
                .conversation_message(&empty, message(1).id, 65_536)
                .await?
                .is_none()
        );
        let current = reader.pin(0).await?;
        assert_eq!(
            reader
                .conversation_message(&current, future.id, 65_536)
                .await?,
            Some(future)
        );
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(matches!(
            reader.conversation_message(&pinned, message(1).id, 1).await,
            Err(Error::Invalid(_))
        ));
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 1);
        assert!(matches!(
            reader
                .conversation_message(&pinned, forged_id, 65_536)
                .await,
            Err(Error::Storage(_))
        ));
        #[cfg(feature = "filesystem")]
        {
            use crate::executor::ExecutionJournal as _;
            let filesystem_provider =
                crate::resources::ProviderRef::new("cold-selected-context", "filesystem", "2")?;
            let host = Arc::new(crate::filesystem::FilesystemHost::new(
                acyclic_fs::Fs::memory(),
                filesystem_provider.clone(),
            )?);
            let private = crate::conversation::VolumeRef::new(
                filesystem_provider,
                "journal",
                crate::conversation::VolumeClass::AgentPrivate,
                crate::conversation::VolumeOwner::Agent(agent),
            )?;
            host.create_volume(&private).await?;
            let journal_scope = issuer().root_for_agent(
                agent,
                "cold-selection-journal",
                Capabilities::new([
                    private.capability(crate::conversation::VolumeOperation::Read)?,
                    private.capability(crate::conversation::VolumeOperation::Write)?,
                ]),
            );
            let journal = crate::filesystem::FilesystemExecutionJournal::new_with_schemas(
                client.clone(),
                host,
                private,
                issuer().verifier(),
                schemas(),
                journal_scope,
                65_536,
            )?
            .with_input_verifier(Arc::new(TestContent));
            let sparse = crate::conversation::ConversationState::selected_view(
                [1, 5_000, 10_000].into_iter().map(message).collect(),
            )?;
            assert_eq!(sparse.messages().len(), 3);
            assert!(sparse.agent.is_none());
            let selected = crate::projection::select_model_context_at_revision(
                &sparse,
                selection.clone(),
                &TestContent,
                3,
                8,
                65_536,
                8,
            )
            .await?;
            provider.observation_reads.store(0, Ordering::SeqCst);
            journal
                .verify_selected_context(selection_operation, &selected)
                .await?;
            // One operation locator/event and three message locator/event pairs.
            // Reopening the old reducer would instead read all 10,000 messages.
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 8);
            assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
            let mut altered = selected.clone();
            altered.messages.pop();
            assert!(matches!(
                journal
                    .verify_selected_context(selection_operation, &altered)
                    .await,
                Err(Error::Conflict(_))
            ));
            let mut wrong_selection = selected;
            wrong_selection.selection.message_ids.pop();
            provider.observation_reads.store(0, Ordering::SeqCst);
            assert!(matches!(
                journal
                    .verify_selected_context(selection_operation, &wrong_selection)
                    .await,
                Err(Error::Conflict(_))
            ));
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        }
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn bounded_resident_suffix_preserves_cold_retries_and_recovery() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                .await?
                .with_resident_event_limit(2)?,
        );
        let original = command(1)?;
        let first = writer.execute(original.clone()).await?;
        for identity in 2..=5 {
            let mut next = command(identity)?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next).await?;
        }
        assert_eq!(writer.reducer().archived_through_revision(), 3);
        assert!(matches!(
            writer.reducer().events_after(0, 10),
            Err(Error::Unsupported(_))
        ));
        assert_eq!(writer.reducer().events_after(3, 10)?.len(), 2);
        let snapshot = writer.reducer().snapshot()?;
        assert_eq!(snapshot.events.len(), 2);
        assert_eq!(writer.read_event_at(1).await?.revision, 1);
        let mut reused = original.clone();
        reused.expected_revision = writer.reducer().revision();
        assert!(matches!(
            writer.reducer().plan(&reused),
            Err(Error::Unsupported(_))
        ));
        assert!(matches!(
            writer.execute(original.clone()).await?,
            ApplyResult::Replayed { .. }
        ));
        let mut conflicting = original.clone();
        conflicting.causal_parent = Some(EventReference {
            authority: authority(),
            revision: 1,
        });
        assert!(matches!(
            writer.execute(conflicting).await,
            Err(Error::Conflict(_))
        ));
        let mut restored = with_content(
            StreamAggregate::open_from_snapshot(
                &client,
                authority(),
                issuer().verifier(),
                schemas(),
                snapshot,
            )
            .await?
            .with_resident_event_limit(2)?,
        );
        assert_eq!(restored.reducer().archived_through_revision(), 3);
        let ApplyResult::Replayed { event } = restored.execute(original).await? else {
            return Err(Error::Invalid("cold retry was not replayed".into()));
        };
        let ApplyResult::Applied {
            event: original_event,
        } = first
        else {
            return Err(Error::Invalid("fixture first event was not applied".into()));
        };
        assert_eq!(event, original_event);
        assert_eq!(restored.reducer().revision(), 5);
        let mut next = command(6)?;
        next.expected_revision = 5;
        restored.execute(next).await?;
        assert_eq!(restored.reducer().archived_through_revision(), 4);
        assert_eq!(restored.reducer().snapshot()?.events.len(), 2);
        Ok(())
    }

    #[tokio::test]
    async fn cold_history_reader_bounds_ten_thousand_events_without_reducer_restore() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        for identity in 1_u64..=10_000 {
            let mut next = command(1)?;
            let mut bytes = [0; 16];
            bytes
                .get_mut(..8)
                .ok_or_else(|| Error::Invalid("fixture identity missing".into()))?
                .copy_from_slice(&identity.to_le_bytes());
            next.operation_id = OperationId::from_bytes(bytes);
            next.idempotency_key = IdempotencyKey::new(format!("history-{identity}"))?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next).await?;
        }
        let last = writer.read_event_at(10_000).await?;
        let mut later = command(2)?;
        later.expected_revision = writer.reducer().revision();
        // A cold reader opens no reducer and does not read any event on construction/pin.
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let reader = HistoryReader::new(&client, &authority(), issuer().verifier())?;
        let cursor = reader.pin(9_997).await?;
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        provider.forbid_writes.store(false, Ordering::SeqCst);
        writer.execute(later).await?;
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let limits = HistoryReadLimits {
            maximum_events: 2,
            maximum_bytes: 131_072,
        };
        let first = reader.read_page(&cursor, limits).await?;
        assert_eq!(first.events.len(), 2);
        assert_eq!(first.cursor.after_revision, 9_999);
        let second = reader.read_page(&first.cursor, limits).await?;
        assert_eq!(second.events, vec![last.clone()]);
        assert_eq!(second.cursor.after_revision, 10_000);
        assert!(
            reader
                .read_page(&second.cursor, limits)
                .await?
                .events
                .is_empty()
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 2);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert_eq!(cursor.after_revision, 9_997);
        let encoded =
            serde_json::to_vec(&first.cursor).map_err(|error| Error::Invalid(error.to_string()))?;
        assert_eq!(
            serde_json::from_slice::<HistoryCursor>(&encoded)
                .map_err(|error| Error::Invalid(error.to_string()))?,
            first.cursor
        );
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(reader.read_page(&cursor, limits).await.is_err());
            assert_eq!(reader.read_page(&cursor, limits).await?, first);
        }
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_bytes: 1,
                        ..limits
                    }
                )
                .await
                .is_err()
        );
        assert!(
            reader
                .read_page(
                    &cursor,
                    HistoryReadLimits {
                        maximum_events: 0,
                        ..limits
                    }
                )
                .await
                .is_err()
        );
        let mut invalid = cursor.clone();
        invalid.through_revision = 10_002;
        assert!(reader.read_page(&invalid, limits).await.is_err());
        invalid = cursor;
        invalid.authority.id = "other".into();
        assert!(matches!(
            reader.read_page(&invalid, limits).await,
            Err(Error::Unauthorized(_))
        ));
        assert_cold_operation_lookup(&provider, &reader, last).await?;
        Ok(())
    }

    async fn assert_cold_operation_lookup(
        provider: &crate::test_stream::LostSessionAck<MemoryStream>,
        reader: &HistoryReader<crate::test_stream::LostSessionAck<MemoryStream>>,
        expected: crate::core::Event,
    ) -> Result<()> {
        use std::sync::atomic::Ordering;
        provider.observation_reads.store(0, Ordering::SeqCst);
        provider.observation_maximum.store(0, Ordering::SeqCst);
        assert_eq!(
            reader.operation_event(expected.operation_id).await?,
            Some(expected)
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 2);
        assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 1);
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        assert!(reader.operation_event(OperationId::new()).await?.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn operation_index_is_atomic_and_recovers_without_duplicate_publication() -> Result<()> {
        use std::sync::atomic::Ordering;
        for fault in 1..=5 {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = StreamClient::new(provider.clone());
            let mut aggregate = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
            );
            let submitted = command(1)?;
            provider
                .aggregate_commit_fault
                .store(fault, Ordering::SeqCst);
            let result = aggregate.execute(submitted.clone()).await;
            if fault == 2 {
                assert!(matches!(result, Ok(ApplyResult::Applied { .. })));
            } else {
                assert!(
                    matches!(result, Err(Error::Indeterminate(id)) if id == submitted.operation_id)
                );
            }
            assert_eq!(provider.aggregate_commits.load(Ordering::SeqCst), 1);
            let indexed = aggregate.operation_event(submitted.operation_id).await?;
            assert_eq!(indexed.is_some(), fault != 1);
            let canonical = match client.stream(authority().stream_path()?)?.read(0, 1).await {
                Ok(records) => records.try_collect::<Vec<_>>().await?,
                Err(StreamError::NotFound) => Vec::new(),
                Err(error) => return Err(error.into()),
            };
            assert_eq!(canonical.len(), usize::from(fault != 1));
            aggregate.execute(submitted.clone()).await?;
            let expected_commits = if fault == 1 { 2 } else { 1 };
            assert_eq!(
                provider.aggregate_commits.load(Ordering::SeqCst),
                expected_commits
            );
            assert_eq!(aggregate.reducer().revision(), 1);
            let mut later = command(2)?;
            later.expected_revision = 1;
            aggregate.execute(later).await?;
            provider.forbid_writes.store(true, Ordering::SeqCst);
            assert!(matches!(
                aggregate.execute(submitted.clone()).await?,
                ApplyResult::Replayed { .. }
            ));
            let mut changed = submitted.clone();
            changed.idempotency_key = IdempotencyKey::new("changed-retry")?;
            assert!(matches!(
                aggregate.execute(changed).await,
                Err(Error::Conflict(_))
            ));
            for read_fault in [1, 3, 4] {
                provider
                    .history_read_fault
                    .store(read_fault, Ordering::SeqCst);
                assert!(aggregate.execute(submitted.clone()).await.is_err());
            }
            assert!(matches!(
                aggregate.reconcile(&submitted).await?,
                Some(ApplyResult::Replayed { .. })
            ));
            assert_eq!(
                provider.aggregate_commits.load(Ordering::SeqCst),
                expected_commits + 1
            );
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            assert_eq!(aggregate.reducer().revision(), 2);
        }
        Ok(())
    }

    #[tokio::test]
    async fn operation_location_must_share_the_canonical_events_atomic_commit() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut reducer = Reducer::new(authority(), issuer().verifier(), schemas());
        let ApplyResult::Applied { event } = reducer.apply(command(1)?)? else {
            return Err(Error::Invalid("fixture needs admitted event".into()));
        };
        client
            .stream(authority().stream_path()?)?
            .append_batch(
                vec![Bytes::from(encode_event(&authority(), &event)?)],
                Some(0),
                None,
            )
            .await?;
        let location = json!({"authority": authority(), "operation_id": event.operation_id,
            "revision": event.revision, "intent_digest": event.intent_digest});
        client
            .stream(operations::operation_path(&authority(), event.operation_id)?.as_str())?
            .append_batch(
                vec![Bytes::from(crate::contract::canonical_json_bytes(
                    &location,
                )?)],
                Some(0),
                None,
            )
            .await?;
        let reader = HistoryReader::new(&client, &authority(), issuer().verifier())?;
        assert!(matches!(reader.operation_event(event.operation_id).await,
            Err(Error::Storage(message)) if message.contains("atomic canonical event")));
        Ok(())
    }

    #[tokio::test]
    async fn history_reader_rejects_unattested_events_and_wrong_authority() -> Result<()> {
        for wrong_authority in [false, true] {
            let client = StreamClient::new(Arc::new(MemoryStream::default()));
            let mut reducer = Reducer::new(authority(), issuer().verifier(), schemas());
            let ApplyResult::Applied { mut event } = reducer.apply(command(1)?)? else {
                return Err(Error::Invalid("fixture needs fresh event".into()));
            };
            let mut wire_authority = authority();
            if wrong_authority {
                wire_authority.id = "other".into();
            } else {
                event.intent_digest = [0; 32];
            }
            client
                .stream(authority().stream_path()?)?
                .append_batch(
                    vec![Bytes::from(encode_event(&wire_authority, &event)?)],
                    Some(0),
                    None,
                )
                .await?;
            let reader = HistoryReader::new(&client, &authority(), issuer().verifier())?;
            let cursor = reader.pin(0).await?;
            assert!(
                reader
                    .read_page(
                        &cursor,
                        HistoryReadLimits {
                            maximum_events: 1,
                            maximum_bytes: 65_536,
                        }
                    )
                    .await
                    .is_err()
            );
        }
        Ok(())
    }

    #[tokio::test]
    async fn incremental_refresh_pins_tail_and_advances_only_bounded_pages() -> Result<()> {
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut reader =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        for identity in 1..=5 {
            let mut next = command(identity)?;
            next.expected_revision = writer.reducer().revision();
            writer.execute(next).await?;
        }
        let through = reader.tail_revision().await?;
        assert_eq!(through, 5);
        let mut later = command(6)?;
        later.expected_revision = writer.reducer().revision();
        writer.execute(later).await?;
        assert!(reader.refresh_through(through, 0).await.is_err());
        assert!(!reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 2);
        assert!(!reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 4);
        assert!(reader.refresh_through(through, 2).await?);
        assert_eq!(reader.reducer().revision(), 5);
        assert!(reader.refresh_through(through, 2).await?);
        assert!(reader.refresh_through(4, 2).await.is_err());
        assert!(reader.refresh_through(7, 2).await.is_err());
        assert!(reader.refresh_through(6, 2).await?);
        assert_eq!(reader.reducer().revision(), 6);
        let reopened =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert_eq!(
            reader.reducer().events_after(0, 6)?,
            reopened.reducer().events_after(0, 6)?
        );
        Ok(())
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

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn execute_and_reopen_emit_spans_without_content_fields() -> Result<()> {
        use crate::obs::capture;
        let (seen, _guard) = capture::install();
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        let mut unbound =
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;
        assert!(unbound.execute(command(1)?).await.is_err());
        with_content(unbound).execute(command(1)?).await?;
        StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?;

        let has = |span, field, value| capture::has(&seen, span, field, value);
        assert!(has(
            "acyclic.harness.store.execute",
            "error.kind",
            "ERROR_CODE_UNSUPPORTED"
        ));
        assert!(has("acyclic.harness.store.execute", "outcome", "ok"));
        assert!(has("acyclic.harness.store.append", "rev", "0"));
        assert!(has("acyclic.harness.reducer.plan", "outcome", "ok"));
        assert!(has("acyclic.harness.reducer.apply_committed", "rev", "1"));
        assert!(has("acyclic.harness.store.open", "items", "1"));
        capture::assert_clean(&seen, &["conversation-1", "hello"]);
        let seen = seen.lock().unwrap();
        assert!(
            seen.iter()
                .all(|(span, ..)| span.starts_with("acyclic.harness."))
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
            .stream(authority().stream_path()?)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
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
            .stream(authority().stream_path()?)?
            .read(0, 3)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
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
    async fn snapshot_reopens_with_full_stream_history() -> Result<()> {
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
        let history = client.stream(authority().stream_path()?)?;
        assert_eq!(history.bounds().await?.tail, 3);
        assert_eq!(
            history
                .read(0, 3)
                .await?
                .try_collect::<Vec<_>>()
                .await?
                .len(),
            3
        );

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

    fn projection_command(identity: u128, revision: u64) -> Result<Command> {
        let mut command = command(1)?;
        command.operation_id = OperationId::from_bytes(identity.to_le_bytes());
        command.idempotency_key = IdempotencyKey::new(format!("projection-{identity}"))?;
        command.expected_revision = revision;
        Ok(command)
    }

    #[tokio::test]
    async fn default_cold_projection_reads_fixed_work_at_increasing_history() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        for cut in [64_u64, 1_024, 10_240] {
            while writer.reducer().revision() < cut + 5 {
                let revision = writer.reducer().revision();
                writer
                    .execute(projection_command(u128::from(revision + 1), revision)?)
                    .await?;
            }
            provider.forbid_writes.store(true, Ordering::SeqCst);
            provider.observation_reads.store(0, Ordering::SeqCst);
            provider.observation_maximum.store(0, Ordering::SeqCst);
            let started = std::time::Instant::now();
            let mut cold = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
            );
            let elapsed = started.elapsed();
            assert_eq!(cold.reducer().revision(), cut + 5);
            assert_eq!(cold.reducer().snapshot()?.events.len(), 6);
            assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 4);
            assert_eq!(provider.observation_maximum.load(Ordering::SeqCst), 5);
            let bytes = crate::contract::canonical_json_bytes(&cold.reducer().snapshot()?)?.len();
            assert!(bytes < 32_768);
            eprintln!(
                "cold-projection retained={} reads=4 max_records=5 resident_events=6 serialized_bytes={} elapsed_ns={}",
                cut + 5,
                bytes,
                elapsed.as_nanos()
            );
            assert!(matches!(
                cold.execute(projection_command(1, 0)?).await?,
                ApplyResult::Replayed { .. }
            ));
            assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
            provider.forbid_writes.store(false, Ordering::SeqCst);
        }
        Ok(())
    }

    #[tokio::test]
    async fn default_cold_projection_enforces_bounds_and_recovers_failed_observations() -> Result<()>
    {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        for revision in 0..69 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?)
                .await?;
        }
        drop(writer);
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let defaults = default_projection_read_limits();
        let loaded =
            checkpoints::load(&client, &authority(), &issuer().verifier(), defaults).await?;
        let suffix = HistoryReader::new(&client, &authority(), issuer().verifier())?
            .read_page(
                &HistoryCursor {
                    authority: authority(),
                    after_revision: 64,
                    through_revision: 69,
                },
                defaults,
            )
            .await?;
        let suffix_bytes = suffix.events.iter().try_fold(0_u64, |sum, event| {
            Ok::<_, Error>(sum + encode_event(&authority(), event)?.len() as u64)
        })?;
        let exact = HistoryReadLimits {
            maximum_events: 6,
            maximum_bytes: loaded.consumed_bytes + suffix_bytes,
        };
        assert_eq!(
            StreamAggregate::open_with_read_limits(
                &client,
                authority(),
                issuer().verifier(),
                schemas(),
                exact
            )
            .await?
            .reducer()
            .revision(),
            69
        );
        for limits in [
            HistoryReadLimits {
                maximum_events: 5,
                ..exact
            },
            HistoryReadLimits {
                maximum_bytes: exact.maximum_bytes - 1,
                ..exact
            },
            HistoryReadLimits {
                maximum_events: 0,
                ..exact
            },
        ] {
            assert!(
                StreamAggregate::open_with_read_limits(
                    &client,
                    authority(),
                    issuer().verifier(),
                    schemas(),
                    limits
                )
                .await
                .is_err()
            );
        }
        for fault in [1, 2, 3, 4] {
            provider.history_read_fault.store(fault, Ordering::SeqCst);
            assert!(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                    .await
                    .is_err()
            );
            assert_eq!(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas())
                    .await?
                    .reducer()
                    .revision(),
                69
            );
        }
        assert!(
            StreamAggregate::open(
                &client,
                authority(),
                AuthorityIssuer::new("test", [8; 32], authority()).verifier(),
                schemas()
            )
            .await
            .is_err()
        );
        assert_eq!(provider.observation_writes.load(Ordering::SeqCst), 0);
        Ok(())
    }

    #[tokio::test]
    async fn projection_checkpoint_commit_loss_preserves_original_admission() -> Result<()> {
        use std::sync::atomic::Ordering;
        for fault in [1, 2, 3, 4, 5] {
            let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
            let client = StreamClient::new(provider.clone());
            let mut writer = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
            );
            for revision in 0..63 {
                writer
                    .execute(projection_command(u128::from(revision + 1), revision)?)
                    .await?;
            }
            let original = projection_command(64, 63)?;
            provider
                .aggregate_commit_fault
                .store(fault, Ordering::SeqCst);
            let _uncertain = writer.execute(original.clone()).await;
            drop(writer);
            let mut cold = with_content(
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
            );
            cold.execute(original.clone()).await?;
            assert_eq!(cold.reducer().revision(), 64);
            assert!(matches!(
                cold.execute(original).await?,
                ApplyResult::Replayed { .. }
            ));
            assert_eq!(
                client
                    .stream("harness/v3/projection-checkpoints/conversations/conversation-1")?
                    .tail()
                    .await?,
                1
            );
        }
        Ok(())
    }

    fn forge_projection_payload(
        location: &mut Value,
        payload: &mut Vec<u8>,
        mode: u8,
    ) -> Result<()> {
        if mode == 1 {
            let mut forged: Value = crate::contract::json_from_slice(payload)
                .map_err(|error| Error::Invalid(error.to_string()))?;
            forged["projection"]["lifecycle"] = json!("completed");
            let digest = crate::contract::canonical_json_digest(&(
                &forged["format_version"],
                &forged["authority"],
                &forged["revision"],
                &forged["events"],
                &forged["projection"],
            ))?;
            forged["state_digest"] = json!(digest);
            *payload = crate::contract::canonical_json_bytes(&forged)?;
            location["digest"] = json!(*blake3::hash(payload).as_bytes());
            location["bytes"] = json!(payload.len());
            location["chunks"] = json!([{
                "digest": *blake3::hash(payload).as_bytes(), "bytes": payload.len()
            }]);
        } else if mode == 4 {
            location["bytes"] = json!(u64::MAX);
        } else if mode == 5 {
            payload[0] ^= 1;
        } else if mode == 6 {
            location["chunks"] = json!([]);
        } else if mode == 7 {
            location["chunks"][0]["bytes"] = json!(0);
        } else if mode == 8 {
            location["chunks"][0]["bytes"] = json!(1);
        }
        Ok(())
    }

    async fn publish_copied_projection(
        client: &StreamClient<MemoryStream>,
        canonical: &[acyclic_stream::Record],
        head_path: acyclic_stream::StreamPath,
        head_bytes: Bytes,
        non_atomic: bool,
    ) -> Result<()> {
        use acyclic_stream::{CommitCondition, CommitMutation, CommitRequest};
        let stream = client.stream(authority().stream_path()?)?;
        stream
            .append_batch(
                canonical[..63]
                    .iter()
                    .map(|record| record.value.clone())
                    .collect(),
                Some(0),
                None,
            )
            .await?;
        if non_atomic {
            stream.append(canonical[63].value.clone()).await?;
            client
                .stream(head_path.as_str())?
                .append(head_bytes)
                .await?;
        } else {
            let outcome = client
                .commit(CommitRequest {
                    conditions: vec![
                        CommitCondition::Tail {
                            path: stream.path().clone(),
                            expected: 63,
                        },
                        CommitCondition::Absent {
                            path: head_path.clone(),
                        },
                    ],
                    mutations: vec![
                        CommitMutation::Append {
                            path: stream.path().clone(),
                            records: vec![canonical[63].value.clone()],
                        },
                        CommitMutation::Append {
                            path: head_path,
                            records: vec![head_bytes],
                        },
                    ],
                    idempotency_key: StreamIdempotencyKey::new("copied-projection")?,
                })
                .await?;
            assert!(matches!(outcome, CommitOutcome::Committed(_)));
        }
        Ok(())
    }

    // Copy authentic events into an independent provider, then vary the derived
    // projection's visibility or bytes. This keeps the signed canonical control
    // valid while attacking the new cold-restore boundary itself.
    async fn copied_projection_client(
        source: &StreamClient<crate::test_stream::LostSessionAck<MemoryStream>>,
        mode: u8,
    ) -> Result<StreamClient<MemoryStream>> {
        use acyclic_stream::StreamPath;
        let head_path =
            StreamPath::new("harness/v3/projection-checkpoints/conversations/conversation-1")?;
        let index = source
            .stream(head_path.as_str())?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut location: Value = crate::contract::json_from_slice(&index[0].value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let digest: [u8; 32] = serde_json::from_value(location["chunks"][0]["digest"].clone())
            .map_err(|error| Error::Invalid(error.to_string()))?;
        let chunk_path = format!(
            "harness/v3/projection-chunks/conversations/conversation-1/{}",
            blake3::Hash::from_bytes(digest).to_hex()
        );
        let chunk = source
            .stream(&chunk_path)?
            .read(0, 1)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let mut payload = chunk[0].value.to_vec();
        forge_projection_payload(&mut location, &mut payload, mode)?;
        let canonical = source
            .stream(authority().stream_path()?)?
            .read(0, 64)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let client = StreamClient::new(Arc::new(MemoryStream::default()));
        if mode != 0 {
            let digest: [u8; 32] = serde_json::from_value(if mode == 6 {
                json!(*blake3::hash(&payload).as_bytes())
            } else {
                location["chunks"][0]["digest"].clone()
            })
            .map_err(|error| Error::Invalid(error.to_string()))?;
            client
                .stream(format!(
                    "harness/v3/projection-chunks/conversations/conversation-1/{}",
                    blake3::Hash::from_bytes(digest).to_hex()
                ))?
                .append(Bytes::from(payload))
                .await?;
        }
        publish_copied_projection(
            &client,
            &canonical,
            head_path,
            Bytes::from(crate::contract::canonical_json_bytes(&location)?),
            mode == 3,
        )
        .await?;
        Ok(client)
    }

    #[tokio::test]
    async fn cold_projection_rejects_forged_state_and_non_atomic_heads() -> Result<()> {
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let source = StreamClient::new(provider);
        let mut writer = with_content(
            StreamAggregate::open(&source, authority(), issuer().verifier(), schemas()).await?,
        );
        for revision in 0..64 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?)
                .await?;
        }
        for mode in 0..=8 {
            let client = copied_projection_client(&source, mode).await?;
            let result =
                StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await;
            match mode {
                1 => assert!(matches!(result, Err(Error::Unauthorized(_)))),
                2 => assert_eq!(result?.reducer().revision(), 64),
                _ => assert!(result.is_err()),
            }
        }
        Ok(())
    }
    #[tokio::test]
    async fn supplied_projection_charges_snapshot_and_suffix_before_io() -> Result<()> {
        use std::sync::atomic::Ordering;
        let provider = Arc::new(crate::test_stream::LostSessionAck::<MemoryStream>::default());
        let client = StreamClient::new(provider.clone());
        let mut writer = with_content(
            StreamAggregate::open(&client, authority(), issuer().verifier(), schemas()).await?,
        );
        for revision in 0..64 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?)
                .await?;
        }
        let snapshot = writer.reducer().snapshot_with_event_limit(1)?;
        let snapshot_bytes = crate::contract::canonical_json_bytes(&snapshot)?.len() as u64;
        for revision in 64..69 {
            writer
                .execute(projection_command(u128::from(revision + 1), revision)?)
                .await?;
        }
        let suffix = client
            .stream(authority().stream_path()?)?
            .read(64, 5)
            .await?
            .try_collect::<Vec<_>>()
            .await?;
        let bytes = snapshot_bytes
            + suffix
                .iter()
                .map(|record| record.value.len() as u64)
                .sum::<u64>();
        provider.forbid_writes.store(true, Ordering::SeqCst);
        let restored = StreamAggregate::open_from_snapshot_with_read_limits(
            &client,
            authority(),
            issuer().verifier(),
            schemas(),
            snapshot.clone(),
            HistoryReadLimits {
                maximum_events: 6,
                maximum_bytes: bytes,
            },
        )
        .await?;
        assert_eq!(restored.reducer().revision(), 69);
        for limits in [
            HistoryReadLimits {
                maximum_events: 5,
                maximum_bytes: bytes,
            },
            HistoryReadLimits {
                maximum_events: 6,
                maximum_bytes: bytes - 1,
            },
        ] {
            assert!(
                StreamAggregate::open_from_snapshot_with_read_limits(
                    &client,
                    authority(),
                    issuer().verifier(),
                    schemas(),
                    snapshot.clone(),
                    limits,
                )
                .await
                .is_err()
            );
        }
        provider.observation_reads.store(0, Ordering::SeqCst);
        assert!(
            StreamAggregate::open_from_snapshot_with_read_limits(
                &client,
                authority(),
                issuer().verifier(),
                schemas(),
                snapshot,
                HistoryReadLimits {
                    maximum_events: 6,
                    maximum_bytes: snapshot_bytes - 1
                },
            )
            .await
            .is_err()
        );
        assert_eq!(provider.observation_reads.load(Ordering::SeqCst), 0);
        Ok(())
    }
}
