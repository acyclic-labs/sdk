use super::*;

/// Exact-version native migration registry for local and customer-hosted runs.
#[derive(Default)]
pub struct NativeExtensionMigrations {
    implementations:
        BTreeMap<(String, u32, u32, [u8; 32]), Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>>,
}

impl NativeExtensionMigrations {
    /// Pins one installed target implementation to a pure state transformation.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        from_version: u32,
        to_version: u32,
        target_implementation_digest: [u8; 32],
        migration: Arc<dyn Fn(&Value) -> Result<Value> + Send + Sync>,
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

/// Caller-retained identity of one exact state migration.
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

pub(super) fn extension_migration_command(
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

impl<P: StreamProvider> StreamAggregate<P> {
    /// Executes a pinned pure migration, stages its bytes through the owning provider, then publishes only the resulting ref.
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
        if let Some(event) = self.operation_event(request.operation_id).await? {
            return self
                .replay_extension_migration(request, scope, publisher, event.revision)
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
                None,
            )?
            .await?
            .0;
        if content.volume() != publisher.volume() {
            return Err(Error::Unauthorized(
                "extension publisher returned another volume".into(),
            ));
        }
        self.execute(
            extension_migration_command(request, scope, content)?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await
    }

    pub(super) async fn replay_extension_migration(
        &mut self,
        request: ExtensionMigrationRequest,
        scope: Scope,
        publisher: &dyn ContentPublisher,
        revision: u64,
    ) -> Result<ApplyResult> {
        let event = self.read_event_at(revision, &mut None).await?.0;
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
        self.execute(
            extension_migration_command(request, scope, migration.record.content.clone())?,
            #[cfg(feature = "filesystem")]
            None,
            &mut None,
        )
        .await
    }

    pub(super) async fn extension_migration_output(
        &self,
        request: &ExtensionMigrationRequest,
    ) -> Result<Vec<u8>> {
        let prior_event = self
            .read_event_at(request.previous.revision, &mut None)
            .await?
            .0;
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
        self.limits.validate_file(&prior.content, &mut None)?;
        let bytes = verified_content_bytes(
            verifier.as_ref(),
            &prior.content,
            (&prior.content).descriptor().byte_length(),
            None,
        )
        .await
        .map(|(bytes, _)| bytes)?;
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

    pub(super) async fn validate_extension_admission_content(
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
            let bytes = verifier
                .read(content, (content).descriptor().byte_length(), None)?
                .await?;
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
            self.limits.validate_file(&prior.content, &mut None)?;
            self.limits.validate_file(content, &mut None)?;
            let old_bytes = verified_content_bytes(
                verifier.as_ref(),
                &prior.content,
                (&prior.content).descriptor().byte_length(),
                None,
            )
            .await?
            .0;
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
            let bytes = verified_content_bytes(
                verifier.as_ref(),
                content,
                (content).descriptor().byte_length(),
                None,
            )
            .await?
            .0;
            self.reducer
                .validate_custom_bytes(name, *to_version, content, &bytes)?;
            if bytes.as_ref() != crate::contract::canonical_json_bytes(&expected)?.as_slice() {
                return Err(Error::Conflict(
                    "staged extension migration differs from its pinned implementation".into(),
                ));
            }
        }
        if let crate::core::Action::ConfigureExtension { extension, content } = action {
            let verifier = self.content_verifier.as_ref().ok_or_else(|| {
                Error::Unsupported("extension content verifier is not bound".into())
            })?;
            let bytes = verifier
                .read(content, (content).descriptor().byte_length(), None)?
                .await?;
            self.reducer
                .validate_configuration_bytes(extension, content, &bytes, None)?;
        }
        Ok(())
    }
}
