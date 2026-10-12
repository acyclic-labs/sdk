//! Immutable extension schema bindings.
use super::*;
/// Immutable schema and implementation binding for one extension version.
#[derive(Clone, Debug, PartialEq, Serialize)]
pub(in crate::core) struct ExtensionBinding {
    pub(in crate::core) schema: Value,
    pub(in crate::core) schema_digest: [u8; 32],
    pub(in crate::core) implementation_digest: [u8; 32],
    pub(in crate::core) fork_policy: ExtensionForkPolicy,
    pub(in crate::core) dependencies: BTreeSet<ExtensionDependency>,
    pub(in crate::core) configuration_schema: Option<Value>,
    pub(in crate::core) configuration_schema_digest: Option<[u8; 32]>,
}

/// Immutable-version registry for namespaced extension implementations and state schemas.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SchemaRegistry {
    pub(in crate::core) schemas: Option<Arc<BTreeMap<(String, u32), ExtensionBinding>>>,
}

impl SchemaRegistry {
    /// Creates an empty registry.
    #[must_use]
    pub const fn new() -> Self {
        Self { schemas: None }
    }
    pub(crate) fn copy(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if let Some(io) = work.as_deref_mut().filter(|io| copying::is_native(&**io)) {
            io.check_cancellation()?;
        }
        Ok(self.clone())
    }

    /// Installs a version with a separately validated, file-backed configuration.
    /// `None` declares that activation requires no configuration.
    #[allow(
        clippy::too_many_arguments,
        reason = "one immutable extension declaration"
    )]
    pub fn register(
        &mut self,
        name: impl Into<String>,
        version: u32,
        schema: Value,
        implementation_digest: [u8; 32],
        fork_policy: ExtensionForkPolicy,
        dependencies: impl IntoIterator<Item = ExtensionDependency>,
        configuration_schema: Option<Value>,
    ) -> Result<()> {
        let name = name.into();
        validate_extension_name(&name)?;
        if version == 0 || implementation_digest == [0; 32] {
            return Err(Error::Invalid(
                "extension binding needs a namespaced name, positive version, and implementation digest".into(),
            ));
        }
        crate::contract::compile_json_schema(&schema, "extension", &mut None)?;
        if let Some(configuration_schema) = &configuration_schema {
            crate::contract::compile_json_schema(configuration_schema, "configuration", &mut None)?;
        }
        let mut required = BTreeSet::new();
        for dependency in dependencies {
            dependency.validate()?;
            if dependency.name == name && dependency.version == version {
                return Err(Error::Conflict("extension depends on itself".into()));
            }
            if !required.insert(dependency) {
                return Err(Error::Conflict("duplicate extension dependency".into()));
            }
        }
        let binding = ExtensionBinding {
            schema_digest: crate::contract::canonical_json_digest(&schema)?,
            schema,
            implementation_digest,
            fork_policy,
            dependencies: required,
            configuration_schema_digest: configuration_schema
                .as_ref()
                .map(crate::contract::canonical_json_digest)
                .transpose()?,
            configuration_schema,
        };
        let key = (name, version);
        if let Some(existing) = self
            .schemas
            .as_deref()
            .and_then(|schemas| schemas.get(&key))
        {
            return if existing == &binding {
                Ok(())
            } else {
                Err(Error::Conflict(
                    "extension identity is already pinned to another implementation".into(),
                ))
            };
        }
        Arc::make_mut(
            self.schemas
                .get_or_insert_with(|| Arc::new(BTreeMap::new())),
        )
        .insert(key, binding);
        Ok(())
    }

    /// Resolves selected versions in dependency-first order.
    pub fn activation_order(
        &self,
        selected: &[ExtensionDependency],
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<(Vec<ExtensionDependency>, u64)> {
        let mut owned = crate::fork::CodecQuota::observed(work)?;
        let mut chosen = BTreeMap::<&str, &ExtensionDependency>::new();
        let mut pending = BTreeSet::new();
        for extension in selected {
            if !pending.contains(extension) {
                owned.slots::<&ExtensionDependency>(1)?;
                pending.insert(extension);
            }
        }
        while let Some(extension) = pending.pop_last() {
            owned.release(std::mem::size_of::<&ExtensionDependency>() as u64)?;
            owned.checkpoint()?;
            extension.validate()?;
            if let Some(prior) = chosen.get(extension.name.as_str()) {
                if prior.version != extension.version {
                    return Err(crate::fork::boundary_error(
                        owned.work(),
                        crate::wire::ErrorCode::Conflict,
                        "extension requires conflicting versions",
                    ));
                }
                continue;
            }
            let binding =
                self.pinned_binding(&extension.name, extension.version, None, owned.work())?;
            owned.slots::<(&str, &ExtensionDependency)>(1)?;
            chosen.insert(&extension.name, extension);
            for dependency in &binding.dependencies {
                if !pending.contains(dependency) {
                    owned.slots::<&ExtensionDependency>(1)?;
                    pending.insert(dependency);
                }
            }
        }
        let mut order =
            copying::reserve_copy_vector::<ExtensionDependency>(chosen.len(), &mut owned)?;
        while order.len() < chosen.len() {
            let mut next = None;
            for extension in chosen.values().copied() {
                owned.checkpoint()?;
                if order.iter().any(|ready| ready == extension) {
                    continue;
                }
                let binding =
                    self.pinned_binding(&extension.name, extension.version, None, owned.work())?;
                let mut ready = true;
                for dependency in &binding.dependencies {
                    owned.checkpoint()?;
                    if !order.contains(dependency) {
                        ready = false;
                        break;
                    }
                }
                if ready {
                    next = Some(extension);
                    break;
                }
            }
            let next = next.ok_or_else(|| {
                crate::fork::boundary_error(
                    owned.work(),
                    crate::wire::ErrorCode::Conflict,
                    "extension dependency cycle",
                )
            })?;
            order.push(copying::copy_dependency(next, &mut owned)?);
        }
        let graph_bytes = (chosen.len() as u64)
            .checked_mul(std::mem::size_of::<(&str, &ExtensionDependency)>() as u64)
            .ok_or_else(|| Error::Invalid("activation graph ownership overflows".into()))?;
        drop((chosen, pending));
        owned.release(graph_bytes)?;
        let produced = owned.retained();
        owned.transfer();
        Ok((order, produced))
    }

    pub(in crate::core) fn pinned_binding(
        &self,
        name: &str,
        version: u32,
        expected: Option<([u8; 32], [u8; 32], ExtensionForkPolicy)>,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<&ExtensionBinding> {
        let mut selected = None;
        if let Some(schemas) = &self.schemas {
            for ((candidate, candidate_version), binding) in schemas.iter() {
                if let Some(io) = work.as_deref_mut() {
                    io.check_cancellation()?;
                }
                if candidate == name && *candidate_version == version {
                    selected = Some(binding);
                    break;
                }
            }
        }
        let binding = selected.ok_or_else(|| {
            crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Unsupported,
                "extension implementation is not installed",
            )
        })?;
        if expected.is_some_and(|(schema, implementation, policy)| {
            schema != binding.schema_digest
                || implementation != binding.implementation_digest
                || policy != binding.fork_policy
        }) {
            return Err(crate::fork::boundary_error(
                work,
                crate::wire::ErrorCode::Conflict,
                "extension binding mismatch",
            ));
        }
        Ok(binding)
    }

    /// Validates exact extension content against its installed schema.
    pub fn validate_bytes(
        &self,
        name: &str,
        version: u32,
        content: &FileRef,
        bytes: &[u8],
    ) -> Result<()> {
        content.validate(&mut None)?;
        if content.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("extension content must be JSON".into()));
        }
        content.descriptor().verify(bytes, &mut None)?;
        let value: Value = crate::contract::json_from_slice(bytes)
            .map_err(|error| Error::Invalid(format!("extension content is not JSON: {error}")))?;
        let binding = self.pinned_binding(name, version, None, &mut None)?;
        crate::contract::validate_json_schema_value(
            &binding.schema,
            &value,
            "extension payload", &mut None)?;
        Ok(())
    }

    pub(in crate::core) fn configuration_record(
        &self,
        extension: &ExtensionDependency,
        content: &FileRef,
        quota: &mut crate::wire_codec::CodecQuota<'_, '_>,
    ) -> Result<ExtensionConfiguration> {
        extension.validate()?;
        let binding =
            self.pinned_binding(&extension.name, extension.version, None, quota.work())?;
        let schema_digest = binding
            .configuration_schema_digest
            .ok_or_else(|| Error::Invalid("extension does not declare configuration".into()))?;
        let record = ExtensionConfiguration {
            extension: copying::copy_dependency(extension, quota)?,
            schema_digest,
            content: copying::copy_file(content, quota)?,
        };
        record.validate()?;
        Ok(record)
    }

    /// Returns the exact installed implementation for a version.
    pub fn implementation_digest(
        &self,
        name: &str,
        version: u32,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<[u8; 32]> {
        Ok(self
            .pinned_binding(name, version, None, work)?
            .implementation_digest)
    }

    /// Checks configuration content against the registry and linked implementation.
    pub(crate) fn verify_configuration_binding(
        &self,
        record: &ExtensionConfiguration,
        expected_implementation_digest: [u8; 32],
        bytes: &[u8],
        limits: crate::conversation::Limits,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        limits.validate_file(&record.content, &mut None)?;
        let byte_length = u64::try_from(bytes.len())
            .map_err(|_| Error::Invalid("configuration byte length is not portable".into()))?;
        if byte_length > limits.file_bytes
            || byte_length > limits.render_bytes
            || record.content.descriptor().byte_length() > limits.render_bytes
        {
            return Err(Error::Invalid(
                "configuration exceeds native option byte limits".into(),
            ));
        }
        record.validate()?;
        let binding = crate::executor::with_history_io(io, |work| {
            self.pinned_binding(&record.extension.name, record.extension.version, None, work)
        })?;
        if binding.configuration_schema_digest != Some(record.schema_digest) {
            return Err(Error::Conflict(
                "configuration registry record mismatch".into(),
            ));
        }
        if binding.implementation_digest != expected_implementation_digest {
            return Err(Error::Conflict(
                "configuration implementation binding mismatch".into(),
            ));
        }
        self.validate_configuration_bytes(&record.extension, &record.content, bytes, io)
    }

    /// Validates staged configuration bytes against the installed schema.
    pub fn validate_configuration_bytes(
        &self,
        extension: &ExtensionDependency,
        content: &FileRef,
        bytes: &[u8],
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<()> {
        extension.validate()?;
        crate::executor::with_history_io(io, |work| content.validate(work))?;
        if content.descriptor().media_type() != "application/json" {
            return Err(Error::Invalid("extension configuration is invalid".into()));
        }
        crate::executor::with_history_io(io, |work| content.descriptor().verify(bytes, work))?;
        let value;
        #[cfg(feature = "filesystem")]
        let decoded;
        let value = match io {
            #[cfg(feature = "filesystem")]
            Some(io) => {
                decoded = io.decode(
                    std::marker::PhantomData::<Value>,
                    bytes,
                    crate::wire::ErrorCode::Storage,
                )?;
                &decoded.0
            }
            _ => {
                value = crate::contract::json_from_slice(bytes).map_err(|error| {
                    Error::Invalid(format!("extension configuration is not JSON: {error}"))
                })?;
                &value
            }
        };
        let binding = crate::executor::with_history_io(io, |work| {
            self.pinned_binding(&extension.name, extension.version, None, work)
        })?;
        let schema = binding
            .configuration_schema
            .as_ref()
            .ok_or_else(|| Error::Invalid("extension does not declare configuration".into()))?;
        crate::executor::with_history_io(io, |work| crate::contract::validate_json_schema_value(schema, value, "configuration", work))
    }
}
