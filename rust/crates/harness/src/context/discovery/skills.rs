use super::*;

/// Frontmatter only; the body is not fetched or injected by discovery.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
pub struct SkillMetadata {
    /// Stable frontmatter name.
    pub name: String,
    /// Frontmatter description.
    pub description: String,
    /// Additional declarative frontmatter fields; no execution authority is inferred.
    #[cfg_attr(feature = "wasm", tsify(type = "Record<string, WasmModelJsonValue>"))]
    pub fields: BTreeMap<String, serde_json::Value>,
    /// Exact lazy body location and provenance.
    pub source: PinnedContextPath,
}

impl SkillMetadata {
    /// Validates restored metadata without performing reads.
    pub fn validate(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        self.source.validate(work)?;
        if self.name.is_empty()
            || self.name.starts_with('-')
            || self.name.ends_with('-')
            || self.name.contains("--")
            || !self
                .name
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
            || self.description.trim().is_empty()
            || self
                .description
                .chars()
                .any(|ch| ch.is_control() && !ch.is_whitespace())
            || self.fields.contains_key("name")
            || self.fields.contains_key("description")
        {
            return Err(discovery_error(
                work,
                crate::wire::ErrorCode::Invalid,
                "skill metadata is invalid",
            ));
        }
        Ok(())
    }
}

pub(super) fn discovery_error(
    work: &Option<&mut dyn crate::store::history::HistoryIo>,
    code: crate::wire::ErrorCode,
    reason: &'static str,
) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if work
        .as_deref()
        .is_some_and(|io| io.original_native_work().is_some())
    {
        return crate::Error::NativeBoundary { code, reason };
    }
    let _ = work;
    match code {
        crate::wire::ErrorCode::Conflict => Error::Conflict(reason.into()),
        _ => Error::Invalid(reason.into()),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn discovery_work_error(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork {
        cause,
        custody: None,
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn bound_skill_values(
    skills: &[SkillMetadata],
    maximum_bytes: u64,
    io: &mut dyn crate::store::history::HistoryIo,
) -> Result<()> {
    fn byte_bound(bytes: usize, maximum: u64) -> Result<()> {
        if bytes as u64 > maximum {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "JSON exceeds declared byte bound",
            });
        }
        Ok(())
    }
    fn provider_bound(provider: &crate::resources::ProviderRef, maximum: u64) -> Result<()> {
        byte_bound(provider.namespace().len(), maximum)?;
        byte_bound(provider.family().len(), maximum)?;
        byte_bound(provider.version().len(), maximum)
    }
    fn value_bound(
        value: &serde_json::Value,
        depth: u32,
        maximum: u64,
        io: &mut dyn crate::store::history::HistoryIo,
    ) -> Result<()> {
        io.check_cancellation()?;
        if depth > 256 {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Invalid,
                reason: "JSON exceeds declared structural bound",
            });
        }
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        match value {
            serde_json::Value::String(text) => byte_bound(text.len(), maximum)?,
            serde_json::Value::Array(values) => {
                byte_bound(values.len(), maximum)?;
                for value in values {
                    value_bound(value, depth + 1, maximum, io)?;
                }
            }
            serde_json::Value::Object(values) => {
                byte_bound(values.len(), maximum)?;
                for (key, value) in values {
                    io.check_cancellation()?;
                    io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                    byte_bound(key.len(), maximum)?;
                    value_bound(value, depth + 1, maximum, io)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    // These are implied byte bounds, not a new cap on discovery or retained workspaces.
    byte_bound(skills.len(), maximum_bytes)?;
    for skill in skills {
        io.check_cancellation()?;
        io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        byte_bound(skill.name.len(), maximum_bytes)?;
        byte_bound(skill.description.len(), maximum_bytes)?;
        byte_bound(skill.fields.len(), maximum_bytes)?;
        byte_bound(skill.source.root.directory.len(), maximum_bytes)?;
        byte_bound(skill.source.path.len(), maximum_bytes)?;
        let volume = &skill.source.root.volume;
        byte_bound(volume.id().len(), maximum_bytes)?;
        provider_bound(volume.provider(), maximum_bytes)?;
        if let crate::conversation::VolumeOwner::Project(name) = volume.owner() {
            byte_bound(name.len(), maximum_bytes)?;
        }
        let generation = skill.source.generation.as_resource();
        provider_bound(generation.provider(), maximum_bytes)?;
        byte_bound(generation.key().len(), maximum_bytes)?;
        if let Some(version) = generation.version() {
            byte_bound(version.len(), maximum_bytes)?;
        }
        for (key, value) in &skill.fields {
            io.check_cancellation()?;
            io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 })?;
            byte_bound(key.len(), maximum_bytes)?;
            // skills array -> skill object -> fields object -> actual field value.
            value_bound(value, 3, maximum_bytes, io)?;
        }
    }
    Ok(())
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn skill_text_native(
    skills: &[SkillMetadata],
    size: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    struct Writer<'a, 'b> {
        bytes: &'a mut Vec<u8>,
        maximum: usize,
        work: &'b acyclic_fs::NativeMaterializationWork,
        failure: Option<Error>,
    }
    impl Writer<'_, '_> {
        fn fail(&mut self, error: Error) -> std::io::Error {
            self.failure = Some(error);
            // Serde's original io-error conversion allocates its one boxed ErrorImpl.
            if let Err(cause) = self.work.spent(acyclic_fs::WorkCounters {
                allocation_operations: 1,
                ..acyclic_fs::WorkCounters::default()
            }) {
                self.failure = Some(discovery_work_error(cause));
            }
            std::io::ErrorKind::Other.into()
        }
    }
    impl std::io::Write for Writer<'_, '_> {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let observed = self.work.spent(acyclic_fs::WorkCounters {
                bytes_encoded: bytes.len() as u64,
                ..acyclic_fs::WorkCounters::default()
            });
            if let Err(cause) = observed {
                return Err(self.fail(discovery_work_error(cause)));
            }
            if let Err(cause) = self.work.remaining() {
                return Err(self.fail(discovery_work_error(cause)));
            }
            if self
                .bytes
                .len()
                .checked_add(bytes.len())
                .is_none_or(|end| end > self.maximum)
            {
                return Err(self.fail(crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "discovered skill serialization exceeded its counted size",
                }));
            }
            if let Err(cause) = self.work.admit(acyclic_fs::WorkCounters {
                bytes_copied: bytes.len() as u64,
                ..acyclic_fs::WorkCounters::default()
            }) {
                return Err(self.fail(discovery_work_error(cause)));
            }
            self.bytes.extend_from_slice(bytes);
            if let Err(cause) = self.work.spent(acyclic_fs::WorkCounters {
                bytes_copied: bytes.len() as u64,
                ..acyclic_fs::WorkCounters::default()
            }) {
                return Err(self.fail(discovery_work_error(cause)));
            }
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let maximum = usize::try_from(size).map_err(|_| crate::Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "discovered skill serialization capacity overflows",
    })?;
    let mut output = (
        Vec::new(),
        work.claim(0, 0, acyclic_fs::WorkCounters::UNCHARGED)
            .map_err(discovery_work_error)?,
    );
    let mut backing = super::super::reserve_context_vec_native(&mut output.0, maximum, work)?;
    output
        .1
        .absorb(&mut backing)
        .map_err(discovery_work_error)?;
    let labels = [
        "SkillMetadata",
        "name",
        "description",
        "fields",
        "source",
        "PinnedContextPath",
        "root",
        "generation",
        "path",
        "ContextRoot",
        "volume",
        "directory",
        "VolumeRef",
        "ProviderRef",
        "namespace",
        "family",
        "version",
        "owner",
        "class",
        "GenerationRef",
        "ResourceRef",
        "kind",
        "provider",
        "key",
        "key must be a string",
        "number out of range",
    ];
    let diagnostic_bytes = crate::contract::audited_serde_diagnostic_bytes(
        size,
        labels.iter().map(|label| label.len() as u64 + 4).sum(),
    )?;
    let diagnostic = work
        .claim(diagnostic_bytes, 0, acyclic_fs::WorkCounters::UNCHARGED)
        .map_err(discovery_work_error)?;
    work.admit(acyclic_fs::WorkCounters {
        bytes_encoded: size,
        bytes_copied: size,
        allocation_operations: 1,
        ..acyclic_fs::WorkCounters::default()
    })
    .map_err(discovery_work_error)?;
    let mut writer = Writer {
        bytes: &mut output.0,
        maximum,
        work,
        failure: None,
    };
    let serialized = serde_json::to_writer(&mut writer, skills);
    let failure = writer.failure.take();
    drop(writer);
    if let Err(source) = serialized {
        if let Some(failure) = failure {
            drop(source);
            drop(diagnostic);
            return Err(failure);
        }
        if let Err(cause) = work.spent(acyclic_fs::WorkCounters {
            allocation_operations: 1,
            ..acyclic_fs::WorkCounters::default()
        }) {
            drop(source);
            return Err(Error::NativeWork {
                cause,
                custody: Some(diagnostic),
            });
        }
        return Err(Error::NativeEncode {
            code: crate::wire::ErrorCode::Invalid,
            source,
            custody: Some(diagnostic),
        });
    }
    drop(diagnostic);
    if output.0.len() != maximum {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "discovered skill serialization changed its counted size",
        });
    }
    work.remaining().map_err(discovery_work_error)?;
    let text = String::from_utf8(output.0).map_err(|_| crate::Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "discovered skill serialization is not UTF-8",
    })?;
    work.remaining().map_err(discovery_work_error)?;
    Ok((text, output.1))
}

#[derive(Deserialize)]
struct Frontmatter {
    name: String,
    description: String,
    #[serde(flatten)]
    fields: BTreeMap<String, serde_json::Value>,
}

/// Parses a bounded prefix.
pub fn parse_skill_metadata(prefix: &[u8], source: PinnedContextPath) -> Result<SkillMetadata> {
    source.validate(&mut None)?;
    let mut offset = 0;
    let mut header_start = None;
    let mut header_end = None;
    for line in prefix.split_inclusive(|byte| *byte == b'\n') {
        let delimiter = line.strip_suffix(b"\n").unwrap_or(line);
        let delimiter = delimiter.strip_suffix(b"\r").unwrap_or(delimiter);
        if offset == 0 {
            if delimiter != b"---" || !line.ends_with(b"\n") {
                return Err(Error::Invalid(
                    "skill requires opening frontmatter delimiter".into(),
                ));
            }
            header_start = Some(line.len());
        } else if delimiter == b"---" && line.ends_with(b"\n") {
            header_end = Some(offset);
            break;
        }
        offset += line.len();
    }
    let (Some(start), Some(end)) = (header_start, header_end) else {
        return Err(Error::Invalid(
            "skill frontmatter is missing or exceeds prefix bound".into(),
        ));
    };
    let header = prefix
        .get(start..end)
        .ok_or_else(|| Error::Invalid("invalid skill header".into()))?;
    let options = serde_saphyr::options! {
        budget: serde_saphyr::budget! {
            max_events: usize::MAX, max_nodes: usize::MAX, max_depth: usize::MAX,
            flow_nesting_limit: usize::MAX, max_documents: 1,
            max_aliases: 0, max_anchors: 0, max_merge_keys: 0,
            max_total_scalar_bytes: usize::MAX,
        },
        duplicate_keys: serde_saphyr::DuplicateKeyPolicy::Error,
        merge_keys: serde_saphyr::MergeKeyPolicy::Error,
        reject_unsupported_tags: true,
        emit_comments: false,
        with_snippet: false,
    };
    let metadata: Frontmatter = serde_saphyr::from_slice_with_options(header, options)
        .map_err(|error| Error::Invalid(format!("invalid skill frontmatter: {error}")))?;
    let metadata = SkillMetadata {
        name: metadata.name,
        description: metadata.description,
        fields: metadata.fields,
        source,
    };
    metadata.validate(&mut None)?;
    Ok(metadata)
}

pub(super) async fn discover_skills(
    reader: &dyn ContentResidencyVerifier,
    skills: &[ContextRoot],
    name: &str,
    work: &mut DiscoveryWork,
) -> Result<Vec<SkillMetadata>> {
    let mut result: Vec<SkillMetadata> = Vec::new();
    for root in skills {
        root.validate(&mut None)?;
        let (generation, entries) = work.list(reader, root, &root.directory, None).await?;
        for entry in entries {
            if entry.kind != PrivateDirectoryEntryKind::Directory {
                continue;
            }
            let directory = child(&root.directory, &entry.name);
            let (_, files) = work
                .list(reader, root, &directory, Some(&generation))
                .await?;
            let Some(entry) = files.iter().find(|entry| entry.name == name) else {
                continue;
            };
            if entry.kind != PrivateDirectoryEntryKind::File {
                return Err(Error::Invalid("skill entry is not a file".into()));
            }
            let source = PinnedContextPath {
                root: root.clone(),
                generation: generation.clone(),
                path: child(&directory, name),
            };
            let prefix = reader
                .read_private_prefix(
                    &root.volume,
                    &root.directory,
                    &source.path,
                    &generation,
                    u64::from(work.limits.header_bytes),
                    None,
                )?
                .await?;
            if prefix.len() > work.limits.header_bytes as usize {
                return Err(Error::Storage("prefix reader exceeded bound".into()));
            }
            let metadata = parse_skill_metadata(&prefix, source)?;
            if result.iter().any(|skill| skill.name == metadata.name) {
                return Err(Error::Conflict(format!(
                    "skill name {} is declared twice",
                    metadata.name
                )));
            }
            result.push(metadata);
        }
    }
    Ok(result)
}
