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

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(super) fn skill_text_native(
    skills: &[SkillMetadata],
    size: u64,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(String, acyclic_fs::NativeAllocation)> {
    use crate::store::history::HistoryIo as _;
    let mut observer =
        crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?);
    let bounds = crate::executor::json::JsonBounds {
        maximum_bytes: size,
        maximum_items: work.remaining()?.items_examined,
        maximum_depth: 256,
    };
    let diagnostic = work.claim(
        crate::executor::json::sdk_json_diagnostic_bytes(),
        0,
        acyclic_fs::WorkCounters::UNCHARGED,
    )?;
    let bytes = match crate::executor::json::json_bytes_bounded(
        &skills,
        bounds,
        Some(crate::wire::ErrorCode::Invalid),
        &mut |charge| observer.codec_charge(charge),
    ) {
        Err(Error::NativeJson {
            code,
            source,
            custody: None,
        }) => {
            return Err(Error::NativeJson {
                code,
                source,
                custody: Some(diagnostic),
            });
        }
        result => result?,
    };
    drop(diagnostic);
    if bytes.len() as u64 != size {
        return Err(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Invalid,
            reason: "discovered skill serialization changed its counted size",
        });
    }
    let text = String::from_utf8(bytes).map_err(|_| Error::NativeBoundary {
        code: crate::wire::ErrorCode::Invalid,
        reason: "discovered skill serialization is not UTF-8",
    })?;
    Ok((text, observer.into_custody()?))
}
