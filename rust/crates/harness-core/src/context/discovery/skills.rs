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
    /// Loads the selected skill at its discovered generation under a finite body limit.
    pub async fn invoke(
        &self,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
    ) -> Result<ModelMessage> {
        self.invoke_observed(reader, maximum_bytes, None).await
    }

    pub(crate) async fn invoke_observed(
        &self,
        reader: &dyn ContentResidencyVerifier,
        maximum_bytes: u64,
        io: Option<&crate::executor::PreparationIo>,
    ) -> Result<ModelMessage> {
        use crate::{executor::stock_boundary, wire::ErrorCode};
        crate::executor::with_history_io(io, |work| self.validate(work))?;
        if maximum_bytes == 0 || maximum_bytes > crate::conversation::MAX_EXACT_JS_INTEGER {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "invalid skill byte bound",
            ));
        }
        #[cfg(feature = "filesystem")]
        let content_io = io;
        #[cfg(not(feature = "filesystem"))]
        let content_io = None;
        let source = &self.source;
        let (file, bytes, hold) = reader
            .read_private_path(
                &source.root.volume,
                &source.root.directory,
                &source.path,
                Some(&source.generation),
                maximum_bytes,
                content_io,
            )?
            .await?;
        if file.volume() != &source.root.volume
            || file.path() != source.path
            || bytes.len() as u64 > maximum_bytes
        {
            return Err(stock_boundary(
                io,
                ErrorCode::Storage,
                "pinned skill reader returned another file",
            ));
        }
        crate::executor::with_history_io(io, |work| file.descriptor().verify(&bytes, work))?;
        crate::model::charge_items(io, bytes.len() as u64)?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| stock_boundary(io, ErrorCode::Invalid, "skill body is invalid text"))?;
        if text.contains('\0') {
            return Err(stock_boundary(
                io,
                ErrorCode::Invalid,
                "skill body is invalid text",
            ));
        }
        let text = if io.is_some() {
            crate::executor::stock_string_copy(text, io)?
        } else {
            let bytes = bytes
                .try_into_ordinary_vec()
                .unwrap_or_else(|bytes| bytes.to_vec());
            String::from_utf8(bytes)
                .map_err(|_| stock_boundary(io, ErrorCode::Invalid, "skill body is invalid text"))?
        };
        #[cfg(feature = "filesystem")]
        drop(hold);
        #[cfg(not(feature = "filesystem"))]
        let _ = hold;
        Ok(ModelMessage {
            role: ModelRole::System,
            content: ModelContent::Text(text),
        })
    }

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
    let metadata: Frontmatter = parse_frontmatter(prefix)?;
    let metadata = SkillMetadata {
        name: metadata.name,
        description: metadata.description,
        fields: metadata.fields,
        source,
    };
    metadata.validate(&mut None)?;
    Ok(metadata)
}

#[doc(hidden)]
pub async fn discover_skills(
    reader: &dyn ContentResidencyVerifier,
    skills: &[ContextRoot],
    name: &str,
    work: &mut DiscoveryWork,
) -> Result<Vec<SkillMetadata>> {
    discover_skills_at(reader, skills, name, work, None).await
}

pub(crate) async fn discover_skills_at(
    reader: &dyn ContentResidencyVerifier,
    skills: &[ContextRoot],
    name: &str,
    work: &mut DiscoveryWork,
    generation: Option<&GenerationRef>,
) -> Result<Vec<SkillMetadata>> {
    let mut result: Vec<SkillMetadata> = Vec::new();
    let mut names = std::collections::HashSet::new();
    for root in skills {
        root.validate(&mut None)?;
        let (generation, entries) = work.list(reader, root, &root.directory, generation).await?;
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
            if !names.insert(metadata.name.clone()) {
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
