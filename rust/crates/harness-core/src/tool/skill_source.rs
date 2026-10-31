use super::*;
use crate::{context::PinnedContextPath, executor::PreparationIo};

/// Executable script selected by the skill's authenticated metadata. The path
/// is volume-relative, beneath the same declared root, at its pinned generation.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize, crate::HeapLayout)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum SkillRuntime {
    /// Pass exact pinned script bytes and JSON input through the target's shell
    /// framing port. A target without that port refuses before execution.
    Shell { script: String },
    /// Execute exact pinned TypeScript with JSON input as data in the original
    /// shared code sandbox. Its original memory admission remains required.
    Code { script: String },
}

impl SkillRuntime {
    pub(crate) fn script(&self) -> &str {
        match self {
            Self::Shell { script } | Self::Code { script } => script,
        }
    }
}

/// Provenance admitted by the shared filesystem skill catalog. It is retained
/// on the original tool revision, never in a second callable registry.
///
/// Only the catalog can construct a provenance marker. A host cannot relabel an
/// ordinary registered function as a skill by attaching an unchecked file.
#[derive(Clone, Debug, Serialize)]
pub struct SkillSource {
    source: PinnedContextPath,
    runtime: SkillRuntime,
    definition: [u8; 32],
}

impl SkillSource {
    /// Catalog-only construction after its original descriptor/schema/runtime
    /// admission. This does not read, execute, or grant access to the file.
    pub(crate) fn admitted(
        source: PinnedContextPath,
        runtime: SkillRuntime,
        definition: &ToolDefinition,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            Self::validate_source(&source, &runtime, work)?;
            Ok(Self {
                source,
                runtime,
                definition: crate::executor::json::canonical_json_digest(
                    definition,
                    u64::MAX,
                    work,
                )?,
            })
        })
    }

    /// Exact lazy metadata source whose schema and runtime this revision admitted.
    pub const fn source(&self) -> &PinnedContextPath {
        &self.source
    }

    /// Runtime admitted with the same source and immutable tool revision.
    pub const fn runtime(&self) -> &SkillRuntime {
        &self.runtime
    }

    fn validate_source(
        source: &PinnedContextPath,
        runtime: &SkillRuntime,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<()> {
        source.validate(work)?;
        let script = runtime.script();
        crate::conversation::validate_content_path(script, work)?;
        if crate::conversation::is_internal_path(script)
            || (!source.root.directory.is_empty()
                && !script
                    .strip_prefix(&source.root.directory)
                    .is_some_and(|tail| tail.starts_with('/')))
        {
            return Err(crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "skill script is outside its pinned source root",
            ));
        }
        Ok(())
    }

    pub(crate) fn verify(
        &self,
        definition: &ToolDefinition,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            Self::validate_source(&self.source, &self.runtime, work)?;
            let digest = crate::executor::json::canonical_json_digest(definition, u64::MAX, work)?;
            if digest != self.definition {
                return Err(crate::executor::stock_boundary(
                    io,
                    crate::wire::ErrorCode::Conflict,
                    "skill provenance belongs to another tool definition",
                ));
            }
            Ok(())
        })
    }

    pub(crate) fn copy(&self, io: Option<&PreparationIo>) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let source =
                crate::core::copy(self, &mut quota, |source, _| source.logical_owned_bytes())?;
            quota.transfer();
            Ok(source)
        })
    }

    pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
        use crate::core::ownership::OwnedBytes as _;
        let Self {
            source,
            runtime,
            definition: _,
        } = self;
        let PinnedContextPath {
            root: crate::context::ContextRoot { volume, directory },
            generation,
            path,
        } = source;
        crate::core::owned_sum([
            volume.owned_heap_bytes()?,
            directory.capacity() as u64,
            generation.owned_heap_bytes()?,
            path.capacity() as u64,
            match runtime {
                SkillRuntime::Shell { script } | SkillRuntime::Code { script } => {
                    script.capacity() as u64
                }
            },
        ])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_provenance_cannot_be_reused_for_changed_input_or_revision() -> Result<()> {
        let mut original = crate::tool::files::read()?;
        let file = super::super::tests::file(b"metadata prefix only")?;
        let source = PinnedContextPath {
            root: crate::context::ContextRoot {
                volume: file.volume().clone(),
                directory: "skills".into(),
            },
            generation: crate::resources::GenerationRef::new(
                file.volume().provider(),
                &[1],
                None,
                None,
            )?
            .0,
            path: "skills/example/SKILL.md".into(),
        };
        for script in ["skills-other/run.ts", "outside.ts", "skills/../outside.ts"] {
            assert!(
                SkillSource::admitted(
                    source.clone(),
                    SkillRuntime::Code {
                        script: script.into(),
                    },
                    &original.definition,
                    None,
                )
                .is_err()
            );
        }
        let source = SkillSource::admitted(
            source,
            SkillRuntime::Code {
                script: "skills/example/run.ts".into(),
            },
            &original.definition,
            None,
        )?;
        original.skill_source = Some(source.clone());
        let mut registry = ToolRegistry::default();
        registry.register((original.clone(), None), &mut None, None)?;
        for change_revision in [false, true] {
            let mut changed = original.clone();
            if change_revision {
                changed.definition.revision.push_str("-changed");
            } else {
                changed.definition.input_schema = serde_json::json!({"const": "changed"});
            }
            let mut refused = ToolRegistry::default();
            assert!(refused.register((changed, None), &mut None, None).is_err());
            assert!(refused.definitions(None)?.is_empty());
        }
        // An ordinary function never acquires the skill marker by registration.
        let mut ordinary = ToolRegistry::default();
        ordinary.register((crate::tool::files::read()?, None), &mut None, None)?;
        assert!(
            ordinary
                .get("read", None, None)?
                .is_some_and(|tool| tool.skill_source.is_none())
        );
        Ok(())
    }
}
