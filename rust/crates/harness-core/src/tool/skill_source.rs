use super::*;
use crate::{conversation::FileRef, executor::PreparationIo};

/// Provenance admitted by the shared filesystem skill catalog. It is retained
/// on the original tool revision, never in a second callable registry.
///
/// Only the catalog can construct a provenance marker. A host cannot relabel an
/// ordinary registered function as a skill by attaching an unchecked file.
#[derive(Clone, Debug, Serialize)]
pub struct SkillSource {
    file: FileRef,
    definition: [u8; 32],
}

impl SkillSource {
    /// Catalog-only construction after its original descriptor/schema/runtime
    /// admission. This does not read, execute, or grant access to the file.
    pub(crate) fn admitted(
        file: FileRef,
        definition: &ToolDefinition,
        io: Option<&PreparationIo>,
    ) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            file.validate(work)?;
            definition.validate(work)?;
            Ok(Self {
                file,
                definition: crate::executor::json::canonical_json_digest(
                    definition,
                    u64::MAX,
                    work,
                )?,
            })
        })
    }

    /// Exact immutable source whose schema and runtime this revision admitted.
    pub const fn file(&self) -> &FileRef {
        &self.file
    }

    pub(crate) fn verify(
        &self,
        definition: &ToolDefinition,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        crate::executor::with_history_io(io, |work| {
            self.file.validate(work)?;
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
        Ok(Self {
            file: crate::model::clone_file(&self.file, io)?,
            definition: self.definition,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skill_provenance_cannot_be_reused_for_changed_input_or_revision() -> Result<()> {
        let mut original = crate::tool::files::read()?;
        let source = SkillSource::admitted(
            super::super::tests::file(b"---\ndescription: source\n---\n")?,
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
