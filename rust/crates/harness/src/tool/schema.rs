//! Generated native tool contracts from their actual Rust argument/result types.

use crate::{Error, Result};
use schemars::{JsonSchema, generate::SchemaSettings};
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Generates a complete draft-2020-12 input contract, including root definitions.
pub fn input<T: JsonSchema>() -> Result<Value> {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_deserialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))
}

/// Generates a complete draft-2020-12 serialized canonical-result contract.
pub fn output<T: JsonSchema>() -> Result<Value> {
    serde_json::to_value(
        SchemaSettings::draft2020_12()
            .for_serialize()
            .into_generator()
            .into_root_schema_for::<T>(),
    )
    .map_err(|error| Error::Invalid(error.to_string()))
}

// This is also the actual serializer used by the native file projectors. Its
// complete schema root includes every nested result definition; it never embeds
// a second root schema whose $refs would incorrectly resolve from the envelope.
#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum JsonProjection<T> {
    Json { value: T },
}

/// Generates the full existing JSON projection envelope for one native result.
pub fn json_projection<T: JsonSchema>() -> Result<Value> {
    output::<JsonProjection<T>>()
}

pub(crate) fn project_json<T: Serialize>(value: T) -> Result<Value> {
    serde_json::to_value(JsonProjection::Json { value })
        .map_err(|error| Error::Invalid(error.to_string()))
}

/// Explicit model representation, independent of the retained canonical result.
#[cfg_attr(feature = "wasm", derive(tsify::Tsify))]
#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionMode {
    /// Complete typed value in the existing JSON envelope.
    Full,
    /// Summary and original immutable file reference; omitted details stay canonical.
    Reference,
}

// Actual serializers shared by file and text-result projectors. These constrain
// this reference-only producer, not the general native media/model contract.
#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ReferencePolicy {
    Reference,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReferencePart {
    Text {
        text: String,
    },
    File {
        file: Box<crate::conversation::FileRef>,
        policy: ReferencePolicy,
    },
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReferenceProjection {
    Parts { parts: [ReferencePart; 2] },
}

/// Complete schema of the actual reference-only parts serializer.
pub fn reference_projection() -> Result<Value> {
    output::<ReferenceProjection>()
}

pub(crate) fn project_reference(file: crate::conversation::FileRef, text: String) -> Result<Value> {
    serde_json::to_value(ReferenceProjection::Parts {
        parts: [
            ReferencePart::Text { text },
            ReferencePart::File {
                file: Box::new(file),
                policy: ReferencePolicy::Reference,
            },
        ],
    })
    .map_err(|error| Error::Invalid(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tool::{ToolInvocation, ToolProjection, ToolResult};
    use crate::{
        AgentId,
        conversation::{FileDescriptor, FileRef, VolumeClass, VolumeOwner, VolumeRef},
        resources::ProviderRef,
        tool::files::{
            self, EditFileInput, FileResult, PatchFileInput, ReadFileInput, WriteFileInput,
        },
    };
    use serde_json::json;

    fn file() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("schema", "filesystem", "1")?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([11; 16])),
            )?,
            "file.txt",
            "pinned-1",
            FileDescriptor::from_bytes(b"exact", "text/plain")?,
            "file.txt",
        )
    }

    #[test]
    fn existing_file_results_support_reference_projection_without_changing_canonical_values()
    -> Result<()> {
        use crate::model::{FileProjectionPolicy, ModelDataPart, ToolResultContent};
        use crate::tool::files::{FileResultProjection, ReadFileProjection};
        let file = file()?;
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "file-projection".into(),
            name: "acyclic.read_file".into(),
            arguments: json!({"file":file}),
        };
        let published = ToolResult {
            value: json!(FileResult { file: file.clone() }),
        };
        let read = ToolResult {
            value: json!("exact"),
        };
        for (canonical, projection, schema) in [
            (
                &published,
                FileResultProjection(ProjectionMode::Reference).project(&invocation, &published)?,
                FileResultProjection(ProjectionMode::Reference).schema()?,
            ),
            (
                &read,
                ReadFileProjection(ProjectionMode::Reference).project(&invocation, &read)?,
                ReadFileProjection(ProjectionMode::Reference).schema()?,
            ),
        ] {
            let unchanged = canonical.clone();
            crate::tool::validate_value(&schema, &projection, "file reference projection")?;
            let wire: ToolResultContent = serde_json::from_value(projection.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?;
            let ToolResultContent::Parts { parts } = wire else {
                panic!("reference parts")
            };
            assert!(
                matches!(parts.get(1),Some(ModelDataPart::File { file:actual,policy:FileProjectionPolicy::Reference }) if actual==&file)
            );
            assert!(
                matches!(parts.first(),Some(ModelDataPart::Text { text }) if text.contains("omitted"))
            );
            let mut changed = projection;
            changed["parts"][1]["policy"] = json!("bounded_full");
            assert!(crate::tool::validate_value(&schema, &changed, "unselected policy").is_err());
            assert_eq!(canonical, &unchanged);
        }
        assert_eq!(
            FileResultProjection(ProjectionMode::Full).project(&invocation, &published)?,
            json!({"kind":"json","value":published.value})
        );
        assert_eq!(
            ReadFileProjection(ProjectionMode::Full).project(&invocation, &read)?,
            json!({"kind":"json","value":read.value})
        );
        assert!(
            ReadFileProjection(ProjectionMode::Reference)
                .project(
                    &invocation,
                    &ToolResult {
                        value: json!("changed")
                    }
                )
                .is_err()
        );
        let mut tool = files::write_file()?;
        let original_digest = tool.definition.digest()?;
        let projector = FileResultProjection(ProjectionMode::Reference);
        tool.definition.projection_schema = projector.schema()?;
        tool.projection = std::sync::Arc::new(projector);
        assert_ne!(tool.definition.digest()?, original_digest);
        assert_eq!(
            tool.projection.project(&invocation, &published)?,
            FileResultProjection(ProjectionMode::Reference).project(&invocation, &published)?
        );
        Ok(())
    }

    #[test]
    #[allow(
        clippy::too_many_lines,
        reason = "one nested-schema scenario checks draft, reference resolution, roundtrip and malformed field boundaries"
    )]
    fn real_nested_file_schemas_compile_and_keep_refs_at_the_complete_root() -> Result<()> {
        let file = file()?;
        let result = FileResult { file: file.clone() };
        let canonical =
            serde_json::to_value(&result).map_err(|error| Error::Invalid(error.to_string()))?;
        let input_schema = input::<ReadFileInput>()?;
        let output_schema = output::<FileResult>()?;
        let projection_schema = json_projection::<FileResult>()?;
        for schema in [&input_schema, &output_schema, &projection_schema] {
            assert_eq!(
                schema.get("$schema"),
                Some(&json!("https://json-schema.org/draft/2020-12/schema"))
            );
            assert!(schema.get("$defs").is_some());
            crate::contract::compile_json_schema(schema, "generated native tool")?;
        }
        crate::tool::validate_value(&input_schema, &json!({"file":file}), "generated read")?;
        crate::tool::validate_value(&output_schema, &canonical, "generated result")?;
        let projection = project_json(result.clone())?;
        crate::tool::validate_value(&projection_schema, &projection, "generated projection")?;
        assert_eq!(projection, json!({"kind":"json","value":canonical}));
        assert_eq!(
            serde_json::from_value::<FileResult>(canonical.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?,
            result
        );
        assert!(
            crate::tool::validate_value(&projection_schema, &canonical, "bare result").is_err()
        );
        assert!(
            crate::tool::validate_value(&output_schema, &projection, "wrong canonical envelope")
                .is_err()
        );
        for pointer in [
            "/file/version",
            "/file/display_name",
            "/file/volume/provider/version",
            "/file/descriptor/media_type",
        ] {
            let mut changed = canonical.clone();
            *changed
                .pointer_mut(pointer)
                .ok_or_else(|| Error::Invalid("fixture field missing".into()))? = json!(1);
            assert!(
                crate::tool::validate_value(&output_schema, &changed, "wrong nested type").is_err()
            );
        }
        let mut changed = canonical.clone();
        changed
            .get_mut("file")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::Invalid("fixture ref missing".into()))?
            .remove("version");
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "missing nested field").is_err()
        );
        let mut changed = canonical.clone();
        changed
            .pointer_mut("/file/descriptor")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::Invalid("fixture descriptor missing".into()))?
            .insert("extra".into(), json!(true));
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "unknown nested field").is_err()
        );
        for digest in [vec![0; 31], vec![0; 33], vec![256; 32]] {
            let mut changed = canonical.clone();
            *changed
                .pointer_mut("/file/descriptor/sha256")
                .ok_or_else(|| Error::Invalid("fixture digest missing".into()))? = json!(digest);
            assert!(
                crate::tool::validate_value(&output_schema, &changed, "invalid digest").is_err()
            );
        }
        let mut changed = canonical;
        *changed
            .pointer_mut("/file/descriptor/byte_length")
            .ok_or_else(|| Error::Invalid("fixture length missing".into()))? =
            json!(crate::conversation::MAX_EXACT_JS_INTEGER + 1);
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "imprecise descriptor").is_err()
        );
        Ok(())
    }

    #[test]
    fn file_factories_use_their_compile_time_argument_and_result_contracts() -> Result<()> {
        let file = file()?;
        let read = files::read_file()?;
        assert_eq!(read.definition.input_schema, input::<ReadFileInput>()?);
        assert_eq!(read.definition.output_schema, output::<String>()?);
        let write = files::write_file()?;
        assert_eq!(write.definition.input_schema, input::<WriteFileInput>()?);
        let edit = files::edit_file()?;
        assert_eq!(edit.definition.input_schema, input::<EditFileInput>()?);
        let patch = files::patch_file(8192, 8)?;
        let mut expected = input::<PatchFileInput>()?;
        expected
            .as_object_mut()
            .ok_or_else(|| Error::Invalid("schema not object".into()))?
            .insert(
                "x-harness-patch-limits".into(),
                json!({"maximum_work":8192,"maximum_hunks":8}),
            );
        assert_eq!(patch.definition.input_schema, expected);
        for tool in [&write, &edit, &patch] {
            assert_eq!(tool.definition.output_schema, output::<FileResult>()?);
            assert_eq!(
                tool.definition.projection_schema,
                json_projection::<FileResult>()?
            );
            tool.definition.validate()?;
        }
        assert!(
            crate::tool::validate_value(
                &edit.definition.input_schema,
                &json!({"file":file,"old_text":"","new_text":"x"}),
                "empty edit"
            )
            .is_err()
        );
        assert!(
            crate::tool::validate_value(
                &patch.definition.input_schema,
                &json!({"file":file,"diff":""}),
                "empty patch"
            )
            .is_err()
        );
        // Shape schemas do not replace business validators or signed authority.
        let mut invalid = json!({"file":file});
        *invalid
            .pointer_mut("/file/path")
            .ok_or_else(|| Error::Invalid("fixture path missing".into()))? = json!("../escape");
        assert!(serde_json::from_value::<ReadFileInput>(invalid).is_err());
        Ok(())
    }
}
