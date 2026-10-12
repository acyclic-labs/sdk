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

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum JsonProjection<T> {
    Json { value: T },
}

/// Generates the full existing JSON projection envelope for one native result.
pub fn json_projection<T: JsonSchema>() -> Result<Value> {
    output::<JsonProjection<T>>()
}

pub(crate) fn project_json<T: Serialize>(
    value: &T, _work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = _work {
        return super::value(&JsonProjection::Json { value }, work.remaining()?.bytes_encoded, work)
            .map(|(value, custody)| (value, Some(custody)));
    }
    serde_json::to_value(JsonProjection::Json { value })
        .map(|value| (value, None))
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

#[derive(Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum ReferencePolicy {
    Reference,
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReferencePart<'a> {
    Text {
        text: &'a str,
    },
    File {
        file: &'a crate::conversation::FileRef,
        policy: ReferencePolicy,
    },
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ReferenceProjection<'a> {
    Parts { parts: [ReferencePart<'a>; 2] },
}

/// Complete schema of the actual reference-only parts serializer.
pub fn reference_projection() -> Result<Value> {
    output::<ReferenceProjection<'_>>()
}

pub(crate) fn project_reference(
    file: &crate::conversation::FileRef, text: &str, _work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    let projection = ReferenceProjection::Parts {
        parts: [
            ReferencePart::Text { text },
            ReferencePart::File { file, policy: ReferencePolicy::Reference },
        ],
    };
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(work) = _work {
        return super::value(&projection, work.remaining()?.bytes_encoded, work)
            .map(|(value, custody)| (value, Some(custody)));
    }
    serde_json::to_value(projection)
        .map(|value| (value, None))
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
        tool::files::{self, FileResult, ReadFileInput},
    };
    use serde_json::json;

    fn file() -> Result<FileRef> {
        FileRef::new(
            VolumeRef::new(
                ProviderRef::new("schema".into(), "filesystem".into(), "1".into(), &mut None)?,
                "private",
                VolumeClass::AgentPrivate,
                VolumeOwner::Agent(AgentId::from_bytes([11; 16])),
             &mut None)?,
            "file.txt",
            "pinned-1",
            FileDescriptor::from_bytes(b"exact", "text/plain", &mut None)?,
            "file.txt",
         &mut None)
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
                FileResultProjection(ProjectionMode::Reference).project(&invocation, &published, None)?.0,
                FileResultProjection(ProjectionMode::Reference).schema()?,
            ),
            (
                &read,
                ReadFileProjection(ProjectionMode::Reference).project(&invocation, &read, None)?.0,
                ReadFileProjection(ProjectionMode::Reference).schema()?,
            ),
        ] {
            let unchanged = canonical.clone();
            crate::tool::validate_value(&schema, &projection, "file reference projection", None)?;
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
            assert!(crate::tool::validate_value(&schema, &changed, "unselected policy", None).is_err());
            assert_eq!(canonical, &unchanged);
        }
        assert_eq!(
            FileResultProjection(ProjectionMode::Full).project(&invocation, &published, None)?.0,
            json!({"kind":"json","value":published.value})
        );
        assert_eq!(
            ReadFileProjection(ProjectionMode::Full).project(&invocation, &read, None)?.0,
            json!({"kind":"json","value":read.value})
        );
        assert!(
            ReadFileProjection(ProjectionMode::Reference)
                .project(
                    &invocation,
                    &ToolResult {
                        value: json!("changed")
                    },
                    None,
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
            tool.projection.project(&invocation, &published, None)?.0,
            FileResultProjection(ProjectionMode::Reference).project(&invocation, &published, None)?.0
        );
        Ok(())
    }

    #[test]
    fn real_nested_file_schemas_compile_and_keep_refs_at_the_complete_root() -> Result<()> {
        let file = file()?;
        let result = FileResult { file: file.clone() };
        let canonical =
            serde_json::to_value(&result).map_err(|error| Error::Invalid(error.to_string()))?;
        let input_schema = input::<ReadFileInput>()?;
        let output_schema = output::<FileResult>()?;
        let projection_schema = json_projection::<FileResult>()?;
        for schema in [&input_schema, &output_schema, &projection_schema] {
            crate::contract::compile_json_schema(schema, "generated native tool", None)?;
        }
        crate::tool::validate_value(&input_schema, &json!({"file":file}), "generated read", None)?;
        crate::tool::validate_value(&output_schema, &canonical, "generated result", None)?;
        let projection = project_json(&result, None)?.0;
        crate::tool::validate_value(&projection_schema, &projection, "generated projection", None)?;
        assert_eq!(projection, json!({"kind":"json","value":canonical}));
        assert_eq!(
            serde_json::from_value::<FileResult>(canonical.clone())
                .map_err(|error| Error::Invalid(error.to_string()))?,
            result
        );
        assert!(
            crate::tool::validate_value(&projection_schema, &canonical, "bare result", None).is_err()
        );
        assert!(
            crate::tool::validate_value(&output_schema, &projection, "wrong canonical envelope", None)
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
                crate::tool::validate_value(&output_schema, &changed, "wrong nested type", None).is_err()
            );
        }
        let mut changed = canonical.clone();
        changed
            .get_mut("file")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::Invalid("fixture ref missing".into()))?
            .remove("version");
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "missing nested field", None).is_err()
        );
        let mut changed = canonical.clone();
        changed
            .pointer_mut("/file/descriptor")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| Error::Invalid("fixture descriptor missing".into()))?
            .insert("extra".into(), json!(true));
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "unknown nested field", None).is_err()
        );
        for digest in [vec![0; 31], vec![0; 33], vec![256; 32]] {
            let mut changed = canonical.clone();
            *changed
                .pointer_mut("/file/descriptor/sha256")
                .ok_or_else(|| Error::Invalid("fixture digest missing".into()))? = json!(digest);
            assert!(
                crate::tool::validate_value(&output_schema, &changed, "invalid digest", None).is_err()
            );
        }
        let mut changed = canonical;
        *changed
            .pointer_mut("/file/descriptor/byte_length")
            .ok_or_else(|| Error::Invalid("fixture length missing".into()))? =
            json!(crate::conversation::MAX_EXACT_JS_INTEGER + 1);
        assert!(
            crate::tool::validate_value(&output_schema, &changed, "imprecise descriptor", None).is_err()
        );
        Ok(())
    }

    #[test]
    fn file_schemas_reject_empty_edits_and_patches() -> Result<()> {
        let file = file()?;
        let edit = files::edit_file()?;
        let patch = files::patch_file(8192, 8)?;
        assert!(
            crate::tool::validate_value(
                &edit.definition.input_schema,
                &json!({"file":file,"old_text":"","new_text":"x"}),
                "empty edit"
            , None)
            .is_err()
        );
        assert!(
            crate::tool::validate_value(
                &patch.definition.input_schema,
                &json!({"file":file,"diff":""}),
                "empty patch"
            , None)
            .is_err()
        );
        let mut invalid = json!({"file":file});
        *invalid
            .pointer_mut("/file/path")
            .ok_or_else(|| Error::Invalid("fixture path missing".into()))? = json!("../escape");
        assert!(serde_json::from_value::<ReadFileInput>(invalid).is_err());
        Ok(())
    }
}
