//! Generated native tool contracts from their actual Rust argument/result types.

use super::edit::{charge, invalid, reserve_string};
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

pub(crate) fn project_json<T: Serialize>(
    value: &T,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    super::value(&JsonProjection::Json { value }, u64::MAX, work)
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

impl ProjectionMode {
    /// Generates the complete model envelope for one canonical result type.
    pub fn schema<T: JsonSchema>(self) -> Result<Value> {
        match self {
            Self::Full => output::<JsonProjection<T>>(),
            Self::Reference => output::<ReferenceProjection<'_>>(),
        }
    }

    /// Generates the model envelope of a result rendered as plain text.
    pub fn text_schema(self) -> Result<Value> {
        match self {
            Self::Full => output::<TextProjection<'_>>(),
            Self::Reference => output::<ReferenceProjection<'_>>(),
        }
    }
}

#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TextPart<'a> {
    Text { text: &'a str },
}

/// Text the model reads verbatim; providers never JSON-quote a parts result.
#[derive(Serialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum TextProjection<'a> {
    Parts { parts: [TextPart<'a>; 1] },
}

pub(crate) fn project_text(
    text: &str,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    super::value(
        &TextProjection::Parts {
            parts: [TextPart::Text { text }],
        },
        u64::MAX,
        work,
    )
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

pub(crate) fn project_reference(
    file: &crate::conversation::FileRef,
    text: &str,
    _work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    let projection = ReferenceProjection::Parts {
        parts: [
            ReferencePart::Text { text },
            ReferencePart::File {
                file,
                policy: ReferencePolicy::Reference,
            },
        ],
    };
    super::value(&projection, u64::MAX, _work)
}

pub(crate) fn summary_reference(
    file: &crate::conversation::FileRef,
    maximum: usize,
    arguments: std::fmt::Arguments<'_>,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    let (text, _allocation) = bounded_format(maximum, arguments, work)?;
    project_reference(file, &text, work)
}

/// Formats a short model-facing summary into a preclaimed, charged buffer,
/// returned with the custody that accounts for it.
pub(crate) fn bounded_format(
    maximum: usize,
    arguments: std::fmt::Arguments<'_>,
    work: crate::NativeWork<'_>,
) -> Result<(String, crate::NativeCustody)> {
    use std::fmt::Write as _;
    let mut allocation = super::edit::allocation(work, std::mem::size_of::<String>())?;
    let mut text = String::new();
    reserve_string(&mut allocation, &mut text, maximum)?;
    struct FixedText<'a> {
        text: &'a mut String,
        work: &'a crate::NativeCustody,
        denial: Option<Error>,
    }
    impl std::fmt::Write for FixedText<'_> {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            if value.len() > self.text.capacity() - self.text.len() {
                self.denial = Some(invalid(
                    self.work,
                    "file summary exceeds its preclaimed capacity",
                ));
                return Err(std::fmt::Error);
            }
            if let Err(error) = charge(self.work, (value.len()) as u64, 0) {
                self.denial = Some(error);
                return Err(std::fmt::Error);
            }
            self.text.push_str(value);
            Ok(())
        }
    }
    let mut writer = FixedText {
        text: &mut text,
        work: &allocation,
        denial: None,
    };
    if writer.write_fmt(arguments).is_err() {
        return Err(writer
            .denial
            .take()
            .unwrap_or_else(|| invalid(&allocation, "file summary formatting failed")));
    }
    drop(writer);
    Ok((text, allocation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::compile_json_schema;
    use crate::tool::{
        ToolInvocation, ToolProjection, ToolResult,
        files::{self, FileProjection, FileResult, ReadFileInput, ReadFileResult},
    };
    use serde_json::json;

    #[test]
    fn file_results_project_as_text_or_reference_without_changing_canonical_values() -> Result<()> {
        use crate::model::{FileProjectionPolicy, ModelDataPart, ToolResultContent};
        let file = crate::tool::tests::file(b"exact")?;
        let invocation = ToolInvocation {
            operation_id: crate::OperationId::new(),
            call_id: "file-projection".into(),
            name: "read".into(),
            arguments: json!({"path":file.path()}),
        };
        let published = ToolResult {
            value: json!(FileResult { file: file.clone() }),
        };
        let read = ToolResult {
            value: json!(ReadFileResult {
                file: file.clone(),
                offset: 1,
                text: "exact".into(),
            }),
        };
        let expected_full = [
            format!("Updated {} (5 bytes).", file.path()),
            "     1\texact".to_owned(),
        ];
        for mode in [ProjectionMode::Full, ProjectionMode::Reference] {
            for ((projector, result), full) in [
                (
                    &FileProjection::Publication(mode) as &dyn ToolProjection,
                    &published,
                ),
                (&FileProjection::Read(mode) as &dyn ToolProjection, &read),
            ]
            .into_iter()
            .zip(&expected_full)
            {
                let projection = projector.project(&invocation, result, None)?.0;
                let validator =
                    compile_json_schema(&mode.text_schema()?, "file projection", &mut None)?;
                assert!(validator.is_valid(&projection));
                let wire: ToolResultContent = serde_json::from_value(projection.clone())
                    .map_err(|error| Error::Invalid(error.to_string()))?;
                let ToolResultContent::Parts { parts } = wire else {
                    panic!("file projections are parts")
                };
                if mode == ProjectionMode::Full {
                    // Text parts reach the model verbatim: no JSON quoting,
                    // no byte arrays, no file reference.
                    assert_eq!(parts, vec![ModelDataPart::Text { text: full.clone() }]);
                    let mut changed = projection;
                    changed["parts"][0]["kind"] = json!("file");
                    assert!(!validator.is_valid(&changed));
                    continue;
                }
                assert!(
                    matches!(parts.get(1), Some(ModelDataPart::File { file:actual, policy:FileProjectionPolicy::Reference }) if actual==&file)
                );
                assert!(
                    matches!(parts.first(),Some(ModelDataPart::Text { text }) if !text.is_empty())
                );
                let mut changed = projection;
                changed["parts"][1]["policy"] = json!("bounded_full");
                assert!(!validator.is_valid(&changed));
            }
        }
        for (offset, text) in [(1, "longer than the file"), (0, "exact")] {
            assert!(
                FileProjection::Read(ProjectionMode::Full)
                    .project(
                        &invocation,
                        &ToolResult {
                            value: json!(ReadFileResult {
                                file: file.clone(),
                                offset,
                                text: text.into(),
                            })
                        },
                        None
                    )
                    .is_err()
            );
        }
        Ok(())
    }

    #[test]
    fn real_nested_file_schemas_compile_and_keep_refs_at_the_complete_root() -> Result<()> {
        let file = crate::tool::tests::file(b"exact")?;
        let result = FileResult { file: file.clone() };
        let canonical = json!(result);
        let input = compile_json_schema(&input::<ReadFileInput>()?, "read", &mut None)?;
        let output = compile_json_schema(&output::<FileResult>()?, "result", &mut None)?;
        let projection = compile_json_schema(
            &ProjectionMode::Full.schema::<FileResult>()?,
            "projection",
            &mut None,
        )?;
        assert!(input.is_valid(&json!({"path":"src/lib.rs","offset":2,"limit":3})));
        for invalid in [
            json!({"file":file}),
            json!({"path":""}),
            json!({"path":"a","offset":0}),
            json!({"path":"a","limit":0}),
        ] {
            assert!(!input.is_valid(&invalid));
        }
        assert!(output.is_valid(&canonical));
        let envelope = project_json(&result, None)?.0;
        assert!(projection.is_valid(&envelope));
        assert!(!projection.is_valid(&canonical));
        assert!(!output.is_valid(&envelope));
        for (pointer, value) in [
            ("/file/version", json!(1)),
            ("/file/display_name", json!(1)),
            ("/file/volume/provider/version", json!(1)),
            ("/file/descriptor/media_type", json!(1)),
            ("/file/descriptor/sha256", json!(vec![0; 31])),
            ("/file/descriptor/sha256", json!(vec![0; 33])),
            ("/file/descriptor/sha256", json!(vec![256; 32])),
            (
                "/file/descriptor/byte_length",
                json!(crate::conversation::MAX_EXACT_JS_INTEGER + 1),
            ),
        ] {
            let mut changed = canonical.clone();
            *changed.pointer_mut(pointer).expect("fixture field") = value;
            assert!(!output.is_valid(&changed));
        }
        let mut changed = canonical.clone();
        changed["file"]
            .as_object_mut()
            .expect("file")
            .remove("version");
        assert!(!output.is_valid(&changed));
        let mut changed = canonical;
        changed["file"]["descriptor"]["extra"] = json!(true);
        assert!(!output.is_valid(&changed));
        Ok(())
    }

    #[test]
    fn file_schemas_reject_empty_edits_and_patches() -> Result<()> {
        let file = crate::tool::tests::file(b"exact")?;
        let edit = files::edit()?;
        let patch = files::patch_file(8192, 8)?;
        let edit_schema = compile_json_schema(&edit.definition.input_schema, "edit", &mut None)?;
        assert!(edit_schema.is_valid(
            &json!({"path":"a.txt","old_string":"x","new_string":"","replace_all":true})
        ));
        for (schema, value) in [
            (
                &edit.definition.input_schema,
                json!({"path":"a.txt","old_string":"","new_string":"x"}),
            ),
            (
                &edit.definition.input_schema,
                json!({"file":file,"old_text":"exact","new_text":"x"}),
            ),
            (
                &patch.definition.input_schema,
                json!({"file":file,"diff":""}),
            ),
        ] {
            assert!(!compile_json_schema(schema, "edit", &mut None)?.is_valid(&value));
        }
        assert!(serde_json::from_value::<ReadFileInput>(json!({"file":file})).is_err());
        Ok(())
    }
}
