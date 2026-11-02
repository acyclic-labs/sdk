//! One lossless spill envelope and one encoded-byte window for all tools.
use super::*;

/// A shared rendering quantum, not a capacity limit. Omitted bytes remain in
/// an immutable volume file and are read through the ordinary file reader.
pub(crate) const VIEW_BYTES: u64 = 30_000;

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SpillRef<'a> {
    #[schemars(length(min = 1))]
    path: &'a str,
    #[schemars(length(min = 1))]
    version: &'a str,
}

#[derive(Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
struct SpillView<'a> {
    output_spill: SpillRef<'a>,
    text: &'a str,
    omitted_bytes: u64,
}

pub(crate) fn schema() -> Result<Value> {
    let mut generator = schemars::generate::SchemaSettings::draft2020_12()
        .for_serialize()
        .into_generator();
    let json = generator.subschema_for::<super::schema::JsonProjection<SpillView<'_>>>();
    let file = generator.subschema_for::<crate::conversation::FileRef>();
    Ok(serde_json::json!({
        "oneOf":[json,{
            "type":"object", "required":["kind","parts"], "additionalProperties":false,
            "properties":{
                "kind":{"const":"parts"},
                "parts":{"type":"array","minItems":1,
                    "prefixItems":[{"type":"object","required":["kind","text"],"additionalProperties":false,
                        "properties":{"kind":{"const":"text"},"text":{"type":"string","pattern":"^\\{\"output_spill\":"}}}],
                    "items":{"type":"object","required":["kind","file","policy"],"additionalProperties":false,
                        "properties":{"kind":{"const":"file"},"file":file,"policy":{}}}
                }
            }
        }],
        "$defs":generator.take_definitions(true)
    }))
}

pub(crate) fn is_spill(value: &Value) -> bool {
    value
        .get("value")
        .and_then(|value| value.get("output_spill"))
        .is_some()
        || value
            .get("parts")
            .and_then(Value::as_array)
            .and_then(|parts| parts.first())
            .and_then(|part| part.get("text"))
            .and_then(Value::as_str)
            .is_some_and(|text| text.starts_with("{\"output_spill\":"))
}

/// Multipart previews contain the same spill record as JSON results, encoded
/// once more as a text part. Borrow its scalar tokens: validation needs neither
/// a second value graph nor copies of the preview or opaque file version.
#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct SpillPreview<'a> {
    #[serde(borrow, deserialize_with = "crate::executor::json::deserialize_object")]
    #[heap_layout(as = SpillIdentity<'a>)]
    output_spill: SpillIdentity<'a>,
    #[serde(borrow)]
    text: &'a serde_json::value::RawValue,
    #[serde(rename = "omitted_bytes")]
    _omitted_bytes: u64,
}

#[derive(Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
struct SpillIdentity<'a> {
    #[serde(borrow)]
    path: &'a serde_json::value::RawValue,
    #[serde(borrow)]
    version: &'a serde_json::value::RawValue,
}

fn valid_preview(preview: &SpillPreview<'_>) -> bool {
    let identity = &preview.output_spill;
    let string = |value: &serde_json::value::RawValue| value.get().starts_with('"');
    // The original codec has already validated JSON syntax. A nonempty JSON
    // string always has more than its two framing quotes, including escapes.
    string(identity.path)
        && identity.path.get().len() > 2
        && string(identity.version)
        && identity.version.get().len() > 2
        && string(preview.text)
}

pub(super) fn check_preview(
    value: &Value,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<()> {
    let Some(text) = value
        .get("parts")
        .and_then(Value::as_array)
        .and_then(|parts| parts.first())
        .and_then(|part| part.get("text"))
        .and_then(Value::as_str)
    else {
        return Ok(());
    };
    let seed = crate::executor::json::JsonObject(std::marker::PhantomData::<SpillPreview<'_>>);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let decoded = io.decode_seed(
            seed,
            text.as_bytes(),
            crate::executor::json::JsonValueSchema::None,
            super::TOOL_SERDE_DIAGNOSTIC_LABEL_BYTES,
            crate::wire::ErrorCode::Invalid,
        )?;
        if valid_preview(&decoded.0) {
            return Ok(());
        }
        return Err(crate::executor::with_history_io(Some(io), |work| {
            crate::store::guarded_boundary(
                work,
                crate::wire::ErrorCode::Invalid,
                "invalid retained output preview",
            )
        }));
    }
    let _ = io;
    let decoded = crate::executor::json::json_from_slice_bounded(
        seed,
        text.as_bytes(),
        crate::executor::json::JsonBounds {
            maximum_bytes: Some(text.len() as u64),
            maximum_items: None,
            maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
        },
        None,
        Some(crate::executor::json::JsonValueSchema::None),
        &mut |_| Ok(()),
    )?;
    if valid_preview(&decoded) {
        Ok(())
    } else {
        Err(Error::tool_rejected("invalid retained output preview"))
    }
}

/// Serialize existing file parts by borrowing them. Only text is replaced;
/// native media continues through the ordinary provider attachment resolver.
struct MediaParts<'a> {
    preview: &'a str,
    source: &'a [Value],
}

impl Serialize for MediaParts<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeSeq as _;
        let mut sequence = serializer.serialize_seq(None)?;
        #[derive(Serialize)]
        struct Text<'a> {
            kind: &'static str,
            text: &'a str,
        }
        sequence.serialize_element(&Text {
            kind: "text",
            text: self.preview,
        })?;
        for part in self
            .source
            .iter()
            .filter(|part| part.get("kind").and_then(Value::as_str) == Some("file"))
        {
            sequence.serialize_element(part)?;
        }
        sequence.end()
    }
}

#[derive(Serialize)]
struct MediaView<'a> {
    kind: &'static str,
    parts: MediaParts<'a>,
}

fn with_media<R>(
    view: &SpillView<'_>,
    source: &[Value],
    io: Option<&crate::executor::PreparationIo>,
    emit: impl FnOnce(&MediaView<'_>) -> Result<R>,
) -> Result<R> {
    #[cfg(feature = "filesystem")]
    let _hold;
    let bytes;
    #[cfg(feature = "filesystem")]
    {
        bytes = if let Some(io) = io {
            io.encode(view, Some(io.remaining()?.bytes_encoded), false)?
        } else {
            serde_json::to_vec(view)
                .map_err(|error| Error::Invalid(error.to_string()))?
                .into()
        };
        _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    }
    #[cfg(not(feature = "filesystem"))]
    {
        let _ = io;
        bytes = serde_json::to_vec(view).map_err(|error| Error::Invalid(error.to_string()))?;
    }
    let preview = std::str::from_utf8(&bytes)
        .map_err(|_| Error::tool_rejected("output preview encoding is not UTF-8"))?;
    emit(&MediaView {
        kind: "parts",
        parts: MediaParts { preview, source },
    })
}

pub(crate) async fn spill(
    projection: &Value,
    context: &crate::runtime::TaskContext,
    operation: OperationId,
    maximum: impl Into<Option<u64>>,
    io: Option<&crate::executor::PreparationIo>,
) -> Result<(Value, crate::NativeCustody)> {
    #[cfg(feature = "filesystem")]
    let _bytes_hold;
    let bytes;
    #[cfg(feature = "filesystem")]
    let encoded = if let Some(io) = io {
        bytes = io.encode(projection, Some(io.remaining()?.bytes_encoded), false)?;
        _bytes_hold = io.guard_bytes(&bytes)?;
        bytes.as_ref()
    } else {
        bytes = acyclic_stream::OwnedBytes::from(
            serde_json::to_vec(projection).map_err(|error| Error::Invalid(error.to_string()))?,
        );
        _bytes_hold = None;
        bytes.as_ref()
    };
    #[cfg(not(feature = "filesystem"))]
    let encoded = {
        bytes =
            serde_json::to_vec(projection).map_err(|error| Error::Invalid(error.to_string()))?;
        bytes.as_slice()
    };
    #[cfg(feature = "filesystem")]
    let _path_hold;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let _file_hold;
    let (file, path, retained) = context.spill_tool_output(operation, encoded, io).await?;
    #[cfg(feature = "filesystem")]
    {
        _path_hold = io
            .map(|io| io.guard_owned(path.capacity() as u64))
            .transpose()?;
    }
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    {
        _file_hold = io
            .filter(|io| io.original_native_work().is_some())
            .map(|io| io.take_native_custody(retained))
            .transpose()?;
    }
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let _ = retained;
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    let work = io.and_then(crate::executor::PreparationIo::original_native_work);
    #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
    let work = None;
    let _version_hold;
    let (version, version_hold) = super::files::encode_version(&file, work)?;
    _version_hold = version_hold;
    render(
        projection,
        encoded,
        SpillRef {
            path: &path,
            version: &version,
        },
        maximum.into().unwrap_or(VIEW_BYTES),
        io,
        work,
    )
}

fn render(
    projection: &Value,
    encoded: &[u8],
    source: SpillRef<'_>,
    maximum: u64,
    io: Option<&crate::executor::PreparationIo>,
    work: crate::NativeWork<'_>,
) -> Result<(Value, crate::NativeCustody)> {
    let text = std::str::from_utf8(encoded)
        .map_err(|_| Error::tool_rejected("tool projection is not UTF-8 JSON"))?;
    let empty = SpillView {
        output_spill: SpillRef {
            path: source.path,
            version: source.version,
        },
        text: "",
        omitted_bytes: u64::MAX,
    };
    let media = projection
        .get("parts")
        .and_then(Value::as_array)
        .filter(|parts| {
            parts
                .iter()
                .any(|part| part.get("kind").and_then(Value::as_str) == Some("file"))
        });
    let overhead = if let Some(parts) = media {
        with_media(&empty, parts, io, |view| {
            crate::executor::with_history_io(io, |history| {
                crate::executor::json::json_byte_length(view, None, history)
            })
        })?
    } else {
        crate::executor::with_history_io(io, |history| {
            crate::executor::json::json_byte_length(&empty, None, history)
        })? + super::schema::json_envelope_bytes()? as u64
    };
    // Reference metadata is indivisible. The display quantum bounds text;
    // caller credit, rather than that quantum, admits native references.
    let maximum = maximum.min(VIEW_BYTES).max(overhead).min(maximum);
    let budget = maximum.checked_sub(overhead).ok_or_else(|| {
        Error::tool_rejected("render credit cannot hold an output file reference")
    })?;
    // A multipart preview is JSON inside a text part. Every escaped byte can
    // at most double at the second string boundary.
    let budget = if media.is_some() { budget / 2 } else { budget };
    let shown = crate::tools::bounds::prefix(text, usize::try_from(budget).unwrap_or(usize::MAX));
    let view = SpillView {
        output_spill: SpillRef {
            path: source.path,
            version: source.version,
        },
        text: shown,
        omitted_bytes: (text.len() - shown.len()) as u64,
    };
    if let Some(parts) = media {
        with_media(&view, parts, io, |view| super::value(view, None, work))
    } else {
        super::schema::project_json(&view, work)
    }
}

#[cfg(test)]
mod checks {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(32))]
        #[test]
        fn spill_bounds_preserve_file_parts_and_exact_omission(text in ".{0,16000}", maximum in 4096u64..32000) {
            let file = crate::tool::tests::file(b"retained").unwrap();
            let projection = serde_json::json!({"kind":"parts", "parts":[
                {"kind":"text", "text":text},
                {"kind":"file", "file":file, "policy":"reference"}
            ]});
            let bytes = serde_json::to_vec(&projection).unwrap();
            let (bounded, _hold) = render(&projection, &bytes, SpillRef {path:"output λ\".json",version:r#"{"source":"λ\\value"}"#}, maximum, None, None).unwrap();
            prop_assert!(serde_json::to_vec(&bounded).unwrap().len() as u64 <= maximum);
            let mut definition = crate::tool::files::read().unwrap().definition;
            definition.projection_schema = crate::tool::json_projection_schema(serde_json::json!({"type":"null"}));
            let content = definition.check_projection(&bounded, crate::conversation::Limits::default(), None).unwrap().unwrap();
            let expected = crate::model::ModelDataPart::File {file, policy:crate::model::FileProjectionPolicy::Reference};
            prop_assert!(matches!(&content, crate::model::ToolResultContent::Parts {parts} if parts.len() == 2 && parts.get(1) == Some(&expected)), "native file part changed or disappeared");
            let preview: Value = serde_json::from_str(bounded["parts"][0]["text"].as_str().unwrap()).unwrap();
            let shown = preview["text"].as_str().unwrap();
            prop_assert!(bytes.starts_with(shown.as_bytes()));
            prop_assert_eq!(shown.len() as u64 + preview["omitted_bytes"].as_u64().unwrap(), bytes.len() as u64);
            let mut broken = bounded;
            broken["parts"][0]["text"] = Value::String("missing retained-output identity".into());
            prop_assert!(definition.check_projection(&broken, crate::conversation::Limits::default(), None).unwrap().is_err());
            for invalid in [
                r#"{"output_spill":"#,
                r#"{"output_spill":null}"#,
                r#"{"output_spill":["p","v"],"text":"","omitted_bytes":0}"#,
                r#"{"output_spill":{"path":"p","version":"v"},"text":[],"omitted_bytes":0}"#,
                r#"{"output_spill":{"path":"","version":"v"},"text":"","omitted_bytes":0}"#,
                r#"{"output_spill":{"path":"p","version":null},"text":"","omitted_bytes":0}"#,
                r#"{"output_spill":{"path":"p","version":"v"},"text":"","omitted_bytes":-1}"#,
                r#"{"output_spill":{"path":"p","version":"v"},"text":"","text":"other","omitted_bytes":0}"#,
                r#"{"output_spill":{"path":"p","version":"v","extra":0},"text":"","omitted_bytes":0}"#,
            ] {
                broken["parts"][0]["text"] = Value::String(invalid.into());
                // The former prefix-only check accepts every one of these.
                prop_assert!(is_spill(&broken));
                prop_assert!(definition.validate_projection(&broken, crate::conversation::Limits::default(), None).is_err());
            }
        }
    }

    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    #[test]
    fn spill_preview_keeps_original_credit_and_cancellation() -> Result<()> {
        let value = serde_json::json!({"kind":"parts","parts":[{
            "kind":"text",
            "text":r#"{"output_spill":{"path":"p","version":"v"},"text":"\u03bb\\text","omitted_bytes":2}"#
        }]});
        for native in [false, true] {
            let cancellation = acyclic_fs::CancellationToken::new();
            let budget = acyclic_fs::performance::work_budget(64 * 1024 * 1024, 4 * 1024 * 1024);
            let work = acyclic_fs::NativeMaterializationWork::new(budget, &cancellation)
                .map_err(|(cause, work)| Error::NativeConstruction { cause, work })?;
            let baseline = work.live_allocation_bytes();
            let io = if native {
                crate::executor::PreparationIo::from_native_work(&work)?
            } else {
                crate::executor::PreparationIo::from_budget(budget, &cancellation)?
            };
            check_preview(&value, Some(&io))?;
            assert_eq!(io.owned_bytes()?, 0);
            for invalid in [
                r#"[["p","v"],"",0]"#,
                r#"{"output_spill":["p","v"],"text":"","omitted_bytes":0}"#,
            ] {
                let mut broken = value.clone();
                broken["parts"][0]["text"] = Value::String(invalid.into());
                assert!(check_preview(&broken, None).is_err());
                let error = check_preview(&broken, Some(&io)).unwrap_err();
                assert_eq!(error.code(), crate::wire::ErrorCode::Invalid);
                assert!(error.is_tool_failure(), "{error:?}");
                drop(error);
                assert_eq!(io.owned_bytes()?, 0);
            }
            cancellation.cancel();
            assert!(check_preview(&value, Some(&io)).is_err());
            drop(io);
            assert_eq!(work.live_allocation_bytes(), baseline);
        }
        Ok(())
    }
}
