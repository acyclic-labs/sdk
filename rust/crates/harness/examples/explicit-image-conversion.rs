//! Consumer-selected conversion through existing admitted tools, not a model fallback.
#![allow(
    missing_docs,
    dead_code,
    reason = "example is also composed by the native boundary fixture"
)]

use acyclic_harness::{
    Error, Result,
    conversation::FileRef,
    model::{
        FileProjectionPolicy, ImageDetail, ModelDataPart, NativeMediaIntent, NativeMediaPolicy,
        ToolResultContent,
    },
    runtime::ToolContext,
    tool::{
        Tool, ToolDefinition, ToolExecutor, ToolInvocation, ToolProjection, ToolResult, files,
        schema,
    },
};
use acyclic_stream::BoxProviderFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::sync::Arc;

const MAXIMUM_BYTES: u64 = 4096;
const MIME: &str = "application/x-harness-native-fixture";

#[derive(Clone, Debug, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConvertImage {
    pub source: FileRef,
    pub destination: String,
    /// Explicit per-pixel XOR mask, retained with the canonical derived reference.
    pub mask: u8,
}

#[derive(Clone, Debug, PartialEq, Deserialize, Serialize, schemars::JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ConvertedImage {
    pub source: FileRef,
    pub derived: FileRef,
    pub mask: u8,
}

pub fn conversion_tool() -> Result<Tool> {
    let mut input_schema = schema::input::<ConvertImage>()?;
    input_schema
        .as_object_mut()
        .ok_or_else(|| Error::Invalid("conversion schema is not an object".into()))?
        .insert("x-fixture-maximum-bytes".into(), json!(MAXIMUM_BYTES));
    let adapter = Arc::new(ImageConversion {
        read: files::read_file()?.executor,
        write: files::write_file()?.executor,
    });
    Ok(Tool {
        definition: ToolDefinition {
            name: "example.convert_image".into(),
            revision: "img1-xor-1".into(),
            description: "Explicitly derive an IMG1 fixture image with a selected pixel mask"
                .into(),
            input_schema,
            output_schema: schema::output::<ConvertedImage>()?,
            projection_schema: json!({"type":"object","required":["kind","parts"],"properties":{"kind":{"const":"parts"},"parts":{"type":"array","minItems":1,"maxItems":1}},"additionalProperties":false}),
        },
        executor: adapter.clone(),
        projection: adapter,
    })
}

struct ImageConversion {
    read: Arc<dyn ToolExecutor>,
    write: Arc<dyn ToolExecutor>,
}

impl ImageConversion {
    fn input(invocation: &ToolInvocation) -> Result<ConvertImage> {
        ConvertImage::deserialize(&invocation.arguments)
            .map_err(|error| Error::Invalid(error.to_string()))
    }

    async fn run(&self, context: ToolContext, invocation: ToolInvocation) -> Result<ToolResult> {
        self.authorize_with_context(&context, &invocation)?;
        let input = Self::input(&invocation)?;
        let mut bytes = context.read_file(&input.source, None).await?;
        let count = bytes
            .get(4..8)
            .and_then(|count| <[u8; 4]>::try_from(count).ok())
            .map(u32::from_le_bytes)
            .ok_or_else(|| Error::Invalid("truncated IMG1 fixture".into()))?;
        if bytes.get(..4) != Some(b"IMG1".as_slice())
            || count == 0
            || bytes.len() as u64 != 8 + u64::from(count)
        {
            return Err(Error::Invalid("invalid IMG1 fixture".into()));
        }
        for pixel in bytes.iter_mut().skip(8) {
            *pixel ^= input.mask;
        }
        let derived = context
            .stage_file_once(invocation.operation_id, &input.destination, &bytes, MIME, "converted.img1", None).await.map(|(file, _)| file)?;
        Ok(ToolResult {
            value: serde_json::to_value(ConvertedImage {
                source: input.source,
                derived,
                mask: input.mask,
            })
            .map_err(|error| Error::Invalid(error.to_string()))?,
        })
    }
}

impl ToolExecutor for ImageConversion {
    fn authorize_with_context(
        &self,
        context: &ToolContext,
        invocation: &ToolInvocation,
    ) -> Result<()> {
        let input = Self::input(invocation)?;
        if input.source.descriptor().media_type() != MIME
            || input.source.descriptor().byte_length() > MAXIMUM_BYTES
        {
            return Err(Error::Invalid(
                "conversion requires bounded fixture bytes".into(),
            ));
        }
        let read = ToolInvocation {
            arguments: json!({"file":input.source}),
            ..invocation.clone()
        };
        let write = ToolInvocation {
            arguments: json!({"path":input.destination}),
            ..invocation.clone()
        };
        self.read.authorize_with_context(context, &read)?;
        self.write.authorize_with_context(context, &write)
    }

    fn execute<'a>(&'a self, _: ToolInvocation) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "conversion needs its original task context".into(),
            ))
        })
    }

    fn execute_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(self.run(context, invocation))
    }

    fn reconcile<'a>(
        &'a self,
        _: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async {
            Err(Error::Unsupported(
                "conversion needs its original task context".into(),
            ))
        })
    }

    fn reconcile_with_context<'a>(
        &'a self,
        context: ToolContext,
        invocation: ToolInvocation,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move { self.run(context, invocation).await.map(Some) })
    }
}

impl ToolProjection for ImageConversion {
    fn project(&self, invocation: &ToolInvocation, result: &ToolResult,
        work: acyclic_harness::NativeWork<'_>) -> Result<(Value, acyclic_harness::NativeCustody)> {
        self.supports_native_projection(work)?;
        let input = Self::input(invocation)?;
        let output = ConvertedImage::deserialize(&result.value)
            .map_err(|error| Error::Invalid(error.to_string()))?;
        if output.source != input.source
            || output.mask != input.mask
            || output.derived.path() != input.destination
        {
            return Err(Error::Conflict(
                "conversion result differs from selected input".into(),
            ));
        }
        serde_json::to_value(ToolResultContent::Parts {
            parts: vec![ModelDataPart::File {
                file: output.derived,
                policy: FileProjectionPolicy::Native(Box::new(NativeMediaPolicy {
                    intent: NativeMediaIntent::Image {
                        detail: ImageDetail::High,
                    },
                    maximum_bytes: MAXIMUM_BYTES,
                    maximum_work: MAXIMUM_BYTES,
                    configuration: None,
                })),
            }],
        })
        .map(|value| (value, None))
        .map_err(|error| Error::Invalid(error.to_string()))
    }
}

fn main() -> Result<()> {
    println!(
        "{}",
        serde_json::to_string_pretty(&conversion_tool()?.definition)
            .map_err(|error| Error::Invalid(error.to_string()))?
    );
    Ok(())
}
