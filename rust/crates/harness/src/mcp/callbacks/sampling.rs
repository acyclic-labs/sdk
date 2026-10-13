use super::*;

#[expect(
    deprecated,
    reason = "sampling is required by the pinned MCP 2025-11-25 protocol; rmcp retains its typed contract"
)]
pub(super) fn validate_sampling(params: &Value) -> Result<()> {
    let _: rmcp::model::CreateMessageRequestParams =
        rmcp::model::CreateMessageRequestParams::deserialize(params)
            .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

#[expect(
    deprecated,
    reason = "sampling is required by the pinned MCP 2025-11-25 protocol; rmcp retains its typed contract"
)]
pub(super) fn validate_sampling_result(value: Value) -> Result<()> {
    let result: rmcp::model::CreateMessageResult =
        serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
    result.validate().map_err(Error::Invalid)
}

pub(super) async fn sampling_messages(
    params: &Value,
    max_tokens: u32,
    context: &ToolContext,
    operation: OperationId,
    continuation: &ModelContent,
    image: Option<&NativeMediaPolicy>,
    audio: Option<&NativeMediaPolicy>,
) -> Result<(ModelContent, Vec<ModelMessage>)> {
    if max_tokens == 0 {
        return Err(Error::Invalid("MCP sampling token bound is zero".into()));
    }
    let mut messages = Vec::new();
    // Server-supplied system prompt remains external user content.
    if let Some(prompt) = params.get("systemPrompt").and_then(Value::as_str) {
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(prompt.into()),
        });
    }
    for (message_index, message) in params
        .get("messages")
        .and_then(Value::as_array)
        .ok_or_else(|| Error::Invalid("MCP sampling messages absent".into()))?
        .iter()
        .enumerate()
    {
        let role = match message.get("role").and_then(Value::as_str) {
            Some("user") => ModelRole::User,
            Some("assistant") => ModelRole::Assistant,
            _ => return Err(Error::Invalid("invalid MCP sampling role".into())),
        };
        let content = message
            .get("content")
            .ok_or_else(|| Error::Invalid("MCP sampling content absent".into()))?;
        let blocks = content
            .as_array()
            .map_or_else(|| std::slice::from_ref(content), Vec::as_slice);
        let mut parts = Vec::new();
        for (block_index, block) in blocks.iter().enumerate() {
            let part = match block.get("type").and_then(Value::as_str) {
                Some("text") => ModelContentPart::Text {
                    text: block
                        .get("text")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::Invalid("MCP sampling text absent".into()))?
                        .into(),
                },
                Some(kind @ ("image" | "audio")) => {
                    let media = if kind == "image" { image } else { audio }.ok_or_else(|| {
                        Error::Unsupported(
                            "MCP sampling media was not selected by this host".into(),
                        )
                    })?;
                    let maximum_bytes =
                        media.maximum_bytes.min(context.scope().limits().file_bytes);
                    let encoded_bound = maximum_bytes
                        .checked_add(2)
                        .and_then(|length| (length / 3).checked_mul(4))
                        .ok_or_else(|| {
                            Error::Invalid("MCP media byte allowance overflows".into())
                        })?;
                    let encoded = block
                        .get("data")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::Invalid("MCP media data absent".into()))?;
                    if maximum_bytes == 0 || encoded.len() as u64 > encoded_bound {
                        return Err(Error::Invalid(
                            "MCP media exceeds selected byte allowance".into(),
                        ));
                    }
                    let bytes = base64::engine::general_purpose::STANDARD
                        .decode(encoded)
                        .map_err(|_| Error::Invalid("MCP media base64 invalid".into()))?;
                    if bytes.len() as u64 > maximum_bytes {
                        return Err(Error::Invalid(
                            "MCP media exceeds selected byte allowance".into(),
                        ));
                    }
                    let mime = block
                        .get("mimeType")
                        .and_then(Value::as_str)
                        .ok_or_else(|| Error::Invalid("MCP media MIME type absent".into()))?;
                    let digest = crate::executor::json::canonical_json_digest(
                        &(
                            "harness/mcp-sampling-media/v1",
                            operation,
                            message_index,
                            block_index,
                        ),
                        u64::MAX,
                        &mut None,
                    )?;
                    let mut identity = [0; 16];
                    for (byte, source) in identity.iter_mut().zip(digest) {
                        *byte = source;
                    }
                    let path = format!("mcp/{operation}/sampling/{message_index}-{block_index}");
                    let file = context
                        .stage_file_once(OperationId::from_bytes(identity), &path, &bytes, mime, "MCP sampling media", None).await.map(|(file, _)| file)?;
                    media.validate(&file, context.scope().limits(), &mut None)?;
                    ModelContentPart::File {
                        file,
                        policy: FileProjectionPolicy::Native(Box::new(media.clone())),
                    }
                }
                _ => {
                    return Err(Error::Unsupported(
                        "MCP sampling content needs a host-selected projection".into(),
                    ));
                }
            };
            parts.push(part);
        }
        messages.push(ModelMessage {
            role,
            content: ModelContent::Parts(parts),
        });
    }
    if messages
        .last()
        .is_some_and(|message| message.role == ModelRole::User)
    {
        let last = messages
            .pop()
            .ok_or_else(|| Error::Invalid("MCP sampling input disappeared".into()))?;
        Ok((last.content, messages))
    } else {
        Ok((continuation.clone(), messages))
    }
}
