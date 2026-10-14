use super::*;

pub(super) async fn sampling_messages(
    params: rmcp::model::CreateMessageRequestParams,
    context: &ToolContext,
    operation: OperationId,
    continuation: &ModelContent,
    image: Option<&NativeMediaPolicy>,
    audio: Option<&NativeMediaPolicy>,
) -> Result<(ModelContent, Vec<ModelMessage>)> {
    if params.max_tokens == 0 {
        return Err(Error::Invalid("MCP sampling token bound is zero".into()));
    }
    let mut messages = Vec::new();
    // Server-supplied system prompt remains external user content.
    if let Some(prompt) = params.system_prompt {
        messages.push(ModelMessage {
            role: ModelRole::User,
            content: ModelContent::Text(prompt),
        });
    }
    for (message_index, message) in params.messages.into_iter().enumerate() {
        let role = match message.role {
            rmcp::model::Role::User => ModelRole::User,
            rmcp::model::Role::Assistant => ModelRole::Assistant,
        };
        let mut parts = Vec::new();
        for (block_index, block) in message.content.into_vec().into_iter().enumerate() {
            let part = match block {
                rmcp::model::SamplingMessageContentBlock::Text(text) => {
                    ModelContentPart::Text { text: text.text }
                }
                block => {
                    sampling_media(
                        block,
                        context,
                        (operation, message_index, block_index),
                        (image, audio),
                    )
                    .await?
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

async fn sampling_media(
    block: rmcp::model::SamplingMessageContentBlock,
    context: &ToolContext,
    identity: (OperationId, usize, usize),
    media: (Option<&NativeMediaPolicy>, Option<&NativeMediaPolicy>),
) -> Result<ModelContentPart> {
    let (operation, message_index, block_index) = identity;
    let (image, audio) = media;

    let (encoded, mime, media) = match block {
        rmcp::model::SamplingMessageContentBlock::Image(content) => {
            (content.data, content.mime_type, image)
        }
        rmcp::model::SamplingMessageContentBlock::Audio(content) => {
            (content.data, content.mime_type, audio)
        }
        _ => {
            return Err(Error::Unsupported(
                "MCP sampling content needs a host-selected projection".into(),
            ));
        }
    };
    let media = media.ok_or_else(|| {
        Error::Unsupported("MCP sampling media was not selected by this host".into())
    })?;
    let maximum_bytes = media.maximum_bytes.min(context.scope().limits().file_bytes);
    let encoded_bound = maximum_bytes
        .checked_add(2)
        .and_then(|length| (length / 3).checked_mul(4))
        .ok_or_else(|| Error::Invalid("MCP media byte allowance overflows".into()))?;
    if maximum_bytes == 0 || encoded.len() as u64 > encoded_bound {
        return Err(Error::Invalid(
            "MCP media exceeds selected byte allowance".into(),
        ));
    }
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(&encoded)
        .map_err(|_| Error::Invalid("MCP media base64 invalid".into()))?;
    if bytes.len() as u64 > maximum_bytes {
        return Err(Error::Invalid(
            "MCP media exceeds selected byte allowance".into(),
        ));
    }
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
        .stage_file_once(
            OperationId::from_bytes(identity),
            &path,
            &bytes,
            &mime,
            "MCP sampling media",
            None,
        )
        .await
        .map(|(file, _)| file)?;
    media.validate(&file, context.scope().limits(), &mut None)?;
    Ok(ModelContentPart::File {
        file,
        policy: FileProjectionPolicy::Native(Box::new(media.clone())),
    })
}
