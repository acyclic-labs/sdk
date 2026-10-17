use super::*;

// The canonical history and tool exchange remain intact. Only a verified repeated
// body in the transient model request is replaced, using the paired call's source.
pub(crate) fn deduplicate_reads(
    messages: &mut [ModelMessage],
    io: Option<&PreparationIo>,
) -> Result<bool> {
    if !messages
        .iter()
        .flat_map(|message| message.content.parts())
        .any(|part| {
            matches!(
                part,
                ModelContentPart::File {
                    policy: FileProjectionPolicy::ProjectInstructions(_),
                    ..
                }
            )
        })
    {
        return Ok(false);
    }
    let mut changed = false;
    for index in 0..messages.len() {
        let (prior, current) = messages.split_at_mut(index);
        let parts = match &mut current[0].content {
            ModelContent::Text(_) => continue,
            ModelContent::Part(part) => std::slice::from_mut(part),
            ModelContent::Parts(parts) => parts.as_mut_slice(),
        };
        for part in parts {
            let ModelContentPart::ToolResult {
                call_id,
                name,
                content:
                    ToolResultContent::Json {
                        value: Value::String(body),
                    },
            } = part
            else {
                continue;
            };
            let call = prior
                .iter()
                .flat_map(|message| message.content.parts())
                .find_map(|part| match part {
                    ModelContentPart::ToolCall(call)
                        if &call.call_id == call_id && &call.name == name =>
                    {
                        Some(call)
                    }
                    _ => None,
                });
            let Some(call) = call else { continue };
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let work = io.and_then(PreparationIo::original_native_work);
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let work = None;
            let input = match crate::tool::decode::<crate::tool::files::ReadFileInput>(
                &call.arguments,
                work,
            ) {
                Ok(input) => input,
                Err(error) if error.code() == crate::wire::ErrorCode::Invalid => continue,
                Err(error) => return Err(error),
            };
            if input.0.range.is_some() {
                continue;
            }
            let source = prior
                .iter()
                .flat_map(|message| message.content.parts())
                .find_map(|part| match part {
                    ModelContentPart::File {
                        file,
                        policy: FileProjectionPolicy::ProjectInstructions(source),
                    } if InstructionSource::with_identity(file, source)
                        .same_content(&input.0.file) =>
                    {
                        Some(InstructionSource::with_identity(file, source))
                    }
                    _ => None,
                });
            let Some(source) = source else { continue };
            if !with_history_io(io, |work| {
                input
                    .0
                    .file
                    .descriptor()
                    .matches_bytes(body.as_bytes(), work)
            })? {
                continue;
            }
            *body = with_history_io(io, |work| {
                crate::executor::preparation_format(
                    format_args!(
                        "Read completed; instructions already included from {} for {}.",
                        source.filename(),
                        source.directory()
                    ),
                    work,
                )
            })?;
            changed = true;
        }
    }
    Ok(changed)
}
