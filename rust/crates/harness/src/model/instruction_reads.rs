use super::*;

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for crate::tool::files::ReadFileInput {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        Ok(Some(
            (std::mem::size_of::<Self>() as u64)
                .checked_add(self.file.logical_owned_bytes()?)
                .ok_or_else(super::inventory::overflow)?,
        ))
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
fn owned_read_input<'a>(
    arguments: &Value,
    io: &'a PreparationIo,
) -> Result<(
    crate::tool::files::ReadFileInput,
    crate::filesystem::DecodeReservation<'a>,
)> {
    let before = io.owned_bytes()?;
    let input = {
        let bytes = io.encode(arguments, io.remaining()?.bytes_encoded, false)?;
        let _hold = io.guard_bytes(&bytes)?;
        crate::filesystem::retain_decoded(io.decode(
            std::marker::PhantomData,
            &bytes,
            crate::wire::ErrorCode::Invalid,
        )?)?
    };
    let hold = io.guard_owned(
        io.owned_bytes()?
            .checked_sub(before)
            .ok_or_else(super::inventory::overflow)?,
    )?;
    Ok((input, hold))
}

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
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let mut _owned_hold = None;
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let owned = io.filter(|io| io.original_native_work().is_none());
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            let decoded = if let Some(io) = owned {
                owned_read_input(&call.arguments, io).map(|(input, hold)| {
                    _owned_hold = Some(hold);
                    (input, None)
                })
            } else {
                crate::tool::decode::<crate::tool::files::ReadFileInput>(&call.arguments, work)
            };
            #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
            let decoded =
                crate::tool::decode::<crate::tool::files::ReadFileInput>(&call.arguments, work);
            let input = match decoded {
                Ok(input) => input,
                Err(Error::Invalid(_)) => continue,
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                Err(Error::NativeJson {
                    code: crate::wire::ErrorCode::Invalid,
                    ref source,
                    ..
                }) if source.is_data() => continue,
                Err(error) => return Err(error),
            };
            if input.0.range.is_some_and(|range| {
                range.start != 0 || range.end != input.0.file.descriptor().byte_length()
            }) {
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
