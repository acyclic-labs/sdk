use super::*;

#[cfg(feature = "filesystem")]
impl crate::filesystem::PreparationRecord for crate::tool::files::ReadFileInput {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        Ok(Some(
            (std::mem::size_of::<Self>() as u64)
                .checked_add(self.path.capacity() as u64)
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
    let mut parts_count = 0usize;
    let mut instructions = false;
    for message in messages.iter() {
        super::instruction_index::step(io, 1)?;
        for part in message.content.parts() {
            super::instruction_index::step(io, 1)?;
            parts_count = parts_count
                .checked_add(1)
                .ok_or_else(super::inventory::overflow)?;
            instructions |= matches!(
                part,
                ModelContentPart::File {
                    policy: FileProjectionPolicy::ProjectInstructions(_),
                    ..
                }
            );
        }
    }
    if !instructions {
        return Ok(false);
    }
    with_history_io(io, |work| {
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let mut index_map = super::instruction_index::Index::new(parts_count, &mut quota)?;
        let mut changed = false;
        for index in 0..messages.len() {
            super::instruction_index::step(io, 1)?;
            let (prior, current) = messages.split_at_mut(index);
            let parts = match &mut current[0].content {
                ModelContent::Text(_) => continue,
                ModelContent::Part(part) => std::slice::from_mut(part),
                ModelContent::Parts(parts) => parts.as_mut_slice(),
            };
            for (part_index, part) in parts.iter_mut().enumerate() {
                super::instruction_index::step(io, 1)?;
                match &*part {
                    ModelContentPart::File {
                        policy: FileProjectionPolicy::ProjectInstructions(source),
                        ..
                    } => {
                        let key = super::instruction_index::path_key(source.path(), io)?;
                        index_map.set(0, key, Some((index, part_index)), io)?;
                    }
                    ModelContentPart::ToolCall(call) => {
                        index_map.set(
                            1,
                            hash_bytes(call.call_id.as_bytes(), io)?,
                            Some((index, part_index)),
                            io,
                        )?;
                    }
                    _ => {}
                }
                let ModelContentPart::ToolResult {
                    call_id,
                    name,
                    content,
                } = part
                else {
                    continue;
                };
                let key = hash_bytes(call_id.as_bytes(), io)?;
                let paired = index_map.get(1, &key, io)?;
                // Consume every result, including non-string/partial/non-read results.
                index_map.set(1, key, None, io)?;
                let Some((call_message, call_part)) = paired else {
                    continue;
                };
                let Some(ModelContentPart::ToolCall(call)) = prior
                    .get(call_message)
                    .and_then(|message| message.content.parts().get(call_part))
                else {
                    continue;
                };
                if &call.call_id != call_id || &call.name != name {
                    continue;
                }
                let ToolResultContent::Parts { parts } = content else {
                    continue;
                };
                let [ModelDataPart::Text { text: body }] = parts.as_mut_slice() else {
                    continue;
                };
                let Some(arguments) = call.arguments.as_object() else {
                    continue;
                };
                if arguments
                    .keys()
                    .any(|key| !matches!(key.as_str(), "path" | "offset" | "limit"))
                {
                    continue;
                }
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
                    let (input, hold) = owned_read_input(&call.arguments, io)?;
                    _owned_hold = Some(hold);
                    Ok((input, None))
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
                // Only a read of the whole file can repeat the instructions.
                if input.0.offset.unwrap_or(1) != 1 || input.0.limit.is_some() {
                    continue;
                }
                let key = super::instruction_index::path_key(&input.0.path, io)?;
                let Some((source_message, source_part)) = index_map.get(0, &key, io)? else {
                    continue;
                };
                let Some(ModelContentPart::File {
                    file,
                    policy: FileProjectionPolicy::ProjectInstructions(identity),
                }) = prior
                    .get(source_message)
                    .and_then(|message| message.content.parts().get(source_part))
                else {
                    continue;
                };
                let source = InstructionSource::with_identity(file, identity);
                // The read names the instruction path; its body must show
                // exactly the instruction file's bytes.
                let Some(lines) = crate::tool::files::unnumbered(body) else {
                    continue;
                };
                if !with_history_io(io, |work| file.descriptor().matches_lines(lines, work))? {
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
        drop(index_map);
        Ok(changed)
    })
}

#[cfg(all(test, feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn owned_read_identity_reserves_before_decoding_with_unfunded_negative_control() {
    let file = super::instruction_tests::file("AGENTS.md", "rules", "rules");
    let arguments = serde_json::json!({"path":file.path()});
    let mut budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
    budget.bytes_encoded = 0;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(budget, &cancellation).unwrap();
    assert!(owned_read_input(&arguments, &io).is_err());
    assert!(
        crate::tool::decode::<crate::tool::files::ReadFileInput>(&arguments, None).is_ok(),
        "the unfunded decoder must miss the same exhausted preparation bound"
    );
}
