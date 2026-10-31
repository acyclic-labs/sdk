use super::*;

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
                        let key = super::instruction_index::source_key(
                            source.volume(),
                            source.path(),
                            io,
                        )?;
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
                let ToolResultContent::Json {
                    value: Value::Object(result),
                } = content
                else {
                    continue;
                };
                let Some(arguments) = call.arguments.as_object() else {
                    continue;
                };
                if name.as_str() != "read"
                    || arguments
                        .keys()
                        .any(|key| !matches!(key.as_str(), "path" | "version" | "range"))
                {
                    continue;
                }
                let Some(path) = result.get("path").and_then(Value::as_str) else {
                    continue;
                };
                if arguments.get("path").and_then(Value::as_str) != Some(path) {
                    continue;
                }
                let Some(version) = result.get("version").and_then(Value::as_str) else {
                    continue;
                };
                #[cfg(feature = "filesystem")]
                let decoded = if let Some(io) = io {
                    io.decode::<FileRef>(
                        std::marker::PhantomData,
                        version.as_bytes(),
                        crate::wire::ErrorCode::Invalid,
                    )
                    .map(|(file, hold)| (file, Some(hold)))
                } else {
                    serde_json::from_str::<FileRef>(version)
                        .map(|file| (file, None))
                        .map_err(|error| Error::Invalid(error.to_string()))
                };
                #[cfg(not(feature = "filesystem"))]
                let decoded = serde_json::from_str::<FileRef>(version)
                    .map(|file| (file, ()))
                    .map_err(|error| Error::Invalid(error.to_string()));
                let (read_file, _hold) = match decoded {
                    Ok(source) => source,
                    Err(Error::Invalid(_)) => continue,
                    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                    Err(Error::NativeJson {
                        code: crate::wire::ErrorCode::Invalid,
                        ref source,
                        ..
                    }) if source.is_data() => continue,
                    Err(error) => return Err(error),
                };
                if read_file.path() != path
                    || arguments.get("range").is_some_and(|range| {
                        !range.is_null()
                            && (range.get("start").and_then(Value::as_u64) != Some(0)
                                || range.get("end").and_then(Value::as_u64)
                                    != Some(read_file.descriptor().byte_length()))
                    })
                {
                    continue;
                }
                let key =
                    super::instruction_index::source_key(read_file.volume(), read_file.path(), io)?;
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
                if !source.same_content(&read_file) {
                    continue;
                }
                let Some(Value::String(body)) = result.get_mut("text") else {
                    continue;
                };
                if !with_history_io(io, |work| {
                    read_file.descriptor().matches_bytes(body.as_bytes(), work)
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
        drop(index_map);
        Ok(changed)
    })
}

#[cfg(test)]
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
#[test]
fn owned_read_identity_reserves_before_decoding_with_unfunded_negative_control() {
    let file = super::instruction_tests::file("AGENTS.md", "rules", "rules");
    let version = serde_json::to_vec(&file).unwrap();
    let mut budget = acyclic_fs::performance::work_budget(1_000_000, 8 * 1024 * 1024);
    budget.items_examined = 0;
    let cancellation = acyclic_fs::CancellationToken::new();
    let io = PreparationIo::from_budget(budget, &cancellation).unwrap();
    assert!(
        io.decode::<FileRef>(
            std::marker::PhantomData,
            &version,
            crate::wire::ErrorCode::Invalid
        )
        .is_err()
    );
    assert!(
        serde_json::from_slice::<FileRef>(&version).is_ok(),
        "the unfunded decoder must miss the same exhausted preparation bound"
    );
}
