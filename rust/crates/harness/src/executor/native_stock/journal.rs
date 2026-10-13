use super::*;

pub(in crate::executor) async fn stage_bytes(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    bytes: &[u8],
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    let copied;
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.preflight(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64 * 2,
            ..Default::default()
        })?;
    }
    use sha2::Digest as _;
    let digest: [u8; 32] = sha2::Sha256::digest(bytes).into();
    let identity = blake3::hash(bytes);
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        io.spent(acyclic_fs::WorkCounters {
            bytes_hashed: bytes.len() as u64 * 2,
            ..Default::default()
        })?;
    }
    copied = with_history_io(io, |work| {
        crate::executor::preparation_format(format_args!("{key}:{}", identity.to_hex()), work)
    })?;
    let reference = journal
        .stage(operation, &copied, bytes, "application/json", io)?
        .await?;
    if reference.volume().class() != VolumeClass::AgentPrivate
        || reference.descriptor().media_type() != "application/json"
        || reference.descriptor().byte_length() != bytes.len() as u64
        || reference.descriptor().sha256() != &digest
    {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            return Err(crate::Error::NativeBoundary {
                code: crate::wire::ErrorCode::Storage,
                reason: "execution journal staged different private JSON bytes",
            });
        }
        return Err(Error::Storage(
            "execution journal staged different private JSON bytes".into(),
        ));
    }
    Ok(reference)
}

pub(crate) async fn stage_json<T: Serialize>(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    value: &T,
    io: Option<&PreparationIo>,
) -> Result<FileRef> {
    #[cfg(feature = "filesystem")]
    if let Some(io) = io {
        let hold;
        let bytes = io.encode(value, io.remaining()?.bytes_encoded, true)?;
        hold = io.guard_bytes(&bytes)?;
        let result = stage_bytes(journal, operation, key, &bytes, Some(io)).await;
        drop(bytes);
        drop(hold);
        return result;
    }
    let bytes = crate::contract::canonical_json_bytes(value)?;
    stage_bytes(journal, operation, key, &bytes, None).await
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn replay_execution_with_native_work<'a>(
    journal: &'a dyn ExecutionJournal,
    operation: OperationId,
    maximum: usize,
    select: impl Fn(&ExecutionEvent) -> bool + Send + 'a,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (u64, Vec<ExecutionRecord>, acyclic_fs::NativeAllocation)>>
{
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution journal is unavailable",
        });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.claim(
            std::mem::size_of::<Vec<ExecutionRecord>>() as u64,
            0,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        let (tail, records) =
            replay_execution(journal, operation, maximum, select, Some(&io)).await?;
        let mut retained = native_size_add(
            std::mem::size_of::<Vec<ExecutionRecord>>() as u64,
            (records.capacity() as u64)
                .checked_mul(std::mem::size_of::<ExecutionRecord>() as u64)
                .ok_or_else(|| crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Invalid,
                    reason: "execution replay size overflow",
                })?,
        )?;
        for record in &records {
            retained = native_size_add(retained, record.idempotency_key.capacity() as u64)?;
            let event_bytes = match &record.event {
                ExecutionEvent::Started { .. } => 0,
                ExecutionEvent::ContextPrepared {
                    projection,
                    accounting,
                    ..
                } => native_size_add(
                    projection.logical_owned_bytes()?,
                    match accounting {
                        Some(reference) => reference.logical_owned_bytes()?,
                        None => 0,
                    },
                )?,
                ExecutionEvent::ContextCompacted {
                    projection,
                    compaction,
                    accounting,
                    ..
                } => native_size_add(
                    native_size_add(
                        projection.logical_owned_bytes()?,
                        compaction.logical_owned_bytes()?,
                    )?,
                    accounting.logical_owned_bytes()?,
                )?,
                ExecutionEvent::ModelStarted { request, .. } => request.logical_owned_bytes()?,
                ExecutionEvent::Model { event, .. } => event.logical_owned_bytes()?,
                ExecutionEvent::ToolStarted {
                    call_id,
                    invocation,
                    ..
                } => native_size_add(call_id.capacity() as u64, invocation.logical_owned_bytes()?)?,
                ExecutionEvent::ToolCompleted {
                    call_id,
                    result,
                    projection,
                    ..
                } => native_size_add(
                    call_id.capacity() as u64,
                    native_size_add(
                        result.logical_owned_bytes()?,
                        projection.logical_owned_bytes()?,
                    )?,
                )?,
                ExecutionEvent::ToolFailed { call_id, .. } => call_id.capacity() as u64,
            };
            retained = native_size_add(retained, event_bytes)?;
        }
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((tail, records, custody))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn stage_json_with_native_work<'a, T: Serialize + Sync>(
    journal: &'a dyn ExecutionJournal,
    operation: OperationId,
    key: &'a str,
    value: &'a T,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (FileRef, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution journal is unavailable",
        });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let reference = stage_json(journal, operation, key, value, Some(&io)).await?;
        let retained = reference.logical_owned_bytes()?;
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((reference, custody))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn load_json_with_native_work<
    'a,
    T: PreparationRecord + crate::filesystem::UploadRecord + Send,
>(
    journal: &'a dyn ExecutionJournal,
    reference: &'a FileRef,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (T, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution journal is unavailable",
        });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let value: T = retain_preparation(load_json::<T>(journal, reference, Some(&io)).await?);
        let retained = native_size_add(
            std::mem::size_of::<T>() as u64,
            crate::filesystem::UploadRecord::logical_owned_bytes(&value)?,
        )?;
        work.remaining()?;
        let custody = io.into_native_custody(retained)?;
        Ok((value, custody))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_size_add(left: u64, right: u64) -> Result<u64> {
    left.checked_add(right)
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native stock logical payload size overflow",
        })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_events_owned_bytes(events: &Vec<ModelEvent>) -> Result<u64> {
    let slots = (events.capacity() as u64)
        .checked_mul(std::mem::size_of::<ModelEvent>() as u64)
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native event slots overflow",
        })?;
    events.iter().try_fold(
        native_size_add(std::mem::size_of::<Vec<ModelEvent>>() as u64, slots)?,
        |bytes, event| {
            native_size_add(
                bytes,
                event
                    .logical_owned_bytes()?
                    .checked_sub(std::mem::size_of::<ModelEvent>() as u64)
                    .ok_or_else(|| crate::Error::NativeBoundary {
                        code: crate::wire::ErrorCode::Storage,
                        reason: "native event metadata extent differs",
                    })?,
            )
        },
    )
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_output_payload_bytes(output: &TurnOutput) -> Result<u64> {
    let slots = (output.attachments.capacity() as u64)
        .checked_mul(std::mem::size_of::<Attachment>() as u64)
        .ok_or_else(|| crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Storage,
            reason: "native attachment slots overflow",
        })?;
    let mut bytes = native_size_add(output.text.capacity() as u64, slots)?;
    bytes = native_size_add(
        bytes,
        crate::executor::json::json_value_logical_owned_bytes(&output.metadata)?,
    )?;
    for attachment in &output.attachments {
        bytes = native_size_add(bytes, attachment.file.logical_owned_bytes()?)?;
        if let Some(label) = &attachment.label {
            bytes = native_size_add(bytes, label.capacity() as u64)?;
        }
    }
    Ok(bytes)
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(in crate::executor) fn native_progress_owned_bytes(
    progress: &ExecutionProgress<TurnOutput>,
) -> Result<u64> {
    let root = std::mem::size_of::<ExecutionProgress<TurnOutput>>() as u64;
    match progress {
        ExecutionProgress::Ready(output) => {
            native_size_add(root, native_output_payload_bytes(output)?)
        }
        ExecutionProgress::Pending(_) | ExecutionProgress::Rejected(_) => Ok(root),
    }
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn retained_model_step_with_native_work<'a>(
    journal: &'a dyn ExecutionJournal,
    records: &'a [ExecutionRecord],
    step: u32,
    purpose: ModelPurpose,
    limits: Limits,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<crate::NativeFutureBundle<'a, (RetainedModelStep, acyclic_fs::NativeAllocation)>> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution journal is unavailable",
        });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        io.absorb_native_custody(work.claim(
            std::mem::size_of::<RetainedModelStep>() as u64,
            0,
            acyclic_fs::WorkCounters::default(),
        )?)?;
        let retained =
            retained_model_step(journal, records, step, purpose, limits, Some(&io)).await?;
        let mut bytes = std::mem::size_of::<RetainedModelStep>() as u64;
        if let Some(request) = &retained.request {
            bytes = native_size_add(
                bytes,
                request.logical_owned_bytes()?
                    - std::mem::size_of::<crate::model::PreparedModelRequest>() as u64,
            )?;
        }
        bytes = native_size_add(
            bytes,
            native_events_owned_bytes(&retained.events)?
                - std::mem::size_of::<Vec<ModelEvent>>() as u64,
        )?;
        bytes = native_size_add(
            bytes,
            (retained.admission.calls.capacity() as u64)
                .checked_mul(std::mem::size_of::<String>() as u64)
                .ok_or_else(|| crate::Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Storage,
                    reason: "native admission slots overflow",
                })?,
        )?;
        for call in &retained.admission.calls {
            bytes = native_size_add(bytes, call.capacity() as u64)?;
        }
        work.remaining()?;
        Ok((retained, io.into_native_custody(bytes)?))
    })
}

#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn completed_tool_projection_with_native_work<'a>(
    journal: &'a dyn ExecutionJournal,
    records: &'a [ExecutionRecord],
    step: u32,
    expected: &'a ToolInvocation,
    tools: &'a [crate::tool::ToolDefinition],
    limits: Limits,
    work: &'a acyclic_fs::NativeMaterializationWork,
) -> Result<
    crate::NativeFutureBundle<
        'a,
        (
            crate::model::ToolResultContent,
            acyclic_fs::NativeAllocation,
        ),
    >,
> {
    if !journal.supports_native_execution(work)? {
        return Err(crate::Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unsupported,
            reason: "native execution journal is unavailable",
        });
    }
    crate::NativeFutureBundle::new(work, || async move {
        let io = PreparationIo::from_native_work(work)?;
        let content =
            completed_tool_projection(journal, records, step, expected, tools, limits, Some(&io))
                .await?;
        let retained = content.logical_owned_bytes()?;
        work.remaining()?;
        Ok((content, io.into_native_custody(retained)?))
    })
}
