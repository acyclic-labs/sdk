//! Inline small model observations without a separate staged generation.
use super::*;

pub(crate) fn matches_turn(event: &ExecutionEvent, step: u32, purpose: ModelPurpose) -> bool {
    matches!(event,
        ExecutionEvent::Model { step: recorded, purpose: recorded_purpose, .. }
        | ExecutionEvent::ModelInline { step: recorded, purpose: recorded_purpose, .. }
        if *recorded == step && *recorded_purpose == purpose)
}

pub(crate) async fn append(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    turn: (u32, ModelPurpose),
    model: &ModelEvent,
    usage: Option<&super::model_usage::ModelCallUsage<'_>>,
    original: Option<&PreparationIo>,
) -> Result<()> {
    validate_journal(journal, original)?;
    let maximum = journal.inline_model_event_bytes();
    let (step, purpose) = turn;
    let usage = if let Some(usage) = usage {
        let key = with_history_io(original, |work| {
            preparation_format(format_args!("{key}:usage"), work)
        })?;
        #[cfg(feature = "filesystem")]
        let _key_hold = original
            .map(|io| io.guard_owned(key.capacity() as u64))
            .transpose()?;
        Some(stage_json(journal, operation, &key, usage, original).await?)
    } else {
        None
    };
    if maximum == 0 {
        let event = stage_json(journal, operation, key, model, original).await?;
        return journal
            .append(
                operation,
                key,
                &ExecutionEvent::Model {
                    step,
                    purpose,
                    event,
                    usage,
                },
                original,
            )?
            .await;
    }
    #[cfg(feature = "filesystem")]
    let scope = crate::filesystem::PreparationScope::new(original)?;
    #[cfg(feature = "filesystem")]
    let io = scope.io();
    #[cfg(not(feature = "filesystem"))]
    let io = original;
    #[cfg(feature = "filesystem")]
    let _hold;
    let bytes: acyclic_stream::OwnedBytes = match io {
        #[cfg(feature = "filesystem")]
        Some(io) => {
            let bytes = io.encode(model, io.remaining()?.bytes_encoded, true)?;
            _hold = io.guard_bytes(&bytes)?;
            bytes
        }
        _ => crate::contract::canonical_json_bytes(model)?.into(),
    };
    let event = if bytes.len() as u64 <= maximum {
        let event = std::str::from_utf8(&bytes).map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "canonical model event is not UTF-8",
            )
        })?;
        ExecutionEvent::ModelInline {
            step,
            purpose,
            event: stock_string_copy(event, io)?,
            usage,
        }
    } else {
        let event = stage_bytes(journal, operation, key, &bytes, true, io).await?;
        ExecutionEvent::Model {
            step,
            purpose,
            event,
            usage,
        }
    };
    journal.append(operation, key, &event, io)?.await
}

pub(crate) async fn load(
    journal: &dyn ExecutionJournal,
    event: &ExecutionEvent,
    io: Option<&PreparationIo>,
) -> Result<Option<ModelEvent>> {
    let model = match event {
        ExecutionEvent::Model { event, .. } => Some(retain_preparation(
            load_json::<ModelEvent>(journal, event, io).await?,
        )?),
        ExecutionEvent::ModelInline { event, .. } => {
            if event.len() as u64 > journal.inline_model_event_bytes() {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Unsupported,
                    "inline model observation exceeds journal capability",
                ));
            }
            Some(retain_preparation(super::payload::decode_json::<
                ModelEvent,
            >(event.as_bytes(), io)?)?)
        }
        _ => None,
    };
    Ok(model)
}
