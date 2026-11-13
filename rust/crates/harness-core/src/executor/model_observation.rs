//! One immutable payload for replay and live model-text observation.
use super::*;

pub(super) const CONTENT_MEDIA: &str = "text/vnd.acyclic.model-content.v1";
const REASONING_MEDIA: &str = "text/vnd.acyclic.model-reasoning.v1";
const RESTART_MEDIA: &str = "text/vnd.acyclic.model-restarted.v1";
const PAYLOAD_MEDIA: &[&str] = &[
    "application/json",
    CONTENT_MEDIA,
    REASONING_MEDIA,
    RESTART_MEDIA,
];

/// Decode an original model observation by its versioned payload media type.
/// The caller verifies the immutable reference and its read authority. Raw text
/// also admits bounded display reads without decoding the complete payload.
///
/// # Errors
/// Refuses unknown encodings, invalid UTF-8, noncanonical JSON and exhausted
/// preparation credit.
pub fn decode_payload(media: &str, bytes: &[u8], io: Option<&PreparationIo>) -> Result<ModelEvent> {
    if media == "application/json" {
        return retain_preparation(super::payload::decode_json::<ModelEvent>(bytes, io)?);
    }
    if !PAYLOAD_MEDIA.contains(&media) || (media == RESTART_MEDIA && !bytes.is_empty()) {
        return Err(stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "unknown model payload encoding",
        ));
    }
    with_history_io(io, |work| {
        if let Some(work) = work.as_deref_mut() {
            work.codec_charge(json::JsonCodecCharge::Inspect {
                items: bytes.len() as u64,
            })?;
        }
        Ok::<_, Error>(())
    })?;
    let text = std::str::from_utf8(bytes).map_err(|_| {
        stock_boundary(
            io,
            crate::wire::ErrorCode::Storage,
            "model text is not UTF-8",
        )
    })?;
    let record = io
        .map(|io| io.reserve_decode(std::mem::size_of::<ModelEvent>() as u64))
        .transpose()?;
    let event = match media {
        CONTENT_MEDIA => ModelEvent::Content {
            delta: stock_string_copy(text, io)?,
        },
        REASONING_MEDIA => ModelEvent::Reasoning {
            delta: stock_string_copy(text, io)?,
        },
        _ => ModelEvent::Restarted,
    };
    if let Some(record) = record {
        record.into_retained()?;
    }
    Ok(event)
}

pub(crate) fn matches_turn(event: &ExecutionEvent, step: u32, purpose: ModelPurpose) -> bool {
    matches!(event,
        ExecutionEvent::Model { step: recorded, purpose: recorded_purpose, .. }
        | ExecutionEvent::ModelInline { step: recorded, purpose: recorded_purpose, .. }
        | ExecutionEvent::ModelWait { step: recorded, purpose: recorded_purpose, .. }
        if *recorded == step && *recorded_purpose == purpose)
}

#[derive(Debug)]
pub(crate) struct ModelObservationReceipt {
    #[cfg(feature = "research")]
    pub(crate) journal_cut: Option<ExecutionJournalCut>,
}

async fn commit(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    event: &ExecutionEvent,
    model: &ModelEvent,
    io: Option<&PreparationIo>,
) -> Result<ModelObservationReceipt> {
    #[cfg(feature = "research")]
    if matches!(model, ModelEvent::Completed { .. })
        && let Some(sequenced) = journal.sequenced_appends()
    {
        return sequenced
            .append_with_sequence(operation, key, event, io)?
            .await
            .map(|sequence| ModelObservationReceipt {
                journal_cut: journal::completion_cut(Some(sequenced), operation, sequence),
            });
    }
    let _ = model;
    journal.append(operation, key, event, io)?.await?;
    Ok(ModelObservationReceipt {
        #[cfg(feature = "research")]
        journal_cut: None,
    })
}

pub(crate) async fn append(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: &str,
    turn: (u32, ModelPurpose),
    model: &ModelEvent,
    usage: Option<&super::model_usage::ModelCallUsage<'_>>,
    original: Option<&PreparationIo>,
) -> Result<ModelObservationReceipt> {
    validate_journal(journal, original)?;
    let (step, purpose) = turn;
    // Visibility selects the original text's path, never its record identity.
    let text_key = key;
    let key = key
        .strip_prefix("model-live:")
        .and_then(|rest| rest.split_once(':'))
        .map_or(key, |(_, original)| original);
    if let ModelEvent::Wait(observation) = model {
        if usage.is_some() {
            return Err(stock_boundary(
                original,
                crate::wire::ErrorCode::Invalid,
                "wait observation cannot carry terminal usage",
            ));
        }
        return commit(
            journal,
            operation,
            key,
            &ExecutionEvent::ModelWait {
                step,
                purpose,
                observation: *observation,
            },
            model,
            original,
        )
        .await;
    }
    let maximum = journal.inline_model_event_bytes();
    let usage = if let Some(usage) = usage {
        let key = with_history_io(original, |work| {
            preparation_format(format_args!("{key}:usage"), work)
        })?;
        let _key_hold = original
            .map(|io| io.guard_owned(key.capacity() as u64))
            .transpose()?;
        Some(stage_json(journal, operation, &key, usage, original).await?)
    } else {
        None
    };
    if maximum == 0 {
        let text = match model {
            ModelEvent::Content { delta } => Some((delta.as_bytes(), CONTENT_MEDIA, "reply")),
            ModelEvent::Reasoning { delta } => {
                Some((delta.as_bytes(), REASONING_MEDIA, "reasoning"))
            }
            ModelEvent::Restarted => Some((&[][..], RESTART_MEDIA, "restart")),
            _ => None,
        };
        let event = if let Some((bytes, media, kind)) = text {
            let text_key = with_history_io(original, |work| {
                preparation_format(format_args!("{text_key}:{kind}"), work)
            })?;
            let _hold = original
                .map(|io| io.guard_owned(text_key.capacity() as u64))
                .transpose()?;
            stage_bytes(
                journal,
                operation,
                &text_key,
                (bytes, media),
                true,
                original,
            )
            .await?
        } else {
            stage_json(journal, operation, key, model, original).await?
        };
        return commit(
            journal,
            operation,
            key,
            &ExecutionEvent::Model {
                step,
                purpose,
                event,
                usage,
            },
            model,
            original,
        )
        .await;
    }
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let _hold;
    let bytes: acyclic_stream::OwnedBytes = match io {
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
        let event = stage_bytes(
            journal,
            operation,
            key,
            (&bytes, "application/json"),
            true,
            io,
        )
        .await?;
        ExecutionEvent::Model {
            step,
            purpose,
            event,
            usage,
        }
    };
    commit(journal, operation, key, &event, model, io).await
}

pub(crate) async fn load(
    journal: &dyn ExecutionJournal,
    event: &ExecutionEvent,
    io: Option<&PreparationIo>,
) -> Result<Option<ModelEvent>> {
    let model = match event {
        ExecutionEvent::ModelWait { observation, .. } => Some(ModelEvent::Wait(*observation)),
        ExecutionEvent::Model { event, .. } => {
            validate_journal(journal, io)?;
            if event.volume().class() != VolumeClass::AgentPrivate
                || !PAYLOAD_MEDIA.contains(&event.descriptor().media_type())
            {
                return Err(stock_boundary(
                    io,
                    crate::wire::ErrorCode::Storage,
                    "model payload is not private",
                ));
            }
            let bytes = journal.load(event, io)?.await?;
            let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
            with_history_io(io, |work| event.descriptor().verify(&bytes, work))?;
            Some(decode_payload(event.descriptor().media_type(), &bytes, io)?)
        }
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
