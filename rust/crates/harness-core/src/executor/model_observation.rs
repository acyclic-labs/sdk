//! Prepared model groups retain their original bytes through one publication.
use super::*;

pub(crate) fn matches_turn(event: &ExecutionEvent, step: u32, purpose: ModelPurpose) -> bool {
    matches!(event,
        ExecutionEvent::Model { step: recorded, purpose: recorded_purpose, .. }
        | ExecutionEvent::ModelInline { step: recorded, purpose: recorded_purpose, .. }
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
    observations: &[PreparedModelObservation<'_>],
    io: Option<&PreparationIo>,
) -> Result<ModelObservationReceipt> {
    let sequence = journal
        .append_prepared_models(operation, observations, io)?
        .await?;
    #[cfg(not(feature = "research"))]
    let _ = sequence;
    Ok(ModelObservationReceipt {
        #[cfg(feature = "research")]
        journal_cut: sequence.map(|sequence| ExecutionJournalCut {
            version: 1,
            operation,
            sequence,
        }),
    })
}

async fn prepare<'a>(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    key: String,
    (step, purpose): (u32, ModelPurpose),
    model: &'a ModelEvent,
    usage: Option<&super::model_usage::ModelCallUsage<'_>>,
    io: Option<&PreparationIo>,
) -> Result<PreparedModelObservation<'a>> {
    validate_journal(journal, io)?;
    let usage = if let Some(usage) = usage {
        let key = with_history_io(io, |work| {
            preparation_format(format_args!("{key}:usage"), work)
        })?;
        let _key_hold = io
            .map(|io| io.guard_owned(key.capacity() as u64))
            .transpose()?;
        // Validate the original canonical bytes before staging. Publication
        // retains this proof instead of reloading the file it just created.
        let bytes = match io {
            Some(io) => io.encode(usage, io.remaining()?.bytes_encoded, true)?,
            None => crate::contract::canonical_json_bytes(usage)?.into(),
        };
        let _bytes_hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
        let decoded = super::payload::decode_json::<ModelUsageRecord>(&bytes, io)?;
        super::model_usage::validate(&decoded.0, operation, step, purpose, model, io)?;
        let reference = stage_bytes(journal, operation, &key, &bytes, true, io).await?;
        drop(decoded);
        Some(reference)
    } else {
        None
    };
    let maximum = journal.inline_model_event_bytes();
    let bytes: acyclic_stream::OwnedBytes = match io {
        Some(io) => io.encode(model, io.remaining()?.bytes_encoded, true)?,
        None => crate::contract::canonical_json_bytes(model)?.into(),
    };
    let _hold = io.map(|io| io.guard_bytes(&bytes)).transpose()?;
    let event = if bytes.len() as u64 <= maximum {
        let text = std::str::from_utf8(&bytes).map_err(|_| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Storage,
                "canonical model event is not UTF-8",
            )
        })?;
        ExecutionEvent::ModelInline {
            step,
            purpose,
            event: stock_string_copy(text, io)?,
            usage,
        }
    } else {
        let event = stage_bytes(journal, operation, &key, &bytes, true, io).await?;
        ExecutionEvent::Model {
            step,
            purpose,
            event,
            usage,
        }
    };
    Ok(PreparedModelObservation {
        operation,
        binding: journal.cache_identity(),
        key,
        event,
        model,
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
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let key = stock_string_copy(key, io)?;
    let observation = prepare(journal, operation, key, turn, model, usage, io).await?;
    commit(journal, operation, std::slice::from_ref(&observation), io).await
}

pub(crate) async fn append_many(
    journal: &dyn ExecutionJournal,
    operation: OperationId,
    turn: (u32, ModelPurpose),
    first: usize,
    models: &[ModelEvent],
    usage: Option<&super::model_usage::ModelCallUsage<'_>>,
    original: Option<&PreparationIo>,
) -> Result<ModelObservationReceipt> {
    let scope = crate::filesystem::PreparationScope::new(original)?;
    let io = scope.io();
    let mut observations = crate::model::vec_with_capacity(models.len(), io)?;
    for (offset, model) in models.iter().enumerate() {
        let index = first.checked_add(offset).ok_or_else(|| {
            stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model observation ordinal overflows",
            )
        })?;
        let key = with_history_io(io, |work| {
            preparation_format(
                format_args!(
                    "{}:{}:{index}",
                    super::dispatch::model_purpose_key(turn.1),
                    turn.0
                ),
                work,
            )
        })?;
        let terminal_usage = matches!(model, ModelEvent::Completed { .. })
            .then_some(usage)
            .flatten();
        observations.push(prepare(journal, operation, key, turn, model, terminal_usage, io).await?);
    }
    commit(journal, operation, &observations, io).await
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
