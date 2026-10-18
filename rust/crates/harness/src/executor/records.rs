use super::*;

impl StockExecution<'_> {
    pub(super) async fn prefix_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
    ) -> Result<Vec<ExecutionRecord>> {
        let maximum = self
            .stock
            .limits
            .model_events_per_step
            .saturating_add(self.stock.limits.tool_calls_per_step.saturating_mul(3))
            .saturating_add(4);
        let (_, records, hold) = replay_execution(
            journal,
            operation_id,
            maximum,
            |event| match event {
                ExecutionEvent::Started { .. } => true,
                ExecutionEvent::ModelStarted {
                    step: recorded,
                    purpose: ModelPurpose::Response,
                    ..
                }
                | ExecutionEvent::Model {
                    step: recorded,
                    purpose: ModelPurpose::Response,
                    ..
                }
                | ExecutionEvent::ModelStopped {
                    step: recorded,
                    purpose: ModelPurpose::Response,
                }
                | ExecutionEvent::ToolStarted { step: recorded, .. }
                | ExecutionEvent::ToolCompleted { step: recorded, .. }
                | ExecutionEvent::ToolFailed { step: recorded, .. }
                | ExecutionEvent::ContextPrepared { step: recorded, .. }
                | ExecutionEvent::ContextCompacted { step: recorded, .. } => *recorded == step,
                ExecutionEvent::ModelStarted { .. }
                | ExecutionEvent::Model { .. }
                | ExecutionEvent::ModelStopped { .. } => false,
            },
            self.io,
        )
        .await?;
        retain_preparation((records, hold))
    }

    pub(super) async fn model_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        purpose: ModelPurpose,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(
            journal,
            operation_id,
            self.stock.limits.model_events_per_step.saturating_add(3),
            |event| {
                matches!(event,
                ExecutionEvent::ModelStarted { step: event_step, purpose: event_purpose, .. }
                | ExecutionEvent::Model { step: event_step, purpose: event_purpose, .. }
                | ExecutionEvent::ModelStopped { step: event_step, purpose: event_purpose }
                if *event_step == step && *event_purpose == purpose)
                    || matches!(event, ExecutionEvent::ContextPrepared { step: event_step, .. }
                    | ExecutionEvent::ContextCompacted { step: event_step, .. }
                    if *event_step == step && purpose == ModelPurpose::Response)
            },
            self.io,
        )
        .await
        .and_then(|(tail, records, hold)| Ok((tail, retain_preparation((records, hold))?)))
    }

    pub(super) async fn tool_records(
        &self,
        journal: &dyn ExecutionJournal,
        operation_id: OperationId,
        step: u32,
        call_id: &str,
    ) -> Result<(u64, Vec<ExecutionRecord>)> {
        replay_execution(
            journal,
            operation_id,
            3,
            |event| {
                matches!(event,
            ExecutionEvent::ToolStarted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolCompleted { step: event_step, call_id: existing, .. }
            | ExecutionEvent::ToolFailed { step: event_step, call_id: existing, .. }
            if *event_step == step && existing == call_id)
            },
            self.io,
        )
        .await
        .and_then(|(tail, records, hold)| Ok((tail, retain_preparation((records, hold))?)))
    }
}

/// Every combination of one call's records, classified once. A settlement is final whether or
/// not a start precedes it (refusals settle without one), so replay never needs a start to
/// return it, and the turn's scan reaches every later call.
pub(super) enum RecordedCall<'a> {
    /// A settled failure, with the start when the tool was dispatched.
    Failed {
        started: Option<&'a FileRef>,
        reason: ToolFailureKind,
        cause: &'a ToolFailureCause,
    },
    /// A settled success; only a dispatched call can complete.
    Completed {
        started: &'a FileRef,
        result: &'a FileRef,
        projection: &'a FileRef,
    },
    /// Dispatched with an uncertain outcome; reconciliation settles it.
    Started(&'a FileRef),
    /// Nothing recorded; only an ordinary run may dispatch it.
    Unstarted,
}

impl<'a> RecordedCall<'a> {
    /// Classifies `records`, which [`StockExecution::tool_records`] filtered to one call.
    pub(super) fn classify(
        records: &'a [ExecutionRecord],
    ) -> std::result::Result<Self, &'static str> {
        let (mut started, mut failed, mut completed) = (None, None, None);
        for record in records {
            let duplicate = match &record.event {
                ExecutionEvent::ToolStarted { invocation, .. } => {
                    started.replace(invocation).is_some()
                }
                ExecutionEvent::ToolFailed { reason, cause, .. } => {
                    failed.replace((*reason, cause)).is_some()
                }
                ExecutionEvent::ToolCompleted {
                    result, projection, ..
                } => completed.replace((result, projection)).is_some(),
                _ => false,
            };
            if duplicate {
                return Err("tool call has duplicate records");
            }
        }
        match (started, failed, completed) {
            (_, Some(_), Some(_)) => Err("tool call settled twice"),
            (None, None, Some(_)) => Err("completed tool has no admitted invocation"),
            (started, Some((reason, cause)), None) => Ok(Self::Failed {
                started,
                reason,
                cause,
            }),
            (Some(started), None, Some((result, projection))) => Ok(Self::Completed {
                started,
                result,
                projection,
            }),
            (Some(started), None, None) => Ok(Self::Started(started)),
            (None, None, None) => Ok(Self::Unstarted),
        }
    }

    pub(super) fn started(&self) -> Option<&'a FileRef> {
        match self {
            Self::Failed { started, .. } => *started,
            Self::Completed { started, .. } | Self::Started(started) => Some(started),
            Self::Unstarted => None,
        }
    }
}
