use super::*;

#[derive(Default)]
pub(super) struct ExecutionSummary {
    pub(super) operation: Option<OperationId>,
    pub(super) tail: u64,
    pub(super) retries: BTreeMap<[u8; 32], (u64, [u8; 32])>,
    pub(super) contexts: BTreeMap<u32, bool>,
    pub(super) models: BTreeMap<(u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>>,
    pub(super) tools: BTreeMap<(u32, [u8; 32]), bool>,
}

impl ExecutionSummary {
    pub(super) fn require_next(
        &self,
        event: &ExecutionEvent,
        io: Option<&PreparationIo>,
    ) -> Result<Option<[u8; 32]>> {
        let hashed = match event {
            ExecutionEvent::ToolStarted { call_id, .. } if self.tail > 0 && !call_id.is_empty() => {
                call_id.len() as u64
            }
            ExecutionEvent::ToolCompleted { call_id, .. }
            | ExecutionEvent::ToolFailed { call_id, .. } => call_id.len() as u64,
            _ => 0,
        };
        let work = acyclic_fs::WorkCounters {
            bytes_hashed: hashed,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(work)?;
        }
        let mut tool_key = None;
        let valid = match event {
            ExecutionEvent::Started { request_digest } => {
                self.tail == 0 && *request_digest != [0; 32]
            }
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.tail > 0
                    && !self.contexts.contains_key(step)
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.get(step) == Some(&false)
                    && matches!(
                        self.models
                            .get(&(*step, crate::executor::ModelPurpose::Summary)),
                        Some(None)
                    )
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response))
            }
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                self.tail > 0
                    && *request_digest != [0; 32]
                    && !self.models.contains_key(&(*step, *purpose))
            }
            ExecutionEvent::Model { step, purpose, .. } => self
                .models
                .get(&(*step, *purpose))
                .is_some_and(Option::is_some),
            ExecutionEvent::ToolStarted { step, call_id, .. } => {
                self.tail > 0 && !call_id.is_empty() && {
                    let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                    tool_key = Some(key);
                    !self.tools.contains_key(&(*step, key))
                }
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. }
            | ExecutionEvent::ToolFailed { step, call_id, .. } => {
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                tool_key = Some(key);
                self.tools.get(&(*step, key)) == Some(&false)
            }
        };
        if let Some(io) = io {
            io.charge(work)?;
        }
        if valid {
            Ok(tool_key)
        } else {
            Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution observation lacks its dispatch or reuses a start",
            ))
        }
    }

    pub(super) fn validate_next(
        &self,
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if let Some(io) = io {
            io.check_cancellation()?;
        }
        self.require_next(event, io)?;
        if let ExecutionEvent::Model { step, purpose, .. } = event {
            self.models
                .get(&(*step, *purpose))
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "model dispatch is already settled",
                    )
                })?
                .validate_next(
                    model.ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "model observation is missing",
                        )
                    })?,
                    limits,
                    io,
                )?;
        }
        Ok(())
    }

    pub(super) fn accept(
        &mut self,
        record: &ExecutionRecord,
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let key_work = acyclic_fs::WorkCounters {
            bytes_hashed: record.idempotency_key.len() as u64,
            ..Default::default()
        };
        if let Some(io) = io {
            io.preflight(key_work)?;
        }
        let key = *blake3::hash(record.idempotency_key.as_bytes()).as_bytes();
        if let Some(io) = io {
            io.charge(key_work)?;
        }
        if record.sequence != self.tail + 1
            || record.idempotency_key.is_empty()
            || self.retries.contains_key(&key)
        {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution journal sequence or retry identity is invalid",
            ));
        }
        let tool_key = self.require_next(&record.event, io)?;
        let digest = crate::executor::with_history_io(io, |work|
            crate::contract::canonical_json_digest(&record.event, u64::MAX, work))?;
        if let Some(io) = io {
            let extra = match &record.event {
                ExecutionEvent::ContextPrepared { .. } => std::mem::size_of::<(u32, bool)>(),
                ExecutionEvent::ModelStarted { .. } => std::mem::size_of::<(
                    (u32, crate::executor::ModelPurpose),
                    Option<ModelEventAdmission>,
                )>(),
                ExecutionEvent::ToolStarted { .. } => {
                    std::mem::size_of::<((u32, [u8; 32]), bool)>()
                }
                _ => 0,
            };
            io.retain_owned((std::mem::size_of::<([u8; 32], (u64, [u8; 32]))>() + extra) as u64)?;
        }
        match &record.event {
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.contexts.insert(*step, false);
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.insert(*step, true);
            }
            ExecutionEvent::ModelStarted { step, purpose, .. } => {
                self.models
                    .insert((*step, *purpose), Some(ModelEventAdmission::default()));
            }
            ExecutionEvent::Model { step, purpose, .. } => {
                let state = self.models.get_mut(&(*step, *purpose)).ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Storage,
                        "model dispatch is missing",
                    )
                })?;
                let admission = state.as_mut().ok_or_else(|| {
                    canonical_boundary(
                        io,
                        crate::wire::ErrorCode::Conflict,
                        "model dispatch is already settled",
                    )
                })?;
                admission.observe(
                    model.ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "model observation is missing",
                        )
                    })?,
                    limits,
                    io,
                )?;
                if admission.completed {
                    *state = None;
                }
            }
            ExecutionEvent::ToolStarted { step, .. } => {
                self.tools.insert(
                    (
                        *step,
                        tool_key.expect("tool start has an authenticated identity"),
                    ),
                    false,
                );
            }
            ExecutionEvent::ToolCompleted { step, .. }
            | ExecutionEvent::ToolFailed { step, .. } => {
                self.tools.insert(
                    (
                        *step,
                        tool_key.expect("tool settlement has an authenticated identity"),
                    ),
                    true,
                );
            }
            ExecutionEvent::Started { .. } => {}
        }
        self.retries.insert(key, (record.sequence, digest));
        self.tail = record.sequence;
        Ok(())
    }

    pub(super) fn quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        for model in self.models.values() {
            crate::model::charge_items(io, 1)?;
            if model.is_some() {
                return Ok(false);
            }
        }
        for settled in self.tools.values() {
            crate::model::charge_items(io, 1)?;
            if !settled {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

pub(super) const OBSERVATION_VERSION: u32 = 2;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    pub(super) version: u32,
    pub(super) operation_id: OperationId,
    pub(super) turn_operation: OperationId,
    pub(super) retry_digest: String,
    pub(super) event: ExecutionEvent,
}

impl PreparationRecord for Observation {
    fn diagnostic_label_bytes() -> u64 {
        const PRIVATE_LABEL_BYTES: u64 = {
            let labels = [
                "Observation",
                "version",
                "operation_id",
                "turn_operation",
                "retry_digest",
                "event",
                "ExecutionEvent",
                "kind",
                "started",
                "context_prepared",
                "context_compacted",
                "model_started",
                "model",
                "tool_started",
                "tool_completed",
                "tool_failed",
                "request_digest",
                "step",
                "projection",
                "accounting",
                "compaction",
                "purpose",
                "request",
                "call_id",
                "invocation",
                "result",
                "reason",
                "ModelPurpose",
                "response",
                "summary",
                "ToolFailureKind",
                "executor_rejected",
                "invalid_output",
                "projection_rejected",
                "publication_rejected",
            ];
            let mut bytes = 0;
            let mut index = 0;
            while index < labels.len() {
                bytes += labels[index].len() as u64 + 4;
                index += 1;
            }
            bytes
        };
        crate::contract::preparation_serde_diagnostic_label_bytes() + PRIVATE_LABEL_BYTES
    }
}
