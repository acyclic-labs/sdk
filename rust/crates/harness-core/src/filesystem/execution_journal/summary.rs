use super::summary_map::SummaryMap;
use super::*;

#[cfg(test)]
#[cfg(not(loom))]
pub(super) mod tests;

#[derive(Default)]
pub(super) struct ExecutionSummary {
    pub(super) operation: Option<OperationId>,
    pub(super) tail: u64,
    pub(super) retries: SummaryMap<[u8; 32], (u64, [u8; 32])>,
    pub(super) contexts: SummaryMap<u32, bool>,
    pub(super) models:
        SummaryMap<(u32, crate::executor::ModelPurpose), Option<ModelEventAdmission>>,
    pub(super) tools: SummaryMap<(u32, [u8; 32]), bool>,
    pub(super) pending_dispatches: u64,
    pub(super) latest_response: Option<u32>,
    pub(super) model_dispatched: bool,
    pub(super) recovery: Option<checkpoint::Recovery>,
    pub(super) pages: persistent_index::NodeCache,
    pub(super) scratch: Option<persistent_index::Scratch>,
    pub(super) partial: bool,
    pub(super) pending_steps: step_index::Pending,
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
                    && !self.contexts.contains_key(step, io)?
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response), io)?
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.get(step, io)? == Some(&false)
                    && matches!(
                        self.models
                            .get(&(*step, crate::executor::ModelPurpose::Summary), io)?,
                        Some(None)
                    )
                    && !self
                        .models
                        .contains_key(&(*step, crate::executor::ModelPurpose::Response), io)?
            }
            ExecutionEvent::ModelStarted {
                step,
                purpose,
                request_digest,
                ..
            } => {
                self.tail > 0
                    && *request_digest != [0; 32]
                    && !self.models.contains_key(&(*step, *purpose), io)?
            }
            ExecutionEvent::Model { step, purpose, .. }
            | ExecutionEvent::ModelInline { step, purpose, .. }
            | ExecutionEvent::ModelStopped { step, purpose } => self
                .models
                .get(&(*step, *purpose), io)?
                .is_some_and(Option::is_some),
            ExecutionEvent::ToolStarted { step, call_id, .. } => {
                self.tail > 0 && !call_id.is_empty() && {
                    let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                    tool_key = Some(key);
                    !self.tools.contains_key(&(*step, key), io)?
                }
            }
            ExecutionEvent::ToolCompleted { step, call_id, .. } => {
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                tool_key = Some(key);
                self.tools.get(&(*step, key), io)? == Some(&false)
            }
            // The length check short-circuits first, so the scan is bounded.
            ExecutionEvent::ToolFailed {
                step,
                call_id,
                reason,
                cause,
            } => {
                let key = *blake3::hash(call_id.as_bytes()).as_bytes();
                tool_key = Some(key);
                // Admission and execution share the same ordinary failure. A failed
                // admission has no dispatch to retire; a failed execution has one.
                let recorded = self.tools.get(&(*step, key), io)?;
                self.tail > 0
                    && !call_id.is_empty()
                    && cause.is_canonical()
                    && if *reason == crate::executor::ToolFailureKind::ExecutorRejected {
                        recorded.is_none() || recorded == Some(&false)
                    } else if reason.precedes_dispatch() {
                        recorded.is_none()
                    } else {
                        recorded == Some(&false)
                    }
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
                "execution observation lacks its dispatch, reuses a start or carries an unbounded cause",
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
        if let ExecutionEvent::Model { step, purpose, .. }
        | ExecutionEvent::ModelInline { step, purpose, .. } = event
        {
            self.models
                .get(&(*step, *purpose), io)?
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
        let key = crate::model::hash_bytes(record.idempotency_key.as_bytes(), io)?;
        if record.idempotency_key.is_empty() {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution journal sequence or retry identity is invalid",
            ));
        }
        #[cfg(not(target_arch = "wasm32"))]
        let recheck = io
            .and_then(PreparationIo::original_native_work)
            .map(|work| PreparationIo::from_native_work(&work.recheck_admitted()))
            .transpose()?;
        #[cfg(not(target_arch = "wasm32"))]
        let digest_io = recheck.as_ref().or(io);
        #[cfg(target_arch = "wasm32")]
        let digest_io = io;
        let digest = crate::executor::with_history_io(digest_io, |work| {
            crate::executor::json::canonical_json_digest(&record.event, limits.file_bytes, work)
        })?;
        self.accept_verified(
            (record.sequence, key, digest),
            &record.event,
            model,
            limits,
            io,
        )
    }

    /// Applies an authenticated observation without encoding or loading its payload again.
    pub(super) fn accept_verified(
        &mut self,
        (sequence, key, digest): (u64, [u8; 32], [u8; 32]),
        event: &ExecutionEvent,
        model: Option<&ModelEvent>,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        if self.tail.checked_add(1) != Some(sequence) || self.retries.contains_key(&key, io)? {
            return Err(canonical_boundary(
                io,
                crate::wire::ErrorCode::Conflict,
                "execution journal sequence or retry identity is invalid",
            ));
        }
        let tool_key = self.require_next(event, io)?;
        match event {
            ExecutionEvent::ContextPrepared { step, .. } => {
                self.contexts.insert(*step, false, io)?;
            }
            ExecutionEvent::ContextCompacted { step, .. } => {
                self.contexts.insert(*step, true, io)?;
            }
            ExecutionEvent::ModelStarted { step, purpose, .. } => {
                self.pending_dispatches =
                    self.pending_dispatches.checked_add(1).ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Invalid,
                            "pending dispatch count overflows",
                        )
                    })?;
                self.models
                    .insert((*step, *purpose), Some(ModelEventAdmission::default()), io)?;
                self.model_dispatched = true;
                if *purpose == crate::executor::ModelPurpose::Response {
                    self.latest_response = Some(
                        self.latest_response
                            .map_or(*step, |previous| previous.max(*step)),
                    );
                }
            }
            ExecutionEvent::Model { step, purpose, .. }
            | ExecutionEvent::ModelInline { step, purpose, .. } => {
                let state = self
                    .models
                    .get_mut(&(*step, *purpose), io)?
                    .ok_or_else(|| {
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
                    release_model(state, io)?;
                    self.pending_dispatches =
                        self.pending_dispatches.checked_sub(1).ok_or_else(|| {
                            canonical_boundary(
                                io,
                                crate::wire::ErrorCode::Storage,
                                "pending dispatch count underflows",
                            )
                        })?;
                }
            }
            ExecutionEvent::ModelStopped { step, purpose } => {
                let state = self
                    .models
                    .get_mut(&(*step, *purpose), io)?
                    .ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "stopped model has no dispatch",
                        )
                    })?;
                release_model(state, io)?;
                self.pending_dispatches =
                    self.pending_dispatches.checked_sub(1).ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "pending dispatch count underflows",
                        )
                    })?;
            }
            ExecutionEvent::ToolStarted { step, .. }
            | ExecutionEvent::ToolCompleted { step, .. }
            | ExecutionEvent::ToolFailed { step, .. } => {
                let key = (
                    *step,
                    tool_key.ok_or_else(|| {
                        canonical_boundary(
                            io,
                            crate::wire::ErrorCode::Storage,
                            "tool observation has no authenticated identity",
                        )
                    })?,
                );
                match event {
                    ExecutionEvent::ToolStarted { .. } => {
                        self.pending_dispatches =
                            self.pending_dispatches.checked_add(1).ok_or_else(|| {
                                canonical_boundary(
                                    io,
                                    crate::wire::ErrorCode::Invalid,
                                    "pending dispatch count overflows",
                                )
                            })?;
                    }
                    ExecutionEvent::ToolFailed { reason, .. }
                        if reason.precedes_dispatch()
                            || (*reason == crate::executor::ToolFailureKind::ExecutorRejected
                                && self.tools.get(&key, io)?.is_none()) => {}
                    _ => {
                        self.pending_dispatches =
                            self.pending_dispatches.checked_sub(1).ok_or_else(|| {
                                canonical_boundary(
                                    io,
                                    crate::wire::ErrorCode::Storage,
                                    "pending dispatch count underflows",
                                )
                            })?;
                    }
                }
                self.tools.insert(
                    key,
                    !matches!(event, ExecutionEvent::ToolStarted { .. }),
                    io,
                )?;
            }
            ExecutionEvent::Started { .. } => {}
        }
        self.retries.insert(key, (sequence, digest), io)?;
        self.pending_steps.push(
            sequence,
            key,
            digest,
            event.step(),
            tool_key.map(|key| (key, !matches!(event, ExecutionEvent::ToolStarted { .. }))),
            io,
        )?;
        self.tail = sequence;
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn quiescent(&self, io: Option<&PreparationIo>) -> Result<bool> {
        crate::model::charge_items(io, 1)?;
        Ok(self.pending_dispatches == 0)
    }
}

fn release_model(
    state: &mut Option<ModelEventAdmission>,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let bytes = if let Some(admission) = state {
        let inline = admission
            .calls
            .capacity()
            .checked_mul(std::mem::size_of::<String>())
            .ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model admission ownership overflows",
                )
            })? as u64;
        admission.calls.iter().try_fold(inline, |bytes, call| {
            bytes.checked_add(call.capacity() as u64).ok_or_else(|| {
                canonical_boundary(
                    io,
                    crate::wire::ErrorCode::Invalid,
                    "model admission ownership overflows",
                )
            })
        })?
    } else {
        0
    };
    *state = None;
    if let Some(io) = io {
        io.release(bytes)?;
    }
    Ok(())
}

// Version 3 settles approval refusals through the ordinary tool-failure kind.
pub(super) const OBSERVATION_VERSION: u32 = 3;

#[derive(Serialize, Deserialize, crate::HeapLayout)]
#[serde(deny_unknown_fields)]
pub(super) struct Observation {
    pub(super) version: u32,
    pub(super) operation_id: OperationId,
    pub(super) turn_operation: OperationId,
    pub(super) retry_digest: String,
    pub(super) event: ExecutionEvent,
}

impl PreparationRecord for Observation {
    fn decoded_owned_bytes(&self) -> Result<Option<u64>> {
        // The conservative admission also funds serde's transient buffers.
        // Release those after each decode, before retaining the next record.
        crate::core::owned_sum([
            std::mem::size_of::<Self>() as u64,
            self.retry_digest.capacity() as u64,
            self.event.payload_bytes(None)?,
        ])
        .map(Some)
    }

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
                "model_stopped",
                "model",
                "model_inline",
                "tool_started",
                "tool_completed",
                "tool_failed",
                "request_digest",
                "step",
                "projection",
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
                "unavailable",
                "invalid_input",
                "interrupted",
            ];
            let mut bytes = 0;
            let mut remaining = labels.as_slice();
            while let Some((label, rest)) = remaining.split_first() {
                bytes += label.len() as u64 + 4;
                remaining = rest;
            }
            bytes
        };
        crate::executor::json::preparation_serde_diagnostic_label_bytes() + PRIVATE_LABEL_BYTES
    }
}
