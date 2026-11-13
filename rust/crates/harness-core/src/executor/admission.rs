use super::*;
use crate::executor::json::JsonCodecCharge;
use crate::wire::ErrorCode::Invalid;

impl ModelEventAdmission {
    fn validate_cardinality(&self, limits: Limits, io: Option<&PreparationIo>) -> Result<()> {
        with_history_io(io, |work| {
            if let Some(work) = work.as_deref_mut() {
                work.codec_charge(JsonCodecCharge::Inspect { items: 1 })?;
            }
            Ok::<_, Error>(())
        })?;
        if limits
            .model_events_per_step
            .is_some_and(|maximum| self.count >= maximum.get())
        {
            return Err(stock_boundary(io, Invalid, "model event limit exceeded"));
        }
        if self.completed {
            return Err(stock_boundary(
                io,
                Invalid,
                "model emitted an event after completion",
            ));
        }
        Ok(())
    }

    /// A count is an accounting round trip, without a generation dispatch.
    /// Its streamed observations still obey the caller's selected event credit.
    pub(super) fn observe_count(
        &mut self,
        event: &ModelEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        self.validate_cardinality(limits, io)?;
        if !matches!(event, ModelEvent::Wait(_) | ModelEvent::Counted { .. }) {
            return Err(stock_boundary(
                io,
                Invalid,
                "token-count stream emitted generation output",
            ));
        }
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| stock_boundary(io, Invalid, "model event count overflow"))?;
        self.count = count;
        self.completed = matches!(event, ModelEvent::Counted { .. });
        Ok(())
    }

    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn from_state(mut state: Self, limits: Limits) -> Result<Self> {
        if limits
            .file_bytes
            .is_some_and(|maximum| state.text_bytes > maximum)
            || limits
                .model_events_per_step
                .is_some_and(|maximum| state.count > maximum.get())
            || limits
                .tool_calls_per_step
                .is_some_and(|maximum| state.calls.len() > maximum.get())
        {
            return Err(Error::Invalid(
                "model event admission state exceeds its limit".into(),
            ));
        }
        for call in &state.calls {
            ToolInvocation::validate_identity(call, Some("admitted.tool"), &mut None)?;
        }
        let count = state.calls.len();
        state.calls.sort_unstable();
        state.calls.dedup();
        if state.calls.len() != count {
            return Err(Error::Invalid(
                "model tool call admission state contains repeated identities".into(),
            ));
        }
        Ok(state)
    }

    pub(crate) fn validate_next(
        &self,
        event: &ModelEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<usize>> {
        if matches!(event, ModelEvent::Counted { .. }) {
            return Err(stock_boundary(
                io,
                Invalid,
                "token-count result appeared in a generation stream",
            ));
        }
        self.validate_cardinality(limits, io)?;
        if let ModelEvent::ReasoningBlock { block } = event {
            with_history_io(io, |work| {
                block.validate_limits(limits, work)?;
                crate::executor::json::json_byte_length(
                    event,
                    limits
                        .render_bytes
                        .into_iter()
                        .chain(limits.file_bytes)
                        .min(),
                    work,
                )
                .map(|_| ())
            })?;
        }
        if let ModelEvent::TextPart { text, native } = event {
            if limits
                .file_bytes
                .is_some_and(|maximum| text.len() as u64 > maximum)
            {
                return Err(stock_boundary(
                    io,
                    Invalid,
                    "assistant output exceeds file limit",
                ));
            }
            if let Some(native) = native {
                with_history_io(io, |work| native.validate_limits(limits, work))?;
            }
        }
        if let Some(maximum) = limits.file_bytes
            && self.next_text_bytes(event)? > maximum
        {
            return Err(stock_boundary(
                io,
                Invalid,
                "assistant output exceeds file limit",
            ));
        }
        let ModelEvent::ToolCall(call) = event else {
            return Ok(None);
        };
        crate::tool::validate_model_call_recorded(call, limits, io)?;
        if let Some(native) = &call.native {
            with_history_io(io, |work| native.validate_limits(limits, work))?;
        }
        let call_id = &call.call_id;
        if limits
            .tool_calls_per_step
            .is_some_and(|maximum| self.calls.len() >= maximum.get())
        {
            return Err(stock_boundary(
                io,
                Invalid,
                "model tool call limit exceeded or identity repeated",
            ));
        }
        let (position, repeated) = self.call_position(call_id, io)?;
        if repeated {
            return Err(stock_boundary(
                io,
                Invalid,
                "model tool call limit exceeded or identity repeated",
            ));
        }
        Ok(Some(position))
    }

    fn next_text_bytes(&self, event: &ModelEvent) -> Result<u64> {
        match event {
            ModelEvent::Restarted => Ok(0),
            ModelEvent::Content { delta } => self
                .text_bytes
                .checked_add(delta.len() as u64)
                .ok_or_else(|| Error::Invalid("model output size overflow".into())),
            ModelEvent::MessageCompleted { text } => Ok(self.text_bytes.max(text.len() as u64)),
            _ => Ok(self.text_bytes),
        }
    }

    fn call_position(&self, call_id: &str, io: Option<&PreparationIo>) -> Result<(usize, bool)> {
        let inspect = || {
            with_history_io(io, |work| {
                if let Some(work) = work.as_deref_mut() {
                    work.codec_charge(JsonCodecCharge::Inspect { items: 1 })?;
                }
                Ok::<_, Error>(())
            })
        };
        let (mut left, mut right) = (0, self.calls.len());
        while left < right {
            inspect()?;
            let middle = left + (right - left) / 2;
            let stored = self
                .calls
                .get(middle)
                .ok_or_else(|| stock_boundary(io, Invalid, "model call index is invalid"))?;
            let mut ordering = std::cmp::Ordering::Equal;
            for (stored_byte, call_byte) in stored.bytes().zip(call_id.bytes()) {
                inspect()?;
                ordering = stored_byte.cmp(&call_byte);
                if ordering != std::cmp::Ordering::Equal {
                    break;
                }
            }
            if ordering == std::cmp::Ordering::Equal {
                inspect()?;
                ordering = stored.len().cmp(&call_id.len());
            }
            match ordering {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok((middle, true)),
            }
        }
        Ok((left, false))
    }

    #[doc(hidden)]
    pub fn observe(
        &mut self,
        event: &ModelEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let position = self.validate_next(event, limits, io)?;
        let text_bytes = self.next_text_bytes(event)?;
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| stock_boundary(io, Invalid, "model event count overflow"))?;
        let overflow = || stock_boundary(io, Invalid, "model output size overflow");
        let mut reasoning_bytes = self.reasoning_bytes;
        let mut abandoned = self.abandoned;
        match event {
            ModelEvent::Restarted => {
                abandoned = ModelAbandonedOutput {
                    attempts: abandoned.attempts.checked_add(1).ok_or_else(overflow)?,
                    content_bytes: abandoned
                        .content_bytes
                        .checked_add(self.text_bytes)
                        .ok_or_else(overflow)?,
                    reasoning_bytes: abandoned
                        .reasoning_bytes
                        .checked_add(reasoning_bytes)
                        .ok_or_else(overflow)?,
                };
                reasoning_bytes = 0;
            }
            ModelEvent::Reasoning { delta } => {
                reasoning_bytes = reasoning_bytes
                    .checked_add(delta.len() as u64)
                    .ok_or_else(overflow)?;
            }
            _ => {}
        }
        match event {
            ModelEvent::Restarted => {
                for call in self.calls.drain(..) {
                    stock_retire_text(call, io)?;
                }
            }
            ModelEvent::ToolCall(crate::model::ToolCall { call_id, .. }) => {
                let position = position.ok_or_else(|| {
                    stock_boundary(io, Invalid, "model call insertion position is missing")
                })?;
                let bytes =
                    (self.calls.len() - position) as u64 * std::mem::size_of::<String>() as u64;
                with_history_io(io, |work| {
                    if let Some(work) = work.as_deref_mut() {
                        work.admit_copy(bytes)?;
                    }
                    let call = stock_string_copy(call_id, io)?;
                    stock_vec_push(&mut self.calls, call, io)?;
                    self.calls
                        .get_mut(position..)
                        .ok_or_else(|| {
                            stock_boundary(io, Invalid, "model call insertion position is invalid")
                        })?
                        .rotate_right(1);
                    if let Some(work) = work.as_deref_mut() {
                        work.copy_bytes(bytes)?;
                    }
                    Ok::<_, Error>(())
                })?;
            }
            ModelEvent::Completed { .. } => self.completed = true,
            ModelEvent::Wait(_)
            | ModelEvent::Counted { .. }
            | ModelEvent::Content { .. }
            | ModelEvent::MessageCompleted { .. }
            | ModelEvent::TextPart { .. }
            | ModelEvent::Reasoning { .. }
            | ModelEvent::ReasoningBlock { .. } => {}
        }
        self.count = count;
        self.text_bytes = text_bytes;
        self.reasoning_bytes = reasoning_bytes;
        self.abandoned = abandoned;
        Ok(())
    }
}

#[cfg(test)]
#[path = "reasoning_admission_tests.rs"]
mod reasoning_tests;

#[cfg(test)]
#[path = "recorded_admission_tests.rs"]
mod recorded_tests;
