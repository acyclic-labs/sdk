use super::*;
use crate::executor::json::JsonCodecCharge;
use crate::wire::ErrorCode::Invalid;

impl ModelEventAdmission {
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    #[doc(hidden)]
    pub fn from_state(mut state: Self, limits: Limits) -> Result<Self> {
        if state.count > limits.model_events_per_step
            || state.calls.len() > limits.tool_calls_per_step
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
        with_history_io(io, |work| {
            if let Some(work) = work.as_deref_mut() {
                work.codec_charge(JsonCodecCharge::Inspect { items: 1 })?;
            }
            Ok::<_, Error>(())
        })?;
        if self.count >= limits.model_events_per_step {
            return Err(stock_boundary(io, Invalid, "model event limit exceeded"));
        }
        if self.completed {
            return Err(stock_boundary(
                io,
                Invalid,
                "model emitted an event after completion",
            ));
        }
        if let ModelEvent::ReasoningBlock { block } = event {
            with_history_io(io, |work| {
                block.validate_limits(limits, work)?;
                crate::executor::json::json_byte_length(
                    event,
                    limits.render_bytes.min(limits.file_bytes),
                    work,
                )
                .map(|_| ())
            })?;
        }
        let ModelEvent::ToolCall(call) = event else {
            return Ok(None);
        };
        crate::tool::validate_model_call_recorded(call, limits, io)?;
        let call_id = &call.call_id;
        if self.calls.len() >= limits.tool_calls_per_step {
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
        match event {
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
            ModelEvent::Content { .. }
            | ModelEvent::Reasoning { .. }
            | ModelEvent::ReasoningBlock { .. } => {}
        }
        self.count += 1;
        Ok(())
    }
}

#[cfg(test)]
#[path = "reasoning_admission_tests.rs"]
mod reasoning_tests;

#[cfg(test)]
#[path = "recorded_admission_tests.rs"]
mod recorded_tests;
