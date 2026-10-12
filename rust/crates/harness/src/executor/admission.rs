use super::*;

impl ModelEventAdmission {
    #[cfg(all(feature = "wasm", target_arch = "wasm32"))]
    pub(crate) fn from_state(state: Self, limits: Limits) -> Result<Self> {
        if state.count > limits.model_events_per_step {
            return Err(Error::Invalid(
                "model event admission state exceeds its limit".into(),
            ));
        }
        let call_count = state.calls.len();
        if call_count > limits.tool_calls_per_step {
            return Err(Error::Invalid(
                "model tool call admission state exceeds its limit".into(),
            ));
        }
        let mut calls = state.calls;
        for call_id in &calls {
            ToolInvocation::validate_identity(call_id, Some("admitted.tool"), &mut None)?;
        }
        calls.sort_unstable();
        calls.dedup();
        if calls.len() != call_count {
            return Err(Error::Invalid(
                "model tool call admission state contains repeated identities".into(),
            ));
        }
        Ok(Self {
            count: state.count,
            calls,
            completed: state.completed,
        })
    }

    pub(crate) fn validate_next(
        &self,
        event: &ModelEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<usize>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.preflight(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        let exhausted = self.count >= limits.model_events_per_step;
        let completed = self.completed;
        #[cfg(feature = "filesystem")]
        if let Some(io) = io {
            io.charge(acyclic_fs::WorkCounters {
                items_examined: 1,
                ..Default::default()
            })?;
        }
        if exhausted {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model event limit exceeded",
            ));
        }
        if completed {
            return Err(stock_boundary(
                io,
                crate::wire::ErrorCode::Invalid,
                "model emitted an event after completion",
            ));
        }
        match event {
            ModelEvent::ToolCall { call_id, name, .. } => {
                crate::executor::with_history_io(io, |work| {
                    ToolInvocation::validate_identity(call_id, Some(name), work)
                })?;
                if self.calls.len() >= limits.tool_calls_per_step {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "model tool call limit exceeded or identity repeated",
                    ));
                }
                let (position, repeated) = self.call_position(call_id, io)?;
                if repeated {
                    return Err(stock_boundary(
                        io,
                        crate::wire::ErrorCode::Invalid,
                        "model tool call limit exceeded or identity repeated",
                    ));
                }
                Ok(Some(position))
            }
            ModelEvent::Completed { .. }
            | ModelEvent::Content { .. }
            | ModelEvent::Reasoning { .. } => Ok(None),
        }
    }

    fn call_position(&self, call_id: &str, io: Option<&PreparationIo>) -> Result<(usize, bool)> {
        let mut left = 0;
        let mut right = self.calls.len();
        while left < right {
            let middle = left + (right - left) / 2;
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.preflight(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let stored = &self.calls[middle];
            #[cfg(feature = "filesystem")]
            if let Some(io) = io {
                io.charge(acyclic_fs::WorkCounters {
                    items_examined: 1,
                    ..Default::default()
                })?;
            }
            let mut ordering = std::cmp::Ordering::Equal;
            for (stored_byte, call_byte) in stored.as_bytes().iter().zip(call_id.as_bytes()) {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.preflight(acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                ordering = stored_byte.cmp(call_byte);
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                if ordering != std::cmp::Ordering::Equal {
                    break;
                }
            }
            if ordering == std::cmp::Ordering::Equal {
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.preflight(acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
                ordering = stored.len().cmp(&call_id.len());
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(acyclic_fs::WorkCounters {
                        items_examined: 1,
                        ..Default::default()
                    })?;
                }
            }
            match ordering {
                std::cmp::Ordering::Less => left = middle + 1,
                std::cmp::Ordering::Greater => right = middle,
                std::cmp::Ordering::Equal => return Ok((middle, true)),
            }
        }
        Ok((left, false))
    }

    pub(crate) fn observe(
        &mut self,
        event: &ModelEvent,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let position = self.validate_next(event, limits, io)?;
        match event {
            ModelEvent::ToolCall { call_id, .. } => {
                let position =
                    position.expect("validated model call identity has an insertion position");
                #[cfg(feature = "filesystem")]
                let copied = acyclic_fs::WorkCounters {
                    bytes_copied: (self.calls.len() - position) as u64
                        * std::mem::size_of::<String>() as u64,
                    ..Default::default()
                };
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.preflight(copied)?;
                }
                let call = stock_string_copy(call_id, io)?;
                stock_vec_push(&mut self.calls, call, io)?;
                self.calls[position..].rotate_right(1);
                #[cfg(feature = "filesystem")]
                if let Some(io) = io {
                    io.charge(copied)?;
                }
            }
            ModelEvent::Completed { .. } => self.completed = true,
            ModelEvent::Content { .. } | ModelEvent::Reasoning { .. } => {}
        }
        self.count += 1;
        Ok(())
    }
}
