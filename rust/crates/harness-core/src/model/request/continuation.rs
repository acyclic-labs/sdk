//! The admitted prefix is immutable. Callers can change only the new suffix;
//! its tool exchanges and instruction budget are checked before publication.
use super::*;

pub(crate) struct RequestContinuation {
    prepared: PreparedModelRequest,
    suffix: Vec<ModelMessage>,
}

impl PreparedModelRequest {
    pub(crate) fn into_continuation(self) -> RequestContinuation {
        RequestContinuation {
            prepared: self,
            suffix: Vec::new(),
        }
    }
}

impl RequestContinuation {
    pub(crate) fn messages(&mut self) -> &mut Vec<ModelMessage> {
        &mut self.suffix
    }

    pub(crate) fn tools(&self) -> &[crate::tool::ToolDefinition] {
        &self.prepared.request.tools
    }

    fn validate_suffix(
        &mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<u64>> {
        let count = self
            .prepared
            .request
            .messages
            .len()
            .checked_add(self.suffix.len())
            .ok_or_else(|| Error::Invalid("model context count overflows".into()))?;
        if count == 0 || count > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if limits == self.prepared.admitted_limits {
            with_history_io(io, |work| limits.validate(work))?;
            let mut remaining = self.prepared.instruction_remaining;
            reserve_instruction_bodies(&self.suffix, &mut remaining, io)?;
            ModelRequest::validate_messages(
                &mut self.suffix,
                &self.prepared.request.tools,
                limits,
                io,
                // Only the suffix is mutable; these declarations belong to the
                // original request admitted under the same limits.
                |_| Ok(()),
            )?;
            Ok(Some(remaining))
        } else {
            Ok(None)
        }
    }

    fn validate(
        mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<(ModelRequest, u64)> {
        let remaining = self.validate_suffix(limits, io)?;
        let count = self.prepared.request.messages.len() + self.suffix.len();
        if count > self.prepared.request.messages.capacity() {
            let capacity = self
                .prepared
                .request
                .messages
                .capacity()
                .saturating_mul(2)
                .max(count)
                .min(limits.context_messages);
            crate::executor::stock_vec_reserve(&mut self.prepared.request.messages, capacity, io)?;
        }
        for message in crate::executor::stock_into_iter(self.suffix, io)? {
            crate::executor::stock_vec_push(&mut self.prepared.request.messages, message, io)?;
        }
        let remaining = match remaining {
            Some(remaining) => remaining,
            None => self
                .prepared
                .request
                .validate_with_instruction_budget(limits, io)?,
        };
        Ok((self.prepared.request, remaining))
    }

    pub(crate) fn finish(
        mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<PreparedModelRequest> {
        PreparedModelRequest::record_preparation();
        // The first continuation establishes the hash cut used by transcript staging.
        if self.suffix.is_empty()
            && self.prepared.admitted_limits == limits
            && !self.prepared.encoding.is_empty()
        {
            with_history_io(io, |work| limits.validate(work))?;
            return Ok(self.prepared);
        }
        let old_count = self.prepared.request.messages.len();
        let mut projection_nodes = self.prepared.projection_nodes.load(Ordering::Relaxed);
        #[cfg(feature = "filesystem")]
        let old_inventory = (
            self.prepared.resident_payload,
            self.prepared.request.messages.capacity(),
        );
        let same_limits = self.prepared.admitted_limits == limits;
        let bytes = std::mem::take(&mut self.prepared.bytes);
        let encoding = std::mem::take(&mut self.prepared.encoding);
        let content_admission = self.prepared.content_admission.take();
        let (request, remaining) = self.validate(limits, io)?;
        if !same_limits {
            return PreparedModelRequest::encode_validated(request, limits, remaining, io);
        }
        #[cfg(feature = "filesystem")]
        let resident_payload = Some(match old_inventory {
            (Some(bytes), capacity) => {
                let added_slots = request
                    .messages
                    .capacity()
                    .checked_sub(capacity)
                    .ok_or_else(super::super::inventory::overflow)?;
                let bytes = add(bytes, slots(added_slots, size_of::<ModelMessage>())?)?;
                request
                    .messages
                    .get(old_count..)
                    .ok_or_else(super::super::inventory::overflow)?
                    .iter()
                    .try_fold(bytes, |bytes, message| {
                        add(bytes, message.content.retained_payload_bytes()?)
                    })?
            }
            (None, _) => request.retained_payload_bytes()?,
        });
        let (bytes, request_digest, encoding) = encoding::EncodingPrefix::append(
            bytes,
            encoding,
            old_count,
            &request,
            io,
            &mut projection_nodes,
        )?;
        Ok(PreparedModelRequest {
            request,
            bytes,
            request_digest,
            projection_nodes: AtomicUsize::new(projection_nodes),
            admitted_limits: limits,
            instruction_remaining: remaining,
            encoding,
            content_admission,
            #[cfg(feature = "filesystem")]
            resident_payload,
        })
    }
}

#[cfg(test)]
mod tests;
