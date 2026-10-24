//! The admitted prefix is immutable. Callers can change only the new suffix;
//! its tool exchanges and instruction budget are checked before publication.
use super::*;

pub(crate) struct RequestContinuation {
    request: ModelRequest,
    limits: Limits,
    instruction_remaining: u64,
    suffix: Vec<ModelMessage>,
    bytes: acyclic_stream::OwnedBytes,
    encoding: Vec<encoding::EncodingPrefix>,
    content_admission: Option<admission::ContentAdmission>,
    #[cfg(feature = "filesystem")]
    resident_payload: Option<u64>,
}

impl PreparedModelRequest {
    pub(crate) fn into_continuation(self) -> RequestContinuation {
        let limits = self.admitted_limits;
        let instruction_remaining = self.instruction_remaining;
        RequestContinuation {
            request: self.request,
            limits,
            instruction_remaining,
            suffix: Vec::new(),
            bytes: self.bytes,
            encoding: self.encoding,
            content_admission: self.content_admission,
            #[cfg(feature = "filesystem")]
            resident_payload: self.resident_payload,
        }
    }
}

impl RequestContinuation {
    pub(crate) fn messages(&mut self) -> &mut Vec<ModelMessage> {
        &mut self.suffix
    }

    pub(crate) fn tools(&self) -> &[crate::tool::ToolDefinition] {
        &self.request.tools
    }

    fn validate_suffix(
        &mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<Option<u64>> {
        let count = self
            .request
            .messages
            .len()
            .checked_add(self.suffix.len())
            .ok_or_else(|| Error::Invalid("model context count overflows".into()))?;
        if count == 0 || count > limits.context_messages {
            return Err(Error::Invalid("model context count is invalid".into()));
        }
        if limits == self.limits {
            with_history_io(io, |work| limits.validate(work))?;
            let mut remaining = self.instruction_remaining;
            reserve_instruction_bodies(&self.suffix, &mut remaining, io)?;
            ModelRequest::validate_messages(&mut self.suffix, &self.request.tools, limits, io)?;
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
        let count = self.request.messages.len() + self.suffix.len();
        if count > self.request.messages.capacity() {
            let capacity = self
                .request
                .messages
                .capacity()
                .saturating_mul(2)
                .max(count)
                .min(limits.context_messages);
            crate::executor::stock_vec_reserve(&mut self.request.messages, capacity, io)?;
        }
        for message in crate::executor::stock_into_iter(self.suffix, io)? {
            crate::executor::stock_vec_push(&mut self.request.messages, message, io)?;
        }
        let remaining = match remaining {
            Some(remaining) => remaining,
            None => self.request.validate_with_instruction_budget(limits, io)?,
        };
        Ok((self.request, remaining))
    }

    pub(crate) fn finish(
        mut self,
        limits: Limits,
        io: Option<&PreparationIo>,
    ) -> Result<PreparedModelRequest> {
        PreparedModelRequest::record_preparation();
        let old_count = self.request.messages.len();
        #[cfg(feature = "filesystem")]
        let old_inventory = (self.resident_payload, self.request.messages.capacity());
        let same_limits = self.limits == limits;
        let bytes = std::mem::take(&mut self.bytes);
        let encoding = std::mem::take(&mut self.encoding);
        let content_admission = self.content_admission.take();
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
        let (bytes, request_digest, encoding) =
            encoding::EncodingPrefix::append(bytes, encoding, old_count, &request, io)?;
        Ok(PreparedModelRequest {
            request,
            bytes,
            request_digest,
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
