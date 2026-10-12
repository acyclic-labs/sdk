use super::*;

/// The closed SDK persistence graph has one borrowed encoder for ordinary and native calls.
pub(crate) enum CanonicalWire<'a> {
    #[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
    Journal(&'a crate::journal::Command),
    Append(&'a crate::AppendRequest),
    Fork(&'a crate::ForkRequest),
    Commit(&'a crate::CommitRequest),
    Condition(&'a CommitCondition),
    Mutation(&'a CommitMutation),
    Record(&'a Record),
    Envelope(&'a CommittedEnvelope),
    CommittedMutation(&'a CommittedMutation),
    Observation(&'a crate::OwnedBytes, &'a [u8; 32], &'a IdempotencyOutcome),
    AppendOutcome(&'a AppendOutcome),
    AppendReceipt(&'a AppendReceipt),
    TailConflict(u64),
    ForkReceipt(&'a ForkReceipt),
    CommitOutcome(&'a CommitOutcome),
    Conflicts(&'a [CommitConflict]),
    Conflict(&'a CommitConflict),
}

struct WireSink<'a> {
    output: Option<&'a mut Vec<u8>>,
    length: usize,
    maximum_record: Option<usize>,
    observer: WireMeter<'a>,
}

impl WireSink<'_> {
    fn bytes(&mut self, value: &[u8]) -> std::io::Result<()> {
        self.length = self.length.checked_add(value.len())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        if let Some(output) = &mut self.output {
            if value.len() > output.capacity() - output.len() {
                return Err(std::io::ErrorKind::InvalidInput.into());
            }
            output.extend_from_slice(value);
        }
        Ok(())
    }

    fn varint(&mut self, mut value: u64) -> std::io::Result<()> {
        let mut encoded = [0_u8; 10];
        let mut length = 0;
        while value >= 128 {
            encoded[length] = (value as u8 & 0x7f) | 0x80;
            length += 1;
            value >>= 7;
        }
        encoded[length] = value as u8;
        self.bytes(&encoded[..=length])
    }

    fn integer(&mut self, tag: u64, value: u64, present: bool) -> std::io::Result<()> {
        if present || value != 0 {
            self.varint(tag << 3)?;
            self.varint(value)?;
        }
        Ok(())
    }

    fn field(&mut self, tag: u64, value: &[u8], present: bool) -> std::io::Result<()> {
        if present || !value.is_empty() {
            self.varint((tag << 3) | 2)?;
            self.varint(value.len() as u64)?;
            self.bytes(value)?;
        }
        Ok(())
    }

    fn record(&mut self, tag: u64, value: &crate::OwnedBytes) -> std::io::Result<()> {
        crate::memory::admit_item(self.observer)?;
        if let Some(length) = self.maximum_record {
            self.varint((tag << 3) | 2)?;
            self.varint(length as u64)?;
            self.length = self.length.checked_add(length)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        } else {
            self.field(tag, value.as_ref(), true)?;
        }
        crate::memory::record_item(self.observer)?;
        Ok(())
    }

    fn nested(&mut self, tag: u64, value: CanonicalWire<'_>) -> std::io::Result<()> {
        self.varint((tag << 3) | 2)?;
        let mut count = WireSink { output: None, length: 0, maximum_record: self.maximum_record, observer: self.observer };
        value.write(&mut count)?;
        self.varint(count.length as u64)?;
        if self.output.is_some() { value.write(self) } else {
            self.length = self.length.checked_add(count.length).ok_or(std::io::ErrorKind::InvalidInput)?;
            Ok(())
        }
    }

    fn message(
        &mut self,
        tag: u64,
        write: impl Fn(&mut WireSink<'_>) -> std::io::Result<()>,
    ) -> std::io::Result<()> {
        let mut count = WireSink { output: None, length: 0, maximum_record: self.maximum_record, observer: self.observer };
        write(&mut count)?;
        self.varint((tag << 3) | 2)?;
        self.varint(count.length as u64)?;
        if self.output.is_some() { write(self) } else {
            self.length = self.length.checked_add(count.length).ok_or(std::io::ErrorKind::InvalidInput)?;
            Ok(())
        }
    }
}

impl CanonicalWire<'_> {
    pub(crate) fn encoded_len(&self, maximum_record: Option<usize>, observer: WireMeter<'_>) -> std::io::Result<usize> {
        let mut sink = WireSink { output: None, length: 0, maximum_record, observer };
        self.write(&mut sink)?;
        Ok(sink.length)
    }

    pub(crate) fn encode(&self, output: &mut Vec<u8>) -> std::io::Result<()> {
        self.write(&mut WireSink { output: Some(output), length: 0, maximum_record: None, observer: None })
    }

    pub(crate) fn encode_to_vec(&self) -> std::io::Result<Vec<u8>> {
        let mut output = Vec::with_capacity(self.encoded_len(None, None)?);
        self.encode(&mut output)?;
        Ok(output)
    }

    fn write(&self, sink: &mut WireSink<'_>) -> std::io::Result<()> {
        match self {
            #[cfg(any(feature = "local", all(feature = "wasm", target_arch = "wasm32")))]
            Self::Journal(command) => match command {
                crate::journal::Command::Append(value) => sink.nested(1, Self::Append(value))?,
                crate::journal::Command::Fork(value) => sink.nested(2, Self::Fork(value))?,
                crate::journal::Command::Commit(value) => sink.nested(5, Self::Commit(value))?,
            },
            Self::Append(value) => {
                sink.field(1, value.path.as_str().as_bytes(), false)?;
                for record in &value.records { sink.record(2, record)?; }
                if let Some(tail) = value.if_tail { sink.integer(3, tail, true)?; }
                if let Some(key) = &value.idempotency_key { sink.field(4, key.as_bytes(), true)?; }
            }
            Self::Fork(value) => {
                sink.field(1, value.source.as_str().as_bytes(), false)?;
                sink.field(2, value.destination.as_str().as_bytes(), false)?;
                if let Some(tail) = value.at_tail { sink.integer(3, tail, true)?; }
                if let Some(key) = &value.idempotency_key { sink.field(4, key.as_bytes(), true)?; }
            }
            Self::Commit(value) => {
                for value in &value.conditions {
                    crate::memory::admit_item(sink.observer)?;
                    sink.nested(1, Self::Condition(value))?;
                    crate::memory::record_item(sink.observer)?;
                }
                for value in &value.mutations {
                    crate::memory::admit_item(sink.observer)?;
                    sink.nested(2, Self::Mutation(value))?;
                    crate::memory::record_item(sink.observer)?;
                }
                sink.field(3, value.idempotency_key.as_bytes(), false)?;
            }
            Self::Condition(value) => match value {
                CommitCondition::Tail { path, expected } => sink.message(1, |sink| {
                    sink.field(1, path.as_str().as_bytes(), false)?;
                    sink.integer(2, *expected, false)
                })?,
                CommitCondition::Absent { path } => sink.message(2, |sink| {
                    sink.field(1, path.as_str().as_bytes(), false)
                })?,
            },
            Self::Mutation(value) => match value {
                CommitMutation::Append { path, records } => sink.message(1, |sink| {
                    sink.field(1, path.as_str().as_bytes(), false)?;
                    for value in records { sink.record(2, value)?; }
                    Ok(())
                })?,
                CommitMutation::Fork { source, destination, at_tail, records } => sink.message(2, |sink| {
                    sink.field(1, source.as_str().as_bytes(), false)?;
                    sink.field(2, destination.as_str().as_bytes(), false)?;
                    sink.integer(3, *at_tail, false)?;
                    for value in records { sink.record(4, value)?; }
                    Ok(())
                })?,
            },
            Self::Record(value) => {
                sink.integer(1, value.sequence, false)?;
                sink.field(2, value.value.as_ref(), false)?;
                sink.field(3, value.commit_id.as_bytes(), false)?;
                sink.integer(4, value.committed_at_micros, false)?;
            }
            Self::Envelope(value) => {
                sink.field(1, value.commit_id.as_bytes(), false)?;
                for value in &value.mutations { sink.nested(2, Self::CommittedMutation(value))?; }
            }
            Self::CommittedMutation(value) => match value {
                CommittedMutation::Append(value) => sink.message(1, |sink| {
                    sink.field(1, value.path.as_str().as_bytes(), false)?;
                    sink.integer(2, value.start, false)?;
                    sink.integer(3, value.end, false)?;
                    sink.integer(4, value.tail, false)?;
                    for value in &value.records { sink.nested(5, Self::Record(value))?; }
                    Ok(())
                })?,
                CommittedMutation::Fork(value) => sink.message(2, |sink| {
                    sink.field(1, value.source.as_str().as_bytes(), false)?;
                    sink.field(2, value.destination.as_str().as_bytes(), false)?;
                    sink.integer(3, value.forked_at, false)?;
                    sink.integer(4, value.tail, false)?;
                    for value in &value.records { sink.nested(5, Self::Record(value))?; }
                    Ok(())
                })?,
            },
            Self::Observation(key, digest, value) => {
                sink.field(1, key.as_ref(), false)?;
                sink.field(2, *digest, false)?;
                match value {
                    IdempotencyOutcome::Append(value) => sink.nested(3, Self::AppendOutcome(value))?,
                    IdempotencyOutcome::Fork(value) => sink.nested(4, Self::ForkReceipt(value))?,
                    IdempotencyOutcome::Commit(value) => sink.nested(7, Self::CommitOutcome(value))?,
                }
            }
            Self::AppendOutcome(value) => match value {
                AppendOutcome::Committed(value) => sink.nested(1, Self::AppendReceipt(value))?,
                AppendOutcome::TailConflict { actual_tail } => sink.nested(2, Self::TailConflict(*actual_tail))?,
            },
            Self::AppendReceipt(value) => {
                sink.integer(1, value.start, false)?;
                sink.integer(2, value.end, false)?;
                sink.integer(3, value.tail, false)?;
                sink.field(4, value.commit_id.as_bytes(), false)?;
            }
            Self::TailConflict(value) => sink.integer(1, *value, false)?,
            Self::ForkReceipt(value) => {
                sink.field(1, value.source.as_str().as_bytes(), false)?;
                sink.field(2, value.destination.as_str().as_bytes(), false)?;
                sink.integer(3, value.forked_at, false)?;
                sink.integer(4, value.tail, false)?;
                sink.field(5, value.commit_id.as_bytes(), false)?;
            }
            Self::CommitOutcome(value) => match value {
                CommitOutcome::Committed(value) => sink.nested(1, Self::Envelope(value))?,
                CommitOutcome::Conflict(value) => sink.nested(2, Self::Conflicts(value))?,
            },
            Self::Conflicts(values) => {
                for value in *values { sink.nested(1, Self::Conflict(value))?; }
            }
            Self::Conflict(value) => match value {
                CommitConflict::Tail { path, expected, actual } => sink.message(1, |sink| {
                    sink.field(1, path.as_str().as_bytes(), false)?;
                    sink.integer(2, *expected, false)?;
                    if let Some(actual) = actual { sink.integer(3, *actual, true)?; }
                    Ok(())
                })?,
                CommitConflict::Exists { path } => sink.message(2, |sink| {
                    sink.field(1, path.as_str().as_bytes(), false)
                })?,
            },
        }
        Ok(())
    }
}
