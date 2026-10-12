//! Shared domain/wire conversion for every Stream transport and persistence adapter.

use bytes::Bytes;

#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};

#[cfg(not(target_arch = "wasm32"))]
pub(crate) type WireMeter<'a> = Option<&'a Arc<dyn NativeReadMeter>>;
#[cfg(target_arch = "wasm32")]
pub(crate) type WireMeter<'a> = Option<&'a ()>;

use crate::memory::MemoryOutput;

impl<'a> MemoryOutput<WireMeter<'a>> {
    pub(crate) fn fund(meter: WireMeter<'a>) -> Self {
        Self {
            value: meter,
            #[cfg(not(target_arch = "wasm32"))]
            allocation: meter.map(ReadAllocation::empty),
            #[cfg(target_arch = "wasm32")]
            allocation: (),
        }
    }

    pub(crate) fn records(&mut self, values: &[crate::OwnedBytes]) -> std::io::Result<Vec<crate::OwnedBytes>> {
        let bytes = values.len().checked_mul(std::mem::size_of::<crate::OwnedBytes>())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        self.copy_admission(bytes)?;
        let mut owned = self.vector(values.len())?;
        owned.extend(values.iter().cloned());
        self.copy_completed(bytes)?;
        Ok(owned)
    }

    pub(crate) fn vector<T>(&mut self, length: usize) -> std::io::Result<Vec<T>> {
        let bytes = length.checked_mul(std::mem::size_of::<T>())
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let before = self.capacity();
        self.allocate(bytes)?;
        let mut owned = Vec::new();
        self.reserve_vector(&mut owned, length)?;
        self.reconcile(before, owned.capacity() * std::mem::size_of::<T>())?;
        Ok(owned)
    }

    pub(crate) fn ensure_vector<T>(&mut self, values: &mut Vec<T>, additional: usize) -> std::io::Result<()> {
        if additional <= values.capacity() - values.len() { return Ok(()); }
        let required = values.len().checked_add(additional)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let capacity = required.max(values.capacity().saturating_mul(2)).max(4);
        let item = std::mem::size_of::<T>();
        let old = values.capacity().checked_mul(item)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let planned = capacity.checked_mul(item)
            .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
        let before = if self.value.is_some() {
            self.capacity().checked_sub(old)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?
        } else { 0 };
        self.allocate(planned - old)?;
        self.reserve_vector(values, capacity - values.len())?;
        self.reconcile(before, values.capacity() * item)
    }

    fn reserve_vector<T>(&self, values: &mut Vec<T>, additional: usize) -> std::io::Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            return values.try_reserve_exact(additional)
                .map_err(|error| meter.allocation_failed(error));
        }
        values.reserve_exact(additional);
        Ok(())
    }

    pub(crate) fn retire(&mut self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = &mut self.allocation { allocation.release(bytes)?; }
        Ok(())
    }

    pub(crate) fn examine(&self, items: u64) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = items;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.examine(items, 0)?;
        }
        Ok(())
    }

    pub(crate) fn capacity(&self) -> usize {
        #[cfg(not(target_arch = "wasm32"))]
        { self.allocation.as_ref().map_or(0, |allocation| allocation.bytes() as usize) }
        #[cfg(target_arch = "wasm32")]
        { 0 }
    }

    pub(crate) fn allocate(&mut self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if bytes != 0
            && let Some(allocation) = &mut self.allocation
        {
            allocation.grow(bytes)?;
        }
        Ok(())
    }

    pub(crate) fn reconcile(&mut self, previous: usize, actual: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = (previous, actual);
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(allocation) = &mut self.allocation {
            let total = previous.checked_add(actual)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))?;
            allocation.reconcile_capacity(total)?;
        }
        Ok(())
    }

    pub(crate) fn copy_admission(&self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.admit_codec(0, bytes as u64, 0)?;
        }
        Ok(())
    }

    pub(crate) fn copy_completed(&self, bytes: usize) -> std::io::Result<()> {
        #[cfg(target_arch = "wasm32")]
        let _ = bytes;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(meter) = self.value {
            meter.record_codec(0, bytes as u64, 0)?;
            meter.check()?;
        }
        Ok(())
    }

}

use crate::{
    AppendOutcome, AppendReceipt, CommitCondition, CommitConflict, CommitId, CommitMutation,
    CommitOutcome, CommittedAppend, CommittedEnvelope, CommittedFork, CommittedMutation,
    ForkReceipt, IdempotencyKey, IdempotencyObservation, IdempotencyOutcome, Record, StreamError,
    StreamPath, wire,
};

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

pub(crate) fn path(value: String) -> Result<StreamPath, StreamError> {
    StreamPath::new(value)
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) fn optional_key(value: Option<Bytes>) -> Result<Option<IdempotencyKey>, StreamError> {
    value.map(IdempotencyKey::new).transpose()
}

pub(crate) fn condition_wire(value: CommitCondition) -> wire::CommitCondition {
    let condition = match value {
        CommitCondition::Tail { path, expected } => {
            wire::commit_condition::Condition::Tail(wire::TailCondition {
                path: path.to_string(),
                expected,
            })
        }
        CommitCondition::Absent { path } => {
            wire::commit_condition::Condition::Absent(wire::AbsentCondition {
                path: path.to_string(),
            })
        }
    };
    wire::CommitCondition {
        condition: Some(condition),
    }
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) fn condition_from_wire(
    value: wire::CommitCondition,
) -> Result<CommitCondition, StreamError> {
    match value.condition.ok_or(StreamError::InvalidArgument)? {
        wire::commit_condition::Condition::Tail(value) => Ok(CommitCondition::Tail {
            path: path(value.path)?,
            expected: value.expected,
        }),
        wire::commit_condition::Condition::Absent(value) => Ok(CommitCondition::Absent {
            path: path(value.path)?,
        }),
    }
}

pub(crate) fn mutation_wire(value: CommitMutation) -> wire::CommitMutation {
    let mutation = match value {
        CommitMutation::Append { path, records } => {
            wire::commit_mutation::Mutation::Append(wire::AppendMutation {
                path: path.to_string(),
                records: records.into_iter().map(Bytes::from_owner).collect(),
            })
        }
        CommitMutation::Fork {
            source,
            destination,
            at_tail,
            records,
        } => wire::commit_mutation::Mutation::Fork(wire::ForkMutation {
            source: source.to_string(),
            destination: destination.to_string(),
            at_tail,
            records: records.into_iter().map(Bytes::from_owner).collect(),
        }),
    };
    wire::CommitMutation {
        mutation: Some(mutation),
    }
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
pub(crate) fn mutation_from_wire(
    value: wire::CommitMutation,
) -> Result<CommitMutation, StreamError> {
    match value.mutation.ok_or(StreamError::InvalidArgument)? {
        wire::commit_mutation::Mutation::Append(value) => Ok(CommitMutation::Append {
            path: path(value.path)?,
            records: value.records.into_iter().map(crate::OwnedBytes::from).collect(),
        }),
        wire::commit_mutation::Mutation::Fork(value) => Ok(CommitMutation::Fork {
            source: path(value.source)?,
            destination: path(value.destination)?,
            at_tail: value.at_tail,
            records: value.records.into_iter().map(crate::OwnedBytes::from).collect(),
        }),
    }
}

pub(crate) fn observation_from_wire(
    value: wire::IdempotencyObservation,
) -> Result<IdempotencyObservation, StreamError> {
    let request_digest = <[u8; 32]>::try_from(value.request_digest.as_ref())
        .map_err(|_| StreamError::Unavailable)?;
    let outcome = match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::idempotency_observation::Outcome::Append(value) => {
            IdempotencyOutcome::Append(append_outcome_from_wire(value)?)
        }
        wire::idempotency_observation::Outcome::Fork(value) => {
            IdempotencyOutcome::Fork(ForkReceipt {
                source: path(value.source)?,
                destination: path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                commit_id: commit_id(&value.commit_id)?,
            })
        }
        wire::idempotency_observation::Outcome::Commit(value) => {
            IdempotencyOutcome::Commit(commit_outcome_from_wire(value)?)
        }
    };
    Ok(IdempotencyObservation {
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
        request_digest,
        outcome,
    })
}

/// Converts an idempotency observation into its canonical wire representation.
pub fn observation_wire(value: IdempotencyObservation) -> wire::IdempotencyObservation {
    let outcome = match value.outcome {
        IdempotencyOutcome::Append(value) => {
            wire::idempotency_observation::Outcome::Append(append_outcome_to_wire(value))
        }
        IdempotencyOutcome::Fork(value) => {
            wire::idempotency_observation::Outcome::Fork(fork_receipt_to_wire(&value))
        }
        IdempotencyOutcome::Commit(value) => {
            wire::idempotency_observation::Outcome::Commit(commit_outcome_to_wire(value))
        }
    };
    wire::IdempotencyObservation {
        idempotency_key: Bytes::from_owner(value.idempotency_key.0.clone()),
        request_digest: Bytes::copy_from_slice(&value.request_digest),
        outcome: Some(outcome),
    }
}

pub(crate) fn append_outcome_from_wire(
    value: wire::AppendResponse,
) -> Result<AppendOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::append_response::Outcome::Committed(receipt) => {
            Ok(AppendOutcome::Committed(append_receipt(&receipt)?))
        }
        wire::append_response::Outcome::Conflict(conflict) => Ok(AppendOutcome::TailConflict {
            actual_tail: conflict.actual_tail,
        }),
    }
}

/// Converts the domain value to its wire representation.
pub fn append_outcome_to_wire(value: AppendOutcome) -> wire::AppendResponse {
    let outcome = match value {
        AppendOutcome::Committed(receipt) => {
            wire::append_response::Outcome::Committed(append_receipt_wire(&receipt))
        }
        AppendOutcome::TailConflict { actual_tail } => {
            wire::append_response::Outcome::Conflict(wire::TailConflict { actual_tail })
        }
    };
    wire::AppendResponse {
        outcome: Some(outcome),
    }
}

pub(crate) fn commit_outcome_from_wire(
    value: wire::CommitResponse,
) -> Result<CommitOutcome, StreamError> {
    match value.outcome.ok_or(StreamError::Unavailable)? {
        wire::commit_response::Outcome::Committed(envelope) => {
            Ok(CommitOutcome::Committed(envelope_from_wire(envelope)?))
        }
        wire::commit_response::Outcome::Conflict(conflicts) => Ok(CommitOutcome::Conflict(
            conflicts
                .conflicts
                .into_iter()
                .map(conflict_from_wire)
                .collect::<Result<_, _>>()?,
        )),
    }
}

/// Converts the domain value to its wire representation.
pub fn commit_outcome_to_wire(value: CommitOutcome) -> wire::CommitResponse {
    let outcome = match value {
        CommitOutcome::Committed(envelope) => {
            wire::commit_response::Outcome::Committed(envelope_to_wire(envelope))
        }
        CommitOutcome::Conflict(conflicts) => {
            wire::commit_response::Outcome::Conflict(wire::CommitConflicts {
                conflicts: conflicts.into_iter().map(conflict_wire).collect(),
            })
        }
    };
    wire::CommitResponse {
        outcome: Some(outcome),
    }
}

/// Converts a wire commit-id byte sequence into the fixed-width domain id.
pub fn commit_id(value: &[u8]) -> Result<CommitId, StreamError> {
    let bytes = <[u8; 32]>::try_from(value).map_err(|_| StreamError::Unavailable)?;
    Ok(CommitId::from_bytes(bytes))
}

pub(crate) fn record(value: wire::Record) -> Result<Record, StreamError> {
    Ok(Record {
        sequence: value.sequence,
        value: value.value.into(),
        commit_id: commit_id(&value.commit_id)?,
        committed_at_micros: value.committed_at_micros,
    })
}

pub(crate) fn append_receipt(value: &wire::AppendReceipt) -> Result<AppendReceipt, StreamError> {
    Ok(AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: commit_id(&value.commit_id)?,
    })
}

/// Converts a domain record into its canonical wire representation.
pub fn record_wire(value: Record) -> wire::Record {
    wire::Record {
        sequence: value.sequence,
        value: Bytes::from_owner(value.value),
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        committed_at_micros: value.committed_at_micros,
    }
}

pub(crate) fn append_receipt_wire(value: &AppendReceipt) -> wire::AppendReceipt {
    wire::AppendReceipt {
        start: value.start,
        end: value.end,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

/// Converts the domain value to its wire representation.
pub fn fork_receipt_to_wire(value: &ForkReceipt) -> wire::ForkReceipt {
    wire::ForkReceipt {
        source: value.source.to_string(),
        destination: value.destination.to_string(),
        forked_at: value.forked_at,
        tail: value.tail,
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
    }
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes an append request from its generated wire representation.
pub fn append_from_wire(value: wire::AppendRequest) -> Result<crate::AppendRequest, StreamError> {
    Ok(crate::AppendRequest {
        path: path(value.path)?,
        records: value.records.into_iter().map(crate::OwnedBytes::from).collect(),
        if_tail: value.if_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes a fork request from its generated wire representation.
pub fn fork_from_wire(value: wire::ForkRequest) -> Result<crate::ForkRequest, StreamError> {
    Ok(crate::ForkRequest {
        source: path(value.source)?,
        destination: path(value.destination)?,
        at_tail: value.at_tail,
        idempotency_key: optional_key(value.idempotency_key)?,
    })
}

#[cfg(any(
    feature = "grpc",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes a read request from its generated wire representation.
pub fn read_from_wire(value: wire::ReadRequest) -> Result<crate::ReadRequest, StreamError> {
    Ok(crate::ReadRequest {
        path: path(value.path)?,
        from: value.from,
        limit: value.limit,
    })
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn follow_from_wire(
    value: wire::FollowRequest,
) -> Result<(StreamPath, u64), StreamError> {
    Ok((path(value.path)?, value.from))
}

#[cfg(any(
    feature = "grpc",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes a children request from its generated wire representation.
pub fn children_from_wire(
    value: wire::ChildrenRequest,
) -> Result<crate::ChildrenRequest, StreamError> {
    Ok(crate::ChildrenRequest {
        parent: value.parent.map(path).transpose()?,
        limit: value.limit,
    })
}

#[cfg(any(
    feature = "grpc",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes a paginated children request from its generated wire representation.
pub fn children_page_from_wire(
    value: wire::ChildrenPageRequest,
) -> Result<crate::ChildrenPageRequest, StreamError> {
    Ok(crate::ChildrenPageRequest {
        parent: value.parent.map(path).transpose()?,
        after: value.after.map(path).transpose()?,
        hierarchy_version: value
            .hierarchy_version
            .map(|bytes| {
                let array: [u8; 32] = bytes
                    .as_ref()
                    .try_into()
                    .map_err(|_| StreamError::InvalidArgument)?;
                Ok(crate::CommitId::from_bytes(array))
            })
            .transpose()?,
        limit: value.limit,
    })
}

#[cfg(any(
    all(feature = "grpc", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Converts a domain children page into its generated wire representation.
pub fn children_page_to_wire(value: crate::ChildrenPage) -> wire::ChildrenPageResponse {
    wire::ChildrenPageResponse {
        hierarchy_version: Bytes::copy_from_slice(value.hierarchy_version.as_bytes()),
        children: value
            .children
            .into_iter()
            .map(|child| wire::Child {
                path: child.path.to_string(),
            })
            .collect(),
        next_after: value.next_after.map(|path| path.to_string()),
    }
}

#[cfg(any(
    feature = "grpc",
    feature = "local",
    all(feature = "http", not(target_arch = "wasm32")),
    all(feature = "wasm", target_arch = "wasm32")
))]
/// Decodes a commit request from its generated wire representation.
pub fn commit_from_wire(value: wire::CommitRequest) -> Result<crate::CommitRequest, StreamError> {
    Ok(crate::CommitRequest {
        conditions: value
            .conditions
            .into_iter()
            .map(condition_from_wire)
            .collect::<Result<_, _>>()?,
        mutations: value
            .mutations
            .into_iter()
            .map(mutation_from_wire)
            .collect::<Result<_, _>>()?,
        idempotency_key: IdempotencyKey::new(value.idempotency_key)?,
    })
}

pub(crate) fn commit_to_wire(value: &crate::CommitRequest) -> wire::CommitRequest {
    wire::CommitRequest {
        conditions: value
            .conditions
            .iter()
            .cloned()
            .map(condition_wire)
            .collect(),
        mutations: value.mutations.iter().cloned().map(mutation_wire).collect(),
        idempotency_key: Bytes::from_owner(value.idempotency_key.0.clone()),
        deadline_unix_millis: None,
    }
}


pub(crate) fn envelope_from_wire(
    value: wire::CommittedEnvelope,
) -> Result<CommittedEnvelope, StreamError> {
    Ok(CommittedEnvelope {
        commit_id: commit_id(&value.commit_id)?,
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation)
            .collect::<Result<_, _>>()?,
    })
}

/// Converts the domain value to its wire representation.
pub fn envelope_to_wire(value: CommittedEnvelope) -> wire::CommittedEnvelope {
    wire::CommittedEnvelope {
        commit_id: Bytes::copy_from_slice(value.commit_id.as_bytes()),
        mutations: value
            .mutations
            .into_iter()
            .map(committed_mutation_wire)
            .collect(),
    }
}



pub(crate) fn committed_mutation(
    value: wire::CommittedMutation,
) -> Result<CommittedMutation, StreamError> {
    match value.mutation.ok_or(StreamError::Unavailable)? {
        wire::committed_mutation::Mutation::Append(value) => {
            Ok(CommittedMutation::Append(CommittedAppend {
                path: path(value.path)?,
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value
                    .records
                    .into_iter()
                    .map(record)
                    .collect::<Result<_, _>>()?,
            }))
        }
        wire::committed_mutation::Mutation::Fork(value) => {
            Ok(CommittedMutation::Fork(CommittedFork {
                source: path(value.source)?,
                destination: path(value.destination)?,
                forked_at: value.forked_at,
                tail: value.tail,
                records: value
                    .records
                    .into_iter()
                    .map(record)
                    .collect::<Result<_, _>>()?,
            }))
        }
    }
}

pub(crate) fn committed_mutation_wire(value: CommittedMutation) -> wire::CommittedMutation {
    let mutation = match value {
        CommittedMutation::Append(value) => {
            wire::committed_mutation::Mutation::Append(wire::CommittedAppend {
                path: value.path.to_string(),
                start: value.start,
                end: value.end,
                tail: value.tail,
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
        CommittedMutation::Fork(value) => {
            wire::committed_mutation::Mutation::Fork(wire::CommittedFork {
                source: value.source.to_string(),
                destination: value.destination.to_string(),
                forked_at: value.forked_at,
                tail: value.tail,
                records: value.records.into_iter().map(record_wire).collect(),
            })
        }
    };
    wire::CommittedMutation {
        mutation: Some(mutation),
    }
}

pub(crate) fn conflict_from_wire(
    value: wire::CommitConflict,
) -> Result<CommitConflict, StreamError> {
    match value.conflict.ok_or(StreamError::Unavailable)? {
        wire::commit_conflict::Conflict::Tail(value) => Ok(CommitConflict::Tail {
            path: path(value.path)?,
            expected: value.expected,
            actual: value.actual,
        }),
        wire::commit_conflict::Conflict::Exists(value) => Ok(CommitConflict::Exists {
            path: path(value.path)?,
        }),
    }
}

pub(crate) fn conflict_wire(value: CommitConflict) -> wire::CommitConflict {
    let conflict = match value {
        CommitConflict::Tail {
            path,
            expected,
            actual,
        } => wire::commit_conflict::Conflict::Tail(wire::TailCommitConflict {
            path: path.to_string(),
            expected,
            actual,
        }),
        CommitConflict::Exists { path } => {
            wire::commit_conflict::Conflict::Exists(wire::ExistsCommitConflict {
                path: path.to_string(),
            })
        }
    };
    wire::CommitConflict {
        conflict: Some(conflict),
    }
}

#[cfg(all(feature = "wasm", target_arch = "wasm32"))]
pub(crate) fn observation_to_wire(
    value: crate::IdempotencyObservation,
) -> wire::InspectIdempotencyResponse {
    wire::InspectIdempotencyResponse {
        observation: Some(observation_wire(value)),
    }
}

/// Builds an uncompressed Read or Follow frame carrying consecutive records.
#[must_use]
pub fn read_response_wire(records: Vec<wire::Record>) -> wire::ReadResponse {
    use prost::Message;
    let data = wire::RecordBatch { records }.encode_to_vec();
    wire::ReadResponse {
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data: data.into(),
        delivery_token: None,
    }
}

/// Decodes one Read or Follow frame to its encoded `RecordBatch`.
pub fn read_response_batch(value: wire::ReadResponse) -> Result<Bytes, StreamError> {
    if value.delivery_token.as_ref().is_some_and(|token| token.len() != 32) {
        return Err(StreamError::Unavailable);
    }
    let length = usize::try_from(value.decoded_length)
        .ok()
        .filter(|length| *length <= crate::MAX_COMMAND_BYTES)
        .ok_or(StreamError::Unavailable)?;
    match wire::Codec::try_from(value.codec) {
        Ok(wire::Codec::None) if value.data.len() == length => Ok(value.data),
        Ok(wire::Codec::Zstd) => zstd(&value.data, length)
            .map(Bytes::from)
            .ok_or(StreamError::Unavailable),
        _ => Err(StreamError::Unavailable),
    }
}

/// Decodes one Read or Follow frame to its records, in frame order.
pub fn read_response_records(value: wire::ReadResponse) -> Result<Vec<wire::Record>, StreamError> {
    use prost::Message;
    wire::RecordBatch::decode(read_response_batch(value)?)
        .map(|batch| batch.records)
        .map_err(|_| StreamError::Unavailable)
}

/// Decompresses one or more complete Zstandard frames to exactly `length` bytes.
fn zstd(mut input: &[u8], length: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    if input.is_empty() {
        return None;
    }
    let mut output = Vec::with_capacity(length);
    while !input.is_empty() {
        let allowed = (length - output.len()) as u64 + 1;
        let decoder = ruzstd::decoding::StreamingDecoder::new(&mut input).ok()?;
        decoder.take(allowed).read_to_end(&mut output).ok()?;
        if output.len() > length {
            return None;
        }
    }
    (output.len() == length).then_some(output)
}

#[cfg(test)]
pub(crate) fn zstd_read_response(records: Vec<wire::Record>) -> wire::ReadResponse {
    let plain = read_response_wire(records);
    wire::ReadResponse {
        codec: wire::Codec::Zstd as i32,
        decoded_length: plain.decoded_length,
        data: ruzstd::encoding::compress_to_vec(
            &plain.data[..],
            ruzstd::encoding::CompressionLevel::Fastest,
        )
        .into(),
        delivery_token: None,
    }
}

#[cfg(test)]
mod read_response_tests {
    use super::{read_response_records, read_response_wire, wire, zstd_read_response};

    fn records(from: u64, count: u64) -> Vec<wire::Record> {
        (from..from + count)
            .map(|sequence| wire::Record {
                sequence,
                value: vec![u8::try_from(sequence % 251).unwrap_or(0); 100].into(),
                commit_id: vec![1; 32].into(),
                committed_at_micros: sequence,
            })
            .collect()
    }

    #[test]
    fn local_frames_never_issue_tokens_and_malformed_hosted_tokens_fail_closed() {
        for compressed in [false, true] {
            let frame = if compressed {
                zstd_read_response(records(0, 2))
            } else {
                read_response_wire(records(0, 2))
            };
            assert!(frame.delivery_token.is_none());
            for length in [0, 1, 31, 32, 33, 64] {
                let mut hosted = frame.clone();
                hosted.delivery_token = Some(vec![7; length].into());
                assert_eq!(read_response_records(hosted).is_ok(), length == 32);
            }
        }
    }

    #[test]
    fn compressed_frame_decodes_to_the_same_records() {
        for count in [0, 1, 64] {
            let expected = records(5, count);
            let frame = zstd_read_response(expected.clone());
            assert!(
                count < 64 || frame.data.len() < usize::try_from(frame.decoded_length).unwrap_or(0)
            );
            assert_eq!(read_response_records(frame).ok(), Some(expected.clone()));
            assert_eq!(
                read_response_records(read_response_wire(expected.clone())).ok(),
                Some(expected)
            );
        }
    }

    #[test]
    fn empty_zstd_data_is_refused_and_an_empty_plain_frame_is_accepted() {
        let empty = wire::ReadResponse {
            codec: wire::Codec::Zstd as i32,
            data: Default::default(),
            decoded_length: 0,
            delivery_token: None,
        };
        assert!(read_response_records(empty).is_err());
        assert_eq!(
            read_response_records(read_response_wire(Vec::new())).ok(),
            Some(Vec::new())
        );
    }

    #[test]
    fn mixed_frames_reassemble_in_order() {
        let expected = records(0, 30);
        let frames = expected
            .chunks(7)
            .enumerate()
            .map(|(index, chunk)| {
                if index % 2 == 0 {
                    zstd_read_response(chunk.to_vec())
                } else {
                    read_response_wire(chunk.to_vec())
                }
            })
            .collect::<Vec<_>>();
        let mut result = Vec::new();
        for frame in frames {
            result.extend(read_response_records(frame).expect("valid frame"));
        }
        assert_eq!(result, expected);
    }

    #[test]
    fn oversized_declared_length_is_refused_before_decompressing() {
        let maximum = crate::MAX_COMMAND_BYTES as u64;
        for length in [maximum + 1, u64::MAX] {
            for codec in [wire::Codec::None, wire::Codec::Zstd] {
                let frame = wire::ReadResponse {
                    codec: codec as i32,
                    data: vec![0xff; 16].into(),
                    decoded_length: length,
                    delivery_token: None,
                };
                assert!(read_response_records(frame).is_err());
            }
        }
        let mut expanding = zstd_read_response(records(0, 64));
        expanding.decoded_length = 100;
        assert!(read_response_records(expanding).is_err());
    }

    #[test]
    fn mismatched_length_unknown_codec_and_corrupt_data_are_refused() {
        let mut long = zstd_read_response(records(0, 3));
        long.decoded_length += 1;
        assert!(read_response_records(long).is_err());
        let mut plain = read_response_wire(records(0, 3));
        plain.decoded_length -= 1;
        assert!(read_response_records(plain).is_err());
        for codec in [0, 3] {
            let mut unknown = read_response_wire(records(0, 3));
            unknown.codec = codec;
            assert!(read_response_records(unknown).is_err());
        }
        let mut corrupt = zstd_read_response(records(0, 30));
        corrupt.data.truncate(corrupt.data.len() / 2);
        assert!(read_response_records(corrupt).is_err());
    }
}
