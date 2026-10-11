//! Canonical authority record envelope for the native Streams v1 service.
//!
//! Streams carries the writer epoch and operation identity in the append
//! request, but its replay records carry only sequence and body. This envelope
//! retains the remaining FS authority fields in that body so a remote adapter
//! can reconstruct the FS hash chain without a private side index.

use crate::foundation::{Digest, DurableCommit, Epoch, OperationId, ProposedCommit, Sequence};
use crate::kernel::ReadAllocationStep;
use bytes::Bytes;
use thiserror::Error;

const DOMAIN: &[u8] = b"acyclic-fs-stream-record-v1\0";
const VERSION: u16 = 1;
const HEADER_BYTES: usize = DOMAIN.len() + 2 + 8 + 16 + 32 + 8;
const DURABLE_DOMAIN: &[u8] = b"acyclic-fs-stream-durable-record-v1\0";
const DURABLE_HEADER_BYTES: usize = DURABLE_DOMAIN.len() + 2 + 8 + 8 + 16 + 32 + 32 + 32 + 8;

/// The minimum encoded record size before its opaque FS payload.
pub const STREAMS_AUTHORITY_RECORD_HEADER_BYTES: u64 = HEADER_BYTES as u64;

/// One FS authority operation encoded as a native Streams record body.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsAuthorityRecord {
    /// Writer fence that admitted the operation.
    pub epoch: Epoch,
    /// Stable retry identity retained by the authority.
    pub operation_id: OperationId,
    /// Fingerprint bound to the operation identity.
    pub fingerprint: Digest,
    /// Canonical opaque FS operation bytes.
    pub payload: Bytes,
}

/// One complete FS authority commit carried as an opaque native Stream record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamsDurableRecord(pub DurableCommit);

/// Validated durable fields borrowing the original provider record.
pub(crate) struct BorrowedStreamsDurableRecord<'a> {
    pub(crate) epoch: Epoch,
    pub(crate) sequence: Sequence,
    pub(crate) operation_id: OperationId,
    pub(crate) fingerprint: Digest,
    pub(crate) previous_digest: Digest,
    pub(crate) digest: Digest,
    pub(crate) payload: &'a [u8],
}

impl StreamsDurableRecord {
    /// Encodes every hash-chain field needed for constant-work head inspection.
    pub fn encode(
        commit: &DurableCommit,
        maximum_record_bytes: u64,
    ) -> Result<Bytes, StreamsAuthorityRecordError> {
        Self::encode_with(commit, maximum_record_bytes, |domain, fields| {
            let capacity = domain.len() + fields.iter().map(|field| field.len()).sum::<usize>();
            let mut encoded = Vec::new();
            encoded.try_reserve_exact(capacity)
                .map_err(|_| StreamsAuthorityRecordError::AllocationFailed)?;
            encoded.extend_from_slice(domain);
            for field in fields { encoded.extend_from_slice(field); }
            Ok(Bytes::from(encoded))
        }).and_then(std::convert::identity)
    }

    /// Uses the canonical field writer with caller-owned allocation admission.
    pub(crate) fn encode_with<E>(
        commit: &DurableCommit,
        maximum_record_bytes: u64,
        write: impl FnOnce(&[u8], &[&[u8]]) -> Result<Bytes, E>,
    ) -> Result<Result<Bytes, E>, StreamsAuthorityRecordError> {
        let borrowed = BorrowedStreamsDurableRecord {
            epoch: commit.epoch, sequence: commit.sequence, operation_id: commit.operation_id,
            fingerprint: commit.fingerprint, previous_digest: commit.previous_digest,
            digest: commit.digest, payload: &commit.payload,
        };
        Self::encode_borrowed_with(&borrowed, maximum_record_bytes, write)
    }

    /// The single canonical writer also accepts already borrowed proposal data.
    pub(crate) fn encode_borrowed_with<E>(
        commit: &BorrowedStreamsDurableRecord<'_>,
        maximum_record_bytes: u64,
        write: impl FnOnce(&[u8], &[&[u8]]) -> Result<Bytes, E>,
    ) -> Result<Result<Bytes, E>, StreamsAuthorityRecordError> {
        let payload_bytes = u64::try_from(commit.payload.len())
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        let encoded_bytes = (DURABLE_HEADER_BYTES as u64).checked_add(payload_bytes)
            .ok_or(StreamsAuthorityRecordError::LengthOverflow)?;
        validate_limit(maximum_record_bytes, encoded_bytes)?;
        Ok(write(DURABLE_DOMAIN, &[
            &VERSION.to_le_bytes(), &commit.epoch.get().to_le_bytes(),
            &commit.sequence.get().to_le_bytes(), &commit.operation_id.into_bytes(),
            commit.fingerprint.as_bytes(), commit.previous_digest.as_bytes(),
            commit.digest.as_bytes(), &payload_bytes.to_le_bytes(), commit.payload,
        ]))
    }

    /// Decodes one complete durable record under a hard allocation bound.
    pub fn decode(
        encoded: &[u8],
        maximum_record_bytes: u64,
    ) -> Result<Self, StreamsAuthorityRecordError> {
        let borrowed = Self::decode_borrowed(encoded, maximum_record_bytes)?;
        let mut payload = Vec::new();
        payload.try_reserve_exact(borrowed.payload.len())
            .map_err(|_| StreamsAuthorityRecordError::AllocationFailed)?;
        payload.extend_from_slice(borrowed.payload);
        Ok(Self(DurableCommit {
            settled_at_micros: None,
            epoch: borrowed.epoch, sequence: borrowed.sequence,
            operation_id: borrowed.operation_id, fingerprint: borrowed.fingerprint,
            previous_digest: borrowed.previous_digest, digest: borrowed.digest,
            payload: Bytes::from(payload),
        }))
    }

    /// Parses canonical durable fields without allocating or copying the payload.
    pub(crate) fn decode_borrowed(
        encoded: &[u8],
        maximum_record_bytes: u64,
    ) -> Result<BorrowedStreamsDurableRecord<'_>, StreamsAuthorityRecordError> {
        match Self::decode_borrowed_engine::<std::convert::Infallible, fn(ReadAllocationStep) -> Result<(), std::convert::Infallible>>(
            encoded, maximum_record_bytes, None,
        ) {
            Ok(record) => Ok(record),
            Err(ObservedDecodeFailure::Codec(error)) => Err(error),
            Err(ObservedDecodeFailure::Observer(error)) => match error {},
        }
    }

    /// Admits each validated fixed-field copy immediately before it occurs,
    /// then records the completed copy. Earlier fields remain spent on refusal.
    pub(crate) fn decode_borrowed_with<'a, E, F>(
        encoded: &'a [u8],
        maximum_record_bytes: u64,
        mut copy: F,
    ) -> Result<Result<BorrowedStreamsDurableRecord<'a>, StreamsAuthorityRecordError>, E>
    where
        F: FnMut(ReadAllocationStep) -> Result<(), E>,
    {
        match Self::decode_borrowed_engine(encoded, maximum_record_bytes, Some(&mut copy)) {
            Ok(record) => Ok(Ok(record)),
            Err(ObservedDecodeFailure::Codec(error)) => Ok(Err(error)),
            Err(ObservedDecodeFailure::Observer(error)) => Err(error),
        }
    }

    fn decode_borrowed_engine<'a, E, F>(
        encoded: &'a [u8],
        maximum_record_bytes: u64,
        mut copy: Option<&mut F>,
    ) -> Result<BorrowedStreamsDurableRecord<'a>, ObservedDecodeFailure<E>>
    where
        F: FnMut(ReadAllocationStep) -> Result<(), E>,
    {
        let encoded_bytes = u64::try_from(encoded.len())
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        validate_limit(maximum_record_bytes, encoded_bytes)?;
        if encoded.len() < DURABLE_HEADER_BYTES {
            return Err(StreamsAuthorityRecordError::Truncated.into());
        }
        let mut cursor = 0;
        if take(encoded, &mut cursor, DURABLE_DOMAIN.len()) != DURABLE_DOMAIN {
            return Err(StreamsAuthorityRecordError::InvalidDomain.into());
        }
        let version = u16::from_le_bytes(read_array_observed::<2, _, _>(encoded, &mut cursor, &mut copy)?);
        if version != VERSION {
            return Err(StreamsAuthorityRecordError::UnsupportedVersion(version).into());
        }
        let epoch = Epoch::new(u64::from_le_bytes(read_array_observed::<8, _, _>(encoded, &mut cursor, &mut copy)?))
            .map_err(|_| StreamsAuthorityRecordError::ZeroEpoch)?;
        let sequence = Sequence::new(u64::from_le_bytes(read_array_observed::<8, _, _>(encoded, &mut cursor, &mut copy)?));
        if sequence == Sequence::GENESIS {
            return Err(StreamsAuthorityRecordError::ZeroSequence.into());
        }
        let operation_id = OperationId::from_bytes(read_array_observed(encoded, &mut cursor, &mut copy)?);
        let fingerprint = Digest::from_bytes(read_array_observed(encoded, &mut cursor, &mut copy)?);
        let previous_digest = Digest::from_bytes(read_array_observed(encoded, &mut cursor, &mut copy)?);
        let digest = Digest::from_bytes(read_array_observed(encoded, &mut cursor, &mut copy)?);
        let payload_bytes = u64::from_le_bytes(read_array_observed::<8, _, _>(encoded, &mut cursor, &mut copy)?);
        let payload_len = usize::try_from(payload_bytes)
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        let expected_end = cursor
            .checked_add(payload_len)
            .ok_or(StreamsAuthorityRecordError::LengthOverflow)?;
        if expected_end != encoded.len() {
            return Err(if expected_end > encoded.len() {
                StreamsAuthorityRecordError::Truncated
            } else {
                StreamsAuthorityRecordError::TrailingBytes
            }.into());
        }
        let body = encoded
            .get(cursor..expected_end)
            .ok_or(StreamsAuthorityRecordError::Truncated)?;
        Ok(BorrowedStreamsDurableRecord {
            epoch, sequence, operation_id, fingerprint, previous_digest, digest,
            payload: body,
        })
    }
}

impl StreamsAuthorityRecord {
    /// Encodes one record with a hard total-byte limit.
    ///
    /// The limit is checked before the output allocation. It should be set to
    /// the exact maximum record body accepted by the configured Streams v1
    /// service.
    ///
    /// # Errors
    ///
    /// Rejects a zero limit, an oversized record, or an allocation failure.
    pub fn encode(
        commit: &ProposedCommit,
        epoch: Epoch,
        maximum_record_bytes: u64,
    ) -> Result<Bytes, StreamsAuthorityRecordError> {
        let payload_bytes = u64::try_from(commit.payload.len())
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        let encoded_bytes = STREAMS_AUTHORITY_RECORD_HEADER_BYTES
            .checked_add(payload_bytes)
            .ok_or(StreamsAuthorityRecordError::LengthOverflow)?;
        validate_limit(maximum_record_bytes, encoded_bytes)?;
        let capacity = usize::try_from(encoded_bytes)
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        let mut encoded = Vec::new();
        encoded
            .try_reserve_exact(capacity)
            .map_err(|_| StreamsAuthorityRecordError::AllocationFailed)?;
        encoded.extend_from_slice(DOMAIN);
        encoded.extend_from_slice(&VERSION.to_le_bytes());
        encoded.extend_from_slice(&epoch.get().to_le_bytes());
        encoded.extend_from_slice(&commit.operation_id.into_bytes());
        encoded.extend_from_slice(commit.fingerprint.as_bytes());
        encoded.extend_from_slice(&payload_bytes.to_le_bytes());
        encoded.extend_from_slice(&commit.payload);
        Ok(Bytes::from(encoded))
    }

    /// Decodes one native Streams record body under a hard total-byte limit.
    ///
    /// # Errors
    ///
    /// Rejects malformed domain/version, truncation, trailing bytes, zero
    /// epochs, oversized records, or an allocation failure.
    pub fn decode(
        encoded: &[u8],
        maximum_record_bytes: u64,
    ) -> Result<Self, StreamsAuthorityRecordError> {
        let encoded_bytes = u64::try_from(encoded.len())
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        validate_limit(maximum_record_bytes, encoded_bytes)?;
        if encoded.len() < HEADER_BYTES {
            return Err(StreamsAuthorityRecordError::Truncated);
        }
        let mut cursor = 0;
        if take(encoded, &mut cursor, DOMAIN.len()) != DOMAIN {
            return Err(StreamsAuthorityRecordError::InvalidDomain);
        }
        let version = read_u16(encoded, &mut cursor)?;
        if version != VERSION {
            return Err(StreamsAuthorityRecordError::UnsupportedVersion(version));
        }
        let epoch = Epoch::new(read_u64(encoded, &mut cursor)?)
            .map_err(|_| StreamsAuthorityRecordError::ZeroEpoch)?;
        let operation_id = OperationId::from_bytes(read_array(encoded, &mut cursor)?);
        let fingerprint = Digest::from_bytes(read_array(encoded, &mut cursor)?);
        let payload_bytes = read_u64(encoded, &mut cursor)?;
        let payload_len = usize::try_from(payload_bytes)
            .map_err(|_| StreamsAuthorityRecordError::LengthOverflow)?;
        let expected_end = cursor
            .checked_add(payload_len)
            .ok_or(StreamsAuthorityRecordError::LengthOverflow)?;
        if expected_end != encoded.len() {
            return Err(if expected_end > encoded.len() {
                StreamsAuthorityRecordError::Truncated
            } else {
                StreamsAuthorityRecordError::TrailingBytes
            });
        }
        let mut payload = Vec::new();
        payload
            .try_reserve_exact(payload_len)
            .map_err(|_| StreamsAuthorityRecordError::AllocationFailed)?;
        let body = encoded
            .get(cursor..expected_end)
            .ok_or(StreamsAuthorityRecordError::Truncated)?;
        payload.extend_from_slice(body);
        Ok(Self {
            epoch,
            operation_id,
            fingerprint,
            payload: Bytes::from(payload),
        })
    }

    /// Converts the decoded envelope to the FS proposal accepted by an
    /// authority adapter.
    #[must_use]
    pub fn proposed_commit(&self) -> ProposedCommit {
        ProposedCommit {
            operation_id: self.operation_id,
            fingerprint: self.fingerprint,
            payload: self.payload.clone(),
        }
    }
}

/// Fail-closed canonical Streams record errors.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
pub enum StreamsAuthorityRecordError {
    /// The maximum record bound was zero.
    #[error("maximum Streams record bytes must be non-zero")]
    InvalidLimit,
    /// The encoded record exceeds the configured bound.
    #[error("Streams record has {observed} bytes; maximum is {maximum}")]
    RecordTooLarge {
        /// Actual encoded bytes.
        observed: u64,
        /// Configured maximum.
        maximum: u64,
    },
    /// An integer conversion or addition overflowed.
    #[error("Streams record length overflow")]
    LengthOverflow,
    /// The record allocation could not be admitted.
    #[error("Streams record allocation failed")]
    AllocationFailed,
    /// The domain separator did not match.
    #[error("invalid Streams authority record domain")]
    InvalidDomain,
    /// The record format version is not supported.
    #[error("unsupported Streams authority record version {0}")]
    UnsupportedVersion(u16),
    /// The encoded body ended before its declared fields or payload.
    #[error("truncated Streams authority record")]
    Truncated,
    /// The encoded body contains bytes after its declared payload.
    #[error("trailing Streams authority record bytes")]
    TrailingBytes,
    /// Epoch zero is reserved by the FS identity contract.
    #[error("Streams authority record epoch must be non-zero")]
    ZeroEpoch,
    /// Sequence zero denotes an empty authority and cannot be a record.
    #[error("Streams durable record sequence must be non-zero")]
    ZeroSequence,
}

fn validate_limit(maximum: u64, observed: u64) -> Result<(), StreamsAuthorityRecordError> {
    if maximum == 0 {
        return Err(StreamsAuthorityRecordError::InvalidLimit);
    }
    if observed > maximum {
        return Err(StreamsAuthorityRecordError::RecordTooLarge { observed, maximum });
    }
    Ok(())
}

fn take<'a>(encoded: &'a [u8], cursor: &mut usize, length: usize) -> &'a [u8] {
    let start = *cursor;
    let end = start.saturating_add(length).min(encoded.len());
    *cursor = end;
    encoded.get(start..end).unwrap_or(&[])
}

fn read_u16(encoded: &[u8], cursor: &mut usize) -> Result<u16, StreamsAuthorityRecordError> {
    let bytes = read_array::<2>(encoded, cursor)?;
    Ok(u16::from_le_bytes(bytes))
}

fn read_u64(encoded: &[u8], cursor: &mut usize) -> Result<u64, StreamsAuthorityRecordError> {
    let bytes = read_array::<8>(encoded, cursor)?;
    Ok(u64::from_le_bytes(bytes))
}

fn read_array<const N: usize>(
    encoded: &[u8],
    cursor: &mut usize,
) -> Result<[u8; N], StreamsAuthorityRecordError> {
    match read_array_observed::<N, std::convert::Infallible, fn(ReadAllocationStep) -> Result<(), std::convert::Infallible>>(
        encoded, cursor, &mut None,
    ) {
        Ok(bytes) => Ok(bytes),
        Err(ObservedDecodeFailure::Codec(error)) => Err(error),
        Err(ObservedDecodeFailure::Observer(error)) => match error {},
    }
}

enum ObservedDecodeFailure<E> {
    Codec(StreamsAuthorityRecordError),
    Observer(E),
}

impl<E> From<StreamsAuthorityRecordError> for ObservedDecodeFailure<E> {
    fn from(error: StreamsAuthorityRecordError) -> Self { Self::Codec(error) }
}

fn read_array_observed<const N: usize, E, F>(
    encoded: &[u8],
    cursor: &mut usize,
    copy: &mut Option<&mut F>,
) -> Result<[u8; N], ObservedDecodeFailure<E>>
where
    F: FnMut(ReadAllocationStep) -> Result<(), E>,
{
    let bytes = take(encoded, cursor, N);
    if bytes.len() != N {
        return Err(StreamsAuthorityRecordError::Truncated.into());
    }
    if let Some(copy) = copy.as_deref_mut() {
        copy(ReadAllocationStep::BeforeCopy(N as u64)).map_err(ObservedDecodeFailure::Observer)?;
    }
    let mut value = [0_u8; N];
    value.copy_from_slice(bytes);
    if let Some(copy) = copy.as_deref_mut() {
        copy(ReadAllocationStep::Copied(N as u64)).map_err(ObservedDecodeFailure::Observer)?;
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durable_size_rejection_precedes_the_shared_writer() {
        let original = proposal();
        let durable = DurableCommit {
            epoch: Epoch::GENESIS, sequence: Sequence::new(1),
            operation_id: original.operation_id, fingerprint: original.fingerprint,
            previous_digest: Digest::ZERO, digest: Digest::from_bytes([4; 32]), payload: original.payload,
        };
        let called = std::cell::Cell::new(false);
        let result = StreamsDurableRecord::encode_with(&durable, 1, |_, _| {
            called.set(true);
            Ok::<Bytes, StreamsAuthorityRecordError>(Bytes::new())
        });
        assert!(matches!(result, Err(StreamsAuthorityRecordError::RecordTooLarge { .. })));
        assert!(!called.get());
    }

    #[test]
    fn borrowed_observer_preserves_canonical_bytes_and_genuine_failed_copy_prefix() {
        let original = proposal();
        let durable = DurableCommit {
            epoch: Epoch::GENESIS, sequence: Sequence::new(1),
            operation_id: original.operation_id, fingerprint: original.fingerprint,
            previous_digest: Digest::ZERO, digest: Digest::from_bytes([4; 32]), payload: original.payload,
        };
        let encoded = StreamsDurableRecord::encode(&durable, 4096).expect("canonical bounded record");
        let mut copied = 0;
        let observed = StreamsDurableRecord::decode_borrowed_with(&encoded, 4096, |step| {
            if let ReadAllocationStep::Copied(bytes) = step { copied += bytes; }
            Ok::<(), ()>(())
        }).expect("observer accepts").expect("canonical record");
        assert_eq!(observed.payload, durable.payload.as_ref());
        assert_eq!(observed.digest, durable.digest);
        assert_eq!(observed.operation_id, durable.operation_id);
        assert_eq!(copied, 138);
        assert_eq!(observed.payload.as_ptr(), encoded[DURABLE_HEADER_BYTES..].as_ptr());
        let mut copied = 0;
        let refused = StreamsDurableRecord::decode_borrowed_with(&encoded, 4096, |step| {
            match step {
                ReadAllocationStep::BeforeCopy(bytes) if copied + bytes > 18 => Err("field denied"),
                ReadAllocationStep::Copied(bytes) => { copied += bytes; Ok(()) }
                _ => Ok(()),
            }
        });
        assert!(matches!(refused, Err("field denied")));
        assert_eq!(copied, 18);
        let mut invalid_version = encoded.to_vec();
        invalid_version[DURABLE_DOMAIN.len()] = 0xff;
        let mut copied = 0;
        let rejected = StreamsDurableRecord::decode_borrowed_with(&invalid_version, 4096, |step| {
            if let ReadAllocationStep::Copied(bytes) = step { copied += bytes; }
            Ok::<(), ()>(())
        }).expect("observer accepts malformed input");
        assert!(matches!(rejected, Err(StreamsAuthorityRecordError::UnsupportedVersion(_))));
        assert_eq!(copied, 2);
    }

    fn proposal() -> ProposedCommit {
        ProposedCommit {
            operation_id: OperationId::from_bytes([1; 16]),
            fingerprint: Digest::from_bytes([2; 32]),
            payload: Bytes::from_static(b"canonical-operation"),
        }
    }

    #[test]
    fn round_trip_preserves_the_complete_streams_envelope() {
        let original = proposal();
        let encoded = StreamsAuthorityRecord::encode(&original, Epoch::GENESIS, 4096)
            .unwrap_or_else(|_| unreachable!("test record fits"));
        let decoded = StreamsAuthorityRecord::decode(&encoded, 4096)
            .unwrap_or_else(|_| unreachable!("test record decodes"));
        assert_eq!(decoded.epoch, Epoch::GENESIS);
        assert_eq!(decoded.proposed_commit(), original);
    }

    #[test]
    fn malformed_records_fail_before_payload_allocation() {
        let original = proposal();
        let encoded = StreamsAuthorityRecord::encode(&original, Epoch::GENESIS, 4096)
            .unwrap_or_else(|_| unreachable!("test record fits"));
        assert_eq!(
            StreamsAuthorityRecord::decode(&encoded[..encoded.len() - 1], 4096),
            Err(StreamsAuthorityRecordError::Truncated)
        );
        assert_eq!(
            StreamsAuthorityRecord::decode(&encoded, (encoded.len() - 1) as u64),
            Err(StreamsAuthorityRecordError::RecordTooLarge {
                observed: encoded.len() as u64,
                maximum: (encoded.len() - 1) as u64,
            })
        );
    }

    #[test]
    fn domain_version_epoch_and_trailing_bytes_are_strict() {
        let original = proposal();
        let encoded = StreamsAuthorityRecord::encode(&original, Epoch::GENESIS, 4096)
            .unwrap_or_else(|_| unreachable!("test record fits"));
        let mut invalid_domain = encoded.to_vec();
        invalid_domain[0] ^= 1;
        assert_eq!(
            StreamsAuthorityRecord::decode(&invalid_domain, 4096),
            Err(StreamsAuthorityRecordError::InvalidDomain)
        );
        let mut invalid_version = encoded.to_vec();
        invalid_version[DOMAIN.len()] = 2;
        assert_eq!(
            StreamsAuthorityRecord::decode(&invalid_version, 4096),
            Err(StreamsAuthorityRecordError::UnsupportedVersion(2))
        );
        let mut invalid_epoch = encoded.to_vec();
        let epoch_start = DOMAIN.len() + 2;
        invalid_epoch[epoch_start..epoch_start + 8].fill(0);
        assert_eq!(
            StreamsAuthorityRecord::decode(&invalid_epoch, 4096),
            Err(StreamsAuthorityRecordError::ZeroEpoch)
        );
        let mut trailing = encoded.to_vec();
        trailing.push(0);
        assert_eq!(
            StreamsAuthorityRecord::decode(&trailing, 4096),
            Err(StreamsAuthorityRecordError::TrailingBytes)
        );
    }
}
