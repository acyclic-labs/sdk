//! Versioned canonical browser-authority records and ordered keys.

use acyclic_fs::{
    AuthorityId, Digest, DurableCommit, Epoch, Head, OperationId, Sequence, authority_commit_digest,
};
use bytes::Bytes;
use thiserror::Error;

const VERSION: u16 = 1;
const HEAD_MAGIC: &[u8; 8] = b"ACYFSHED";
const COMMIT_MAGIC: &[u8; 8] = b"ACYFSCMT";
const OPERATION_MAGIC: &[u8; 8] = b"ACYFSOPR";
pub(crate) const HEAD_BYTES: usize = 58;
pub(crate) const COMMIT_PREFIX_BYTES: usize = 146;
pub(crate) const OPERATION_BYTES: usize = 50;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct OperationRecord {
    pub(crate) sequence: Sequence,
    pub(crate) fingerprint: Digest,
}

pub(crate) fn authority_key(authority_id: AuthorityId) -> String {
    encode_hex(&authority_id.into_bytes())
}

pub(crate) fn commit_key(authority_id: AuthorityId, sequence: Sequence) -> String {
    let mut key = authority_key(authority_id);
    key.push(':');
    key.push_str(&encode_hex(&sequence.get().to_be_bytes()));
    key
}

pub(crate) fn operation_key(authority_id: AuthorityId, operation_id: OperationId) -> String {
    let mut key = authority_key(authority_id);
    key.push(':');
    key.push_str(&encode_hex(&operation_id.into_bytes()));
    key
}

pub(crate) fn encode_head(head: Head) -> Vec<u8> {
    let mut output = Vec::with_capacity(HEAD_BYTES);
    output.extend_from_slice(HEAD_MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&head.epoch.get().to_le_bytes());
    output.extend_from_slice(&head.sequence.get().to_le_bytes());
    output.extend_from_slice(head.digest.as_bytes());
    output
}

pub(crate) fn decode_head(bytes: &[u8]) -> Result<Head, AuthorityCodecError> {
    if bytes.len() != HEAD_BYTES || &bytes[..8] != HEAD_MAGIC {
        return Err(AuthorityCodecError::InvalidHead);
    }
    check_version(bytes)?;
    Ok(Head {
        epoch: Epoch::new(read_u64(bytes, 10)?).map_err(|_| AuthorityCodecError::InvalidEpoch)?,
        sequence: Sequence::new(read_u64(bytes, 18)?),
        digest: Digest::from_bytes(read_array(bytes, 26)?),
    })
}

pub(crate) fn encode_commit(commit: &DurableCommit) -> Result<Vec<u8>, AuthorityCodecError> {
    let payload_bytes =
        u64::try_from(commit.payload.len()).map_err(|_| AuthorityCodecError::LengthOverflow)?;
    let capacity = COMMIT_PREFIX_BYTES
        .checked_add(commit.payload.len())
        .ok_or(AuthorityCodecError::LengthOverflow)?;
    let mut output = Vec::with_capacity(capacity);
    output.extend_from_slice(COMMIT_MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&commit.epoch.get().to_le_bytes());
    output.extend_from_slice(&commit.sequence.get().to_le_bytes());
    output.extend_from_slice(&commit.operation_id.into_bytes());
    output.extend_from_slice(commit.fingerprint.as_bytes());
    output.extend_from_slice(commit.previous_digest.as_bytes());
    output.extend_from_slice(commit.digest.as_bytes());
    output.extend_from_slice(&payload_bytes.to_le_bytes());
    output.extend_from_slice(&commit.payload);
    Ok(output)
}

#[cfg(test)]
pub(crate) fn decode_commit(
    authority_id: AuthorityId,
    bytes: &[u8],
    maximum_payload_bytes: u64,
) -> Result<DurableCommit, AuthorityCodecError> {
    if bytes.len() < COMMIT_PREFIX_BYTES || &bytes[..8] != COMMIT_MAGIC {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    check_version(bytes)?;
    let epoch = Epoch::new(read_u64(bytes, 10)?).map_err(|_| AuthorityCodecError::InvalidEpoch)?;
    let sequence = Sequence::new(read_u64(bytes, 18)?);
    if sequence == Sequence::GENESIS {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    let operation_id = OperationId::from_bytes(read_array(bytes, 26)?);
    let fingerprint = Digest::from_bytes(read_array(bytes, 42)?);
    let previous_digest = Digest::from_bytes(read_array(bytes, 74)?);
    let digest = Digest::from_bytes(read_array(bytes, 106)?);
    let payload_length = read_u64(bytes, 138)?;
    if payload_length > maximum_payload_bytes {
        return Err(AuthorityCodecError::PayloadTooLarge {
            observed: payload_length,
            maximum: maximum_payload_bytes,
        });
    }
    let payload_length =
        usize::try_from(payload_length).map_err(|_| AuthorityCodecError::LengthOverflow)?;
    if bytes.len() != COMMIT_PREFIX_BYTES.saturating_add(payload_length) {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    let payload = Bytes::copy_from_slice(&bytes[COMMIT_PREFIX_BYTES..]);
    let expected = authority_commit_digest(
        authority_id,
        epoch,
        sequence,
        operation_id,
        fingerprint,
        previous_digest,
        &payload,
    );
    if expected != digest {
        return Err(AuthorityCodecError::DigestMismatch);
    }
    Ok(DurableCommit {
        epoch,
        sequence,
        operation_id,
        fingerprint,
        previous_digest,
        digest,
        payload,
    })
}

#[cfg(target_arch = "wasm32")]
pub(crate) fn decode_commit_owned(
    authority_id: AuthorityId,
    bytes: Vec<u8>,
    maximum_payload_bytes: u64,
) -> Result<DurableCommit, AuthorityCodecError> {
    if bytes.len() < COMMIT_PREFIX_BYTES || &bytes[..8] != COMMIT_MAGIC {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    check_version(&bytes)?;
    let epoch = Epoch::new(read_u64(&bytes, 10)?).map_err(|_| AuthorityCodecError::InvalidEpoch)?;
    let sequence = Sequence::new(read_u64(&bytes, 18)?);
    if sequence == Sequence::GENESIS {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    let operation_id = OperationId::from_bytes(read_array(&bytes, 26)?);
    let fingerprint = Digest::from_bytes(read_array(&bytes, 42)?);
    let previous_digest = Digest::from_bytes(read_array(&bytes, 74)?);
    let digest = Digest::from_bytes(read_array(&bytes, 106)?);
    let payload_length = read_u64(&bytes, 138)?;
    if payload_length > maximum_payload_bytes {
        return Err(AuthorityCodecError::PayloadTooLarge {
            observed: payload_length,
            maximum: maximum_payload_bytes,
        });
    }
    let payload_length =
        usize::try_from(payload_length).map_err(|_| AuthorityCodecError::LengthOverflow)?;
    if bytes.len() != COMMIT_PREFIX_BYTES.saturating_add(payload_length) {
        return Err(AuthorityCodecError::InvalidCommit);
    }
    let payload = Bytes::from(bytes).slice(COMMIT_PREFIX_BYTES..);
    let expected = authority_commit_digest(
        authority_id,
        epoch,
        sequence,
        operation_id,
        fingerprint,
        previous_digest,
        &payload,
    );
    if expected != digest {
        return Err(AuthorityCodecError::DigestMismatch);
    }
    Ok(DurableCommit {
        epoch,
        sequence,
        operation_id,
        fingerprint,
        previous_digest,
        digest,
        payload,
    })
}

pub(crate) fn encode_operation(record: OperationRecord) -> Vec<u8> {
    let mut output = Vec::with_capacity(OPERATION_BYTES);
    output.extend_from_slice(OPERATION_MAGIC);
    output.extend_from_slice(&VERSION.to_le_bytes());
    output.extend_from_slice(&record.sequence.get().to_le_bytes());
    output.extend_from_slice(record.fingerprint.as_bytes());
    output
}

pub(crate) fn decode_operation(bytes: &[u8]) -> Result<OperationRecord, AuthorityCodecError> {
    if bytes.len() != OPERATION_BYTES || &bytes[..8] != OPERATION_MAGIC {
        return Err(AuthorityCodecError::InvalidOperation);
    }
    check_version(bytes)?;
    let sequence = Sequence::new(read_u64(bytes, 10)?);
    if sequence == Sequence::GENESIS {
        return Err(AuthorityCodecError::InvalidOperation);
    }
    Ok(OperationRecord {
        sequence,
        fingerprint: Digest::from_bytes(read_array(bytes, 18)?),
    })
}

fn check_version(bytes: &[u8]) -> Result<(), AuthorityCodecError> {
    let version = u16::from_le_bytes(read_array(bytes, 8)?);
    if version == VERSION {
        Ok(())
    } else {
        Err(AuthorityCodecError::UnsupportedVersion(version))
    }
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64, AuthorityCodecError> {
    Ok(u64::from_le_bytes(read_array(bytes, offset)?))
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N], AuthorityCodecError> {
    let end = offset
        .checked_add(N)
        .ok_or(AuthorityCodecError::LengthOverflow)?;
    bytes
        .get(offset..end)
        .ok_or(AuthorityCodecError::Truncated)?
        .try_into()
        .map_err(|_| AuthorityCodecError::Truncated)
}

fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut output = String::with_capacity(bytes.len().saturating_mul(2));
    for byte in bytes {
        output.push(char::from(HEX[usize::from(byte >> 4)]));
        output.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    output
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub(crate) enum AuthorityCodecError {
    #[error("browser authority head record is invalid")]
    InvalidHead,
    #[error("browser authority commit record is invalid")]
    InvalidCommit,
    #[error("browser authority operation record is invalid")]
    InvalidOperation,
    #[error("browser authority record is truncated")]
    Truncated,
    #[error("browser authority record version {0} is unsupported")]
    UnsupportedVersion(u16),
    #[error("browser authority record has an invalid epoch")]
    InvalidEpoch,
    #[error("browser authority record length overflowed")]
    LengthOverflow,
    #[error("browser authority commit digest does not match its canonical fields")]
    DigestMismatch,
    #[error("browser authority payload has {observed} bytes; maximum is {maximum}")]
    PayloadTooLarge { observed: u64, maximum: u64 },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn authority_keys_preserve_full_width_and_sequence_order() {
        let authority_id = AuthorityId::from_bytes([0xab; 16]);
        assert_eq!(authority_key(authority_id), "ab".repeat(16));
        assert!(
            commit_key(authority_id, Sequence::new(255))
                < commit_key(authority_id, Sequence::new(256))
        );
        assert_eq!(
            operation_key(authority_id, OperationId::from_bytes([1; 16])).len(),
            65
        );
    }

    #[test]
    fn head_encoding_is_a_locked_canonical_vector() -> Result<(), Box<dyn std::error::Error>> {
        let head = Head {
            epoch: Epoch::new(2)?,
            sequence: Sequence::new(3),
            digest: Digest::from_bytes([4; 32]),
        };
        let mut expected =
            b"ACYFSHED\x01\x00\x02\x00\x00\x00\x00\x00\x00\x00\x03\x00\x00\x00\x00\x00\x00\x00"
                .to_vec();
        expected.extend_from_slice(&[4; 32]);
        assert_eq!(encode_head(head), expected);
        assert_eq!(decode_head(&expected)?, head);
        Ok(())
    }

    #[test]
    fn commit_and_operation_records_round_trip_and_authenticate()
    -> Result<(), Box<dyn std::error::Error>> {
        let authority_id = AuthorityId::from_bytes([1; 16]);
        let epoch = Epoch::new(7)?;
        let sequence = Sequence::new(9);
        let operation_id = OperationId::from_bytes([2; 16]);
        let fingerprint = Digest::from_bytes([3; 32]);
        let previous_digest = Digest::from_bytes([4; 32]);
        let payload = Bytes::from_static(b"authority-vector");
        let digest = authority_commit_digest(
            authority_id,
            epoch,
            sequence,
            operation_id,
            fingerprint,
            previous_digest,
            &payload,
        );
        let commit = DurableCommit {
            epoch,
            sequence,
            operation_id,
            fingerprint,
            previous_digest,
            digest,
            payload,
        };
        let encoded = encode_commit(&commit)?;
        assert_eq!(decode_commit(authority_id, &encoded, 1_024)?, commit);
        let mut corrupted = encoded;
        let last = corrupted
            .last_mut()
            .ok_or("encoded commit unexpectedly empty")?;
        *last ^= 1;
        assert_eq!(
            decode_commit(authority_id, &corrupted, 1_024),
            Err(AuthorityCodecError::DigestMismatch)
        );

        let operation = OperationRecord {
            sequence,
            fingerprint,
        };
        assert_eq!(decode_operation(&encode_operation(operation))?, operation);
        Ok(())
    }
}
