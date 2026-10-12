//! Shared native/browser command journal codec and Rust replay.
use crate::wire_codec;
use crate::{AppendRequest, CommitRequest, ForkRequest, MemoryStream, StreamError, StreamProvider};
#[cfg(test)]
use crate::OwnedBytes;
use prost::Message;
pub(crate) enum Command {
    Append(AppendRequest),
    Fork(ForkRequest),
    Commit(CommitRequest),
}

pub(crate) async fn replay(provider: &MemoryStream, command: Command) -> Result<(), StreamError> {
    match command {
        Command::Append(request) => provider.append(request).await.map(|_| ()),
        Command::Fork(request) => provider.fork(request).await.map(|_| ()),
        Command::Commit(request) => provider.commit(request).await.map(|_| ()),
    }
}

pub(crate) fn decode_command(encoded: &[u8]) -> Result<Command, StreamError> {
    let (&tag, mut payload) = encoded.split_first().ok_or(StreamError::InvalidArgument)?;
    let length = prost::encoding::decode_varint(&mut payload).map_err(|_| StreamError::InvalidArgument)?;
    if usize::try_from(length).ok() != Some(payload.len()) { return Err(StreamError::InvalidArgument); }
    let command = match tag {
        10 => wire_codec::append_from_wire(crate::wire::AppendRequest::decode(payload)
            .map_err(|_| StreamError::InvalidArgument)?).map(Command::Append),
        18 => wire_codec::fork_from_wire(crate::wire::ForkRequest::decode(payload)
            .map_err(|_| StreamError::InvalidArgument)?).map(Command::Fork),
        42 => wire_codec::commit_from_wire(crate::wire::CommitRequest::decode(payload)
            .map_err(|_| StreamError::InvalidArgument)?).map(Command::Commit),
        _ => Err(StreamError::InvalidArgument),
    }?;
    // Persist only canonical bytes emitted by the current first-party writer.
    if crate::wire_codec::CanonicalWire::Journal(&command).encode_to_vec().map_err(|_| StreamError::InvalidArgument)? != encoded {
        return Err(StreamError::InvalidArgument);
    }
    Ok(command)
}

#[cfg(all(test, not(target_arch = "wasm32")))]
#[allow(
    clippy::indexing_slicing,
    reason = "Malformed codec fixtures deliberately slice canonical bytes at every boundary"
)]
mod tests {
    use super::*;

    #[test]
    fn current_writer_roundtrips_and_rejects_noncanonical_payloads() -> Result<(), StreamError> {
        let append = Command::Append(AppendRequest {
            path: crate::StreamPath::new("codec/events")?,
            records: vec![OwnedBytes::from_static(b"one"), OwnedBytes::new()],
            if_tail: Some(0),
            idempotency_key: Some(crate::IdempotencyKey::new(OwnedBytes::from_static(b"key"))?),
        });
        let fork = Command::Fork(ForkRequest {
            source: crate::StreamPath::new("codec/events")?,
            destination: crate::StreamPath::new("codec/fork")?,
            at_tail: Some(0),
            idempotency_key: None,
        });
        let commit = Command::Commit(CommitRequest {
            conditions: vec![crate::CommitCondition::Tail {
                path: crate::StreamPath::new("codec/events")?,
                expected: 2,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: crate::StreamPath::new("codec/events")?,
                records: vec![OwnedBytes::from_static(b"two")],
            }],
            idempotency_key: crate::IdempotencyKey::new(OwnedBytes::from_static(b"commit"))?,
        });
        for command in [append, fork, commit] {
            let encoded = crate::wire_codec::CanonicalWire::Journal(&command).encode_to_vec().map_err(|_| StreamError::Capacity)?;
            let (tag, payload) = match &command {
                Command::Append(request) => (10, crate::wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request.records.iter().map(|value| bytes::Bytes::copy_from_slice(value)).collect(),
                    if_tail: request.if_tail,
                    idempotency_key: request.idempotency_key.as_ref()
                        .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
                }.encode_to_vec()),
                Command::Fork(request) => (18, crate::wire::ForkRequest {
                    source: request.source.to_string(), destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: request.idempotency_key.as_ref()
                        .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
                }.encode_to_vec()),
                Command::Commit(request) => (42, wire_codec::commit_to_wire(request).encode_to_vec()),
            };
            let mut expected = vec![tag];
            prost::encoding::encode_varint(payload.len() as u64, &mut expected);
            expected.extend_from_slice(&payload);
            assert_eq!(encoded, expected);
            assert_eq!(
                crate::wire_codec::CanonicalWire::Journal(&decode_command(&encoded)?).encode_to_vec().map_err(|_| StreamError::Capacity)?,
                encoded
            );
            for end in 0..encoded.len() {
                assert!(decode_command(&encoded[..end]).is_err());
            }
            let mut unknown = encoded.clone();
            unknown.extend_from_slice(&[0x78, 0]);
            assert!(decode_command(&unknown).is_err());
            let mut duplicate = encoded.clone();
            duplicate.extend_from_slice(&encoded);
            assert!(decode_command(&duplicate).is_err());
        }
        Ok(())
    }

    #[tokio::test]
    async fn native_journal_matches_the_browser_correspondence_fixture() -> Result<(), StreamError>
    {
        let provider = MemoryStream::default();
        let request = AppendRequest {
            path: crate::StreamPath::new("events")?,
            records: vec![OwnedBytes::from_static(&[1])],
            if_tail: None,
            idempotency_key: Some(crate::IdempotencyKey::new(OwnedBytes::from_static(b"first"))?),
        };
        let encoded = crate::wire_codec::CanonicalWire::Journal(&Command::Append(request.clone())).encode_to_vec().map_err(|_| StreamError::Capacity)?;
        replay(&provider, decode_command(&encoded)?).await?;
        let crate::AppendOutcome::Committed(receipt) = provider.append(request).await? else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(receipt.start, 0);
        assert_eq!(receipt.end, 1);
        assert_eq!(
            *receipt.commit_id.as_bytes(),
            [
                103, 88, 88, 43, 235, 47, 0, 131, 35, 251, 167, 33, 80, 169, 30, 73, 72, 104, 186,
                58, 197, 155, 218, 185, 49, 156, 25, 42, 172, 54, 132, 40,
            ]
        );
        Ok(())
    }
}
