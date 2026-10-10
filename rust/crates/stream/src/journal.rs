//! Shared native/browser command journal codec and Rust replay.
use crate::wire_codec;
use crate::{AppendRequest, CommitRequest, ForkRequest, MemoryStream, StreamError, StreamProvider};
use bytes::Bytes;
use prost::Message;
#[derive(Clone)]
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

pub(crate) fn journal_command(command: &Command) -> JournalCommand {
    let operation = match command {
        Command::Append(request) => journal_command::Operation::Append(wire_append(request)),
        Command::Fork(request) => journal_command::Operation::Fork(wire_fork(request)),
        Command::Commit(request) => {
            journal_command::Operation::Commit(wire_codec::commit_to_wire(request))
        }
    };
    JournalCommand {
        operation: Some(operation),
    }
}

pub(crate) fn decode_command(encoded: &[u8]) -> Result<Command, StreamError> {
    let journal = JournalCommand::decode(encoded).map_err(|_| StreamError::InvalidArgument)?;
    let command = match journal.operation.ok_or(StreamError::InvalidArgument)? {
        journal_command::Operation::Append(request) => {
            wire_codec::append_from_wire(request).map(Command::Append)
        }
        journal_command::Operation::Fork(request) => {
            wire_codec::fork_from_wire(request).map(Command::Fork)
        }
        journal_command::Operation::Commit(request) => {
            wire_codec::commit_from_wire(request).map(Command::Commit)
        }
    }?;
    // Persist only canonical bytes emitted by the current first-party writer.
    if journal_command(&command).encode_to_vec() != encoded {
        return Err(StreamError::InvalidArgument);
    }
    Ok(command)
}

#[derive(Clone, PartialEq, prost::Message)]
pub(crate) struct JournalCommand {
    #[prost(oneof = "journal_command::Operation", tags = "1, 2, 5")]
    operation: Option<journal_command::Operation>,
}

mod journal_command {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub(super) enum Operation {
        #[prost(message, tag = "1")]
        Append(crate::wire::AppendRequest),
        #[prost(message, tag = "2")]
        Fork(crate::wire::ForkRequest),
        #[prost(message, tag = "5")]
        Commit(crate::wire::CommitRequest),
    }
}

fn wire_append(request: &AppendRequest) -> crate::wire::AppendRequest {
    crate::wire::AppendRequest {
        path: request.path.to_string(),
        records: request.records.clone(),
        if_tail: request.if_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
}

fn wire_fork(request: &ForkRequest) -> crate::wire::ForkRequest {
    crate::wire::ForkRequest {
        source: request.source.to_string(),
        destination: request.destination.to_string(),
        at_tail: request.at_tail,
        idempotency_key: request
            .idempotency_key
            .as_ref()
            .map(|key| Bytes::copy_from_slice(key.as_bytes())),
    }
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
            records: vec![Bytes::from_static(b"one"), Bytes::new()],
            if_tail: Some(0),
            idempotency_key: Some(crate::IdempotencyKey::new(Bytes::from_static(b"key"))?),
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
                records: vec![Bytes::from_static(b"two")],
            }],
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"commit"))?,
        });
        for command in [append, fork, commit] {
            let encoded = journal_command(&command).encode_to_vec();
            assert_eq!(
                journal_command(&decode_command(&encoded)?).encode_to_vec(),
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
            records: vec![Bytes::from_static(&[1])],
            if_tail: None,
            idempotency_key: Some(crate::IdempotencyKey::new(Bytes::from_static(b"first"))?),
        };
        let encoded = journal_command(&Command::Append(request.clone())).encode_to_vec();
        replay(&provider, decode_command(&encoded)?).await?;
        let crate::AppendOutcome::Committed(receipt) = provider.append(request).await? else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(receipt.start, 0);
        assert_eq!(receipt.end, 1);
        // The actual Chromium test asserts this same complete content-bound ID,
        // rather than merely comparing two WASM projections of memory state.
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
