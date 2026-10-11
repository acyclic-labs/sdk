//! Shared native/browser command journal codec and Rust replay.
use crate::wire_codec;
use crate::{AppendRequest, CommitRequest, ForkRequest, MemoryStream, StreamError, StreamProvider};
#[cfg(not(target_arch = "wasm32"))]
use std::sync::Arc;
#[cfg(not(target_arch = "wasm32"))]
use acyclic_native_runtime::NativeReadMeter;
#[cfg(test)]
use crate::OwnedBytes;
use prost::Message;
#[derive(Clone)]
pub(crate) enum Command {
    Append(AppendRequest),
    Fork(ForkRequest),
    Commit(CommitRequest),
}

/// A borrowed view of the same canonical command, without copying request
/// vectors just to prepare a durable frame.
#[derive(Clone, Copy)]
pub(crate) enum CommandRef<'a> {
    Append(&'a AppendRequest),
    Fork(&'a ForkRequest),
    Commit(&'a CommitRequest),
}

impl<'a> From<&'a Command> for CommandRef<'a> {
    fn from(command: &'a Command) -> Self {
        match command {
            Command::Append(request) => Self::Append(request),
            Command::Fork(request) => Self::Fork(request),
            Command::Commit(request) => Self::Commit(request),
        }
    }
}

pub(crate) async fn replay(provider: &MemoryStream, command: Command) -> Result<(), StreamError> {
    match command {
        Command::Append(request) => provider.append(request).await.map(|_| ()),
        Command::Fork(request) => provider.fork(request).await.map(|_| ()),
        Command::Commit(request) => provider.commit(request).await.map(|_| ()),
    }
}

pub(crate) fn journal_command<'a>(
    command: impl Into<CommandRef<'a>>,
) -> wire_codec::CanonicalWire<'a> {
    wire_codec::CanonicalWire::Journal(command.into())
}

#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn journal_command_observed<'a>(
    command: impl Into<CommandRef<'a>>,
    meter: Option<&Arc<dyn NativeReadMeter>>,
) -> std::io::Result<wire_codec::ObservedWire<wire_codec::CanonicalWire<'a>>> {
    let funding = wire_codec::WireFunding::new(meter);
    if let Some(meter) = meter { meter.admit_examination(1, 0)?; }
    let command = journal_command(command);
    if let Some(meter) = meter {
        meter.record_examination(1, 0)?;
        meter.check()?;
    }
    Ok(funding.finish(command))
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
    if journal_command(&command).encode_to_vec().map_err(|_| StreamError::InvalidArgument)? != encoded {
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
            let encoded = journal_command(&command).encode_to_vec().map_err(|_| StreamError::Capacity)?;
            // Compare the ONE borrowed writer against the unchanged generated
            // schema, not merely against itself on canonical decode.
            let operation = match &command {
                Command::Append(request) => journal_command::Operation::Append(crate::wire::AppendRequest {
                    path: request.path.to_string(),
                    records: request.records.iter().map(|value| bytes::Bytes::copy_from_slice(value)).collect(),
                    if_tail: request.if_tail,
                    idempotency_key: request.idempotency_key.as_ref()
                        .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
                }),
                Command::Fork(request) => journal_command::Operation::Fork(crate::wire::ForkRequest {
                    source: request.source.to_string(), destination: request.destination.to_string(),
                    at_tail: request.at_tail,
                    idempotency_key: request.idempotency_key.as_ref()
                        .map(|key| bytes::Bytes::copy_from_slice(key.as_bytes())),
                }),
                Command::Commit(request) => journal_command::Operation::Commit(wire_codec::commit_to_wire(request)),
            };
            assert_eq!(encoded, JournalCommand { operation: Some(operation) }.encode_to_vec());
            assert_eq!(
                journal_command(&decode_command(&encoded)?).encode_to_vec().map_err(|_| StreamError::Capacity)?,
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
        let encoded = journal_command(&Command::Append(request.clone())).encode_to_vec().map_err(|_| StreamError::Capacity)?;
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
