//! Shared native/browser command journal codec and Rust replay.
#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
use crate::OwnedBytes as Bytes;
use crate::wire_codec;
use crate::{AppendRequest, CommitRequest, ForkRequest, MemoryStream, StreamError, StreamProvider};
use prost::Message;
#[derive(Clone)]
pub(crate) enum Command {
    Append(AppendRequest),
    Fork(ForkRequest),
    Commit(CommitRequest),
}

pub(crate) enum Outcome {
    Append(crate::AppendOutcome),
    Fork(crate::ForkReceipt),
    Commit(crate::CommitOutcome),
}

pub(crate) async fn replay(provider: &MemoryStream, command: Command) -> Result<(), StreamError> {
    match command {
        Command::Append(request) => match provider.append(request).await? {
            crate::AppendOutcome::Committed(_) => Ok(()),
            crate::AppendOutcome::TailConflict { .. } => Err(StreamError::InvalidResponse),
        },
        Command::Fork(request) => provider.fork(request).await.map(|_| ()),
        Command::Commit(mut request) => {
            // This command already passed its publication deadline. Recovery
            // installs that accepted decision rather than admitting a new one.
            request.deadline_unix_millis = None;
            match provider.commit(request).await? {
                crate::CommitOutcome::Committed(_) => Ok(()),
                crate::CommitOutcome::Conflict(_) => Err(StreamError::InvalidResponse),
            }
        }
    }
}

impl Command {
    pub(crate) fn validated(mut self) -> Result<Self, StreamError> {
        match &mut self {
            Self::Append(request) => {
                crate::request::validate_records(&request.records)?;
                crate::request::validate_append_size(request)?;
            }
            Self::Fork(request) => crate::request::validate_fork_size(request)?,
            Self::Commit(request) => {
                crate::request::normalize_commit(request)?;
                crate::request::validate_commit_shape(request)?;
            }
        }
        if self.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(self)
    }

    pub(crate) fn encoded_len(&self) -> usize {
        use wire_codec::requests::*;
        block_len(match self {
            Self::Append(request) => append_len(request),
            Self::Fork(request) => fork_len(request),
            Self::Commit(request) => commit_len(request),
        })
    }
    pub(crate) fn encode(&self, output: &mut Vec<u8>) {
        use wire_codec::requests::*;
        match self {
            Self::Append(request) => {
                header(output, 1, append_len(request));
                append(output, request);
            }
            Self::Fork(request) => {
                header(output, 2, fork_len(request));
                fork(output, request);
            }
            Self::Commit(request) => {
                header(output, 5, commit_len(request));
                commit(output, request);
            }
        }
    }
    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.encoded_len());
        self.encode(&mut output);
        output
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
    if command.encode_to_vec() != encoded {
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

#[cfg(test)]
#[cfg(not(target_arch = "wasm32"))]
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
            records: vec![(Bytes::from_static(b"one")).into(), (Bytes::new()).into()],
            if_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"key"))?,
        });
        let fork = Command::Fork(ForkRequest {
            exclude: Vec::new(),
            source: crate::StreamPath::new("codec/events")?,
            destination: crate::StreamPath::new("codec/fork")?,
            at_tail: Some(0),
            idempotency_key: crate::IdempotencyKey::unique(),
        });
        let commit = Command::Commit(CommitRequest {
            deadline_unix_millis: None,
            conditions: vec![crate::CommitCondition::Tail {
                path: crate::StreamPath::new("codec/events")?,
                expected: 2,
            }],
            mutations: vec![crate::CommitMutation::Append {
                path: crate::StreamPath::new("codec/events")?,
                records: vec![(Bytes::from_static(b"two")).into()],
            }],
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"commit"))?,
        });
        for command in [append, fork, commit] {
            let encoded = command.encode_to_vec();
            assert_eq!(decode_command(&encoded)?.encode_to_vec(), encoded);
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
            records: vec![(Bytes::from_static(&[1])).into()],
            if_tail: None,
            idempotency_key: crate::IdempotencyKey::new(Bytes::from_static(b"first"))?,
        };
        let encoded = Command::Append(request.clone()).encode_to_vec();
        replay(&provider, decode_command(&encoded)?).await?;
        let crate::AppendOutcome::Committed(receipt) = provider.append(request).await? else {
            return Err(StreamError::Unavailable);
        };
        assert_eq!(receipt.start, 0);
        assert_eq!(receipt.end, 1);
        // The actual Chromium test asserts this same complete content-bound ID,
        assert_eq!(
            *receipt.commit_id.as_bytes(),
            [
                38, 143, 36, 24, 145, 152, 169, 224, 7, 28, 82, 129, 78, 208, 207, 70, 142, 223,
                186, 183, 148, 195, 120, 23, 80, 176, 19, 93, 220, 251, 47, 177,
            ]
        );
        Ok(())
    }
}
