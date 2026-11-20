//! Shared native/browser command journal codec and Rust replay.
use crate::wire_codec;
use crate::{StreamError, StreamProvider, WriteOutcome, WriteRequest};
use prost::Message;

pub async fn replay<P: StreamProvider + ?Sized>(
    provider: &P,
    mut command: WriteRequest,
) -> Result<(), StreamError> {
    if let WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) = &mut command {
        // Recovery installs an accepted decision rather than admitting a new deadline.
        request.deadline_unix_millis = None;
    }
    match provider.write(command).await? {
        WriteOutcome::Append(crate::AppendOutcome::Committed(_))
        | WriteOutcome::Fork(_)
        | WriteOutcome::Commit(crate::CommitOutcome::Committed(_))
        | WriteOutcome::CommitAccepted(Ok(..)) => Ok(()),
        WriteOutcome::Append(crate::AppendOutcome::TailConflict { .. })
        | WriteOutcome::Commit(crate::CommitOutcome::Conflict(_))
        | WriteOutcome::CommitAccepted(Err(_)) => Err(StreamError::InvalidResponse),
    }
}

pub fn command_payload(encoded: &[u8]) -> Result<&[u8], StreamError> {
    crate::persistence::header(encoded, b"ACYCLIC-STREAM-COMMAND-V", b"2\0")
}

pub fn decode_command(encoded: &[u8]) -> Result<WriteRequest, StreamError> {
    let payload = command_payload(encoded)?;
    let journal = JournalCommand::decode(payload).map_err(|_| StreamError::InvalidArgument)?;
    let command = match journal.operation.ok_or(StreamError::InvalidArgument)? {
        journal_command::Operation::Append(request) => {
            wire_codec::append_from_wire(request).map(WriteRequest::Append)
        }
        journal_command::Operation::Fork(request) => {
            wire_codec::fork_from_wire(request).map(WriteRequest::Fork)
        }
        journal_command::Operation::Commit(request) => {
            wire_codec::commit_from_wire(request).map(WriteRequest::Commit)
        }
    }?;
    // Persist only canonical bytes emitted by the current first-party writer.
    if command.encode_to_vec() != encoded {
        return Err(StreamError::InvalidArgument);
    }
    Ok(command)
}

#[derive(Clone, PartialEq, prost::Message)]
pub struct JournalCommand {
    #[prost(oneof = "journal_command::Operation", tags = "1, 2, 5")]
    operation: Option<journal_command::Operation>,
}

mod journal_command {
    #[derive(Clone, PartialEq, prost::Oneof)]
    pub enum Operation {
        #[prost(message, tag = "1")]
        Append(acyclic_stream_wire::wire::AppendRequest),
        #[prost(message, tag = "2")]
        Fork(acyclic_stream_wire::wire::ForkRequest),
        #[prost(message, tag = "5")]
        Commit(acyclic_stream_wire::wire::CommitRequest),
    }
}
