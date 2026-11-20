//! The canonical journal command: one validated `WriteRequest` and its exact bytes.
use super::encode::{append, commit, fork, header};
use super::len::{append_len, block_len, commit_len, fork_len};
use crate::{CommitRequest, StreamError, WriteRequest};

/// Version 2: commits carry ordered effects. Version 1 journals are refused
/// as unsupported, never reread as corrupt.
const MAGIC: &[u8] = b"ACYCLIC-STREAM-COMMAND-V2\0";

/// A validated command with the commit digest its admission computed, if any.
pub struct CanonicalCommand {
    /// The command in canonical form.
    pub command: WriteRequest,
    /// The commit digest returned by the admission callback.
    pub digest: Option<[u8; 32]>,
}

impl WriteRequest {
    /// Validates the command and normalizes a commit, without a digest.
    pub fn validated(self) -> Result<Self, StreamError> {
        self.canonicalized_with(|request| {
            super::normalize_commit(request)?;
            Ok(None)
        })
        .map(|canonical| canonical.command)
    }

    /// Validates the command, admits a commit through `commit`, and bounds the encoded size.
    pub fn canonicalized_with(
        mut self,
        mut commit: impl FnMut(&mut CommitRequest) -> Result<Option<[u8; 32]>, StreamError>,
    ) -> Result<CanonicalCommand, StreamError> {
        let mut digest = None;
        match &mut self {
            Self::Append(request) => {
                super::validate_records(&request.records)?;
                super::validate_append_size(request)?;
            }
            Self::Fork(request) => super::validate_fork_size(request)?,
            Self::Commit(request) | Self::CommitAccepted(request) => {
                digest = commit(request)?;
            }
        }
        if self.encoded_len() > crate::MAX_COMMAND_BYTES {
            return Err(StreamError::LimitExceeded);
        }
        Ok(CanonicalCommand {
            command: self,
            digest,
        })
    }

    /// The exact length of the encoded journal command, magic included.
    pub fn encoded_len(&self) -> usize {
        MAGIC.len()
            + block_len(match self {
                Self::Append(request) => append_len(request),
                Self::Fork(request) => fork_len(request),
                Self::Commit(request) | Self::CommitAccepted(request) => commit_len(request),
            })
    }

    /// Appends the canonical journal bytes to `output`.
    pub fn encode(&self, output: &mut Vec<u8>) {
        output.extend_from_slice(MAGIC);
        match self {
            Self::Append(request) => {
                header(output, 1, append_len(request));
                append(output, request);
            }
            Self::Fork(request) => {
                header(output, 2, fork_len(request));
                fork(output, request);
            }
            Self::Commit(request) | Self::CommitAccepted(request) => {
                header(output, 5, commit_len(request));
                commit(output, request);
            }
        }
    }

    /// The canonical journal bytes.
    pub fn encode_to_vec(&self) -> Vec<u8> {
        let mut output = Vec::with_capacity(self.encoded_len());
        self.encode(&mut output);
        output
    }
}
