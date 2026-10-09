//! Portable text editing through the existing pinned content publication path.

use super::FilesystemHost;
use crate::{
    Error, IdempotencyKey, Result,
    conversation::{ContentGrant, FileRef, VolumeOperation, is_internal_path},
    tool::edit::{exact_replace, validate_edit_bound},
};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore};
use serde::{Deserialize, Serialize};

/// Exact text to locate and replace once in a pinned source file.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExactTextReplacement {
    /// Nonempty exact needle; overlapping matches are ambiguous.
    pub old_text: String,
    /// Exact replacement, which may be empty.
    pub new_text: String,
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> FilesystemHost<A, O> {
    /// Edits one immutable source under separate verified read/write grants.
    ///
    /// The source generation is the transaction precondition. Retry binds the
    /// source generation and resulting complete file contract through the
    /// existing content receipt; it does not rebase onto concurrent user edits.
    #[allow(
        clippy::too_many_arguments,
        reason = "each authority, pinned input, finite bound and retry identity is explicit"
    )]
    pub async fn edit_text(
        &self,
        source: &FileRef,
        read: &ContentGrant,
        write: &ContentGrant,
        replacement: &ExactTextReplacement,
        maximum_bytes: u64,
        idempotency_key: &IdempotencyKey,
    ) -> Result<FileRef> {
        validate_edit_bound(maximum_bytes)?;
        source.validate()?;
        if is_internal_path(source.path()) {
            return Err(Error::Unauthorized(
                "internal storage is not editable content".into(),
            ));
        }
        write.require(source.volume(), VolumeOperation::Write)?;
        let bytes = self.read_content(source, read, maximum_bytes).await?;
        let text = std::str::from_utf8(&bytes)
            .map_err(|_| Error::Invalid("exact text edit requires UTF-8 content".into()))?;
        let result = exact_replace(
            text,
            &replacement.old_text,
            &replacement.new_text,
            maximum_bytes,
        )?;
        let generation = self.file_generation(source)?;
        self.put_content_at(
            source.volume(),
            write,
            source.path(),
            result.as_bytes(),
            source.descriptor().media_type(),
            source.display_name(),
            maximum_bytes,
            idempotency_key,
            &generation,
        )
        .await
    }
}
