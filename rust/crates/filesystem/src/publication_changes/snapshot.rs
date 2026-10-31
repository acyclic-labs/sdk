use crate::{PublicationContext, VolumeId};

/// One immutable generation and its original change-stream boundary.
///
/// The selected volume can differ from `context.volume` for inherited history.
/// `context.volume` identifies the original appending stream. This metadata is
/// not authority: callers still authenticate the selected immutable generation
/// and authorize each file or followed entry through the owning workspace.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct PublicationSnapshot {
    /// Volume whose change stream was observed.
    pub volume: VolumeId,
    /// Full generation reference and original publication identity.
    pub context: PublicationContext,
    /// Exclusive change-stream position observed before reading its last record.
    pub next: u64,
    /// Original last record's commit, used to resume the same prefix.
    pub preceding: acyclic_stream::CommitId,
}

impl PublicationContext {
    pub(crate) fn validate_record(
        self,
        record: &acyclic_stream::Record,
        scope: [u8; 16],
    ) -> Result<(), acyclic_stream::StreamError> {
        let mut text = [0; super::CHANGES_PATH_LEN];
        if !acyclic_stream::is_changes(&record.headers)
            || !super::changes_path_text(scope, self.volume, &mut text)
                .is_some_and(|path| path == record.origin.as_str())
        {
            return Err(acyclic_stream::StreamError::InvalidArgument);
        }
        Ok(())
    }
}
