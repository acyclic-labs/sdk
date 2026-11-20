//! Filesystem metadata changes over ordinary credential-bound Streams follows.
mod resource;
pub use resource::{namespace_resource, resource};

/// Canonical change stream of a volume on this filesystem's authority owner.
/// The path is an identity, not an authorization grant.
pub fn changes_path<A: crate::AsyncAuthorityStore, O: 'static>(
    fs: &crate::Fs<A, O>,
    volume: crate::VolumeId,
) -> Result<acyclic_stream::StreamPath, acyclic_stream::StreamError> {
    let mut text = [0; crate::publication_changes::CHANGES_PATH_LEN];
    acyclic_stream::StreamPath::new(
        crate::publication_changes::changes_path_text(
            fs.authority_actor_scope(),
            volume,
            &mut text,
        )
        .ok_or(acyclic_stream::StreamError::InvalidArgument)?,
    )
}

/// The publication a followed changes record carries, once Streams' `origin`
/// proves that the volume it names appended it. A forked volume's changes
/// stream shares its source's records unchanged, so a follow of the fork also
/// delivers records an ancestor appended; the follow's own path, not the
/// record, selects the stream.
///
/// # Errors
/// The record carries no publication, or the volume it names did not append it.
pub fn appended_publication<A: crate::AsyncAuthorityStore, O: 'static>(
    fs: &crate::Fs<A, O>,
    record: &acyclic_stream::Record,
) -> Result<crate::PublicationContext, acyclic_stream::StreamError> {
    let context = crate::PublicationContext::from_bytes(
        acyclic_stream::changes_context(&record.value, &record.headers)?
            .ok_or(acyclic_stream::StreamError::InvalidArgument)?,
    )?;
    if appended_by_context(record, &context, fs.authority_actor_scope()) {
        Ok(context)
    } else {
        Err(acyclic_stream::StreamError::InvalidArgument)
    }
}

/// A record's context names the volume whose changes stream appended it.
/// That is this volume's, or for a forked volume an ancestor's whose changes
/// it shares; Streams, not the record, names the appending stream.
fn appended_by_context(
    record: &acyclic_stream::Record,
    context: &crate::PublicationContext,
    scope: [u8; 16],
) -> bool {
    let mut text = [0; crate::publication_changes::CHANGES_PATH_LEN];
    crate::publication_changes::changes_path_text(scope, context.volume, &mut text)
        .is_some_and(|path| path == record.origin.as_str())
}
mod resources;
pub use resources::ChangeProvider;
