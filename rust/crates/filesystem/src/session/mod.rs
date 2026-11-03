//! Filesystem metadata changes over ordinary credential-bound Streams follows.
mod resource;
pub use resource::resource;

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
        .ok_or(acyclic_stream::StreamError::InvalidArgument)?
        .to_owned(),
    )
}
#[cfg(feature = "session")]
mod resources;
#[cfg(feature = "session")]
pub use resources::ChangeProvider;
