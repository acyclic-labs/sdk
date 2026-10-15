use super::*;
mod binding;
mod content;
mod fork;
mod publication;
pub(in crate::objects) use content::{content_identity, storage};

/// One authenticated Objects bucket/volume binding. The bucket identity is
/// part of the logical volume, so a ref cannot silently route to another bucket.
pub struct ObjectContentStore {
    objects: Arc<dyn ObjectsProvider>,
    pub(in crate::objects) bucket: wire::BucketRef,
    pub(in crate::objects) volume: VolumeRef,
    pub(in crate::objects) verifier: AuthorityVerifier,
    pub(in crate::objects) scope: Scope,
    maximum_bytes: u64,
    writer: Option<ContentGrant>,
}
