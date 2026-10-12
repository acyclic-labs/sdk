use super::*;

/// Immutable filesystem-object storage over one exact public Objects bucket.
#[derive(Clone)]
pub struct LogicalObjectStore<P> {
    pub(super) provider: Arc<P>,
    pub(super) bucket: wire::BucketRef,
    pub(super) maximum_object_bytes: u64,
}

impl<P> LogicalObjectStore<P> {
    /// Binds an authenticated provider to the account's dedicated filesystem bucket.
    #[must_use]
    pub fn new(provider: Arc<P>, bucket: wire::BucketRef) -> Self {
        Self {
            provider,
            bucket,
            maximum_object_bytes: 5 * 1024 * 1024 * 1024,
        }
    }

    /// Applies the composition's existing positive per-object bound.
    pub fn with_object_limit(mut self, maximum: u64) -> Result<Self, ObjectStoreError> {
        if maximum == 0 || usize::try_from(maximum).is_err() {
            return Err(ObjectStoreError::Rejected(
                "invalid object size limit".to_owned(),
            ));
        }
        self.maximum_object_bytes = maximum;
        Ok(self)
    }

    /// Returns the exact backing bucket identity used by this adapter.
    #[must_use]
    pub fn bucket(&self) -> &wire::BucketRef {
        &self.bucket
    }

    /// Returns the exact public provider used by this stateless adapter.
    #[must_use]
    pub fn provider(&self) -> &Arc<P> {
        &self.provider
    }
}
