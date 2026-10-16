use super::{Cursor, Observation, Query, Result, TaskVolume};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform, StreamProvider};

/// Snapshot/cursor-follow over canonical files, independent of their physical layout.
pub trait FileQuerySource: ProviderPlatform {
    /// Opens or resumes one bounded selection. A reset supplies a new snapshot.
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, Result<Observation>>;
}

impl<P: StreamProvider + ?Sized> FileQuerySource for TaskVolume<P> {
    fn observe_files(
        &self,
        query: Query,
        cursor: Option<Cursor>,
        maximum_files: usize,
        _maximum_bytes: usize,
    ) -> BoxProviderFuture<'_, Result<Observation>> {
        Box::pin(self.observe(query, cursor, maximum_files))
    }
}
