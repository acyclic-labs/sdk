use super::{Error, FileKey, Query, Result, TaskVolume};
use acyclic_stream::StreamProvider;

impl<P: StreamProvider + ?Sized> TaskVolume<P> {
    // Refresh before taking the leaf index lock, as for body snapshots. Listing
    // selects only keys: payloads cannot enter its output or byte accounting.
    pub(crate) async fn list_files(
        &self,
        query: &Query,
        maximum_files: usize,
    ) -> Result<Vec<FileKey>> {
        self.refresh().await?;
        let index = self.index.lock().await;
        let mut keys = Vec::new();
        for range in query.ranges() {
            for (key, _) in index.files.range(range) {
                if query.matches(key) {
                    if keys.len() == maximum_files {
                        return Err(Error::LimitExceeded);
                    }
                    keys.push(key.clone());
                }
            }
        }
        Ok(keys)
    }
}
