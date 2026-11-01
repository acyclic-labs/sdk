//! Static forwarding to the sole Streams preparation and receipt path.
use super::*;

impl<P: crate::distributed::AuthorityProvider + ?Sized> crate::async_storage::AuthorityComposition
    for &StreamAuthorityStore<P>
{
    async fn contribute<'a>(
        &self,
        commit: &mut FilesystemCommit,
        requests: &'a [crate::GuardedAppend],
        cuts: &[crate::PolicyCut],
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemAppendPreparation<'a>, AuthorityStoreError> {
        self.contribute_append_group(commit, requests, cuts, work)
            .await
    }

    fn seal(
        &self,
        commit: &mut FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<(), AuthorityStoreError> {
        StreamAuthorityStore::seal(self, commit, work)
    }

    async fn submit(
        &self,
        commit: FilesystemCommit,
        work: &mut AuthorityWork<'_>,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        StreamAuthorityStore::submit(self, commit, work).await
    }

    fn settle(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        self.settle_recorded(commit, outcome, |commit, commit_id| {
            self.refresh_committed(commit, commit_id);
        })
    }

    fn prepare_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: CommitOutcome,
    ) -> Result<PreparedFilesystemCommit, FilesystemCommitOutcome> {
        StreamAuthorityStore::prepare_settlement(self, commit, outcome)
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn prepare_native_settlement(
        &self,
        commit: FilesystemCommit,
        outcome: acyclic_stream::NativeStreamOutput<CommitOutcome>,
    ) -> Result<PreparedFilesystemCommit, FilesystemCommitOutcome> {
        StreamAuthorityStore::prepare_native_settlement(self, commit, outcome)
    }

    fn finish_settlement(
        &self,
        prepared: PreparedFilesystemCommit,
    ) -> Result<FilesystemCommitOutcome, AuthorityStoreError> {
        StreamAuthorityStore::finish_settlement(self, prepared)
    }
}
