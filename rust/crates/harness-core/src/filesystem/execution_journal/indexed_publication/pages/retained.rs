//! One original Cut can hand its funded producer to the execution owner.
use super::*;

/// Borrowed payloads already admitted by the original indexed publication.
pub(in crate::filesystem::execution_journal) struct Publication<'a> {
    pub(in crate::filesystem::execution_journal) pages_tail: u64,
    pub(in crate::filesystem::execution_journal) records: &'a [OwnedBytes],
    pub(in crate::filesystem::execution_journal) observation: &'a OwnedBytes,
    pub(in crate::filesystem::execution_journal) checkpoint: &'a OwnedBytes,
}

/// Preparation owns the exact request; confirmation still uses the same Cut.
pub(in crate::filesystem::execution_journal) struct NativeCut<
    'a,
    'cut,
    P: crate::store::JournalProvider + ?Sized,
> {
    cut: &'a Cut<'cut>,
    publication: Publication<'a>,
    commit: crate::distributed::NativeCommit<P>,
}

impl<'cut> Cut<'cut> {
    /// Seals the request built by `request`, without another submit or codec.
    /// Return the sole producer to the original execution owner; observers own
    /// only this retained continuation and never launch another publication.
    pub(in crate::filesystem::execution_journal) fn prepare_native<
        'a,
        P: crate::store::JournalProvider + ?Sized,
    >(
        &'a self,
        provider: &Arc<P>,
        (request, mut quota): (
            acyclic_stream::CommitRequest,
            crate::wire_codec::CodecQuota<'_, '_>,
        ),
        publication: Publication<'a>,
    ) -> Result<(
        NativeCut<'a, 'cut, P>,
        acyclic_stream::NativeProviderFuture<'static, ()>,
    )> {
        let (commit, producer) = crate::distributed::NativeCommit::prepare(
            request,
            &mut quota,
            Arc::clone(provider),
            None,
        )?;
        Ok((
            NativeCut {
                cut: self,
                publication,
                commit,
            },
            producer,
        ))
    }
}

impl<P: crate::store::JournalProvider + ?Sized> NativeCut<'_, '_, P> {
    pub(in crate::filesystem::execution_journal) fn original(
        &self,
    ) -> &acyclic_fs::NativeMaterializationWork {
        self.commit.original()
    }

    /// Confirms every contributed member before the caller advances its cache.
    /// Result storage stays on the original ledger through receipt admission.
    pub(in crate::filesystem::execution_journal) async fn finish(
        self,
        work: &mut Option<&mut dyn crate::store::HistoryIo>,
    ) -> Result<Option<acyclic_stream::CommitId>> {
        let mut quota = crate::wire_codec::CodecQuota::from_retained(work, 0);
        let (outcome, retained) = self
            .commit
            .finish(Some(self.cut.operation), None, &mut quota)
            .await?;
        quota.adopt(retained)?;
        let result = self.cut.confirm(
            &outcome,
            self.publication.pages_tail,
            self.publication.records,
            self.publication.observation,
            self.publication.checkpoint,
        );
        drop(outcome);
        drop(quota);
        result
    }
}
