use super::*;

/// A producer's actual publication transaction, not durable filesystem authority.
pub trait SourceDirectoryPublication: Send + Sync {
    /// Maximum infallible commit/abort cleanup still reserved by the producer.
    fn cleanup_budget(&self) -> WorkCounters;
    /// Actual private held backing, excluding the public entry view's backing.
    fn retained_bytes(&self) -> u64;
    /// Publishes cursor progress after fallible consumer projection.
    fn accept(
        &mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
    /// Restores the original position even after cancellation.
    fn abort(
        &mut self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError>;
}

impl std::fmt::Debug for dyn SourceDirectoryPublication {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("SourceDirectoryPublication")
    }
}

/// Completes the producer's pre-admitted publication transaction.
pub async fn accept_publication<W: crate::facade::FsWork>(
    publication: &mut Option<Box<dyn SourceDirectoryPublication>>,
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<(), DemandError> {
    let mut accounting = work.accounting(cancellation);
    let answer = if let Some(guard) = publication.as_mut() {
        guard.accept(&mut accounting)
    } else {
        accounting
            .check()
            .map_err(SourceCursor::cursor_accounting_error)
    };
    if answer.is_ok() {
        publication.take();
    }
    work.finish(answer, &accounting)
}

/// Aborts without a cancellation check; cleanup cannot be skipped.
pub fn abort_publication<W: crate::facade::FsWork>(
    publication: &mut Option<Box<dyn SourceDirectoryPublication>>,
    work: W,
) -> W::Output<(), DemandError> {
    let cancellation = CancellationToken::new();
    let mut accounting = work.accounting(&cancellation);
    let answer = publication
        .as_mut()
        .map_or(Ok(()), |guard| guard.abort(&mut accounting));
    if answer.is_ok() {
        publication.take();
    }
    work.finish(answer, &accounting)
}
