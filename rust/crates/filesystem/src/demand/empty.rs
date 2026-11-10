use super::*;
use crate::facade::FsWork;
use crate::kernel::{KernelAccounting, Owned, OwnedSourceChunk};

/// An empty directory source, suitable as the base of a computed namespace.
#[derive(Clone, Copy, Default)]
pub struct Empty;

impl DemandSource for Empty {
    fn watch<W: FsWork + Send>(
        &self,
        _: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = ctx
            .check()
            .map_err(DemandError::from)
            .map(|()| Some(Box::new(EmptyWatch) as Box<dyn SourceWatch>));
        work.finish(result, &ctx)
    }
    fn reference(&self) -> SourceReference {
        SourceReference {
            identity: *b"acyclic:empty:v1\0",
            epoch: 0,
        }
    }

    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = ctx.check().map_err(DemandError::from).and_then(|()| {
            if source != self.reference() {
                return Err(DemandError::StaleSource);
            }
            Ok(path
                .is_root()
                .then(|| directory_node(SourceVersion([0; 32]))))
        });
        work.finish(result, &ctx)
    }

    async fn list_page<W: FsWork + Send>(
        &self,
        source: SourceReference,
        directory: &NamespacePath,
        cursor: Option<SourceCursor>,
        maximum_entries: u32,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<SourceDirectoryPage, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            if source != self.reference() {
                return Err(DemandError::StaleSource);
            }
            if !directory.is_root() {
                return Err(DemandError::Absent);
            }
            if cursor.is_some() {
                return Err(DemandError::StaleCursor);
            }
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            let entries = Owned::allocate(0, &mut ctx)?;
            SourceDirectoryPage::stateless(entries, None, SourceVersion([0; 32]), &mut ctx)
        })();
        work.finish(result, &ctx)
    }

    async fn read_range<W: FsWork + Send>(
        &self,
        _: SourceReference,
        _: &NamespacePath,
        _: SourceVersion,
        _: u64,
        _: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        empty_read(work, cancellation)
    }

    async fn open_file<W: FsWork + Send>(
        &self,
        _: SourceReference,
        _: &NamespacePath,
        _: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        empty_read(work, cancellation)
    }

    async fn read_link<W: FsWork + Send>(
        &self,
        _: SourceReference,
        _: &NamespacePath,
        _: SourceVersion,
        _: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        empty_read(work, cancellation)
    }
}

struct EmptyWatch;
impl SourceWatch for EmptyWatch {
    fn fence(&self, ctx: &mut KernelAccounting<'_>) -> Result<SourceFence, DemandError> {
        ctx.check()?;
        Ok(SourceFence::Confirmed)
    }
    fn stop(&self, _: &mut KernelAccounting<'_>) -> Result<(), DemandError> {
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, cancel: &CancellationToken) -> Result<(), DemandError> {
        cancel.check().map_err(|_| DemandError::Cancelled)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        Ok(())
    }
    fn is_stopped(&self) -> bool {
        true
    }
    fn logical_owned_bytes(&self) -> u64 {
        0
    }
    fn is_exact(&self) -> bool {
        true
    }
}

fn empty_read<W: FsWork, T>(
    work: W,
    cancellation: &CancellationToken,
) -> W::Output<T, DemandError> {
    let ctx = work.accounting(cancellation);
    let result = ctx
        .check()
        .map_err(DemandError::from)
        .and(Err(DemandError::NotRegularFile));
    work.finish(result, &ctx)
}

pub(super) fn directory_node(version: SourceVersion) -> SourceNode {
    SourceNode {
        kind: SourceNodeKind::Directory,
        file_identity: version.0,
        link_count: None,
        device: None,
        logical_bytes: None,
        version,
        metadata: SourceMetadata::default(),
    }
}
