use super::*;
use crate::{
    facade::FsWork,
    kernel::{Owned, OwnedSourceChunk},
};

/// An immutable empty directory. Useful as the base of a routed source tree.
#[derive(Clone, Copy, Default)]
pub struct Empty;

impl Empty {
    /// The immutable empty namespace needs no retained backing.
    pub const fn reference(&self) -> SourceReference {
        SourceReference::new(*b"acyclic.fs.empty", 1)
    }
    pub(super) fn node() -> SourceNode {
        SourceNode {
            kind: SourceNodeKind::Directory,
            file_identity: *blake3::hash(b"acyclic.fs.demand.empty.v1").as_bytes(),
            link_count: None,
            device: None,
            logical_bytes: None,
            version: SourceVersion(*blake3::hash(b"acyclic.fs.demand.empty.v1").as_bytes()),
            metadata: SourceMetadata::default(),
        }
    }

    fn check(
        &self,
        source: SourceReference,
        cancellation: &CancellationToken,
    ) -> Result<(), DemandError> {
        cancellation.check().map_err(|_| DemandError::Cancelled)?;
        if source != self.reference() {
            return Err(DemandError::StaleSource);
        }
        Ok(())
    }
}

impl DemandSource for Empty {
    fn identity(&self) -> [u8; 16] {
        *b"acyclic.fs.empty"
    }
    fn watch<W: FsWork + Send>(
        &self,
        _sink: std::sync::Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        let mut ctx = work.accounting(cancellation);
        let result = (|| {
            ctx.check()?;
            let allocation = ctx.claim_owned(std::mem::size_of::<ImmutableWatch>() as u64, 1)?;
            Ok(Some(Box::new(ImmutableWatch {
                _allocation: allocation,
            }) as Box<dyn SourceWatch>))
        })();
        work.finish(result, &ctx)
    }
    fn reference(
        &self,
        accounting: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        accounting.check()?;
        Ok(self.reference())
    }

    async fn lookup<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<SourceNode>, DemandError> {
        let ctx = work.accounting(cancellation);
        work.finish(
            self.check(source, cancellation)
                .map(|()| path.is_root().then(Self::node)),
            &ctx,
        )
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
            self.check(source, cancellation)?;
            if maximum_entries == 0 {
                return Err(DemandError::InvalidRequest);
            }
            if !directory.is_root() {
                return Err(DemandError::Absent);
            }
            if cursor.is_some() {
                return Err(DemandError::StaleCursor);
            }
            let entries = Owned::allocate(0, &mut ctx)?;
            SourceDirectoryPage::stateless(entries, None, Self::node().version, &mut ctx)
        })();
        work.finish(result, &ctx)
    }

    async fn read_range<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        _expected: SourceVersion,
        _offset: u64,
        _length: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = self.check(source, cancellation).and_then(|()| {
            Err(if path.is_root() {
                DemandError::NotRegularFile
            } else {
                DemandError::Absent
            })
        });
        work.finish(result, &ctx)
    }

    async fn open_file<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        _expected: SourceVersion,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Box<dyn DemandFile>, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = self.check(source, cancellation).and_then(|()| {
            Err(if path.is_root() {
                DemandError::NotRegularFile
            } else {
                DemandError::Absent
            })
        });
        work.finish(result, &ctx)
    }

    async fn read_link<W: FsWork + Send>(
        &self,
        source: SourceReference,
        path: &NamespacePath,
        _expected: SourceVersion,
        _maximum_bytes: u64,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<OwnedSourceChunk, DemandError> {
        let ctx = work.accounting(cancellation);
        let result = self.check(source, cancellation).and_then(|()| {
            Err(if path.is_root() {
                DemandError::InvalidRequest
            } else {
                DemandError::Absent
            })
        });
        work.finish(result, &ctx)
    }
}

struct ImmutableWatch {
    _allocation: crate::kernel::KernelOwnedAllocation,
}
impl SourceWatch for ImmutableWatch {
    fn fence(
        &self,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<SourceFence, DemandError> {
        ctx.check()?;
        Ok(SourceFence::Confirmed)
    }
    fn stop(&self, _ctx: &mut crate::kernel::KernelAccounting<'_>) -> Result<(), DemandError> {
        Ok(())
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_with_cleanup_wait(&self, wait: &CancellationToken) -> Result<(), DemandError> {
        wait.check().map_err(|_| DemandError::Cancelled)
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn stop_accepted_cleanup(&self) -> Result<(), DemandError> {
        Ok(())
    }
    fn is_stopped(&self) -> bool {
        true
    }
    fn logical_owned_bytes(&self) -> u64 {
        std::mem::size_of::<Self>() as u64
    }
    fn is_exact(&self) -> bool {
        true
    }
}
