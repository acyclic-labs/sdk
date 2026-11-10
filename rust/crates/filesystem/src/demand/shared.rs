//! Shared handles forward on the original provider and original operation.
use super::*;
use crate::facade::FsWork;
use std::sync::Arc;

macro_rules! forward {
    ($name:ident($($arg:ident: $ty:ty),*) -> $out:ty) => {
        fn $name<W: FsWork + Send>(
            &self, $($arg: $ty,)* work: W, cancellation: &CancellationToken,
        ) -> impl Future<Output = W::Output<$out, DemandError>> + Send {
            self.as_ref().$name($($arg,)* work, cancellation)
        }
    };
}
impl<D: DemandSource> DemandSource for Arc<D> {
    fn overlay(&self, components: &[crate::kernel::LogicalName]) -> SourceOverlay {
        self.as_ref().overlay(components)
    }
    fn identity(&self) -> [u8; 16] {
        self.as_ref().identity()
    }
    async fn reference(
        &self,
        ctx: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<SourceReference, DemandError> {
        self.as_ref().reference(ctx).await
    }
    forward!(lookup(source: SourceReference, path: &NamespacePath) -> Option<SourceNode>);
    forward!(list_page(source: SourceReference, path: &NamespacePath, cursor: Option<SourceCursor>, maximum: u32) -> SourceDirectoryPage);
    forward!(read_range(source: SourceReference, path: &NamespacePath, expected: SourceVersion, offset: u64, length: u64) -> crate::kernel::OwnedSourceChunk);
    forward!(open_file(source: SourceReference, path: &NamespacePath, expected: SourceVersion) -> Box<dyn DemandFile>);
    forward!(read_link(source: SourceReference, path: &NamespacePath, expected: SourceVersion, maximum: u64) -> crate::kernel::OwnedSourceChunk);
    forward!(read_metadata(source: SourceReference, path: &NamespacePath, expected: Option<SourceVersion>) -> Option<SourceNode>);
    fn watch<W: FsWork + Send>(
        &self,
        sink: Arc<dyn SourceChangeSink>,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<Box<dyn SourceWatch>>, DemandError> {
        self.as_ref().watch(sink, work, cancellation)
    }
}
