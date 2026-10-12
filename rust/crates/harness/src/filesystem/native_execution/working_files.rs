use super::*;
use acyclic_fs::NativeMaterializationWork;

trait IssuedWorkingView {}

/// SDK-issued namespace bound to its original task and operation.
pub trait NativeTaskWorkingView: IssuedWorkingView + std::any::Any + Send + Sync {
    /// Compares the held namespace and owner.
    fn same_identity(&self, other: &dyn NativeTaskWorkingView) -> bool;
    /// Original task and creation fence.
    fn task_binding(&self) -> (crate::TaskId, &crate::scheduler::LeaseFence);
    /// Immutable namespace declaration.
    fn manifest(&self) -> &NativeViewManifest;
    /// Original operation ledger.
    fn original_native_work(&self) -> &NativeMaterializationWork;
    /// Authenticates the original protected preparation.
    fn verify_with_native_work<'a>(
        &'a self,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
    /// Selects a mounted volume with the requested authority.
    fn volume_for_builtin_path<'a>(
        &'a self,
        path: &'a Path,
        operation: VolumeOperation,
        work: &NativeMaterializationWork,
    ) -> Result<&'a VolumeRef>;
    /// Reads bounded verified physical bytes.
    fn read_builtin_file<'a>(
        &'a self,
        path: &'a Path,
        maximum: usize,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, acyclic_stream::OwnedBytes>>;
    /// Writes an authorized physical path with an optional preimage.
    fn write_builtin_file<'a>(
        &'a self,
        path: &'a Path,
        content: &'a str,
        parents: bool,
        expected: Option<[u8; 32]>,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>>;
}
