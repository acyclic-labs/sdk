use super::*;
use crate::executor::PreparationIo;

crate::core::ownership::owned_struct!(Directory { path, after, done });
crate::core::ownership::owned_struct!(Position { path, offset });
crate::core::ownership::owned_struct!(FileCursor {
    version,
    root,
    path,
    kind,
    pattern,
    generation,
    directories,
    pending
});

pub(super) fn retire<T: crate::core::ownership::OwnedBytes>(
    value: T,
    io: Option<&PreparationIo>,
) -> Result<()> {
    let bytes = if io.is_some() {
        value.owned_heap_bytes()?
    } else {
        0
    };
    crate::executor::stock_retire(value, bytes, io)
}

pub(super) fn generation(
    value: &GenerationRef,
    io: Option<&PreparationIo>,
) -> Result<GenerationRef> {
    crate::executor::with_history_io(io, |work| {
        crate::wire_codec::CodecQuota::produce(work, |quota| value.clone_in_quota(quota))
            .map(|(value, _)| value)
    })
}

impl FileCursor {
    pub(super) fn copy_with_io(&self, io: Option<&PreparationIo>) -> Result<Self> {
        crate::executor::with_history_io(io, |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                Ok(Self {
                    version: self.version,
                    root: self.root.clone_in_quota(quota)?,
                    path: crate::core::copy_text(&self.path, quota)?,
                    kind: self.kind,
                    pattern: crate::core::copy_copy_vector(&self.pattern, quota)?,
                    generation: self
                        .generation
                        .as_ref()
                        .map(|value| value.clone_in_quota(quota))
                        .transpose()?,
                    directories: crate::core::copy_vector(
                        &self.directories,
                        quota,
                        |directory, quota| {
                            Ok(Directory {
                                path: crate::core::copy_text(&directory.path, quota)?,
                                after: directory
                                    .after
                                    .as_ref()
                                    .map(|token| crate::core::copy_copy_vector(token, quota))
                                    .transpose()?,
                                done: directory.done,
                            })
                        },
                    )?,
                    pending: self
                        .pending
                        .as_ref()
                        .map(|position| -> Result<_> {
                            Ok(Position {
                                path: crate::core::copy_text(&position.path, quota)?,
                                offset: position.offset,
                            })
                        })
                        .transpose()?,
                })
            })
            .map(|(value, _)| value)
        })
    }

    /// Constructs the original file-tree selection on the caller's native
    /// ledger. Returned custody covers the independently owned cursor.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn new_with_native_work(
        root: &ContextRoot,
        path: &str,
        kind: FileRootKind,
        pattern: &[u8],
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::kernel::Retained<Self>> {
        let io = PreparationIo::from_native_work(work)?;
        let (root, path, pattern) = crate::executor::with_history_io(Some(&io), |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                Ok((
                    root.clone_in_quota(quota)?,
                    crate::core::copy_text(path, quota)?,
                    crate::core::copy_copy_vector(pattern, quota)?,
                ))
            })
            .map(|(value, _)| value)
        })?;
        let cursor = Self::new_using(root, path, kind, pattern, Some(&io))?;
        let custody = io.take_native_custody(io.owned_bytes()?)?;
        Ok(acyclic_fs::kernel::RetainedAllocation::from(custody).into_value(cursor))
    }

    /// Pages the same search with the original native reader, work and
    /// cancellation. There is no portable fallback. Output owns its custody.
    #[cfg(not(target_arch = "wasm32"))]
    pub fn page_with_native_work<'a>(
        &'a self,
        reader: &'a dyn ContentResidencyVerifier,
        ceiling: Option<&'a Capabilities>,
        budget: NonZeroU32,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_fs::NativeFutureBundle<'a, acyclic_fs::kernel::Retained<FilePage>, Error>>
    {
        acyclic_fs::NativeFutureBundle::new(work, || async move {
            let io = PreparationIo::from_native_work(work)?;
            let page = self.page_using(reader, ceiling, budget, Some(&io)).await?;
            let custody = io.take_native_custody(io.owned_bytes()?)?;
            Ok(acyclic_fs::kernel::RetainedAllocation::from(custody).into_value(page))
        })
    }
}
