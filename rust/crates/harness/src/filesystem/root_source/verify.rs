
use super::*;
use crate::filesystem::fork_preparer_io::{self, failure};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FsRootProjectSourceProvider<A, O> {
    async fn verify_source(
        &self, context: RootProjectSourceContext<'_>, snapshot: &ProjectSourceSnapshot,
        io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<()> {
        let check = || context.cancellation.check().map_err(|_| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(crate::filesystem::PreparationIo::original_native_work).is_some() {
                return Error::NativeWork { cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None };
            }
            failure(io, crate::wire::ErrorCode::Unauthorized, "project source operation cancelled")
        });
        if io.is_some() { check()?; }
        if context.owner != &self.owner || context.graph_root != self.graph_root || !context.created_project_authenticated {
            return Err(failure(io, crate::wire::ErrorCode::Unauthorized, "project source lacks this graph's authenticated creator"));
        }
        if self.native_source(&snapshot.project).is_some() && snapshot.native_snapshot.is_none() {
            return Err(failure(io, crate::wire::ErrorCode::Unauthorized, "bound lazy project requires its original native snapshot"));
        }
        if io.is_none() { snapshot.project.validate(&mut None)?; snapshot.generation.validate(&mut None)?; }
        if snapshot.project.class() != VolumeClass::Project || snapshot.project.provider() != &self.host.provider
            || snapshot.generation.as_resource().provider() != &self.host.provider {
            return Err(failure(io, crate::wire::ErrorCode::Unauthorized, "project source belongs to another namespace or provider"));
        }
        let budget = source_budget(context.maximum_work, context.maximum_bytes)
            .map_err(|_| failure(io, crate::wire::ErrorCode::Invalid, "project source requires finite positive admission"))?;
        let ordinary;
        let io = match io {
            Some(io) => io,
            None => { ordinary = crate::filesystem::PreparationIo::from_budget(budget, context.cancellation)?; &ordinary }
        };
        io.check_cancellation()?;
        if !io.contains_capability(&context.source.grants, &snapshot.project, "volume:read:")? {
            return Err(failure(Some(io), crate::wire::ErrorCode::Unauthorized, "selected source admission lacks original project read authority"));
        }
        let reference_hold;
        let before = io.owned_bytes()?;
        let reference = fork_preparer_io::content_workspace(&self.host.provider, &snapshot.project, Some(io))?;
        reference_hold = io.guard_owned(io.owned_bytes()?.checked_sub(before).ok_or_else(|| io.invalid("source reference custody underflows"))?)?;
        let reference = (reference, reference_hold);
        let opened = fork_preparer_io::open(&self.host, &reference.0, io).await?;
        #[cfg(not(target_arch = "wasm32"))]
        if io.original_native_work().is_none() { io.retain_owned(opened.name().as_str().len() as u64)?; }
        #[cfg(target_arch = "wasm32")]
        io.retain_owned(opened.name().as_str().len() as u64)?;
        let selected = fork_preparer_io::generation(&self.host, &opened, &snapshot.generation, io).await?;
        let generation_id = selected.id();
        drop(selected);
        if let Some(value) = &snapshot.native_snapshot {
            let lazy = self.native_source(&snapshot.project).ok_or_else(|| failure(
                Some(io), crate::wire::ErrorCode::Unauthorized, "native source has no original provider binding"))?;
            if lazy.backing.workspace_id() != opened.id() {
                return Err(failure(Some(io), crate::wire::ErrorCode::Unauthorized, "native source names another original workspace"));
            }
            let bounds = JsonBounds { maximum_bytes: context.maximum_bytes, maximum_items: context.maximum_work, maximum_depth: 32 };
            let encoded_hold;
            let encoded = source_json(value, bounds, io)?;
            encoded_hold = io.guard_bytes(&encoded)?;
            #[cfg(not(target_arch = "wasm32"))]
            let frozen_hold = io.original_native_work().map(|work| work.claim(
                std::mem::size_of::<LazySnapshotRef>() as u64, 0, acyclic_fs::WorkCounters::UNCHARGED)).transpose()?;
            let frozen = crate::contract::json_from_slice_bounded(
                std::marker::PhantomData::<LazySnapshotRef>, &encoded, bounds, 0,
                io.original_native_work().is_some().then_some(crate::wire::ErrorCode::Invalid),
                Some(crate::contract::JsonValueSchema::None), &mut |charge| io.codec_charge(charge, false))?;
            drop(encoded);
            drop(encoded_hold);
            if frozen.workspace_id != opened.id() || frozen.authored_generation != generation_id {
                return Err(failure(Some(io), crate::wire::ErrorCode::Unauthorized, "native capture does not match the selected generation"));
            }
            lazy.backing.validate(frozen, io)?.await?;
            #[cfg(not(target_arch = "wasm32"))]
            drop(frozen_hold);
        }
        check()?;
        io.remaining()?;
        Ok(())
    }
}
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> RootProjectSourceProvider for FsRootProjectSourceProvider<A, O> {
    fn verify<'a>(&'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot)
        -> acyclic_stream::BoxProviderFuture<'a, Result<()>>
    { Box::pin(self.verify_source(context, snapshot, None)) }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self, context: RootProjectSourceContext<'a>, snapshot: &'a ProjectSourceSnapshot,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        context.cancellation.check().map_err(|_| Error::NativeWork {
            cause: acyclic_fs::NativeTreeMaterializationError::Cancelled, custody: None,
        })?;
        crate::NativeFutureBundle::new(work, || async move {
            work.remaining()?;
            let io = crate::filesystem::PreparationIo::from_native_work(work)?;
            self.verify_source(context, snapshot, Some(&io)).await
        })
    }
}

fn source_json<T: serde::Serialize>(
    value: &T, bounds: JsonBounds, io: &crate::filesystem::PreparationIo,
) -> Result<acyclic_stream::OwnedBytes> {
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(work) = io.original_native_work() {
        let diagnostic = work.claim(crate::contract::sdk_json_diagnostic_bytes(), 0, acyclic_fs::WorkCounters::UNCHARGED)?;
        let bytes = crate::contract::canonical_json_bytes_bounded(
            value, bounds, Some(crate::wire::ErrorCode::Invalid),
            &mut |charge| io.codec_charge(charge, true),
        ).map_err(|error| match error {
            Error::NativeEncode { code, source, custody: None } =>
                Error::NativeEncode { code, source, custody: Some(diagnostic) },
            error => error,
        })?;
        return crate::filesystem::fork_preparer_io::source_bytes_from_vec(bytes, Some(io));
    }
    canonical_json_bytes_bounded(value, bounds, None, &mut |charge| io.codec_charge(charge, true)).map(Into::into)
}

fn source_budget(maximum_work: u64, maximum_bytes: u64) -> Result<WorkBudget> {
    if maximum_work == 0 || maximum_work == u64::MAX || maximum_bytes == 0 || maximum_bytes == u64::MAX {
        return Err(Error::Invalid("project source requires finite positive admission".into()));
    }
    let mut budget = acyclic_fs::performance::work_budget(maximum_work / 14, maximum_bytes / 9);
    budget.peak_allocation_bytes = maximum_bytes;
    Ok(budget)
}

