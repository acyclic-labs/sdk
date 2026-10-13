use super::*;
use crate::{executor::stock_boundary, filesystem::fork_preparer_io, wire::ErrorCode};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static>
    FsRootProjectSourceProvider<A, O>
{
    async fn verify_source(
        &self,
        context: RootProjectSourceContext<'_>,
        snapshot: &ProjectSourceSnapshot,
        io: Option<&PreparationIo>,
    ) -> Result<()> {
        let denied = |reason| stock_boundary(io, ErrorCode::Unauthorized, reason);
        let check = || {
            context.cancellation.check().map_err(|_| {
                #[cfg(not(target_arch = "wasm32"))]
                if io.and_then(PreparationIo::original_native_work).is_some() {
                    return Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled);
                }
                denied("project source operation cancelled")
            })
        };
        if io.is_some() {
            check()?;
        }
        if context.owner != &self.owner || context.graph_root != self.graph_root {
            return Err(denied("project source belongs to another service graph"));
        }
        if self.native_source(&snapshot.project).is_some() && snapshot.native_snapshot.is_none() {
            return Err(denied(
                "bound lazy project requires its original native snapshot",
            ));
        }
        if io.is_none() {
            snapshot.project.validate(&mut None)?;
            snapshot.generation.validate(&mut None)?;
        }
        if snapshot.project.class() != VolumeClass::Project
            || snapshot.project.provider() != &self.host.provider
            || snapshot.generation.as_resource().provider() != &self.host.provider
        {
            return Err(denied(
                "project source belongs to another namespace or provider",
            ));
        }
        let budget = source_budget(context.maximum_work, context.maximum_bytes).map_err(|_| {
            stock_boundary(
                io,
                ErrorCode::Invalid,
                "project source requires finite positive admission",
            )
        })?;
        let ordinary;
        let io = match io {
            Some(io) => io,
            None => {
                ordinary = PreparationIo::from_budget(budget, context.cancellation)?;
                &ordinary
            }
        };
        io.check_cancellation()?;
        if !context.created_project_authenticated {
            #[cfg(not(target_arch = "wasm32"))]
            {
                let proof = self.bootstrap.as_ref().ok_or_else(|| {
                    denied("project source lacks this graph's authenticated creator")
                })?;
                if context.source.task_id() != self.graph_root
                    || proof.request().volume != snapshot.project
                    || proof.request().generation != snapshot.generation
                    || with_history_io(Some(io), |work| {
                        context.source.canonical_digest_with_work(work)
                    })? != proof.admission_digest
                    || io
                        .original_native_work()
                        .is_some_and(|work| !proof.matches_native_work(work))
                {
                    return Err(denied(
                        "project source differs from its protected original Root",
                    ));
                }
                proof.reauthenticate_with_io(io).await?;
            }
            #[cfg(target_arch = "wasm32")]
            return Err(denied(
                "project source lacks this graph's authenticated creator",
            ));
        }
        if !io.contains_capability(&context.source.grants, &snapshot.project, "volume:read:")? {
            return Err(denied(
                "selected source admission lacks original project read authority",
            ));
        }
        let reference =
            fork_preparer_io::content_workspace(&self.host.provider, &snapshot.project, Some(io))?;
        let _reference = io.guard_owned(reference.logical_owned_bytes()?)?;
        let opened = fork_preparer_io::open(&self.host, &reference, io).await?;
        if io.original_native_work().is_none() {
            io.retain_owned(opened.name().as_str().len() as u64)?;
        }
        let selected =
            fork_preparer_io::generation(&self.host, &opened, &snapshot.generation, io).await?;
        let generation = selected.id();
        drop(selected);
        if let Some(value) = &snapshot.native_snapshot {
            let lazy = self
                .native_source(&snapshot.project)
                .ok_or_else(|| denied("native source has no original provider binding"))?;
            if lazy.backing.workspace_id() != opened.id() {
                return Err(denied("native source names another original workspace"));
            }
            let (frozen, _frozen) = source_snapshot(
                value,
                JsonBounds {
                    maximum_bytes: context.maximum_bytes,
                    maximum_items: context.maximum_work,
                    maximum_depth: 32,
                },
                io,
            )?;
            if frozen.workspace_id != opened.id() || frozen.authored_generation != generation {
                return Err(denied(
                    "native capture does not match the selected generation",
                ));
            }
            lazy.backing.validate(frozen, io)?.await?;
        }
        check()?;
        io.remaining()?;
        Ok(())
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> RootProjectSourceProvider
    for FsRootProjectSourceProvider<A, O>
{
    fn verify<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<()>> {
        Box::pin(self.verify_source(context, snapshot, None))
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn verify_with_native_work<'a>(
        &'a self,
        context: RootProjectSourceContext<'a>,
        snapshot: &'a ProjectSourceSnapshot,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, ()>> {
        context
            .cancellation
            .check()
            .map_err(|_| Error::NativeWork {
                cause: acyclic_fs::NativeTreeMaterializationError::Cancelled,
                custody: None,
            })?;
        crate::NativeFutureBundle::new(work, || async move {
            work.remaining()?;
            let io = PreparationIo::from_native_work(work)?;
            self.verify_source(context, snapshot, Some(&io)).await
        })
    }
}

impl crate::filesystem::PreparationRecord for LazySnapshotRef {
    fn maximum_slot_bytes() -> u64 {
        0
    }
}

pub(super) fn source_snapshot<'a>(
    value: &serde_json::Value,
    bounds: JsonBounds,
    io: &'a PreparationIo,
) -> Result<(LazySnapshotRef, crate::filesystem::DecodeReservation<'a>)> {
    crate::executor::json::json_owned_value_shape_bounded(
        value,
        bounds,
        crate::executor::json::JsonValueSchema::None,
        &mut |charge| io.codec_charge(charge, false),
    )?;
    let bytes = io.encode(value, bounds.maximum_bytes, true)?;
    let _bytes = io.guard_bytes(&bytes)?;
    io.decode(std::marker::PhantomData, &bytes, ErrorCode::Invalid)
}

pub(super) fn source_budget(maximum_work: u64, maximum_bytes: u64) -> Result<WorkBudget> {
    if [maximum_work, maximum_bytes]
        .into_iter()
        .any(|limit| limit == 0 || limit == u64::MAX)
    {
        return Err(Error::Invalid(
            "project source requires finite positive admission".into(),
        ));
    }
    let mut budget = acyclic_fs::performance::work_budget(maximum_work / 14, maximum_bytes / 9);
    budget.peak_allocation_bytes = maximum_bytes;
    Ok(budget)
}
