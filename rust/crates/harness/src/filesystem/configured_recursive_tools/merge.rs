use super::*;
use crate::executor::with_history_io;
use crate::filesystem::{
    PreparationIo,
    fork_preparer_io::{failure, item},
};
use crate::wire::ErrorCode;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn merge_binding(
        &self,
        context: &ToolContext,
        arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<RecursiveMergeToolBinding<P, A, O>> {
        check_cancellation(cancellation, io)?;
        let owner = self.source_owner(context.task(), cancellation, io).await?;
        let caller = self
            .authenticate_caller(context.task(), Some(&owner), true, io)
            .await?;
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(io) = io {
            for prefix in ["volume:read:", "volume:write:"] {
                item(Some(io), 0)?;
                if !io.contains_capability(
                    context.effective_scope().grants(),
                    &caller.project,
                    prefix,
                )? {
                    return Err(failure(
                        Some(io),
                        ErrorCode::Unauthorized,
                        "effective scope denies its Project operation",
                    ));
                }
            }
        }
        let source = self.caller(arguments.source, io)?;
        let mut allocation = None;
        for merge in &self.config.merges {
            if let Some(io) = io {
                io.codec_charge(
                    crate::executor::json::JsonCodecCharge::Work {
                        bytes: 32,
                        items: 1,
                    },
                    true,
                )?;
            }
            if merge.caller == caller.task && merge.source == arguments.source {
                allocation = Some(merge);
                break;
            }
        }
        let allocation = allocation
            .ok_or_else(|| failure(io, ErrorCode::Unauthorized, "merge has no notice binding"))?;
        let runtime = self.runtime(io)?;
        let ordinary;
        #[cfg(not(target_arch = "wasm32"))]
        let native;
        let snapshots = 'snapshots: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                native = runtime
                    .task_host()
                    .observe_volume_snapshots_with_native_work(arguments.source, work)
                    .await?;
                break 'snapshots native.0.as_slice();
            }
            ordinary = runtime
                .inspect_source(caller.task, arguments.source)
                .await?;
            if ordinary
                .volumes()
                .iter()
                .filter(|snapshot| snapshot.volume == source.private_volume)
                .count()
                != 1
                || source.result_volume.as_ref().is_some_and(|result| {
                    ordinary
                        .volumes()
                        .iter()
                        .filter(|snapshot| &snapshot.volume == result)
                        .count()
                        != 1
                })
            {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "merge source differs from its admitted allocation",
                ));
            }
            ordinary.volumes()
        };
        let mut selected = None;
        for snapshot in snapshots {
            item(io, 0)?;
            if snapshot.volume.class() == crate::conversation::VolumeClass::Project
                && (snapshot.volume != source.project || selected.replace(snapshot).is_some())
            {
                return Err(failure(
                    io,
                    ErrorCode::Unauthorized,
                    "merge differs from its unique admitted Project",
                ));
            }
        }
        let selected = selected
            .ok_or_else(|| failure(io, ErrorCode::Unauthorized, "merge source has no Project"))?;
        if let Some(original) = original
            && (original.caller != caller.task
                || original.arguments != *arguments
                || &original.destination != caller.owner.authority()
                || &original.source != source.owner.authority()
                || original.target_project != caller.project
                || original.source_project != selected.volume)
        {
            return Err(failure(
                io,
                ErrorCode::Unauthorized,
                "merge retry changed its original owners",
            ));
        }
        let generation =
            original.map_or(&selected.generation, |original| &original.source_generation);
        let generation = match io {
            Some(io) => generation.clone_with_io(io)?,
            None => generation.clone(),
        };
        let notice = with_history_io(io, |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                original
                    .map_or(&allocation.notice, |original| &original.notice)
                    .clone_in_quota(quota)
            })
            .map(|(notice, _)| notice)
        })?;
        let source_read = 'source: {
            if let Some(authorization) = original.and_then(|plan| plan.authorized_source.as_ref()) {
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(work) = io.and_then(PreparationIo::original_native_work) {
                    let (handle, custody) = owner
                        .restore_project_source_authorization_with_native_work(
                            context.task(),
                            authorization,
                            work,
                        )?
                        .await?;
                    io.expect("native merge observer")
                        .absorb_native_custody(custody)?;
                    break 'source Some(handle);
                }
                break 'source Some(
                    owner
                        .restore_project_source_authorization(context.task(), authorization)
                        .await?,
                );
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(io) = io {
                if !io.contains_capability(
                    context.effective_scope().grants(),
                    &selected.volume,
                    "volume:read:",
                )? {
                    if original.is_some() || self.project_policy.get().is_none() {
                        return Err(failure(
                            Some(io),
                            ErrorCode::Unauthorized,
                            "merge lacks its Project read authority",
                        ));
                    }
                    io.original_native_work()
                        .expect("native merge work")
                        .remaining()?;
                    return Err(failure(
                        Some(io),
                        ErrorCode::Unsupported,
                        "native Project policy requires originally funded installed metadata",
                    ));
                }
            }
            if io.is_some()
                || original.is_some()
                || arguments.source == caller.task
                || context.scope().grants().contains(
                    &selected
                        .volume
                        .capability(VolumeOperation::Read, &mut None)?,
                )
            {
                break 'source None;
            }
            let (issuer, provider) = self.project_policy.get().ok_or_else(|| {
                failure(
                    None,
                    ErrorCode::Unauthorized,
                    "foreign merge lacks a RootService Project policy",
                )
            })?;
            Some(
                owner
                    .authorize_project_source(
                        context.task(),
                        issuer,
                        provider.as_ref(),
                        arguments.source,
                        context.operation_id(),
                        ProjectSourceSnapshot {
                            project: selected.volume.clone(),
                            generation: generation.clone(),
                            native_snapshot: source.native_snapshot.clone(),
                        },
                        self.config.content_budget.items_examined,
                        self.config.history_limits.maximum_bytes,
                    )
                    .await?,
            )
        };
        if let Some(handle) = &source_read {
            #[cfg(not(target_arch = "wasm32"))]
            let work = io.and_then(PreparationIo::original_native_work);
            #[cfg(target_arch = "wasm32")]
            let work = None;
            let snapshot = if io.is_some() {
                handle.authorization().native_snapshot.as_ref()
            } else {
                source.native_snapshot.as_ref()
            };
            handle.require_selection(
                context.operation_id(),
                arguments.source,
                &selected.volume,
                &generation,
                snapshot,
                work,
            )?;
        }
        let ordinary_io;
        let reader = match io {
            Some(io) => io,
            None => {
                ordinary_io = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
                &ordinary_io
            }
        };
        drop(self.read(context, &notice.content, reader).await?);
        let (verifier, scope, project, source_authority, source_project) =
            with_history_io(io, |work| {
                let verifier = caller.owner.issuer().verifier(work)?;
                let source_authority = match work.as_deref_mut() {
                    Some(work) => work.clone_authority(source.owner.authority())?,
                    None => source.owner.authority().clone(),
                };
                Ok((
                    verifier,
                    caller.owner.scope.copy(work)?,
                    caller.project.copy(work)?,
                    source_authority,
                    selected.volume.copy(work)?,
                ))
            })?;
        let resolver = self.resolver(context, owner, io)?;
        check_cancellation(cancellation, io)?;
        let destination = self.open(&caller.owner, None, io).await?;
        check_cancellation(cancellation, io)?;
        let destination =
            self.bind_aggregate(&caller.owner, &scope, destination, Some(resolver), io)?;
        Ok(RecursiveMergeToolBinding {
            filesystem: Arc::clone(&self.filesystem),
            destination,
            verifier,
            scope,
            project,
            source: source_authority,
            source_project,
            source_generation: generation,
            source_read,
            notice,
        })
    }
}
