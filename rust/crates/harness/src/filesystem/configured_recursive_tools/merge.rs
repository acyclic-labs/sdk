use super::*;
use crate::filesystem::{PreparationIo, fork_preparer_io::failure};
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
        #[cfg(not(target_arch = "wasm32"))]
        if io.and_then(PreparationIo::original_native_work).is_some() {
            cancellation
                .check()
                .map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        }
        #[cfg(not(target_arch = "wasm32"))]
        let owner = if io.and_then(PreparationIo::original_native_work).is_some() {
            Some(self.source_owner(context.task(), cancellation, io).await?)
        } else {
            None
        };
        let caller = 'caller: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(owner) = &owner {
                let caller = self
                    .authenticate_caller(context.task(), Some(owner), true, io)
                    .await?;
                let io = io.expect("native merge observer");
                for prefix in ["volume:read:", "volume:write:"] {
                    crate::filesystem::fork_preparer_io::item(Some(io), 0)?;
                    if !io.contains_capability(
                        context.effective_scope().grants(),
                        &caller.project,
                        prefix,
                    )? {
                        return Err(failure(
                            Some(io),
                            ErrorCode::Unauthorized,
                            "native recursive tool effective scope denies its selected volume operation",
                        ));
                    }
                }
                break 'caller caller;
            }
            cancellation
                .check()
                .map_err(|error| Error::Storage(error.to_string()))?;
            self.authenticate_caller(context.task(), None, true, None)
                .await?
        };
        let source = self.caller(arguments.source, io)?;
        let mut allocation = None;
        for merge in &self.config.merges {
            if let Some(io) = io {
                io.codec_charge(
                    crate::contract::JsonCodecCharge::Work {
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
        let allocation = allocation.ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "configured merge has no original notice binding",
            )
        })?;
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
                break 'snapshots &native.0;
            }
            ordinary = runtime
                .inspect_source(caller.task, arguments.source)
                .await?;
            if ordinary.project()?.volume != source.project
                || ordinary
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
                    "configured merge source is not its admitted Project",
                ));
            }
            ordinary.volumes()
        };
        let mut selected = None;
        for snapshot in snapshots {
            if let Some(io) = io {
                io.codec_charge(
                    crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 },
                    true,
                )?;
            }
            if snapshot.volume.class() == crate::conversation::VolumeClass::Project {
                if snapshot.volume != source.project || selected.replace(snapshot).is_some() {
                    return Err(failure(
                        io,
                        ErrorCode::Unauthorized,
                        "merge differs from its unique admitted Project",
                    ));
                }
            }
        }
        let selected = selected.ok_or_else(|| {
            failure(
                io,
                ErrorCode::Unauthorized,
                "merge source has no authenticated Project",
            )
        })?;
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
                "configured merge retry changed its original owners",
            ));
        }
        let generation =
            original.map_or(&selected.generation, |original| &original.source_generation);
        let generation = match io {
            Some(io) => generation.clone_with_io(io)?,
            None => generation.clone(),
        };
        let notice = crate::executor::with_history_io(io, |work| {
            let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
            let notice = original
                .map_or(&allocation.notice, |original| &original.notice)
                .clone_in_quota(&mut quota)?;
            quota.transfer();
            Ok(notice)
        })?;
        let source_read = 'source_read: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(owner) = &owner {
                let io = io.expect("native merge IO");
                let work = io.original_native_work().expect("native merge work");
                let read = if let Some(authorization) =
                    original.and_then(|plan| plan.authorized_source.as_ref())
                {
                    let (handle, custody) = owner
                        .restore_project_source_authorization_with_native_work(
                            context.task(),
                            authorization,
                            work,
                        )?
                        .await?;
                    io.absorb_native_custody(custody)?;
                    Some(handle)
                } else {
                    let allowed = io.contains_capability(
                        context.effective_scope().grants(),
                        &selected.volume,
                        "volume:read:",
                    )?;
                    if !allowed {
                        if original.is_some() || self.project_policy.get().is_none() {
                            return Err(failure(
                                Some(io),
                                ErrorCode::Unauthorized,
                                "merge lacks its original Project read authority",
                            ));
                        }
                        work.remaining()?;
                        return Err(failure(
                            Some(io),
                            ErrorCode::Unsupported,
                            "native Project policy requires originally funded installed metadata",
                        ));
                    }
                    None
                };
                if let Some(handle) = &read {
                    handle.require_selection(
                        context.operation_id(),
                        arguments.source,
                        &selected.volume,
                        &generation,
                        handle.authorization().native_snapshot.as_ref(),
                        Some(work),
                    )?;
                }
                drop(
                    context
                        .read_file_with_native_work(&notice.content, work)
                        .await?,
                );
                break 'source_read read;
            }
            let read = if let Some(authorization) =
                original.and_then(|plan| plan.authorized_source.as_ref())
            {
                let owner = Arc::new(
                    runtime
                        .task_host()
                        .journal_owner(caller.task, context.task().owned_task_fence()?)
                        .await?
                        .with_invocation_cancellation(cancellation),
                );
                let handle = owner
                    .restore_project_source_authorization(context.task(), authorization)
                    .await?;
                handle.require_selection(
                    context.operation_id(),
                    arguments.source,
                    &selected.volume,
                    &generation,
                    source.native_snapshot.as_ref(),
                    None,
                )?;
                Some(handle)
            } else if original.is_none()
                && arguments.source != caller.task
                && !context.scope().grants().contains(
                    &selected
                        .volume
                        .capability(VolumeOperation::Read, &mut None)?,
                )
            {
                let (issuer, provider) = self.project_policy.get().ok_or_else(|| {
                    failure(
                        None,
                        ErrorCode::Unauthorized,
                        "foreign merge lacks an explicit RootService Project policy",
                    )
                })?;
                let owner = Arc::new(
                    runtime
                        .task_host()
                        .journal_owner(caller.task, context.task().owned_task_fence()?)
                        .await?
                        .with_invocation_cancellation(cancellation),
                );
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
            } else {
                None
            };
            drop(
                context
                    .read_file_measured(&notice.content, self.config.content_budget, cancellation)
                    .await
                    .map_err(|failure| failure.error)?,
            );
            read
        };
        let (verifier, scope, project, source_authority, source_project) =
            crate::executor::with_history_io(io, |work| {
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
        let resolver = 'resolver: {
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(owner) = owner {
                break 'resolver self.resolver(context, owner, io)?;
            }
            self.resolver(
                context,
                self.source_owner(context.task(), cancellation, None)
                    .await?,
                None,
            )?
        };
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
