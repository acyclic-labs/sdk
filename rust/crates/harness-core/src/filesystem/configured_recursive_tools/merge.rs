use super::*;
use crate::executor::with_history_io;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where
    P: StreamProvider + 'static,
    A: AsyncAuthorityStore + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(in crate::filesystem) async fn merge(
        &self,
        context: &ToolContext,
        arguments: &MergeToolArguments,
        original: Option<&RecursiveMergeToolPlan>,
        cancellation: &CancellationToken,
        io: Option<&PreparationIo>,
    ) -> Result<RecursiveMergeToolBinding<P, A, O>> {
        native_io(io)?;
        check_cancellation(cancellation, io)?;
        let denied = |reason| failure(io, ErrorCode::Unauthorized, reason);
        let owner = self.source_owner(context.task(), cancellation, io).await?;
        let caller = self
            .authenticate(context, Some(&owner), true, cancellation, io)
            .await?;
        if let Some(io) = io {
            for prefix in [
                acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                acyclic_fs::access::VolumeOperation::Write.capability_prefix(),
            ] {
                crate::model::charge_items(Some(io), 1)?;
                if !io.contains_capability(
                    context.effective_scope().grants(),
                    &caller.project,
                    prefix,
                )? {
                    return Err(denied("effective scope denies its Project operation"));
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
        let allocation = allocation.ok_or_else(|| denied("merge has no notice binding"))?;
        let (snapshots, _hold) = self.snapshots(arguments.source, io).await?;
        let (mut selected, mut private, mut result) = (None, 0, 0);
        for snapshot in &snapshots {
            crate::model::charge_items(io, 1)?;
            private += usize::from(snapshot.volume == source.private_volume);
            result += usize::from(source.result_volume.as_ref() == Some(&snapshot.volume));
            if snapshot.volume.class() == crate::conversation::VolumeClass::Project
                && (snapshot.volume != source.project || selected.replace(snapshot).is_some())
            {
                return Err(denied("merge differs from its unique admitted Project"));
            }
        }
        if io.is_none() && (private != 1 || (source.result_volume.is_some() && result != 1)) {
            return Err(denied("merge source differs from its admitted allocation"));
        }
        let selected = selected.ok_or_else(|| denied("merge source has no Project"))?;
        if let Some(original) = original
            && (original.caller != caller.task
                || original.arguments != *arguments
                || &original.destination != caller.owner.issuer.audience()
                || &original.source != source.owner.issuer.audience()
                || original.target_project != caller.project
                || original.source_project != selected.volume)
        {
            return Err(denied("merge retry changed its original owners"));
        }
        let generation = original.map_or(&selected.generation, |plan| &plan.source_generation);
        let source_read = if let Some(authorization) =
            original.and_then(|plan| plan.authorized_source.as_ref())
        {
            Some(
                owner
                    .restore_project_source_authorization(context.task(), authorization, io)
                    .await?,
            )
        } else {
            let readable = match io {
                Some(io) => io.contains_capability(
                    context.effective_scope().grants(),
                    &selected.volume,
                    acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                )?,
                None => context.scope().grants().contains(
                    &selected
                        .volume
                        .capability(VolumeOperation::Read, &mut None)?,
                ),
            };
            if readable || (io.is_none() && (original.is_some() || arguments.source == caller.task))
            {
                None
            } else {
                let (issuer, provider) =
                    self.project_policy
                        .get()
                        .filter(|_| original.is_none())
                        .ok_or_else(|| denied("merge lacks its Project read authority"))?;
                #[cfg(not(target_arch = "wasm32"))]
                if let Some(io) = io {
                    io.original_native_work()
                        .ok_or_else(|| io.invalid("merge requires original native work"))?
                        .remaining()?;
                    return Err(failure(
                        Some(io),
                        ErrorCode::Unsupported,
                        "native Project policy requires originally funded installed metadata",
                    ));
                }
                Some(
                    owner
                        .authorize_project_source(
                            context.task(),
                            issuer,
                            provider.as_ref(),
                            arguments.source,
                            context.operation_id(),
                            &ProjectSourceSnapshot {
                                project: selected.volume.clone(),
                                generation: generation.clone(),
                                native_snapshot: source.native_snapshot.clone(),
                            },
                            self.config.content_budget.items_examined,
                            self.config.history_limits.maximum_bytes,
                            None,
                        )
                        .await?,
                )
            }
        };
        if let Some(handle) = &source_read {
            let snapshot = if io.is_some() {
                handle.authorization().native_snapshot.as_ref()
            } else {
                source.native_snapshot.as_ref()
            };
            handle.require_selection(
                context.operation_id(),
                arguments.source,
                &selected.volume,
                generation,
                snapshot,
                io.and_then(PreparationIo::original_native_work),
            )?;
        }
        let ordinary;
        let reader = if let Some(io) = io {
            io
        } else {
            ordinary = PreparationIo::from_budget(self.config.content_budget, cancellation)?;
            &ordinary
        };
        let notice = with_history_io(io, |work| {
            crate::wire_codec::CodecQuota::produce(work, |quota| {
                original
                    .map_or(&allocation.notice, |plan| &plan.notice)
                    .clone_in_quota(quota)
            })
            .map(|(notice, _)| notice)
        })?;
        drop(context.read_file(&notice.content, Some(reader)).await?);
        let (verifier, scope, project, source_authority, source_project) =
            with_history_io(io, |work| {
                let source_authority = match work.as_deref_mut() {
                    Some(work) => work.clone_authority(source.owner.issuer.audience())?,
                    None => source.owner.issuer.audience().clone(),
                };
                Ok::<_, Error>((
                    caller.owner.issuer.verifier(work)?,
                    caller.owner.scope.copy(work)?,
                    caller.project.copy(work)?,
                    source_authority,
                    selected.volume.copy(work)?,
                ))
            })?;
        let generation = match io {
            Some(io) => generation.clone_with_io(io)?,
            None => generation.clone(),
        };
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
