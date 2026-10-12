use super::*;

impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub async fn restore_preparation_source(&self, reader: &TaskContext, token: &TaskResourcePreparation,
        cancellation: &CancellationToken) -> Result<RetainedSourceBinding<A, O>> {
        let preparation = token.request();
        preparation.validate(&mut None)?;
        let caller = preparation.child.parent.ok_or_else(|| Error::Unauthorized("configured preparation has no original caller".into()))?;
        let binding = self.caller(caller, None)?;
        if reader.durable_task_id() != Some(token.origin().preparation_task)
            || token.origin().parent_task != caller {
            return Err(Error::Unauthorized("source restore lacks the actual sealed helper and original caller".into()));
        }
        let runtime = self.runtime(None)?;
        let admitted = runtime.task_host().observe_admission(caller).await?;
        if admitted.parent.is_some() {
            let actual = runtime.inspect_source(caller, caller).await?;
            if actual.project()?.volume != binding.project
                || actual.volumes().iter().filter(|snapshot| snapshot.volume == binding.private_volume).count() != 1
                || binding.result_volume.as_ref().is_some_and(|result|
                    actual.volumes().iter().filter(|snapshot| &snapshot.volume == result).count() != 1) {
                return Err(Error::Unauthorized("restored descendant cannot reuse another caller's source volumes".into()));
            }
        }
        let request: ForkRequest = serde_json::from_value(preparation.payload.clone()).map_err(invalid_json)?;
        let restored;
        let aggregate;
        let reducer = if let Some(checkpoint) = &preparation.source_checkpoint {
            drop(reader.read_file(checkpoint).await?);
            let receipt = reader.read_file_measured(checkpoint, self.config.content_budget, cancellation)
                .await.map_err(|failure| failure.error)?;
            let capture: SourceCheckpoint = serde_json::from_slice(&receipt.value).map_err(invalid_json)?;
            if capture.request != request || capture.caller != caller {
                return Err(Error::Unauthorized("configured preparation checkpoint changed original caller/request".into()));
            }
            restored = self.restore_snapshot(&binding.owner, &request, capture.snapshot, cancellation, None).await?;
            &restored
        } else if let Some(original) = self.config.original_sources.iter().find(|original| original.request.operation_id == request.operation_id) {
            if original.caller != caller || original.request != request {
                return Err(Error::Unauthorized("trusted original record differs from the sealed preparation request".into()));
            }
            restored = self.restore_snapshot(&binding.owner, &request, original.snapshot.copy(&mut None)?, cancellation, None).await?;
            &restored
        } else {
            let authority = binding.owner.authority().clone();
            let authority_verifier = verifier(&binding.owner)?;
            let mut open_work = crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_budget(self.config.content_budget, cancellation)?);
            let opened = async {
                open_work.preflight()?;
                StreamAggregate::open(&self.stream, authority, authority_verifier, SchemaRegistry::default(), None, self.config.history_limits, crate::store::DEFAULT_TERMINAL_EFFECTS, Some(request.parent_revision), &mut Some(&mut open_work)).await
            }.await;
            aggregate = open_work.finish(opened).map_err(|failure| failure.error)?.value;
            aggregate.reducer()
        };
        restore_recursive_fork_source(self.filesystem.clone(), &self.stream, reducer, verifier(&binding.owner)?,
            binding.owner.scope.clone(), binding.project.clone(), self.config.stream_provider.clone(), request, self.config.history_limits,
            token.project_source().cloned(), self.ordinary_resolver(reader, cancellation).await?, cancellation).await
    }
    pub(super) async fn restore_snapshot(
        &self, binding: &ConfiguredRecursiveAuthorityBinding, request: &ForkRequest, snapshot: Snapshot,
        cancellation: &CancellationToken, io: Option<&crate::filesystem::PreparationIo>,
    ) -> Result<Reducer> {
        use crate::filesystem::fork_preparer_io::failure;
        use crate::wire::ErrorCode;
        if snapshot.authority != request.parent || snapshot.revision != request.parent_revision
            || &snapshot.authority != binding.authority() || snapshot.revision == 0 {
            return Err(failure(io, ErrorCode::Unauthorized, "configured checkpoint differs from original caller cut"));
        }
        let check = || cancellation.check().map_err(|error| {
            #[cfg(not(target_arch = "wasm32"))]
            if io.and_then(crate::filesystem::PreparationIo::original_native_work).is_some() {
                return Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled);
            }
            Error::Storage(error.to_string())
        });
        check()?;
        let mut history = io.map(crate::filesystem::PreparationIo::history_io);
        let mut observer = history.as_mut().map(|work| work as &mut dyn HistoryIo);
        let verifier = binding.issuer().verifier(&mut observer)?;
        let expected = snapshot.events.last().ok_or_else(|| failure(io, ErrorCode::Unauthorized, "checkpoint has no canonical head"))?;
        let head_hold;
        let (head, _) = crate::store::operations::bounded_operation_with_retained_quota(
            &self.stream, binding.authority(), &verifier, expected.operation_id,
            self.config.history_limits.maximum_bytes, true, &mut observer).await?;
        head_hold = io.map(|io| io.guard_owned(head.as_ref().map_or(0, |(_, retained)| *retained))).transpose()?;
        let head = (head, head_hold);
        check()?;
        if head.0.as_ref().map(|(event, _)| event) != Some(expected) {
            return Err(failure(io, ErrorCode::Unauthorized, "checkpoint head differs from immutable canonical Stream event"));
        }
        drop(head);
        Reducer::restore(snapshot, verifier, SchemaRegistry::new(), &mut observer)
    }
}
use super::*;

#[cfg(not(target_arch = "wasm32"))]
impl<P, A, O> ConfiguredRecursiveToolSource<P, A, O>
where P: StreamProvider + 'static, A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static {
    pub(super) async fn native_restore_source(
        &self, context: &ToolContext, caller: &ConfiguredRecursiveCaller,
        owner: Arc<crate::durable_host::TaskJournalOwner<P>>,
        request: (ForkRequest, acyclic_fs::NativeAllocation),
        restored: Option<Reducer>,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)> {
        let selected = match &request.0.authorized_source {
            Some(authorization) => Some(owner.restore_project_source_authorization_with_native_work(
                context.task(), authorization, work,
            )?.await?),
            None => None,
        };
        let mut _pinned_custody = None;
        let reducer = match restored {
            Some(reducer) => reducer,
            None => {
                let pinned = self.native_open_aggregate(
                    &caller.owner, Some(request.0.parent_revision), cancellation, work,
                ).await?;
                _pinned_custody = Some(pinned.1);
                pinned.0.into_reducer()
            }
        };
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        let verifier = {
            let mut history = io.history_io();
            let mut observer: Option<&mut dyn crate::store::history::HistoryIo> = Some(&mut history);
            caller.owner.issuer().verifier(&mut observer)?
        };
        let resolver = self.native_resolver(context, owner, work)?;
        crate::filesystem::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, reducer, &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, request, self.config.history_limits,
            selected.as_ref().map(|selected| &selected.0), resolver, cancellation, work,
        ).await
    }

    pub(super) async fn native_preparation_source(
        &self, context: &Arc<crate::runtime::TaskContext>, token: &TaskResourcePreparation,
        cancellation: &CancellationToken, work: &acyclic_fs::NativeMaterializationWork,
        reconciliation_only: bool,
    ) -> Result<(RetainedSourceBinding<A, O>, acyclic_fs::NativeAllocation)> {
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        let task_context = context;
        task_context.owned_task_binding_with_native_work(work)?;
        if reconciliation_only {
            token.verify_reconciliation_with_native_work(work)?.await?;
        } else {
            task_context.require_original_native_dispatch(work)?;
            token.verify_creation_with_native_work(work)?.await?;
        }
        let owner = self.native_source_owner(task_context, cancellation, work).await?;
        let resolver = crate::runtime::content_residency_verifier(Arc::clone(context), owner, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let preparation = token.request();
        let io = crate::filesystem::PreparationIo::from_native_work(work)?;
        {
            let mut history = io.history_io();
            preparation.validate(&mut Some(&mut history))?;
        }
        let caller_task = preparation.child.parent.ok_or(Error::NativeBoundary {
            code: crate::wire::ErrorCode::Unauthorized,
            reason: "native recursive preparation has no original caller",
        })?;
        let caller = self.caller(caller_task, Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let request = crate::filesystem::retain_decoded(io.value_decode_preparation::<ForkRequest>(&preparation.payload, crate::wire::ErrorCode::Storage)?);
        let runtime = self.runtime(Some(&crate::filesystem::PreparationIo::from_native_work(work)?))?;
        let admission = runtime.task_host().observe_admission_with_native_work(caller_task, work).await?;
        if admission.0.parent.is_some() {
            let snapshots = runtime.task_host().observe_volume_snapshots_with_native_work(caller_task, work).await?;
            let mut project = 0;
            let mut scratch = 0;
            for snapshot in &snapshots.0 {
                io.codec_charge(crate::contract::JsonCodecCharge::Work { bytes: 0, items: 1 }, true)?;
                project += usize::from(snapshot.volume == caller.project);
                scratch += usize::from(snapshot.volume == caller.private_volume);
            }
            if project != 1 || scratch != 1 {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native restored preparation differs from original caller allocation" });
            }
        }
        if request.authorized_source.as_ref() != token.project_source().map(ProjectSourceReadHandle::authorization) {
            return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                reason: "native preparation changed its retained sealed Project source" });
        }
        let restored = if let Some(checkpoint) = &preparation.source_checkpoint {
            let retained = context.read_file_with_native_work(checkpoint, work).await?;
            let capture = crate::filesystem::retain_decoded(io.decode(std::marker::PhantomData::<SourceCheckpoint>, &retained, crate::wire::ErrorCode::Storage)?);
            if capture.request != request || capture.caller != caller_task {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native preparation checkpoint changed original caller or request" });
            }
            let snapshot_bytes = capture.snapshot.predecoded_native_owned_bytes()?;
            let snapshot_guard = io.take_native_custody(snapshot_bytes)?;
            self.restore_snapshot(&caller.owner, &request, capture.snapshot.adopt_predecoded_native_custody(snapshot_guard)?, cancellation, Some(&io)).await?
        } else if let Some(original) = self.config.original_sources.iter()
            .find(|original| original.request.operation_id == request.operation_id) {
            if original.caller != caller_task || original.request != request {
                return Err(Error::NativeBoundary { code: crate::wire::ErrorCode::Unauthorized,
                    reason: "native trusted original source record differs from sealed preparation" });
            }
            let snapshot = original.snapshot.copy(&mut Some(&mut io.history_io()))?;
            self.restore_snapshot(&caller.owner, &request, snapshot, cancellation, Some(&io)).await?
        } else {
            let original = self.native_open_aggregate(&caller.owner, Some(request.parent_revision), cancellation, work).await?;
            io.absorb_native_custody(original.1)?;
            original.0.into_reducer()
        };
        let verifier = {
            let mut history = io.history_io();
            caller.owner.issuer().verifier(&mut Some(&mut history))?
        };
        let copied_request = {
            let mut history = io.history_io();
            request.copy(&mut Some(&mut history))?
        };
        let request_guard = io.take_native_custody(copied_request.1)?;
        let source = crate::filesystem::recursive::restore_recursive_fork_source_with_native_work(
            Arc::clone(&self.filesystem), &self.stream, restored, &verifier, &caller.owner.scope,
            &caller.project, &self.config.stream_provider, (copied_request.0, request_guard),
            self.config.history_limits, token.project_source(), resolver, cancellation, work,
        ).await?;
        if reconciliation_only {
            token.verify_reconciliation_with_native_work(work)?.await?;
        } else {
            token.verify_creation_with_native_work(work)?.await?;
        }
        cancellation.check().map_err(|_| Error::from(acyclic_fs::NativeTreeMaterializationError::Cancelled))?;
        Ok(source)
    }
}
