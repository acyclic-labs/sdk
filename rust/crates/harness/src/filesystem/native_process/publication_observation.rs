use super::*;
use crate::conversation::{VolumeClass, VolumeOperation, VolumeRef};
use crate::filesystem::{NativeVolumeNamespace, TaskSourceInspection};
use acyclic_fs::{AsyncAuthorityStore, AsyncObjectStore, LazySnapshotRef, NativeAllocation, NativeMaterializationWork, WorkCounters};

fn boundary(code: crate::wire::ErrorCode, reason: &'static str) -> Error {
    Error::NativeBoundary { code, reason }
}

fn native(cause: acyclic_fs::NativeTreeMaterializationError) -> Error {
    Error::NativeWork { cause, custody: None }
}

impl<P: StreamProvider + 'static> NativeProcessProvider<P> {
    fn publication_read_preflight(&self, attempt: crate::EffectAttemptId,
        project: &VolumeRef, work: &NativeMaterializationWork) -> Result<()> {
        finite_dispatch::require_finite(work)?;
        if self._custody.is_none() {
            return Err(boundary(crate::wire::ErrorCode::Unsupported,
                "published native metadata requires original provider payload custody"));
        }
        if attempt != self.original_attempt() {
            return Err(boundary(crate::wire::ErrorCode::Conflict,
                "published native metadata differs from the original attempt"));
        }
        if project.class() != VolumeClass::Project {
            return Err(boundary(crate::wire::ErrorCode::Unauthorized,
                "published native metadata requires a selected Project volume"));
        }
        if !self.authority.content.supports_native_read(self.plan.request.volume(), work)?
            || !self.authority.result_content.supports_native_read(self.authority.results.volume(), work)? {
            return Err(boundary(crate::wire::ErrorCode::Unsupported,
                "published native metadata requires actual finite request and result readers"));
        }
        Ok(())
    }

    /// Reads only authentic original final-publication metadata.
    pub fn published_project_candidate_with_native_work<'a>(
        &'a self, attempt: crate::EffectAttemptId, project: &'a VolumeRef,
        work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (LazySnapshotRef, NativeAllocation)>> {
        self.publication_read_preflight(attempt, project, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            self.original_published_project_with_native_work(attempt, project, work).await
        })
    }

    /// Selects the same accepted immutable final snapshot only after the actual operation-bound Common Project reader authenticates current READ.
    pub fn inspect_published_project_snapshot_with_native_work<'a, A, O>(
        &'a self, attempt: crate::EffectAttemptId,
        inspection: &'a TaskSourceInspection<P, A, O>, project: &'a VolumeRef,
        candidate: LazySnapshotRef, work: &'a NativeMaterializationWork,
    ) -> Result<crate::NativeFutureBundle<'a, (LazySnapshotRef, NativeAllocation)>>
    where
        A: AsyncAuthorityStore + 'static,
        O: AsyncObjectStore + 'static,
    {
        self.publication_read_preflight(attempt, project, work)?;
        crate::NativeFutureBundle::new(work, || async move {
            let authorization = inspection.project_authorization().ok_or(boundary(
                crate::wire::ErrorCode::Unsupported,
                "published native source requires the genuine operation-bound Project reader"))?;
            let generation = &authorization.generation;
            inspection.require_native_project_selection_with_native_work(
                self.owner.task_binding().0, project, generation, Some(candidate), work,
            ).await?;
            let (snapshot, custody) = self.original_published_project_with_native_work(
                attempt, project, work,
            ).await?;
            let custody = custody;
            let snapshot = snapshot;
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
            if snapshot != candidate {
                return Err(boundary(crate::wire::ErrorCode::Conflict,
                    "published native source candidate changed its original accepted snapshot"));
            }
            inspection.require_native_project_selection_with_native_work(
                self.owner.task_binding().0, project, generation, Some(snapshot), work,
            ).await?;
            Ok((snapshot, custody))
        })
    }

    async fn original_published_project_with_native_work(
        &self, attempt: crate::EffectAttemptId, project: &VolumeRef,
        work: &NativeMaterializationWork,
    ) -> Result<(LazySnapshotRef, NativeAllocation)> {
        let custody = work.retain_owned(std::mem::size_of::<(LazySnapshotRef, NativeAllocation)>() as u64)
            .map_err(native)?;
        let (before, before_custody) = self.owner.outcome_with_native_work(work).await?;
        let before_custody = before_custody;
        let before = before;
        if before.is_none() { self.owner.verify_with_native_work(true, work).await?; }
        if before.as_ref().is_some_and(|outcome| !matches!(outcome, crate::Outcome::Succeeded(_))) {
            return Err(Error::Indeterminate(self.command));
        }
        let request = self.original_request_with_native_work(work).await?;
        let receipt = self.approved_original_receipt_with_native_work(
            attempt, request.request(), work,
        ).await?;
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
        if (receipt.task, &receipt.original_fence) != self.owner.task_binding()
            || receipt.command != self.command || !receipt.observed
            || receipt.publication_failure.is_some() || receipt.invocation_failure.is_some() {
            return Err(Error::Indeterminate(self.command));
        }
        let EffectStatus::Succeeded { result } = &receipt.effect.status else {
            return Err(Error::Indeterminate(self.command));
        };
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
        if result.volume() != self.authority.results.volume()
            || result.descriptor().byte_length() > u64::from(request.request().maximum_result_bytes)
            || result.descriptor().media_type() != "application/json" {
            return Err(boundary(crate::wire::ErrorCode::Conflict,
                "published native metadata has no pinned original result"));
        }
        self.owner.require_volume_grant_with_native_work(result.volume(), VolumeOperation::Read, work)?;
        let bytes = self.authority.result_content.read_with_native_work(
            result, u64::from(request.request().maximum_result_bytes), work,
        )?.await?;
        let (result, result_custody) = NativeProcessResult::decode_with_native_work(bytes.as_ref(), work)?;
        let result_custody = result_custody;
        let result = result;
        if result.stop.as_ref().is_some_and(|stop| !stop.cleanup_completed) {
            return Err(Error::Indeterminate(self.command));
        }
        drop(result);
        drop(result_custody);
        drop(bytes);

        let mut selected = None;
        for (index, mapping) in request.request().view.volumes.iter().enumerate() {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
            if &mapping.volume != project { continue; }
            if selected.replace(index).is_some() {
                return Err(boundary(crate::wire::ErrorCode::Conflict,
                    "published native project has more than one original mapping"));
            }
        }
        let index = selected.ok_or(boundary(crate::wire::ErrorCode::Unauthorized,
            "published native project is absent from the original approved request"))?;
        let mapping = &request.request().view.volumes[index];
        let NativeVolumeNamespace::LazyMount { snapshot: baseline } = &mapping.namespace else {
            return Err(boundary(crate::wire::ErrorCode::Unsupported,
                "published native project is not an original qualified lazy namespace"));
        };
        let mut accepted = None;
        let mut previous = None;
        for published in &receipt.published {
            work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
            if published.index >= request.request().view.volumes.len()
                || previous.is_some_and(|previous| previous >= published.index) {
                return Err(boundary(crate::wire::ErrorCode::Conflict,
                    "published native metadata differs from the original ordered manifest"));
            }
            previous = Some(published.index);
            if published.index == index { accepted = Some(&published.snapshot); }
        }
        let accepted = accepted.ok_or(Error::Indeterminate(self.command))?;
        let snapshot = accepted.snapshot().lazy_snapshot_with_native_work(work, work.cancellation())?
            .ok_or(boundary(crate::wire::ErrorCode::Conflict,
                "published native project changed its SDK namespace kind"))?;
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
        if snapshot.workspace_id != baseline.workspace_id || snapshot.source.identity != baseline.source.identity {
            return Err(boundary(crate::wire::ErrorCode::Conflict,
                "published native project changed its original immutable namespace"));
        }
        let (after, after_custody) = self.owner.outcome_with_native_work(work).await?;
        let after_custody = after_custody;
        let after = after;
        if after.is_none() { self.owner.verify_with_native_work(true, work).await?; }
        work.charge(WorkCounters { items_examined: 1, ..WorkCounters::UNCHARGED }).map_err(native)?;
        if after.as_ref().is_some_and(|outcome| !matches!(outcome, crate::Outcome::Succeeded(_)))
            || before.is_some() && before != after {
            return Err(Error::Indeterminate(self.command));
        }
        work.remaining().map_err(native)?;
        drop(after);
        drop(after_custody);
        drop(receipt);
        drop(request);
        drop(before);
        drop(before_custody);
        Ok((snapshot, custody))
    }
}
