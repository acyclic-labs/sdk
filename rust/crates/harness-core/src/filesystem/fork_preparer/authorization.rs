//! Original-parent and preparation-task authority checks.
use super::*;
use crate::executor::{stock_boundary, with_history_io};
use crate::wire::ErrorCode::{Conflict, Invalid, Unauthorized, Unsupported};

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn require_native_projection_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining()?;
        if self
            .native
            .as_ref()
            .is_none_or(|custody| !custody.work().shares_allocation_context(work))
        {
            return Err(Error::NativeBoundary {
                code: Unauthorized,
                reason: "fork projection belongs to another original resource",
            });
        }
        Ok(())
    }

    pub(super) fn sealed_project_read<'a>(
        &'a self,
        request: &ForkRequest,
        volume: &VolumeRef,
        generation: &crate::resources::GenerationRef,
        io: Option<&PreparationIo>,
        task: Option<&'a TaskResourcePreparation>,
    ) -> Result<&'a ContentGrant> {
        let reject = |reason| stock_boundary(io, Unauthorized, reason);
        let authorization = request
            .authorized_source
            .as_ref()
            .ok_or_else(|| reject("fork source has no sealed project read"))?;
        let handle = task
            .and_then(TaskResourcePreparation::project_source)
            .or(self.project_source.as_ref())
            .ok_or_else(|| reject("fork source descriptor has no sealed project read"))?;
        if handle.authorization() != authorization {
            return Err(reject(
                "fork source descriptor differs from its sealed project read",
            ));
        }
        if authorization.source_task != authorization.caller_task
            && (request.preparation.inherited_through_sequence != 0
                || request.preparation.summary.is_some())
        {
            return Err(stock_boundary(
                io,
                Unsupported,
                "foreign workspace source cannot inherit conversation history",
            ));
        }
        handle.require_selection(
            request.operation_id,
            authorization.source_task,
            volume,
            generation,
            authorization.native_snapshot.as_ref(),
            io.and_then(PreparationIo::original_native_work),
        )?;
        Ok(handle.grant())
    }

    pub(super) fn check_request(
        &self,
        request: &ForkRequest,
        io: Option<&PreparationIo>,
        task: Option<&TaskResourcePreparation>,
    ) -> Result<()> {
        #[cfg(not(target_arch = "wasm32"))]
        if let Some(work) = io.and_then(PreparationIo::original_native_work) {
            self.require_native_projection_work(work)?;
        }
        let reject = |code, reason| stock_boundary(io, code, reason);
        let read_granted = |volume: &VolumeRef| -> Result<bool> {
            let parent = match io {
                Some(io) => io.contains_capability(
                    self.scope.capabilities(),
                    volume,
                    acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                )?,
                None => self
                    .scope
                    .capabilities()
                    .contains(&volume.capability(VolumeOperation::Read, &mut None)?),
            };
            if !parent {
                return Ok(false);
            }
            match task {
                Some(task) => io
                    .ok_or_else(|| reject(Invalid, "task preparation lacks its IO ledger"))?
                    .contains_capability(
                        &task.origin().source_grants,
                        volume,
                        acyclic_fs::access::VolumeOperation::Read.capability_prefix(),
                    ),
                None => Ok(true),
            }
        };
        with_history_io(io, |work| request.validate(work))?;
        if request.parent.kind == crate::core::AggregateKind::Task
            && task.is_some_and(|task| task.origin().owner != *self.parent.authority())
        {
            return Err(reject(
                Unauthorized,
                "task fork requires its original preparation owner",
            ));
        }
        if !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(reject(
                Unauthorized,
                "parent project operation is not granted",
            ));
        }
        if request.parent != *self.parent.authority()
            || request.parent_revision != self.parent.revision()
        {
            return Err(reject(
                Conflict,
                "fork request differs from its original parent revision",
            ));
        }
        if request.boundary.is_some() {
            return Err(reject(
                Unsupported,
                "this preparer cannot attest a cross-provider boundary",
            ));
        }
        if self.scope.agent() == Some(request.child_agent) {
            return Err(reject(
                Invalid,
                "fork child must have a distinct agent identity",
            ));
        }
        if creation_volumes(request).any(|volume| volume.provider() != &self.host.provider) {
            return Err(reject(Invalid, "fork resource belongs to another provider"));
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference)
                    if reference.as_resource().provider() != &self.stream_provider =>
                {
                    return Err(reject(
                        Invalid,
                        "fork history belongs to another Stream provider",
                    ));
                }
                ResourceRevision::Project { volume, generation } => {
                    if request.authorized_source.is_some() {
                        self.sealed_project_read(request, volume, generation, io, task)?
                            .require(volume, VolumeOperation::Read, io)?;
                    } else if !read_granted(volume)? {
                        return Err(reject(
                            Unauthorized,
                            "selected project source-read is not granted",
                        ));
                    }
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    let agent = self.scope.agent().ok_or_else(|| {
                        Error::Unauthorized("fork scope has no parent agent".into())
                    })?;
                    if volume.owner() != &crate::conversation::VolumeOwner::Agent(agent)
                        || !read_granted(volume)?
                    {
                        return Err(reject(
                            Unauthorized,
                            "scratch source-read is outside the direct parent",
                        ));
                    }
                }
                _ => {}
            }
        }
        if let Some(task) = task {
            let io = io.ok_or_else(|| reject(Invalid, "task preparation lacks its IO ledger"))?;
            let bound = task.request();
            if task.origin().intended_child != bound.child.operation_id
                || bound.child.parent != Some(task.origin().parent_task)
            {
                return Err(reject(
                    Unauthorized,
                    "fork preparation differs from its genuine immutable owner admission",
                ));
            }
            if self.task_preparation.as_deref().is_some_and(|original| {
                original.request() != bound
                    || !same_task_origin(Some(original.origin()), Some(task.origin()))
            }) {
                return Err(reject(
                    Unauthorized,
                    "fork preparer belongs to another genuine preparation task",
                ));
            }
            if bound.provider != self.host.provider
                || bound.creation_grant != capability::FORK_PUBLISH
                || bound.read_authorization != request.authorized_source
                || request.preparation.maximum_inherited_bytes > bound.maximum_bytes
                || bound.resources().count() != creation_volumes(request).count()
            {
                return Err(reject(
                    Unauthorized,
                    "fork differs from its admitted preparation boundary",
                ));
            }
            {
                let encoded = io.encode(request, bound.maximum_bytes, true)?;
                let encoded_hold = io.guard_bytes(&encoded)?;
                let payload = io.encode(&bound.payload, bound.maximum_bytes, true)?;
                let payload_hold = io.guard_bytes(&payload)?;
                if encoded != payload {
                    return Err(reject(
                        Unauthorized,
                        "fork request differs from its admitted preparation",
                    ));
                }
                drop((payload, encoded));
                drop((payload_hold, encoded_hold));
            }
            for (resource, volume) in bound.resources().zip(creation_volumes(request)) {
                let before = io.owned_bytes()?;
                let actual = bounded_io::content_workspace(&self.host.provider, volume, Some(io))?
                    .into_resource();
                let hold = io.guard_owned(io.owned_bytes()? - before)?;
                if resource != &actual {
                    return Err(reject(
                        Unauthorized,
                        "fork allocations differ from their admitted preparation",
                    ));
                }
                drop(actual);
                drop(hold);
            }
            match (&request.authorized_source, task.project_source()) {
                (Some(authorization), Some(handle))
                    if handle.authorization() == authorization
                        && authorization.owner == task.origin().owner
                        && authorization.caller_task == task.origin().parent_task => {}
                (None, None) => {}
                _ => {
                    return Err(reject(
                        Unauthorized,
                        "fork preparation lacks its original owner's project source",
                    ));
                }
            }
        }
        if let Some(history) = &self.history {
            if history.through_sequence() != request.preparation.inherited_through_sequence {
                return Err(reject(
                    Conflict,
                    "fork archive differs from its selected logical cut",
                ));
            }
        } else if self.parent.conversation().is_some_and(|state| {
            state.resident_after_sequence() > 0
                && request.preparation.inherited_through_sequence > 0
        }) {
            return Err(reject(
                Invalid,
                "archived fork requires its authenticated history capture",
            ));
        }
        if let Some(selection) = &request.preparation.summary {
            #[cfg(not(target_arch = "wasm32"))]
            if self.native.is_some() != io.and_then(PreparationIo::original_native_work).is_some() {
                return Err(reject(
                    Unauthorized,
                    "summary preparation requires its original native work",
                ));
            }
            let summary = self.summary.as_ref().ok_or_else(|| {
                reject(
                    Unsupported,
                    "summary fork owner-bound checkpoint provider is not installed",
                )
            })?;
            if summary.prepared.selection() != selection
                || summary.prepared.through_sequence()
                    != request.preparation.inherited_through_sequence
                || self
                    .parent
                    .conversation()
                    .map_or(0, |state| state.logical_revision())
                    != request.preparation.inherited_through_sequence
            {
                return Err(reject(
                    Conflict,
                    "summary fork differs from its original capture",
                ));
            }
        }
        Ok(())
    }
}
