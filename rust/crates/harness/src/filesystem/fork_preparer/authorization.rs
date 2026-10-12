//! Original-parent and preparation-task authority checks.
use super::*;
impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> FilesystemForkPreparer<A, O> {
    #[cfg(not(target_arch = "wasm32"))]
    pub(super) fn require_native_projection_work(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<()> {
        work.remaining().map_err(Error::from)?;
        if !self
            .native
            .as_ref()
            .is_some_and(|(_, custody)| custody.matches_work(work))
        {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unauthorized,
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
        let authorization = request.authorized_source.as_ref().ok_or_else(|| {
            bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "fork source has no sealed project read",
            )
        })?;
        let handle = task
            .and_then(TaskResourcePreparation::project_source)
            .or(self.project_source.as_ref())
            .ok_or_else(|| {
                bounded_io::failure(
                    io,
                    crate::wire::ErrorCode::Unauthorized,
                    "fork source descriptor has no sealed project read",
                )
            })?;
        if handle.authorization() != authorization {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unauthorized,
                "fork source descriptor differs from its sealed project read",
            ));
        }
        if authorization.source_task != authorization.caller_task
            && (request.preparation.inherited_through_sequence != 0
                || request.preparation.summary.is_some())
        {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unsupported,
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
        let reject = |code, reason| bounded_io::failure(io, code, reason);
        let read_granted = |volume: &VolumeRef| -> Result<bool> {
            let parent = match io {
                Some(io) => {
                    io.contains_capability(self.scope.capabilities(), volume, "volume:read:")?
                }
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
                    .ok_or_else(|| {
                        reject(
                            crate::wire::ErrorCode::Invalid,
                            "task preparation lacks its IO ledger",
                        )
                    })?
                    .contains_capability(task.grants(), volume, "volume:read:"),
                None => Ok(true),
            }
        };
        let mut history = io.map(PreparationIo::history_io);
        let mut observer = history
            .as_mut()
            .map(|history| history as &mut dyn crate::store::history::HistoryIo);
        request.validate(&mut observer)?;
        if !self.scope.capabilities().contains(capability::FORK_PUBLISH) {
            return Err(reject(
                crate::wire::ErrorCode::Unauthorized,
                "parent project operation is not granted",
            ));
        }
        if request.parent != *self.parent.authority()
            || request.parent_revision != self.parent.revision()
        {
            return Err(reject(
                crate::wire::ErrorCode::Conflict,
                "fork request differs from its original parent revision",
            ));
        }
        if request.boundary.is_some() {
            return Err(reject(
                crate::wire::ErrorCode::Unsupported,
                "this preparer cannot attest a cross-provider boundary",
            ));
        }
        if self.scope.agent() == Some(request.child_agent) {
            return Err(reject(
                crate::wire::ErrorCode::Invalid,
                "fork child must have a distinct agent identity",
            ));
        }
        for (volume, _) in creation_volumes(request) {
            if volume.provider() != &self.host.provider {
                return Err(reject(
                    crate::wire::ErrorCode::Invalid,
                    "fork resource belongs to another provider",
                ));
            }
        }
        for selection in &request.selections {
            match &selection.revision {
                ResourceRevision::History(reference)
                    if reference.as_resource().provider() != &self.stream_provider =>
                {
                    return Err(reject(
                        crate::wire::ErrorCode::Invalid,
                        "fork history belongs to another Stream provider",
                    ));
                }
                ResourceRevision::Project { volume, generation } => {
                    if request.authorized_source.is_some() {
                        self.sealed_project_read(request, volume, generation, io, task)?
                            .require(volume, VolumeOperation::Read, io)?;
                    } else if !read_granted(volume)? {
                        return Err(reject(
                            crate::wire::ErrorCode::Unauthorized,
                            "selected project source-read is not granted",
                        ));
                    }
                }
                ResourceRevision::PrivateVolume { volume, .. } => {
                    let agent = self
                        .scope
                        .agent()
                        .expect("fork scope is bound to its parent agent");
                    if volume.owner() != &crate::conversation::VolumeOwner::Agent(agent)
                        || !read_granted(volume)?
                    {
                        return Err(reject(
                            crate::wire::ErrorCode::Unauthorized,
                            "scratch source-read is outside the direct parent",
                        ));
                    }
                }
                _ => {}
            }
        }
        drop(observer);
        drop(history);
        if let Some(task) = task {
            let io = io.ok_or_else(|| {
                reject(
                    crate::wire::ErrorCode::Invalid,
                    "task preparation lacks its IO ledger",
                )
            })?;
            let bound = task.request();
            if task.origin().intended_child != bound.child.operation_id
                || bound.child.parent != Some(task.origin().parent_task)
                || task.grants() != &task.origin().source_grants
            {
                return Err(reject(
                    crate::wire::ErrorCode::Unauthorized,
                    "fork preparation differs from its genuine immutable owner admission",
                ));
            }
            if let Some(original) = self.task_preparation.as_deref() {
                if original.request() != bound
                    || !same_task_origin(Some(original.origin()), Some(task.origin()))
                {
                    return Err(reject(
                        crate::wire::ErrorCode::Unauthorized,
                        "fork preparer belongs to another genuine preparation task",
                    ));
                }
            }
            if bound.provider != self.host.provider
                || bound.creation_grant != capability::FORK_PUBLISH
                || bound.read_authorization != request.authorized_source
                || request.preparation.maximum_inherited_bytes > bound.maximum_bytes
                || bound.resources.len() != creation_volumes(request).count()
            {
                return Err(reject(
                    crate::wire::ErrorCode::Unauthorized,
                    "fork differs from its admitted preparation boundary",
                ));
            }
            let request_hold;
            let encoded = io.encode(request, bound.maximum_bytes, true)?;
            request_hold = io.guard_bytes(&encoded)?;
            let payload_hold;
            let payload = io.encode(&bound.payload, bound.maximum_bytes, true)?;
            payload_hold = io.guard_bytes(&payload)?;
            if encoded != payload {
                return Err(reject(
                    crate::wire::ErrorCode::Unauthorized,
                    "fork request differs from its admitted preparation",
                ));
            }
            drop(payload);
            drop(payload_hold);
            drop(encoded);
            drop(request_hold);
            for (resource, (volume, _)) in bound.resources.iter().zip(creation_volumes(request)) {
                let before = io.owned_bytes()?;
                let resource_hold;
                let actual = bounded_io::content_workspace(&self.host.provider, volume, Some(io))?
                    .into_resource();
                resource_hold = io.guard_owned(io.owned_bytes()? - before)?;
                if resource != &actual {
                    return Err(reject(
                        crate::wire::ErrorCode::Unauthorized,
                        "fork allocations differ from their admitted preparation",
                    ));
                }
                drop(actual);
                drop(resource_hold);
            }
            match (&request.authorized_source, task.project_source()) {
                (Some(authorization), Some(handle))
                    if handle.authorization() == authorization
                        && authorization.owner == task.origin().owner
                        && authorization.caller_task == task.origin().parent_task => {}
                (None, None) => {}
                _ => {
                    return Err(reject(
                        crate::wire::ErrorCode::Unauthorized,
                        "fork preparation lacks its original owner's project source",
                    ));
                }
            }
        }
        if let Some(history) = &self.history {
            if history.through_sequence() != request.preparation.inherited_through_sequence {
                return Err(reject(
                    crate::wire::ErrorCode::Conflict,
                    "fork archive differs from its selected logical cut",
                ));
            }
        } else if self.parent.conversation().is_some_and(|state| {
            state.resident_after_sequence() > 0
                && request.preparation.inherited_through_sequence > 0
        }) {
            return Err(reject(
                crate::wire::ErrorCode::Invalid,
                "archived fork requires its authenticated history capture",
            ));
        }
        if let Some(selection) = &request.preparation.summary {
            let summary = self.summary.as_ref().ok_or(reject(
                crate::wire::ErrorCode::Unsupported,
                "summary fork owner-bound checkpoint provider is not installed",
            ))?;
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
                    crate::wire::ErrorCode::Conflict,
                    "summary fork differs from its original capture",
                ));
            }
        }
        Ok(())
    }

    pub(super) fn prepare_bound_summary(
        &self,
        request: &ForkRequest,
        io: Option<&PreparationIo>,
    ) -> Result<Option<&PreparedSummaryFork>> {
        let Some(selection) = &request.preparation.summary else {
            return Ok(None);
        };
        #[cfg(not(target_arch = "wasm32"))]
        match (
            &self.native,
            io.and_then(PreparationIo::original_native_work),
        ) {
            (Some(_), Some(work)) => self.require_native_projection_work(work)?,
            (None, None) => {}
            _ => {
                return Err(Error::NativeBoundary {
                    code: crate::wire::ErrorCode::Unauthorized,
                    reason: "summary preparation requires its original native work",
                });
            }
        }
        let bound = self.summary.as_ref().ok_or_else(|| {
            bounded_io::failure(
                io,
                crate::wire::ErrorCode::Unsupported,
                "summary fork owner-bound checkpoint provider is not installed",
            )
        })?;
        if selection != bound.prepared.selection()
            || request.preparation.inherited_through_sequence != bound.prepared.through_sequence()
        {
            return Err(bounded_io::failure(
                io,
                crate::wire::ErrorCode::Conflict,
                "summary fork request differs from its original capture",
            ));
        }
        Ok(Some(&bound.prepared))
    }
}
