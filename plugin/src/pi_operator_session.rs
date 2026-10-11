//! Host-only signed operator requests and passive original writeback observations.
//! These controls reuse the original query work; approval forwards the genuine
//! Root checkpoint custody, and passive status uses the caller's native binding.
use super::*;

impl PiSession {
    pub(super) async fn operator_control(&mut self, event: &str, arguments: &Value,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let work = Self::query_work(original_native_work)?;
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(
            &self.resources.private_state_root, &self.relative_directory, &work,
        )?;
        self.runtime.verify_owner_with_native_work(&owner, &work)?;
        let cancellation = work.cancellation();
        let request = pi_operator::PiOperatorRequest::from_value_with_native_work(arguments, &work)?;
        pi_operator::verify_request(&owner, &self.state.root_session_id, &request.0, &work)?;
        if event == "OperatorInspect" {
            if self.roots.is_empty() { self.restore_root_sources(&work).await?; }
            return pi_operator_host::inspect_operator(&self.runtime, &self.filesystem, &self.stream, &self.resources, &owner,
                &self.state.root_session_id, &self.state, &self.roots, &request.0, &work).await
                .map(|(value, allocation)| (value, Some(allocation)));
        }
        if event != "OperatorApproval" { return Err(RootError::Invalid("unknown trusted Root operator control".to_owned())); }
        // Invalid consent must not reserve/poison the immutable publish slot.
        pi_operator_host::validate_approval_request(&self.runtime, &self.filesystem, &owner, &self.state.root_session_id,
            &request.0, &work).await?;
        if self.roots.is_empty() { self.restore_root_sources(&work).await?; }
        let command = pi_operator_host::stage_operator_command(&self.runtime, &owner, &self.filesystem, &request.0, &work)
            .await?;
        let native = self.native_host_with_native_work(&work)?;
        let protected = native.original_owner();
        let fork = pi_fork_host::PiForkHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, Arc::clone(&protected), &self.state, &self.roots, native.clone());
        let selected = pi_selected_host::PiSelectedHost::new_with_native_work(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, Arc::clone(&protected), &self.state, &self.roots, native, &work)?
            .with_work(work.clone()).with_invocation_cancellation(cancellation);
        let operator = pi_operator_host::PiOperatorHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            protected, &self.resources, &self.state.root_session_id, &selected, work.clone())?;
        pi_workflow::PiWorkflowHost::new(&self.relative_directory, self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, &self.state).with_controls(&fork, &operator).with_work(work).control_command_with_native_work(command.0).await
            .map(|(value, allocation)| (value, Some(allocation)))
    }

    pub(super) async fn writeback_status(&self, caller: TaskId, arguments: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let owner = pi_owner::PiOwnerConfig::load_with_native_work(
            &self.resources.private_state_root, &self.relative_directory, work,
        )?;
        self.runtime.verify_owner_with_native_work(&owner, work)?;
        if caller != owner.policy().root_task { return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Unauthorized, reason: "writeback status requires its original Root caller" }); }
        let operation = arguments.get("operation").and_then(Value::as_str)
            .filter(|operation| !operation.is_empty() && operation.len() <= 512)
            .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status requires its original SDK operation" })?;
        let source = arguments.get("source").and_then(Value::as_str)
            .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status requires its original source task" })?;
        let source = self.alias_task(source)?;
        let root = arguments.get("root").and_then(Value::as_str)
            .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status requires its original selected Root" })?;
        if !self.binding(source)?.project_volumes.contains_key(root) {
            return Err(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status changed its original selected Root" });
        }
        let selected = arguments.get("paths").and_then(Value::as_array)
            .filter(|paths| !paths.is_empty() && paths.len() <= 256)
            .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status requires its bounded original selected paths" })?;
        let retained = selected.iter().try_fold(
            selected.len() as u64 * std::mem::size_of::<String>() as u64,
            |bytes, path| {
                let path = path.as_str().filter(|path| !path.is_empty() && path.len() <= 4096)
                    .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status path exceeds its original bounds" })?;
                Ok::<_, RootError>(bytes + path.len() as u64)
            },
        )?;
        // Admission for ordinary transport DTO copies is not a fabricated SDK
        // allocator event. Keep it until the vector and strings actually die.
        let _paths_custody = work.retain_owned(retained)
            .map_err(|cause| RootError::NativeWork { cause, custody: None })?;
        let mut paths = Vec::with_capacity(selected.len());
        for path in selected {
            let path = path.as_str()
                .ok_or(RootError::NativeBoundary { code: acyclic_harness::wire::ErrorCode::Invalid, reason: "writeback status path exceeds its original bounds" })?;
            paths.push(path.to_owned());
        }
        pi_operator_host::passive_status(&self.runtime, &self.filesystem, &self.stream, &self.resources, &owner,
            &self.state.root_session_id, &self.binding(caller)?.native_session_id,
            caller, operation, source, root, &paths, work).await
            .map(|(value, allocation)| (value, Some(allocation)))
    }
}
