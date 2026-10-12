//! Selected workspace controls dispatched under the original caller's owner.
use super::*;
use acyclic_harness::{filesystem::TaskCommandProgress, workflow::WorkflowCommand};

impl PiSession {
    pub(super) async fn selected_control(
        &mut self, caller: TaskId, event: &str, arguments: &Value, owner: &pi_owner::PiOwnerConfig,
        original_native_work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        self.runtime.verify_owner_with_native_work(owner, original_native_work)?;
        let sdk_operation = string(arguments, "operation").map_err(acyclic_harness::Error::Invalid)?;
        if sdk_operation.is_empty() || sdk_operation.len() > 512 {
            return Err(acyclic_harness::Error::Invalid("Pi selected control requires its original bounded SDK operation".to_owned()));
        }
        let (kind, source_field) = match event {
            "TaskInspect" => ("acyclic.pi.inspect", "id"),
            "TaskDiff" => ("acyclic.pi.diff", "id"),
            "TaskImport" => ("acyclic.pi.import", "source"),
            "TaskWriteback" => ("acyclic.pi.writeback", "source"),
            _ => return Err(acyclic_harness::Error::Invalid("unknown selected workspace control".to_owned())),
        };
        if event == "TaskWriteback" && caller != owner.policy().root_task {
            return Err(acyclic_harness::Error::Invalid("Pi writeback requires the original Root caller and operator consent".to_owned()));
        }
        let source = self.alias_task(&string(arguments, source_field).map_err(acyclic_harness::Error::Invalid)?)?;
        let root = optional_selected_string(arguments, "root", 128)?;
        let source_binding = self.binding(source)?;
        let root_key = match root.as_deref() {
            Some(key) if source_binding.project_volumes.contains_key(key) => key.to_owned(),
            Some(_) => return Err(acyclic_harness::Error::Invalid("selected root is not an original source project".to_owned())),
            None if source_binding.project_volumes.len() == 1 => source_binding.project_volumes.keys().next().ok_or_else(|| acyclic_harness::Error::Invalid("selected source has no project".to_owned()))?.clone(),
            None => return Err(acyclic_harness::Error::Invalid("multiple source projects require an explicit root identity".to_owned())),
        };
        let directory = optional_selected_string(arguments, "directory", 4096)?.unwrap_or_default();
        let cursor = optional_selected_string(arguments, "cursor", 16384)?;
        let maximum_entries = match arguments.get("maximum_entries") {
            None | Some(Value::Null) => None,
            Some(value) => Some(value.as_u64().filter(|value| (1..=256).contains(value)).ok_or_else(|| acyclic_harness::Error::Invalid("selected page requires 1..256 entries".to_owned()))? as u32),
        };
        let destination = match arguments.get("destination") {
            None | Some(Value::Null) => None,
            Some(Value::String(alias)) => Some(self.alias_task(alias)?),
            Some(_) => return Err(acyclic_harness::Error::Invalid("selected destination must be an actual task alias".to_owned())),
        };
        let paths = match arguments.get("paths") {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(paths)) if !paths.is_empty() && paths.len() <= 256 => paths.iter().map(|path|
                path.as_str().filter(|path| !path.is_empty() && path.len() <= 4096)
                    .map(str::to_owned).ok_or_else(|| acyclic_harness::Error::Invalid("selected paths must be bounded relative paths".to_owned())))
                .collect::<Result<Vec<_>, _>>()?,
            Some(_) => return Err(acyclic_harness::Error::Invalid("selected paths require 1..256 bounded paths".to_owned())),
        };
        let confirm = match arguments.get("confirm") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(value)) => *value,
            Some(_) => return Err(acyclic_harness::Error::Invalid("selected confirmation must be explicit".to_owned())),
        };
        let approval_digest = optional_selected_string(arguments, "approval_digest", 64)?;
        if approval_digest.as_deref().is_some_and(|digest| digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit())) {
            return Err(acyclic_harness::Error::Invalid("writeback consent must name the exact prepared digest".to_owned()));
        }
        let native = self.native_host_with_native_work(original_native_work)?;
        let work = if caller == owner.policy().root_task {
            original_native_work.clone()
        } else {
            native.invocation_work(caller).await?
        };
        let original = pi_fork::original_operation(owner.policy().root_task, caller, &sdk_operation, &work)?;
        let operation = pi_fork::derived(original, if event == "TaskWriteback" && approval_digest.is_some() {
            "selected-writeback-publish"
        } else { match event {
            "TaskInspect" => "selected-inspect", "TaskDiff" => "selected-diff",
            "TaskImport" => "selected-import", _ => "selected-writeback",
        }}, &work)?;
        let cancellation = work.cancellation();
        let mut root_cut_custody = None;
        let mut root_cut = None;
        if source == owner.policy().root_task && caller != source {
            let observer = pi_fork_host::PiObserveSourceCommand {
                caller, sdk_operation: sdk_operation.clone(), root_key: root_key.clone(),
            };
            let observe_operation = pi_fork::derived(original, "observe-root-source", &work)?;
            let selected = pi_selected_host::PiControlHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
                &self.resources, native.original_owner(), &self.state, &self.roots, native.clone(), &work)?;
            let workflow = pi_workflow::PiWorkflowHost::new(&self.relative_directory, self.runtime.clone(), self.filesystem.clone(),
                self.stream.clone(), &self.resources, &self.state, work.clone()).with_controls(&selected);
            let (input, mut input_custody) = pi_root_provider::guarded_pi_value(
                &observer, owner.policy().maximum_payload_bytes, &work)?;
            let input = input;
            let (observed, custody) = workflow.control_with_native_work(
                observe_operation, pi_fork_host::PI_OBSERVE_SOURCE_COMMAND_KIND, &input).await?;
            drop(input);
            let observed: pi_fork_host::PiRootSource = serde_json::from_value(observed).map_err(|error| acyclic_harness::Error::Invalid(error.to_string()))?;
            if observed.root_key != root_key { return Err(acyclic_harness::Error::Invalid("Root source acknowledgement selected another root".to_owned())); }
            root_cut = Some(observed);
            let mut custody = custody;
            if let Err(cause) = custody.absorb(&mut input_custody) {
                drop(root_cut);
                return Err(native_error(cause));
            }
            root_cut_custody = Some(custody);
        }
        let payload = pi_selected_host::PiSelectedCommand {
            caller, sdk_operation, source, root: Some(root_key), directory, cursor, maximum_entries,
            destination, paths, confirm, approval_digest, import_operation: None,
        };
        let (reference, mut reference_custody) = pi_root_provider::stage_pi_input(
            owner, &self.filesystem, operation, "command", &payload, &work).await?;
        let command = WorkflowCommand { operation_id: operation, kind: kind.to_owned(), payload: reference };
        let mut selected = pi_selected_host::PiControlHost::new(self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
            &self.resources, native.original_owner(), &self.state, &self.roots, native.clone(), &work)?;
        selected.root_source = root_cut.as_ref();
        let (value, mut custody) = if caller == owner.policy().root_task {
            pi_workflow::PiWorkflowHost::new(&self.relative_directory, self.runtime.clone(), self.filesystem.clone(), self.stream.clone(),
                &self.resources, &self.state, work.clone()).with_controls(&selected)
                .control_command_with_native_work(command).await?
        } else {
            let live = native.live_source(caller, &work).await?.ok_or_else(|| acyclic_harness::Error::Invalid("selected caller has no current native owner".to_owned()))?;
            let fence = live.context.context().owned_task_fence()?;
            let (input, mut input_custody) = pi_root_provider::guarded_pi_value(
                &payload, owner.policy().maximum_payload_bytes, &work)?;
            let progress = selected.run(live.context, fence, command, input, false, &work).await?;
            match progress {
                TaskCommandProgress::ReadyWithNativeCustody { value, mut custody } => {
                    if let Err(cause) = custody.absorb(&mut input_custody) {
                        drop(value);
                        return Err(native_error(cause));
                    }
                    (value, custody)
                }
                TaskCommandProgress::Ready(_) => return Err(acyclic_harness::Error::Invalid("selected native acknowledgement lost its original SDK custody".to_owned())),
                TaskCommandProgress::Pending => return Err(acyclic_harness::Error::Invalid("original selected acknowledgement remains uncertain".to_owned())),
                TaskCommandProgress::Indeterminate => return Err(acyclic_harness::Error::Invalid("original selected effect remains indeterminate".to_owned())),
            }
        };
        drop(selected);
        drop(root_cut);
        if let Err(cause) = custody.absorb(&mut reference_custody) {
            drop(value);
            return Err(native_error(cause));
        }
        if let Some(mut source_custody) = root_cut_custody {
            if let Err(cause) = custody.absorb(&mut source_custody) {
                drop(value);
                return Err(native_error(cause));
            }
        }
        Ok((value, custody))
    }
}

fn optional_selected_string(arguments: &Value, name: &str, maximum: usize) -> acyclic_harness::Result<Option<String>> {
    match arguments.get(name) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(value)) if (name == "directory" || !value.is_empty()) && value.len() <= maximum => Ok(Some(value.clone())),
        _ => Err(acyclic_harness::Error::Invalid(format!("{name} must be a bounded string"))),
    }
}

fn native_error(cause: acyclic_fs::NativeTreeMaterializationError) -> acyclic_harness::Error {
    acyclic_harness::Error::NativeWork { cause, custody: None }
}
