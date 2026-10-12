//! Spawn input and the SDK-native session intent.
use super::*;
use acyclic_harness::conversation::{VolumeClass, VolumeOwner, VolumeRef};

impl PiSession {
    fn spawn_project<'a>(
        &'a self,
        caller: TaskId,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
    ) -> Result<(TaskId, &'a str)> {
        let task = match arguments.get("source") {
            None | Some(Value::Null) => caller,
            Some(Value::String(alias)) => self.alias_task(alias)?,
            _ => return Err(invalid("Pi workspace source must be a registered agent")),
        };
        if task != caller && !owner.policy().grants.contains("pi:source:select") {
            return Err(invalid("Pi original policy denies selected-source reads"));
        }
        let projects = &self.binding(task)?.project_volumes;
        let key = match arguments.get("root") {
            Some(Value::String(key)) => projects.get_key_value(key).map(|(key, _)| key.as_str()),
            None | Some(Value::Null) if projects.len() == 1 => {
                projects.keys().next().map(String::as_str)
            }
            _ => None,
        }
        .ok_or_else(|| invalid("Pi source requires its explicit registered root"))?;
        let root = decode_fixed::<16>(key).map_err(Error::Invalid)?;
        if key != crate::root_key(WorkspaceRootId::from_bytes(root))
            || !self.state.roots.contains_key(key)
        {
            return Err(invalid(
                "Pi project is outside the original Root allocation",
            ));
        }
        Ok((task, key))
    }

    fn spawn_arguments<'a>(
        &self,
        arguments: &'a Value,
        owner: &pi_owner::PiOwnerConfig,
    ) -> Result<(
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        &'a str,
        pi_native::PiNativeSourceMode,
    )> {
        let text = |key| {
            arguments
                .get(key)
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("Pi spawn requires string arguments"))
        };
        let (operation, alias, prompt, provider, model) = (
            text("operation")?,
            text("id")?,
            text("prompt")?,
            text("provider")?,
            text("model")?,
        );
        if operation.is_empty()
            || operation.len() > 512
            || alias.is_empty()
            || alias.len() > 128
            || alias == "root"
            || alias == self.state.root_agent_id
            || alias == self.state.root_session_id
            || !alias
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
        {
            return Err(invalid(
                "Pi spawn requires bounded tool and child identities",
            ));
        }
        if prompt.len() as u64 > owner.policy().limits.file_bytes
            || provider.is_empty()
            || provider.len() > 256
            || model.is_empty()
            || model.len() > 256
        {
            return Err(invalid("Pi native instruction or model exceeds its bounds"));
        }
        let mode = match arguments
            .get("context")
            .and_then(Value::as_str)
            .unwrap_or("scratch")
        {
            "scratch" => pi_native::PiNativeSourceMode::Scratch,
            "native_cut" => pi_native::PiNativeSourceMode::CallerTool,
            _ => return Err(invalid("Pi context must be scratch or native_cut")),
        };
        Ok((operation, alias, prompt, provider, model, mode))
    }

    pub(in crate::service_session) fn prepare_spawn(
        &self,
        caller: TaskId,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> Result<(Value, NativeAllocation)> {
        let (operation, alias, _, _, _, _) = self.spawn_arguments(arguments, owner)?;
        if self.state.routes.contains_key(alias)
            || self
                .state
                .pending
                .iter()
                .any(|entry| entry.agent_id.as_deref() == Some(alias))
        {
            return Err(invalid("Pi child identity is already registered"));
        }
        let parent = self.binding(caller)?;
        if self.state.pending.iter().any(|entry| entry.tool_use_id == operation
            && matches!(&entry.task_binding, PluginTaskBinding::Pi(binding) if binding.agent_id != parent.agent_id))
        {
            return Err(invalid("Pi original preparation is uncertain; observe it without reseeding"));
        }
        owner.native_runtime()?.validate(work)?;
        let original =
            pi_fork::original_operation(owner.policy().root_task, caller, operation, work)?;
        let task = acyclic_harness::runtime::task_id_for_admission(
            PiForkOperations::new(original, work)?.child,
        );
        let (source, key) = self.spawn_project(caller, arguments, owner)?;
        let paths = pi_native::planned_native_paths(
            &self.resources,
            &self.state.root_session_id,
            task,
            key,
            work,
        )?;
        #[derive(Serialize)]
        struct Child<'a> {
            id: &'a str,
            task: TaskId,
            cwd: &'a Path,
            session_file: &'a Path,
            session_directory: &'a Path,
        }
        #[derive(Serialize)]
        struct Source {
            task: TaskId,
        }
        #[derive(Serialize)]
        struct Preparation<'a> {
            operation: &'a str,
            state: &'static str,
            child: Child<'a>,
            source: Source,
        }
        pi_root_provider::guarded_pi_value(
            &Preparation {
                operation,
                state: "preparing",
                child: Child {
                    id: alias,
                    task,
                    cwd: &paths.0.cwd,
                    session_file: &paths.0.session_file,
                    session_directory: paths
                        .0
                        .session_file
                        .parent()
                        .expect("planned scratch parent"),
                },
                source: Source { task: source },
            },
            MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            work,
        )
    }

    pub(super) fn spawn_native_commit(
        &self,
        caller: TaskId,
        arguments: &Value,
        owner: &pi_owner::PiOwnerConfig,
        work: &NativeMaterializationWork,
    ) -> Result<(RetainedSpawnCommit, NativeAllocation)> {
        let (operation, alias, prompt, provider, model, mode) =
            self.spawn_arguments(arguments, owner)?;
        let (source_task, key) = self.spawn_project(caller, arguments, owner)?;
        let original =
            pi_fork::original_operation(owner.policy().root_task, caller, operation, work)?;
        let operations = PiForkOperations::new(original, work)?;
        let task = acyclic_harness::runtime::task_id_for_admission(operations.child);
        let parent = self.binding(caller)?;
        let session = arguments
            .get("native_session_id")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("Pi child requires its independent SDK session"))?;
        let uuid =
            uuid::Uuid::parse_str(session).map_err(|error| Error::Invalid(error.to_string()))?;
        if uuid.get_version() != Some(uuid::Version::Random)
            || uuid.get_variant() != uuid::Variant::RFC4122 || uuid.to_string() != session
            || session == parent.native_session_id
            || std::iter::once(&self.state.task_binding)
                .chain(self.state.routes.values().map(|entry| &entry.task_binding))
                .chain(self.state.pending.iter().map(|entry| &entry.task_binding))
                .any(|entry| matches!(entry, PluginTaskBinding::Pi(binding) if binding.native_session_id == session && binding.task_id != task))
            || self.state.pending.iter().any(|entry| entry.agent_id.as_deref() == Some(alias) && entry.fork_key != operations.fork.into_bytes())
        {
            return Err(invalid("Pi SDK child session or alias belongs to another task"));
        }
        let project = &self.binding(source_task)?.project_volumes[key];
        if project.provider() != parent.scratch.volume.provider()
            || project.provider() != owner.policy().journal.provider()
        {
            return Err(invalid(
                "Pi selected project changed its filesystem provider",
            ));
        }
        let cut = pi_native::PiNativeSourceCut::deserialize(
            arguments
                .get("cut")
                .ok_or_else(|| invalid("Pi spawn requires its actual SDK source cut"))?,
        )
        .map_err(|error| Error::Invalid(error.to_string()))?;
        let cwd = arguments
            .get("cwd")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("Pi spawn requires caller cwd"))?;
        if arguments
            .get("context")
            .is_some_and(|value| !value.is_null() && !value.is_string())
            || cut.mode != mode
            || cut.parent_session_id != parent.native_session_id
            || cut.parent_cwd != cwd
            || !Path::new(cwd).is_absolute()
            || (caller == owner.policy().root_task
                && !self
                    .state
                    .roots
                    .values()
                    .any(|root| root.path == Path::new(cwd)))
        {
            return Err(invalid(
                "Pi native context differs from its original caller",
            ));
        }
        let session_bytes = arguments
            .get("session_bytes")
            .and_then(Value::as_str)
            .ok_or_else(|| invalid("Pi spawn requires SDK session bytes"))?;
        let maximum = owner
            .policy()
            .limits
            .file_bytes
            .min(owner.policy().maximum_payload_bytes)
            .min(MAXIMUM_ADAPTER_STATE_BYTES)
            .min(512 * 1024);
        if maximum == 0
            || prompt.is_empty()
            || prompt.len() as u64 > maximum.min(262_144)
            || provider.contains('\0')
            || model.contains('\0')
            || session_bytes.len() as u64 > maximum
            || cut.entries_json.len() as u64 > maximum
        {
            return Err(invalid("Pi SDK session exceeds its context allowance"));
        }
        let paths = pi_native::planned_native_paths(
            &self.resources,
            &self.state.root_session_id,
            task,
            key,
            work,
        )?;
        let invocation = owner.native_runtime()?.intent_invocation(
            &paths.0.session_file,
            &paths.0.cwd,
            provider,
            model,
            prompt,
            pi_native::capture_limits(owner.policy().limits, owner.policy().maximum_payload_bytes)?,
            work,
        )?;
        let launch = pi_native::PiNativeLaunchIntent {
            agent_alias: alias.into(),
            child_session_id: session.into(),
            source: cut,
            invocation: invocation.0,
            session_path: "session.jsonl".into(),
            session_bytes: session_bytes.into(),
            maximum_entries: 4096,
            maximum_bytes: maximum,
        };
        launch.validate(work)?;
        let volume = |name: &str, class, owner| {
            VolumeRef::new(project.provider().clone(), name, class, owner)
        };
        let child = PiTaskBinding {
            native_session_id: session.into(),
            task_id: task,
            agent_id: operations.agent,
            project_volumes: BTreeMap::from([(
                key.into(),
                volume(
                    &format!("project-{task}"),
                    VolumeClass::Project,
                    VolumeOwner::Project(key.into()),
                )?,
            )]),
            scratch: PiScratchBinding {
                volume: volume(
                    "scratch",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(operations.agent),
                )?,
                allocation: PiScratchAllocation::Preparing,
            },
            results: Some(PiScratchBinding {
                volume: volume(
                    "native-results",
                    VolumeClass::AgentPrivate,
                    VolumeOwner::Agent(operations.agent),
                )?,
                allocation: PiScratchAllocation::Preparing,
            }),
        };
        child.validate(None, [key], true)?;
        Ok((
            RetainedSpawnCommit {
                sdk_operation: operation.into(),
                alias: alias.into(),
                prompt: prompt.into(),
                caller,
                source_task,
                child,
                launch,
            },
            invocation.1,
        ))
    }
}
