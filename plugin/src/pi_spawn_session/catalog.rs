//! Durable Preparing and Created records in main's adapter state.
use super::*;

struct BorrowedRoot<'a> {
    key: &'a str,
    root: RouteRoot,
}
impl Serialize for BorrowedRoot<'_> {
    fn serialize<S: serde::Serializer>(
        &self,
        serializer: S,
    ) -> std::result::Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(self.key, &self.root)?;
        map.end()
    }
}
#[derive(Serialize)]
struct BorrowedRoute<'a> {
    task_binding: PluginTaskBinding<&'a PiTaskBinding>,
    agent_id: &'a str,
    turn_id: &'a str,
    context_id: [u8; 16],
    root_id: [u8; 16],
    parent_agent_id: &'a str,
    roots: BorrowedRoot<'a>,
    mount_path: &'a Path,
    lifecycle: RouteLifecycle,
}
const ROUTE_DIAGNOSTIC_BYTES: u64 = pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
    + pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
        "Route",
        "task_binding",
        "agent_id",
        "turn_id",
        "context_id",
        "root_id",
        "parent_agent_id",
        "roots",
        "mount_path",
        "lifecycle",
        "RouteRoot",
        "repository_workspace_id",
        "published_generation",
        "RouteLifecycle",
        "mounting",
        "active",
        "stop_requested",
        "frozen",
        "PluginTaskBinding",
        "kind",
        "binding",
        "pi",
        "host_managed",
        "a string",
        "path string",
        "an array of length 16",
        "an array of length 32",
    ]);

impl PiSession {
    fn spawn_parent_alias(&self, caller: TaskId) -> Result<&str> {
        if matches!(&self.state.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == caller)
        {
            return Ok(&self.state.root_session_id);
        }
        self.state.routes.values().find_map(|route|
            matches!(&route.task_binding, PluginTaskBinding::Pi(binding) if binding.task_id == caller)
                .then_some(route.agent_id.as_str()))
            .ok_or_else(|| invalid("Pi caller has no retained route alias"))
    }

    pub(super) fn spawn_validate_pending(
        &self,
        pending: &PendingSpawn,
        commit: &RetainedSpawnCommit,
        operations: PiForkOperations,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let (key, project) = commit
            .child
            .project_volumes
            .iter()
            .next()
            .ok_or_else(|| invalid("Pi pending lost its project"))?;
        let paths = pi_native::planned_native_paths(
            &self.resources,
            &self.state.root_session_id,
            commit.child.task_id,
            key,
            work,
        )?;
        let mut io = HistoryWork::new(PreparationIo::from_native_work(work)?);
        let workspace = project.storage_name(&mut Some(&mut io))?;
        if !matches!(&pending.task_binding, PluginTaskBinding::Pi(binding) if binding.as_ref() == &commit.child)
            || pending.agent_id.as_deref() != Some(commit.alias.as_str())
            || pending.parent_agent_id != self.spawn_parent_alias(commit.caller)?
            || pending.tool_use_id != commit.sdk_operation
            || pending.active_root_id != Some(decode_fixed(key).map_err(Error::Invalid)?)
            || pending.workspace_name != workspace
            || pending.fork_key != operations.fork.into_bytes()
            || pending.mount_path != paths.0.view_root
            || !pending.roots.is_empty()
            || pending.lifecycle != PendingSpawnLifecycle::Preparing
            || pending.expires_at_millis <= now_millis()
        {
            return Err(invalid("Pi retry changed or expired its Preparing record"));
        }
        Ok(())
    }

    pub(super) fn spawn_validate_route(
        &self,
        route: &Route,
        commit: &RetainedSpawnCommit,
        operations: PiForkOperations,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let PluginTaskBinding::Pi(binding) = &route.task_binding else {
            return Err(invalid("Pi route is host-managed"));
        };
        pi_native_host::validate_child_binding(binding, &commit.child)?;
        let key = commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi route lost its project"))?;
        let paths = pi_native::planned_native_paths(
            &self.resources,
            &self.state.root_session_id,
            commit.child.task_id,
            key,
            work,
        )?;
        if route.agent_id != commit.alias
            || route.turn_id != commit.sdk_operation
            || route.parent_agent_id != self.spawn_parent_alias(commit.caller)?
            || route.context_id != operations.fork.into_bytes()
            || route.root_id != decode_fixed::<16>(key).map_err(Error::Invalid)?
            || route.mount_path != paths.0.view_root
        {
            return Err(invalid("Pi duplicate alias changed its admitted child"));
        }
        binding.validate(None, route.roots.keys().map(String::as_str), false)
    }

    pub(super) async fn spawn_validate_admitted(
        &self,
        plan: &pi_fork::PiForkPlan,
        maximum: u64,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let host = self.runtime.runtime.task_host();
        for original in [&plan.preparation, &plan.request.child] {
            let admission = host
                .observe_admission_with_native_work(original.task_id(), work)
                .await?;
            if admission.0 != *original {
                return Err(invalid(
                    "Pi retained child changed its preparation or sealed admission",
                ));
            }
        }
        let outcome = host
            .outcome_with_native_work(plan.preparation.task_id(), work)
            .await?;
        let Some(Outcome::Succeeded(value)) = outcome.0 else {
            return Err(invalid(
                "Pi child lacks a successful original creator outcome",
            ));
        };
        let report: acyclic_harness::fork::ForkReport =
            serde_json::from_value(value).map_err(|error| Error::Invalid(error.to_string()))?;
        report.validate(&mut Some(&mut HistoryWork::new(
            PreparationIo::from_native_work(work)?,
        )))?;
        let request = pi_root_provider::guarded_pi_value(&report.request, maximum, work)?;
        if request.0 != plan.request.payload {
            return Err(invalid(
                "Pi creator outcome changed its resource preparation",
            ));
        }
        Ok(())
    }

    pub(super) fn spawn_save_pending(
        &mut self,
        pending: Option<(Vec<PendingSpawn>, NativeAllocation)>,
        commit: &RetainedSpawnCommit,
        operations: PiForkOperations,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let Some(pending) = pending else {
            return Ok(());
        };
        if pending.0.len() != 1
            || self
                .state
                .pending
                .iter()
                .any(|entry| entry.fork_key == operations.fork.into_bytes())
        {
            return Err(invalid(
                "Pi preparation returned a duplicate or unrelated reservation",
            ));
        }
        self.spawn_validate_pending(&pending.0[0], commit, operations, work)?;
        self.prepare_native_catalog_slot(work)?;
        let length = self.state.pending.len();
        if self.state.pending.capacity() == length {
            let capacity =
                length
                    .checked_add(1)
                    .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(
                        acyclic_fs::WorkError::Overflow,
                    ))?;
            let bytes = capacity
                .checked_mul(std::mem::size_of::<PendingSpawn>())
                .ok_or(acyclic_fs::NativeTreeMaterializationError::Work(
                    acyclic_fs::WorkError::Overflow,
                ))? as u64;
            let backing = work.claim(bytes, u64::from(bytes != 0), WorkCounters::UNCHARGED)?;
            work.charge(WorkCounters {
                bytes_copied: (length * std::mem::size_of::<PendingSpawn>()) as u64,
                ..WorkCounters::UNCHARGED
            })?;
            let mut replacement = std::collections::VecDeque::with_capacity(capacity);
            replacement.append(&mut self.state.pending);
            let old = std::mem::replace(&mut self.state.pending, replacement);
            drop(old);
            drop(self.native_pending_backing_custody.replace(backing));
        }
        self.state.pending.extend(pending.0);
        let component = crate::service_session::NativeCatalogComponent::Pending;
        let previous =
            self.install_native_catalog_custody(commit.child.task_id, component, pending.1);
        if let Err(error) = self.slots.save_with_native_work(
            &self.resources.private_state_root,
            &self.relative_directory,
            &self.state,
            Survives::PowerLoss,
            work,
        ) {
            self.state.pending.truncate(length);
            drop(self.take_native_catalog_custody(commit.child.task_id, component));
            if let Some(previous) = previous {
                self.install_native_catalog_custody(commit.child.task_id, component, previous);
            }
            return Err(error);
        }
        Ok(())
    }

    pub(super) fn spawn_save_created(
        &mut self,
        commit: &RetainedSpawnCommit,
        operations: PiForkOperations,
        prepared: &PreparedPiFork,
        work: &NativeMaterializationWork,
    ) -> Result<()> {
        let index = self
            .state
            .pending
            .iter()
            .position(|entry| entry.fork_key == operations.fork.into_bytes())
            .ok_or_else(|| invalid("Pi creator has no durably saved Preparing record"))?;
        self.prepare_native_catalog_slot(work)?;
        let pending = &self.state.pending[index];
        self.spawn_validate_pending(pending, commit, operations, work)?;
        let key = commit
            .child
            .project_volumes
            .keys()
            .next()
            .ok_or_else(|| invalid("Pi child lost its root"))?;
        let binding = &prepared.binding;
        pi_native_host::validate_child_binding(binding, &commit.child)?;
        if prepared.admission.task_id() != binding.task_id
            || prepared.admission.parent != Some(commit.caller)
            || prepared.report.request.operation_id != operations.fork
            || prepared.report.request.child_agent != operations.agent
            || prepared.native_lazy.workspace_id != prepared.workspace.workspace().id()
        {
            return Err(invalid(
                "Pi creator substituted its child admission or workspace",
            ));
        }
        prepared.report.validate(&mut Some(&mut HistoryWork::new(
            PreparationIo::from_native_work(work)?,
        )))?;
        binding.validate(None, [key.as_str()], false)?;
        let route = BorrowedRoute {
            task_binding: PluginTaskBinding::Pi(binding),
            agent_id: &commit.alias,
            turn_id: &commit.sdk_operation,
            context_id: operations.fork.into_bytes(),
            root_id: pending
                .active_root_id
                .ok_or_else(|| invalid("Pi reservation lost its root"))?,
            parent_agent_id: &pending.parent_agent_id,
            roots: BorrowedRoot {
                key,
                root: RouteRoot {
                    root_id: decode_fixed(key).map_err(Error::Invalid)?,
                    repository_workspace_id: prepared.workspace.workspace().id().into_bytes(),
                    published_generation: prepared
                        .native_lazy
                        .authored_generation
                        .digest()
                        .into_bytes(),
                },
            },
            mount_path: &pending.mount_path,
            lifecycle: RouteLifecycle::Mounting,
        };
        if self.state.routes.contains_key(&commit.alias) {
            return Err(invalid("Pi created alias is already registered"));
        }
        let bytes = pi_root_provider::canonical_pi_value(
            &(commit.alias.as_str(), &route),
            MAXIMUM_ADAPTER_STATE_BYTES,
            work,
        )?;
        let mut decoded: ((String, Route), NativeAllocation) = pi_root_provider::guarded_pi_decode(
            &bytes.0,
            MAXIMUM_ADAPTER_STATE_BYTES,
            std::mem::size_of::<(String, Route)>() as u64,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            ROUTE_DIAGNOSTIC_BYTES,
            work,
        )?;
        let mut slot = work.claim(
            std::mem::size_of::<(String, Route)>() as u64,
            1,
            WorkCounters::UNCHARGED,
        )?;
        decoded.1.absorb(&mut slot)?;
        let pending_component = crate::service_session::NativeCatalogComponent::Pending;
        let route_component = crate::service_session::NativeCatalogComponent::Route;
        let consumed_custody =
            self.take_native_catalog_custody(commit.child.task_id, pending_component);
        let consumed = self
            .state
            .pending
            .remove(index)
            .ok_or_else(|| invalid("Pi pending record disappeared"))?;
        self.state.routes.insert(decoded.0.0, decoded.0.1);
        let previous =
            self.install_native_catalog_custody(commit.child.task_id, route_component, decoded.1);
        if let Err(error) = self.slots.save_with_native_work(
            &self.resources.private_state_root,
            &self.relative_directory,
            &self.state,
            Survives::PowerLoss,
            work,
        ) {
            drop(self.state.routes.remove_entry(&commit.alias));
            drop(self.take_native_catalog_custody(commit.child.task_id, route_component));
            self.state.pending.insert(index, consumed);
            for (component, allocation) in [
                (pending_component, consumed_custody),
                (route_component, previous),
            ] {
                if let Some(allocation) = allocation {
                    self.install_native_catalog_custody(
                        commit.child.task_id,
                        component,
                        allocation,
                    );
                }
            }
            return Err(error);
        }
        drop(consumed);
        drop(consumed_custody);
        Ok(())
    }
}
