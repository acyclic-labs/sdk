//! Tool admission and lease rollback.

use super::*;

impl ControlPlane {
    #[allow(
            clippy::needless_pass_by_value,
            reason = "every hook handler takes its decoded hook input by value"
        )]
        pub(crate) fn user_prompt(&mut self, input: Value) -> Result<Value, String> {
            let session_id = string(&input, "session_id")?;
            if session_id != self.state.root_session_id {
                return Err("user prompt belongs to another root session".to_owned());
            }
            let turn_id = string(&input, "turn_id")?;
            if self.state.turns.contains_key(&turn_id) {
                return Err("root turn identity is already bound to a subagent".to_owned());
            }
            self.remember_root_turn(turn_id);
            self.persist_unflushed()?;
            Ok(json!({"suppressOutput": true}))
        }

    pub(crate) async fn pre_tool(&mut self, input: Value) -> Result<Value, String> {
            self.require_session(&input)?;
            // Quarantine every expired writer before the mount can be advanced or
            // reused for a new operation. Recovery owns remounting after fencing.
            self.recover_expired_adapter_leases().await?;
            let tool_name = string(&input, "tool_name")?;
            let turn_id = string(&input, "turn_id")?;
            let tool_use_id = string(&input, "tool_use_id")?;
            let caller = self.resolve_turn(&turn_id)?;
            let caller_route = if caller == self.state.root_agent_id {
                None
            } else {
                let route = self
                    .state
                    .routes
                    .get(&caller)
                    .cloned()
                    .ok_or_else(|| "subagent route is missing".to_owned())?;
                if !route.lifecycle.accepts_tools() {
                    return Err("subagent workspace is sealed after SubagentStop".to_owned());
                }
                Some(route)
            };
            if is_spawn_tool(&tool_name) {
                let now = now_millis();
                self.discard_expired_pending_spawns(now).await?;
                if let Some(existing) =
                    self.state.pending.iter().find(|spawn| {
                        spawn.parent_agent_id == caller && spawn.tool_use_id == tool_use_id
                    })
                {
                    return if existing.lifecycle == PendingSpawnLifecycle::Prepared {
                        Ok(json!({}))
                    } else {
                        Err("the retried subagent spawn is not fully prepared".to_owned())
                    };
                }
                if !self.state.pending.is_empty() {
                    return Err(
                        "a subagent spawn handshake is already pending; retry after SubagentStart"
                            .to_owned(),
                    );
                }
                let fork_key = IdempotencyKey::new();
                self.state.pending.push_back(PendingSpawn {
                    task_binding: PluginTaskBinding::HostManaged,
                    agent_id: None,
                    parent_agent_id: caller,
                    tool_use_id,
                    active_root_id: input
                        .get("_caller_root_id")
                        .and_then(Value::as_str)
                        .and_then(|value| decode_fixed::<16>(value).ok()),
                    expires_at_millis: now.saturating_add(2 * 60 * 1_000),
                    workspace_name: format!(
                        "agent-{}-{}",
                        short_hash(self.state.root_session_id.as_bytes()),
                        hex::encode(fork_key.into_bytes())
                    ),
                    fork_key: fork_key.into_bytes(),
                    roots: BTreeMap::new(),
                    mount_path: self
                        .workspace_mount_root()
                        .join(compact_id(&fork_key.into_bytes())),
                    lifecycle: PendingSpawnLifecycle::Preparing,
                });
                self.persist()?;
                if let Err(error) = self
                    .prepare_pending_spawn(fork_key.into_bytes(), Survives::ServiceCrash)
                    .await
                {
                    let cleanup = self.discard_pending_spawn(fork_key.into_bytes()).await;
                    return match cleanup {
                        Ok(()) => Err(error),
                        Err(cleanup) => Err(format!(
                            "{error}; failed to recover the rejected spawn: {cleanup}"
                        )),
                    };
                }
                return Ok(json!({}));
            }
            if caller == self.state.root_agent_id {
                if tool_name.starts_with("mcp__acyclic__") {
                    let mut updated =
                        normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
                    let object = updated
                        .as_object_mut()
                        .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
                    object.insert("_caller_turn_id".to_owned(), Value::String(turn_id));
                    object.insert(
                        "_session_id".to_owned(),
                        Value::String(self.state.root_session_id.clone()),
                    );
                    return Ok(json!({
                        "hookSpecificOutput": {
                            "hookEventName": "PreToolUse",
                            "permissionDecision": "allow",
                            "updatedInput": updated
                        }
                    }));
                }
                let original =
                    normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
                if is_acyclic_cli_invocation(&tool_name, &original)? {
                    return Ok(pre_tool_update(&original));
                }
                return Ok(json!({}));
            }
            let agent_id = caller;
            if is_known_non_filesystem_tool(&tool_name) {
                return Ok(json!({}));
            }
            if !is_filesystem_tool(&tool_name) {
                return Err(format!(
                    "unclassified tool '{tool_name}' is denied inside an isolated subagent workspace"
                ));
            }
            let route = caller_route.ok_or_else(|| "subagent route is missing".to_owned())?;
            let selected_root_id = input
                .get("_caller_root_id")
                .and_then(Value::as_str)
                .and_then(|value| decode_fixed::<16>(value).ok())
                .map_or_else(
                    || WorkspaceRootId::from_bytes(route.root_id),
                    WorkspaceRootId::from_bytes,
                );
            let selected_route_root = route
                .roots
                .get(&root_key(selected_root_id))
                .ok_or_else(|| "caller root is absent from its workspace context".to_owned())?;
            let selected_binding = self
                .state
                .roots
                .get(&root_key(selected_root_id))
                .ok_or_else(|| "caller physical root binding is missing".to_owned())?;
            let selected_path = route.mount_path.join(selected_route_root.mount_name());
            let original =
                normalize_tool_input(input.get("tool_input").cloned().unwrap_or(Value::Null))?;
            for binding in self.state.roots.values() {
                reject_original_root_references(&original, None, &binding.path.to_string_lossy())?;
            }
            let acyclic_cli = is_acyclic_cli_invocation(&tool_name, &original)?;
            let mut updated =
                rewrite_tool_input(&tool_name, original, &selected_binding.path, &selected_path)?;
            if acyclic_cli {
                // The CLI opens its own core operation lease. Wrapping it in the host-tool
                // lease would deadlock the compatibility command behind itself.
                return Ok(pre_tool_update(&updated));
            }
            if tool_name.starts_with("mcp__acyclic__") {
                let object = updated
                    .as_object_mut()
                    .ok_or_else(|| "workspace control tool input must be an object".to_owned())?;
                object.insert("_caller_turn_id".to_owned(), Value::String(turn_id.clone()));
                object.insert(
                    "_session_id".to_owned(),
                    Value::String(self.state.root_session_id.clone()),
                );
            }
            let now = now_millis();
            if let Some(existing) = self.state.leases.get(&tool_use_id) {
                if existing.agent_id != agent_id {
                    return Err("tool hook retry belongs to another agent".to_owned());
                }
                if existing.expires_at_millis > now {
                    return Ok(pre_tool_update(&updated));
                }
                self.state.leases.remove(&tool_use_id);
            }
            let first_overlapping_operation = !self
                .state
                .leases
                .values()
                .any(|lease| lease.agent_id == agent_id);
            if first_overlapping_operation {
                self.mounts
                    .get(&agent_id)
                    .ok_or_else(|| "subagent mount is unavailable".to_owned())?
                    .advance_to_head()
                    .await
                    .map_err(display)?;
            }
            let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
            let parent_context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(self.context_for_agent(&route.parent_agent_id)?)
                .await
                .map_err(display)?;
            let mut leases = Vec::with_capacity(route.roots.len());
            for route_root in route.roots.values() {
                let root_id = WorkspaceRootId::from_bytes(route_root.root_id);
                let acquired = async {
                    let workspace = self.workspace_root(&route, root_id).await?;
                    let parent_root = parent_context
                        .roots
                        .get(&root_id)
                        .ok_or_else(|| "direct parent root is missing".to_owned())?;
                    let parent = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(display)?;
                    operations
                        .recover_workspace(&workspace, now, OperationReconcileLimits::default())
                        .await
                        .map_err(display)?;
                    operations
                        .begin(
                            workspace.id(),
                            parent.head(WorkBudget::UNBOUNDED, &CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?.id(),
                            format!("{agent_id}:{tool_use_id}:{}", route_root.mount_name()),
                            now,
                            now.saturating_add(15 * 60 * 1_000),
                        )
                        .await
                        .map_err(display)
                }
                .await;
                match acquired {
                    Ok(lease) => leases.push((root_id, lease)),
                    Err(error) => {
                        self.rollback_started_leases(&route, &leases, now).await?;
                        return Err(error);
                    }
                }
            }
            self.state.leases.insert(
                tool_use_id.clone(),
                LeaseRecord::from_leases(agent_id, turn_id, tool_name, leases),
            );
            // The record is the request's last transition. Losing it leaves the
            // opened leases unrecorded, as a crash before this save does, and the
            // store fences and reconciles them when they expire.
            if let Err(error) = self.persist_unflushed() {
                let leases = self
                    .state
                    .leases
                    .get(&tool_use_id)
                    .map_or_else(Vec::new, |record| {
                        record
                            .roots
                            .values()
                            .map(|root| (WorkspaceRootId::from_bytes(root.root_id), root.lease()))
                            .collect()
                    });
                self.state.leases.remove(&tool_use_id);
                self.rollback_started_leases(&route, &leases, now).await?;
                return Err(error);
            }
            Ok(pre_tool_update(&updated))
        }

    pub(crate) async fn rollback_started_leases(
            &self,
            route: &Route,
            leases: &[(WorkspaceRootId, OperationWindowLease)],
            now: u64,
        ) -> Result<(), String> {
            let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
            for (root_id, lease) in leases.iter().rev() {
                let workspace = self.workspace_root(route, *root_id).await?;
                match operations.finish(lease, now).await.map_err(display)? {
                    acyclic_fs::OperationWindowFinish::Reconcile(reconcile) => {
                        operations
                            .reconcile_workspace(
                                &workspace,
                                reconcile,
                                OperationReconcileLimits::default(),
                            )
                            .await
                            .map_err(display)?;
                    }
                    acyclic_fs::OperationWindowFinish::StillActive { .. }
                    | acyclic_fs::OperationWindowFinish::AlreadyClosed => {}
                }
            }
            Ok(())
        }

}
