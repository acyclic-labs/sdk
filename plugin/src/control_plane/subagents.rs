//! Subagent startup and stop ordering.

use super::*;

impl ControlPlane {
    pub(crate) async fn subagent_start(&mut self, input: Value) -> Result<Value, String> {
            self.require_session(&input)?;
            let agent_id = string(&input, "agent_id")?;
            let turn_id = string(&input, "turn_id")?;
            let authenticated_parent = input
                .get("_authenticated_parent_agent_id")
                .and_then(Value::as_str);
            if self.state.root_turns.contains(&turn_id) {
                return Err("subagent turn identity is already bound to the root".to_owned());
            }
            if self
                .state
                .turns
                .get(&turn_id)
                .is_some_and(|bound| bound != &agent_id)
            {
                return Err("subagent turn identity is already bound to another agent".to_owned());
            }
            if self.state.routes.contains_key(&agent_id) {
                let route = self
                    .state
                    .routes
                    .get(&agent_id)
                    .cloned()
                    .ok_or("route missing")?;
                if route.lifecycle == RouteLifecycle::Mounting {
                    if authenticated_parent != Some(route.parent_agent_id.as_str()) {
                        return Err(
                            "subagent mount recovery is not authenticated by its direct parent"
                                .to_owned(),
                        );
                    }
                    fs::create_dir_all(&route.mount_path).map_err(display)?;
                    let mount = self.mount_route(&route).await?;
                    WorkspaceContextRegistry::new(self.store.clone())
                        .set_active(WorkspaceContextId::from_bytes(route.context_id), true)
                        .await
                        .map_err(display)?;
                    let recovered = self
                        .state
                        .routes
                        .get_mut(&agent_id)
                        .ok_or("route missing")?;
                    recovered.lifecycle = RouteLifecycle::Active;
                    recovered.turn_id.clone_from(&turn_id);
                    self.mounts.insert(agent_id.clone(), mount);
                    self.state.turns.insert(turn_id, agent_id.clone());
                    self.persist()?;
                    let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
                    return Ok(subagent_context(&route.active_path()?));
                }
                if route.lifecycle == RouteLifecycle::StopRequested {
                    return Err("subagent stop is pending until its live descendants finish".to_owned());
                }
                if !route.lifecycle.is_frozen() && route.turn_id != turn_id {
                    return Err("subagent identity is already bound to another turn".to_owned());
                }
                if route.lifecycle.is_frozen() {
                    if authenticated_parent != Some(route.parent_agent_id.as_str()) {
                        return Err(
                            "subagent resume is not authenticated by its direct parent".to_owned()
                        );
                    }
                    fs::create_dir_all(&route.mount_path).map_err(display)?;
                    let mount = self.mount_route(&route).await?;
                    WorkspaceContextRegistry::new(self.store.clone())
                        .set_active(WorkspaceContextId::from_bytes(route.context_id), true)
                        .await
                        .map_err(display)?;
                    let resumed = self
                        .state
                        .routes
                        .get_mut(&agent_id)
                        .ok_or("route missing")?;
                    resumed.lifecycle = RouteLifecycle::Active;
                    resumed.turn_id.clone_from(&turn_id);
                    self.mounts.insert(agent_id.clone(), mount);
                }
                self.state.turns.insert(turn_id, agent_id.clone());
                self.persist()?;
                let route = self.state.routes.get(&agent_id).ok_or("route missing")?;
                return Ok(subagent_context(&route.active_path()?));
            }
            let pending = self
                .state
                .pending
                .front()
                .cloned()
                .ok_or_else(|| "subagent start has no serialized spawn handshake".to_owned())?;
            if pending.expires_at_millis <= now_millis() {
                self.discard_pending_spawn(pending.fork_key).await?;
                return Err("subagent spawn handshake expired before SubagentStart".to_owned());
            }
            if authenticated_parent.is_some_and(|parent| parent != pending.parent_agent_id) {
                return Err("subagent start parent does not match its spawn permit".to_owned());
            }
            if pending.lifecycle != PendingSpawnLifecycle::Prepared {
                return Err("subagent spawn workspace is not fully prepared".to_owned());
            }
            let active_root_id = self.pending_active_root(&pending)?;
            if !pending.roots.contains_key(&root_key(active_root_id)) {
                return Err("active root is missing from prepared child context".to_owned());
            }
            let mount = self
                .pending_mounts
                .remove(&pending.fork_key)
                .ok_or_else(|| "prepared child mount is unavailable".to_owned())?;
            let route = Route {
                task_binding: pending.task_binding.clone(),
                agent_id: agent_id.clone(),
                turn_id: turn_id.clone(),
                context_id: pending.fork_key,
                root_id: active_root_id.into_bytes(),
                parent_agent_id: pending.parent_agent_id.clone(),
                roots: pending.roots.clone(),
                mount_path: pending.mount_path.clone(),
                lifecycle: RouteLifecycle::Active,
            };
            let active_path = route.active_path()?;
            let consumed = self
                .state
                .pending
                .pop_front()
                .ok_or_else(|| "subagent spawn handshake disappeared during start".to_owned())?;
            if consumed.fork_key != pending.fork_key {
                return Err("subagent spawn handshake changed during start".to_owned());
            }
            self.state.turns.insert(turn_id, agent_id.clone());
            self.state.routes.insert(agent_id.clone(), route);
            // The route is the request's last transition. Losing it leaves the
            // prepared spawn, as a crash before this save does, which recovery
            // prepares again or discards.
            if let Err(error) = self.persist_unflushed() {
                self.state.routes.remove(&agent_id);
                self.state.turns.retain(|_, bound| bound != &agent_id);
                self.state.pending.push_front(consumed);
                self.pending_mounts.insert(pending.fork_key, mount);
                return Err(error);
            }
            self.mounts.insert(agent_id.clone(), mount);
            Ok(subagent_context(&active_path))
        }

    pub(crate) async fn subagent_stop(&mut self, input: Value) -> Result<Value, String> {
            self.require_session(&input)?;
            self.recover_expired_adapter_leases().await?;
            let agent_id = string(&input, "agent_id")?;
            let turn_id = string(&input, "turn_id")?;
            if self.resolve_turn(&turn_id)? != agent_id {
                return Err("subagent stop does not belong to the routed agent turn".to_owned());
            }
            let route = self
                .state
                .routes
                .get(&agent_id)
                .cloned()
                .ok_or_else(|| "subagent stop refers to an unknown agent".to_owned())?;
            if route.lifecycle.is_frozen() {
                return Ok(json!({"suppressOutput": true}));
            }
            if self
                .state
                .leases
                .values()
                .any(|lease| lease.agent_id == agent_id)
            {
                return Err("subagent cannot stop while filesystem tools are still active".to_owned());
            }
            self.state
                .routes
                .get_mut(&agent_id)
                .ok_or_else(|| "subagent route disappeared during stop".to_owned())?
                .lifecycle = RouteLifecycle::StopRequested;
            // Persist the intent before unmounting. If this process dies, restore
            // remounts the route and the next lifecycle event can finish the same
            // transition without losing a descendant's publication target.
            self.persist()?;
            self.finalize_requested_stops(&agent_id).await?;
            let descendants = self
                .state
                .routes
                .values()
                .filter(|candidate| candidate.parent_agent_id == agent_id)
                .map(|candidate| format!("agents/{}", candidate.agent_id))
                .collect::<Vec<_>>();
            let changes = self
                .agent_changes_as(
                    &route.parent_agent_id,
                    json!({"agent": agent_id, "path": "."}),
                )
                .await
                .unwrap_or_else(|error| json!({"unavailable": error}));
            let reference = format!("agents/{agent_id}");
            let deferred = self
                .state
                .routes
                .get(&agent_id)
                .is_some_and(|route| route.lifecycle == RouteLifecycle::StopRequested);
            let summary = json!({
                "agent": reference,
                "state": if deferred { "stopping" } else { "frozen" },
                "changed": changes,
                "tests": {"status": "not-reported", "detail": "Acyclic does not infer test execution from process output"},
                "pendingDescendants": descendants,
                "actions": {
                    "inspect": format!("acyclic git diff {reference}"),
                    "merge": format!("acyclic git merge {reference}"),
                    "discard": format!("acyclic discard {reference}"),
                }
            });
            Ok(json!({
                "suppressOutput": deferred,
                "systemMessage": if deferred {
                    format!("Acyclic will freeze {reference} after its live descendants finish.")
                } else {
                    format!(
                        "Acyclic stopped {reference}. Inspect with `acyclic git diff {reference}`; merge with `acyclic git merge {reference}`; discard with `acyclic discard {reference}`."
                    )
                },
                "acyclicSummary": summary,
            }))
        }

    pub(crate) fn has_live_descendant(&self, ancestor: &str) -> bool {
            self.state.routes.values().any(|candidate| {
                if candidate.lifecycle.is_frozen() {
                    return false;
                }
                let mut parent = candidate.parent_agent_id.as_str();
                let mut visited = BTreeSet::new();
                while parent != self.state.root_agent_id && visited.insert(parent.to_owned()) {
                    if parent == ancestor {
                        return true;
                    }
                    let Some(route) = self.state.routes.get(parent) else {
                        break;
                    };
                    parent = &route.parent_agent_id;
                }
                false
            })
        }

    pub(crate) async fn finalize_requested_stops(&mut self, agent_id: &str) -> Result<(), String> {
            let mut candidate = Some(agent_id.to_owned());
            while let Some(agent_id) = candidate {
                let Some(route) = self.state.routes.get(&agent_id).cloned() else {
                    break;
                };
                if route.lifecycle != RouteLifecycle::StopRequested
                    || self.has_live_descendant(&agent_id)
                {
                    break;
                }
                if self
                    .state
                    .leases
                    .values()
                    .any(|lease| lease.agent_id == agent_id)
                {
                    return Err(format!(
                        "subagent '{agent_id}' cannot finish stopping while filesystem tools are active"
                    ));
                }
                self.unmount_agent(&agent_id).await?;
                WorkspaceContextRegistry::new(self.store.clone())
                    .set_active(WorkspaceContextId::from_bytes(route.context_id), false)
                    .await
                    .map_err(display)?;
                self.state
                    .routes
                    .get_mut(&agent_id)
                    .ok_or_else(|| "subagent route disappeared during stop".to_owned())?
                    .lifecycle = RouteLifecycle::Frozen;
                self.persist()?;
                candidate = (route.parent_agent_id != self.state.root_agent_id)
                    .then_some(route.parent_agent_id);
            }
            Ok(())
        }

}
