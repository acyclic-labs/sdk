//! Tool completion and publication acknowledgement.

use super::*;

impl ControlPlane {
    pub(crate) async fn post_tool(&mut self, input: Value) -> Result<Value, String> {
            self.require_session(&input)?;
            let turn_id = string(&input, "turn_id")?;
            let tool_name = string(&input, "tool_name")?;
            let caller = self.resolve_turn(&turn_id)?;
            let tool_use_id = string(&input, "tool_use_id")?;
            let Some(record) = self.state.leases.get(&tool_use_id).cloned() else {
                if caller != self.state.root_agent_id
                    && !is_known_non_filesystem_tool(&tool_name)
                    && !is_filesystem_tool(&tool_name)
                    && !is_spawn_tool(&tool_name)
                {
                    return Err(format!(
                        "unclassified post-tool '{tool_name}' is denied inside an isolated subagent workspace"
                    ));
                }
                return Ok(json!({}));
            };
            if record.agent_id != caller
                || (!record.turn_id.is_empty() && record.turn_id != turn_id)
                || (!record.tool_name.is_empty() && record.tool_name != tool_name)
            {
                return Err("post-tool hook does not match its pre-tool owner".to_owned());
            }
            let route = self
                .state
                .routes
                .get(&record.agent_id)
                .cloned()
                .ok_or_else(|| "post-tool route is missing".to_owned())?;
            let now = now_millis();
            // A single live lease captures the shared mount once. Earlier closes
            // only release their fences; an expired peer cannot remain the capture
            // owner. The core still decides whether this close owns reconciliation.
            let last_live_child_lease = !self.state.leases.iter().any(|(id, lease)| {
                id != &tool_use_id && lease.agent_id == record.agent_id && lease.expires_at_millis > now
            });
            // An active parent tool has no newer stable generation yet; the child
            // catches up in a later operation window or explicit join.
            if last_live_child_lease && self.ensure_agent_idle(&route.parent_agent_id).is_ok() {
                self.sync_agent(&route.parent_agent_id)
                    .await
                    .map_err(|error| format!("cannot synchronize the parent workspace: {error}"))?;
            }
            // The final close rebases each root onto its parent's head itself.
            let operations = OperationWindowCoordinator::new(self.fs.operation_window_store());
            let mut sync_error = None;
            let mut expired = false;
            let mut conflicts = Vec::new();
            let mut truncated = false;
            let mut finished_roots = Vec::new();
            for root_record in record.roots.values() {
                let root_id = WorkspaceRootId::from_bytes(root_record.root_id);
                let workspace = self
                    .workspace_root(&route, root_id)
                    .await
                    .map_err(|error| format!("cannot resolve the child workspace root: {error}"))?;
                let lease = root_record.lease();
                if last_live_child_lease && sync_error.is_none() {
                    let sync = match self.mounts.get(&record.agent_id) {
                        Some(mount) => mount
                            .sync_route_with_permit(
                                route_name(root_id).as_bytes(),
                                lease.publication_permit(),
                            )
                            .await
                            .map_err(display),
                        None => Err("subagent mount is quarantined after a fenced writer".to_owned()),
                    };
                    if let Err(error) = sync {
                        sync_error = Some(error);
                    }
                }
                match operations
                    .finish_workspace(&workspace, &lease, now, OperationReconcileLimits::default())
                    .await
                    .map_err(|error| format!("cannot close the filesystem operation lease: {error}"))?
                {
                    WorkspaceOperationFinish::StillActive { .. } => {}
                    WorkspaceOperationFinish::Reconciled(rebase) => {
                        finished_roots.push(root_id);
                        if let acyclic_fs::WorkspaceRebase::Conflicted {
                            conflicts: root_conflicts,
                            truncated: root_truncated,
                        } = rebase
                        {
                            conflicts.extend(root_conflicts);
                            truncated |= root_truncated;
                        }
                    }
                    WorkspaceOperationFinish::AlreadyClosed => expired = true,
                }
            }
            self.state.leases.remove(&tool_use_id);
            let propagated = if sync_error.is_none() && !expired {
                let mut propagated = Ok(());
                for root_id in finished_roots {
                    propagated = self
                        .propagate_parent_advance(&record.agent_id, root_id)
                        .await;
                    if propagated.is_err() {
                        break;
                    }
                }
                propagated
            } else {
                Ok(())
            };
            // The close is saved last, so that it is the request's final effect;
            // the operation is already closed in the store, and recovery closes a
            // lease whose close was lost.
            self.persist_unflushed()
                .map_err(|error| format!("cannot persist the closed tool lease: {error}"))?;
            if let Some(error) = sync_error {
                let quarantine_error = self
                    .mounts
                    .remove(&record.agent_id)
                    .and_then(|mount| mount.abandon().err())
                    .map(|cleanup| format!("; mount quarantine also failed: {cleanup}"))
                    .unwrap_or_default();
                return Err(format!(
                    "filesystem tool publication was fenced before lease close: {error}{quarantine_error}"
                ));
            }
            if expired {
                return Err("filesystem tool lease expired; late writes were fenced".to_owned());
            }
            propagated?;
            if conflicts.is_empty() {
                Ok(json!({}))
            } else {
                Ok(json!({
                    "systemMessage": format!(
                        "Acyclic workspace rebase reported {} typed conflict(s); truncated={truncated}",
                        conflicts.len()
                    ),
                    "conflicts": conflicts.iter().map(conflict_json).collect::<Vec<_>>(),
                    "truncated": truncated
                }))
            }
        }

}
