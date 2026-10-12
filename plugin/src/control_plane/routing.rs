//! Session and workspace route selection.

use super::*;

impl ControlPlane {
    #[cfg(test)]
        pub(crate) fn caller(&self, input: &Value) -> Result<String, String> {
            if input.get("_caller_agent_id").is_some() {
                return Err(
                    "workspace control caller identity must be derived by the service".to_owned(),
                );
            }
            let turn = input
                .get("_caller_turn_id")
                .and_then(Value::as_str)
                .ok_or_else(|| "workspace control call lacks a stable caller identity".to_owned())?;
            self.resolve_turn(turn)
                .map_err(|_| "workspace control caller is unknown".to_owned())
        }

    pub(crate) fn route_from_cwd(&self, cwd: &Path) -> Result<(String, Option<Route>), String> {
            self.route_root_from_cwd(cwd)
                .map(|(agent, route, _)| (agent, route))
        }

    pub(crate) fn route_root_from_cwd(
            &self,
            cwd: &Path,
        ) -> Result<(String, Option<Route>, WorkspaceRootId), String> {
            self.state.require_created_roots()?;
            let canonical = cwd.canonicalize().map_err(display)?;
            let mut mounted = Vec::new();
            for route in self
                .state
                .routes
                .values()
                .filter(|route| route.lifecycle.needs_mount())
            {
                for root in route.roots.values() {
                    let path = route.mount_path.join(root.mount_name());
                    if let Ok(path) = path.canonicalize()
                        && canonical.starts_with(&path)
                    {
                        mounted.push((
                            path.components().count(),
                            route.clone(),
                            WorkspaceRootId::from_bytes(root.root_id),
                        ));
                    }
                }
            }
            if let Some((_, route, root_id)) = mounted.into_iter().max_by_key(|(depth, _, _)| *depth) {
                return Ok((route.agent_id.clone(), Some(route), root_id));
            }
            if let Some((_, root)) = self
                .state
                .roots
                .values()
                .filter_map(|root| {
                    root.path
                        .canonicalize()
                        .ok()
                        .filter(|path| canonical.starts_with(path))
                        .map(|path| (path.components().count(), root))
                })
                .max_by_key(|(depth, _)| *depth)
            {
                return Ok((
                    self.state.root_agent_id.clone(),
                    None,
                    WorkspaceRootId::from_bytes(root.root_id),
                ));
            }
            Err("cwd is outside every root registered in this Acyclic session".to_owned())
        }

    pub(crate) fn require_session(&self, input: &Value) -> Result<(), String> {
            let session_id = string(input, "session_id")?;
            if session_id != self.state.root_session_id {
                return Err("lifecycle event belongs to another root session".to_owned());
            }
            Ok(())
        }

    pub(crate) fn context_for_agent(&self, agent_id: &str) -> Result<WorkspaceContextId, String> {
            if agent_id == self.state.root_agent_id {
                return Ok(WorkspaceContextId::from_bytes(self.state.root_context_id));
            }
            self.state
                .routes
                .get(agent_id)
                .map(|route| WorkspaceContextId::from_bytes(route.context_id))
                .ok_or_else(|| "workspace context alias is unknown".to_owned())
        }

    pub(crate) fn agent_for_context(&self, context_id: WorkspaceContextId) -> Option<String> {
            if self.state.root_context_id == context_id.into_bytes() {
                return Some(self.state.root_agent_id.clone());
            }
            self.state
                .routes
                .values()
                .find(|route| route.context_id == context_id.into_bytes())
                .map(|route| route.agent_id.clone())
        }

    pub(crate) fn resolve_turn(&self, turn_id: &str) -> Result<String, String> {
            if let Some(agent) = self.state.turns.get(turn_id) {
                return Ok(agent.clone());
            }
            if self.state.root_turns.contains(turn_id) {
                return Ok(self.state.root_agent_id.clone());
            }
            Err("tool call has no stable root or subagent identity".to_owned())
        }

    pub(crate) fn remember_root_turn(&mut self, turn_id: String) {
            self.state.root_turns.insert(turn_id);
            while self.state.root_turns.len() > MAXIMUM_ADAPTER_ROOT_TURNS {
                self.state.root_turns.pop_first();
            }
        }

    pub(crate) fn authorize_inspection(&self, caller: &str, target: &str) -> Result<(), String> {
            if caller == self.state.root_agent_id || caller == target {
                return Ok(());
            }
            let mut next = self.state.routes.get(target);
            while let Some(route) = next {
                if route.parent_agent_id == caller {
                    return Ok(());
                }
                next = self.state.routes.get(&route.parent_agent_id);
            }
            Err("caller is not an ancestor of the requested agent".to_owned())
        }

    pub(crate) fn descendants(&self, agent: &str) -> Vec<String> {
            let mut result = Vec::new();
            let mut frontier = vec![agent.to_owned()];
            while let Some(parent) = frontier.pop() {
                for route in self.state.routes.values() {
                    if route.parent_agent_id == parent {
                        result.push(route.agent_id.clone());
                        frontier.push(route.agent_id.clone());
                    }
                }
            }
            result
        }

}
