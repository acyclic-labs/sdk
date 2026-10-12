//! Scoped agent status and change inspection.

use super::*;

impl ControlPlane {
    pub(crate) fn author_for_argv(
            &self,
            argv: &[String],
            fallback: &str,
        ) -> Result<String, String> {
            let uses_default = argv.first().is_some_and(|command| command == "commit")
                && !argv
                    .iter()
                    .any(|argument| argument == "--author" || argument.starts_with("--author="));
            if !uses_default {
                return Ok(fallback.to_owned());
            }
            let path = self.config_root.join("author.json");
            let bytes = match fs::read(&path) {
                Ok(bytes) => bytes,
                Err(error) if error.kind() == io::ErrorKind::NotFound => {
                    return Ok(fallback.to_owned());
                }
                Err(error) => return Err(format!("cannot read {}: {error}", path.display())),
            };
            #[derive(Deserialize)]
            #[serde(deny_unknown_fields)]
            struct AuthorConfiguration {
                version: u32,
                author: String,
            }
            let configuration: AuthorConfiguration = serde_json::from_slice(&bytes)
                .map_err(|error| format!("invalid {}: {error}", path.display()))?;
            if configuration.version != 1 || configuration.author.trim().is_empty() {
                return Err(format!(
                    "invalid {}: expected version 1 and a non-empty author",
                    path.display()
                ));
            }
            Ok(configuration.author)
        }

    pub(crate) async fn agents_status(&mut self, caller: &str) -> Result<Value, String> {
            let routes = self
                .state
                .routes
                .values()
                .filter(|route| {
                    route.parent_agent_id == caller
                        || caller == self.state.root_agent_id
                        || route.agent_id == caller
                })
                .cloned()
                .collect::<Vec<_>>();
            let mut agents = Vec::with_capacity(routes.len());
            for route in routes {
                let mut depth = 1_usize;
                let mut parent = route.parent_agent_id.as_str();
                let mut visited = BTreeSet::new();
                while parent != self.state.root_agent_id && visited.insert(parent.to_owned()) {
                    let Some(ancestor) = self.state.routes.get(parent) else {
                        break;
                    };
                    depth += 1;
                    parent = &ancestor.parent_agent_id;
                }
                let active_leases = self
                    .state
                    .leases
                    .values()
                    .filter(|lease| lease.agent_id == route.agent_id)
                    .count();
                let descendants = self
                    .state
                    .routes
                    .values()
                    .filter(|candidate| candidate.parent_agent_id == route.agent_id)
                    .count();
                let conflict = self
                    .pending_conflict_for_parent(WorkspaceContextId::from_bytes(route.context_id))
                    .await?
                    .is_some();
                let context = WorkspaceContextRegistry::new(self.store.clone())
                    .resolve(WorkspaceContextId::from_bytes(route.context_id))
                    .await
                    .map_err(display)?;
                let mut roots = Vec::with_capacity(route.roots.len());
                let mut pending_publications = 0_usize;
                for root in route.roots.values() {
                    let workspace_id = context
                        .roots
                        .get(&WorkspaceRootId::from_bytes(root.root_id))
                        .ok_or_else(|| "adapter route is absent from its core context".to_owned())?
                        .workspace_id;
                    let current = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(display)?.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                        .map_err(display)?
                        .id();
                    let current = *current.digest().as_bytes();
                    let unpublished = root.published_generation != current;
                    pending_publications += usize::from(unpublished);
                    roots.push(json!({
                        "id": hex::encode(root.root_id),
                        "route": root.mount_name(),
                        "workspace": hex::encode(workspace_id.into_bytes()),
                        "generation": hex::encode(current),
                        "publishedGeneration": if root.published_generation == [0; 32] {
                            Value::Null
                        } else {
                            Value::String(hex::encode(root.published_generation))
                        },
                        "unpublished": unpublished,
                    }));
                }
                let changes = if active_leases == 0 {
                    self.agent_changes_as(caller, json!({"agent": route.agent_id, "path": "."}))
                        .await
                        .ok()
                } else {
                    None
                };
                let file_changes = changes
                    .as_ref()
                    .and_then(|value| value.get("fileChanges"))
                    .and_then(Value::as_u64);
                let binding_changes = changes
                    .as_ref()
                    .and_then(|value| value.get("bindingChanges"))
                    .and_then(Value::as_u64);
                let changed_paths = changes
                    .as_ref()
                    .and_then(|value| value.get("roots"))
                    .and_then(Value::as_array)
                    .into_iter()
                    .flatten()
                    .flat_map(|root| {
                        root.get("paths")
                            .and_then(Value::as_array)
                            .into_iter()
                            .flatten()
                    })
                    .take(128)
                    .cloned()
                    .collect::<Vec<_>>();
                agents.push(json!({
                    "ref": format!("agents/{}", route.agent_id),
                    "agent": route.agent_id,
                    "parent": if route.parent_agent_id == self.state.root_agent_id {
                        Value::String("root".to_owned())
                    } else {
                        Value::String(format!("agents/{}", route.parent_agent_id))
                    },
                    "depth": depth,
                    "state": match route.lifecycle {
                        RouteLifecycle::Mounting => "mounting",
                        RouteLifecycle::Active => "running",
                        RouteLifecycle::StopRequested => "stopping",
                        RouteLifecycle::Frozen => "frozen",
                    },
                    "activeLeases": active_leases,
                    "conflicts": usize::from(conflict),
                    "descendants": descendants,
                    "fileChanges": file_changes,
                    "bindingChanges": binding_changes,
                    "changedPaths": changed_paths,
                    "workRemaining": file_changes.zip(binding_changes).map(|(files, bindings)| files.saturating_add(bindings)),
                    "pendingPublications": pending_publications,
                    "mount": route.active_path()?,
                    "roots": roots,
                }));
            }
            agents.sort_by(|left, right| {
                left.get("depth")
                    .and_then(Value::as_u64)
                    .cmp(&right.get("depth").and_then(Value::as_u64))
                    .then_with(|| {
                        left.get("ref")
                            .and_then(Value::as_str)
                            .cmp(&right.get("ref").and_then(Value::as_str))
                    })
            });
            Ok(json!({"schemaVersion": 1, "root": self.state.root_agent_id, "agents": agents}))
        }

    #[cfg(test)]
        pub(crate) async fn agent_changes(&mut self, input: Value) -> Result<Value, String> {
            let caller = self.caller(&input)?;
            self.agent_changes_as(&caller, input).await
        }

    pub(crate) async fn agent_changes_as(
            &mut self,
            caller: &str,
            input: Value,
        ) -> Result<Value, String> {
            let agent = string(&input, "agent")?;
            self.authorize_inspection(caller, &agent)?;
            self.ensure_agent_idle(&agent)?;
            if let Some(mount) = self.mounts.get(&agent) {
                mount.sync().await.map_err(display)?;
            }
            let route = self
                .state
                .routes
                .get(&agent)
                .cloned()
                .ok_or("unknown agent")?;
            let graph = WorkspaceGraph::new(self.store.clone());
            let parent_context = WorkspaceContextRegistry::new(self.store.clone())
                .resolve(self.context_for_agent(&route.parent_agent_id)?)
                .await
                .map_err(display)?;
            let requested_path = input.get("path").and_then(Value::as_str);
            let selected_root = input
                .get("_caller_root_id")
                .and_then(Value::as_str)
                .and_then(|value| decode_fixed::<16>(value).ok());
            let mut roots = Vec::new();
            let mut file_changes = 0_usize;
            let mut binding_changes = 0_usize;
            let mut truncated = false;
            for route_root in route.roots.values() {
                if selected_root.is_some_and(|root| root != route_root.root_id) {
                    continue;
                }
                let root_id = WorkspaceRootId::from_bytes(route_root.root_id);
                let workspace = self.workspace_root(&route, root_id).await?;
                let repository_id = repository_id(
                    workspace.id().into_bytes(),
                    route_root.repository_workspace_id,
                );
                let mut lineage_cursor = workspace.clone();
                let mut visited = BTreeSet::new();
                let mut branch_base = None;
                while lineage_cursor.id() != repository_id {
                    if !visited.insert(lineage_cursor.id()) || visited.len() > 64 {
                        return Err("Git compatibility branch lineage is cyclic or too deep".to_owned());
                    }
                    let lineage = graph.resolve(lineage_cursor.id()).await.map_err(display)?;
                    branch_base.get_or_insert(lineage.initial_generation);
                    let parent_id = lineage.parent_workspace_id.ok_or_else(|| {
                        "Git compatibility branch does not reach its repository workspace".to_owned()
                    })?;
                    let parent = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                        .await
                        .map_err(display)?;
                    graph
                        .authorize_join(lineage_cursor.id(), parent.id())
                        .await
                        .map_err(display)?;
                    lineage_cursor = parent;
                }
                let parent_root = parent_context
                    .roots
                    .get(&root_id)
                    .ok_or_else(|| "direct parent root is missing".to_owned())?;
                let parent = WorkspaceGraph::new(self.store.clone()).workspace(&self.fs, parent_root.workspace_id, WorkBudget::UNBOUNDED, &CancellationToken::new())
                    .await
                    .map_err(display)?;
                let lineage = graph
                    .authorize_join(lineage_cursor.id(), parent.id())
                    .await
                    .map_err(display)?;
                let base = workspace.generation(branch_base.unwrap_or(lineage.initial_generation), acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                    .map_err(display)?;
                let head = workspace.head(acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new()).await.map(|receipt| receipt.value).map_err(|failure| failure.error).map_err(display)?;
                let changes = workspace
                    .diff(&base, &head, 100_000)
                    .await
                    .map_err(display)?;
                let (root_files, root_bindings, paths) = if let Some(path) = requested_path {
                    let filter = agent_change_filter(path, &route, route_root, workspace.profile())?;
                    let paths = changes
                        .changed_paths(100_000, acyclic_fs::WorkBudget::UNBOUNDED, &acyclic_fs::CancellationToken::new())
                        .await.map(|receipt| receipt.value).map_err(|failure| failure.error)
                        .map_err(display)?
                        .into_iter()
                        .filter(|change| change.path.is_within(&filter))
                        .collect::<Vec<_>>();
                    let files = paths
                        .iter()
                        .filter(|change| change.before.is_some() && change.after.is_some())
                        .count();
                    let bindings = paths.len().saturating_sub(files);
                    let paths = paths
                        .iter()
                        .map(|change| namespace_path_text(&change.path))
                        .collect::<Result<Vec<_>, _>>()?;
                    (files, bindings, paths)
                } else {
                    (
                        changes.changes().files.len(),
                        changes.changes().bindings.len(),
                        Vec::new(),
                    )
                };
                file_changes = file_changes.saturating_add(root_files);
                binding_changes = binding_changes.saturating_add(root_bindings);
                truncated |= changes.changes().truncated;
                roots.push(json!({
                    "root": hex::encode(route_root.root_id),
                    "workspace": hex::encode(workspace.id().into_bytes()),
                    "generation": hex::encode(head.id().digest().as_bytes()),
                    "fileChanges": root_files,
                    "bindingChanges": root_bindings,
                    "truncated": changes.changes().truncated,
                    "paths": paths,
                }));
            }
            if roots.is_empty() {
                return Err("the selected path is outside the inspected workspace context".to_owned());
            }
            Ok(json!({
                "agent": agent,
                "fileChanges": file_changes,
                "bindingChanges": binding_changes,
                "truncated": truncated,
                "path": input.get("path").cloned().unwrap_or(Value::Null),
                "roots": roots,
            }))
        }

}
