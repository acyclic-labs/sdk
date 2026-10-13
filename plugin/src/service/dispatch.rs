//! Dispatch for the service.
use super::*;

impl ConcurrentControlRequestDispatcher for ConcurrentServiceControl {
    async fn dispatch_request(
        &self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        if request.command == ControlCommand::Ping {
            let catalog = self.catalog.lock().await;
            let active_sessions = catalog.active.len();
            let opening_sessions = catalog.opening.len();
            let draining_sessions = catalog.draining.len();
            return Ok((
                json!({
                    "identity": self.binary_identity,
                    "instanceId": self.instance_id,
                    "sessions": active_sessions + opening_sessions + draining_sessions,
                    "activeSessions": active_sessions,
                    "openingSessions": opening_sessions,
                    "drainingSessions": draining_sessions,
                }),
                None,
            ));
        }
        self.ensure_running().map_err(Error::Invalid)?;
        if request.command == ControlCommand::Doctor {
            parse_read_only_arguments(&request.argv).map_err(Error::Invalid)?;
            let executable = executable_digests_async().await.map_err(Error::Storage)?;
            let (session_claims, handles) = {
                let catalog = self.catalog.lock().await;
                (
                    catalog.active.len() + catalog.opening.len() + catalog.draining.len(),
                    catalog
                        .active
                        .values()
                        .chain(catalog.draining.values())
                        .cloned()
                        .collect::<Vec<_>>(),
                )
            };
            let mut routes = 0;
            let mut leases = 0;
            let mut pending_recovery = false;
            for handle in &handles {
                handle
                    .with_snapshot(|snapshot| match snapshot {
                        Some(SessionSnapshot::HostManaged(state)) => {
                            routes += state.routes.len();
                            leases += state.leases.len();
                            pending_recovery |=
                                !state.pending_discards.is_empty() || !state.pending.is_empty();
                        }
                        Some(SessionSnapshot::Pi {
                            routes: count_routes,
                            leases: count_leases,
                            pending_recovery: pending,
                            ..
                        }) => {
                            routes += count_routes;
                            leases += count_leases;
                            pending_recovery |= pending;
                        }
                        None => {}
                    })
                    .map_err(Error::Storage)?;
            }
            return doctor_report(
                &self.data,
                &self.binary_identity,
                &executable.sha256,
                &executable.blake3,
                session_claims,
                routes,
                leases,
                pending_recovery,
            )
            .map(|value| (value, None))
            .map_err(Error::Storage);
        }
        if request.command == ControlCommand::Hook {
            let (host, event) = request.name.split_once(':').ok_or_else(|| {
                Error::Invalid("native hook request has no host/event pair".to_owned())
            })?;
            let session_id =
                hook_id(&request.arguments, "session_id", "sessionId").map_err(Error::Invalid)?;
            if host == "pi"
                && let Some(caller) = request.arguments.get("caller")
            {
                let caller: acyclic_harness::core::Scope =
                    serde_json::from_value(caller.clone())
                        .map_err(|error| Error::Invalid(display(error)))?;
                let root_session = pi_caller::routing_session(&caller).map_err(Error::Invalid)?;
                // Routing is not authentication: only the existing original
                // actor verifies this proof and its admitted native child.
                // A recursive child can never bootstrap another root budget.
                return self
                    .session(&root_session)
                    .await
                    .map_err(Error::Invalid)?
                    .dispatch(request, original_native_work)
                    .await;
            }
            if matches!(event, "SessionStart" | "sessionStart") {
                let root =
                    hook_root_path(&request.arguments).unwrap_or_else(|| request.cwd.clone());
                let (_, output) = self
                    .start_session(
                        &session_id,
                        host,
                        root,
                        request.arguments.clone(),
                        original_native_work,
                    )
                    .await?;
                return Ok(output);
            }
            if matches!(event, "SessionEnd" | "sessionEnd") {
                let status = self
                    .end_session(&session_id, true, original_native_work)
                    .await?;
                return Ok((
                    match status {
                        SessionCloseState::Closed => json!({}),
                        SessionCloseState::Failed => json!({"closed": false, "state": "failed"}),
                    },
                    None,
                ));
            }
            return self
                .session(&session_id)
                .await
                .map_err(Error::Invalid)?
                .dispatch(request, original_native_work)
                .await;
        }
        let handle = if let Some(handle) = self
            .route_session(&request.cwd)
            .await
            .map_err(Error::Storage)?
        {
            handle
        } else {
            let catalog = self.catalog.lock().await;
            if !catalog.active.is_empty()
                || !catalog.opening.is_empty()
                || !catalog.draining.is_empty()
            {
                return Err(Error::Invalid("cwd is outside every registered Acyclic session; refusing service-assisted filesystem access".to_owned()));
            }
            drop(catalog);
            let canonical = request
                .cwd
                .canonicalize()
                .map_err(|error| Error::Storage(display(error)))?;
            let session_id = format!(
                "cwd:{}",
                blake3::hash(canonical.as_os_str().to_string_lossy().as_bytes()).to_hex()
            );
            self.start_session(
                &session_id,
                "cli",
                canonical,
                json!({}),
                original_native_work,
            )
            .await?
            .0
        };
        handle.dispatch(request, original_native_work).await
    }
}

pub(crate) async fn dispatch_session_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    control.make_durable().await?;
    if request.command == ControlCommand::Hook {
        let (host, event) = request
            .name
            .split_once(':')
            .ok_or_else(|| "native hook request has no host/event pair".to_owned())?;
        return dispatch_native_session_hook(control, host, event, request.arguments, &request.cwd)
            .await;
    }
    let selected_cwd = selected_cli_cwd(&request)?;
    let (invocation_caller, _) = control.route_from_cwd(&request.cwd)?;
    let (selected_caller, _) = control.route_from_cwd(&selected_cwd)?;
    if selected_caller != invocation_caller {
        return Err("acyclic -C cannot cross workspace-context authority boundaries".to_owned());
    }
    let mut request = request;
    request.cwd = selected_cwd;
    request.arguments = Value::Null;
    dispatch_plane_request(control, request).await
}

pub(crate) async fn dispatch_plane_request(
    control: &mut ControlPlane,
    request: ControlRequest,
) -> Result<Value, String> {
    match request.command {
        ControlCommand::Ping => Ok(json!({})),
        ControlCommand::Doctor => {
            parse_read_only_arguments(&request.argv)?;
            let executable = executable_digests_async().await?;
            doctor_report(
                &control.config_root,
                &executable.service_identity,
                &executable.sha256,
                &executable.blake3,
                1,
                control.state.routes.len(),
                control.state.leases.len(),
                !control.state.pending_discards.is_empty() || !control.state.pending.is_empty(),
            )
        }
        ControlCommand::Agents => {
            parse_read_only_arguments(&request.argv)?;
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            control.agents_status(&caller).await
        }
        ControlCommand::Discard => {
            let (caller, _) = control.route_from_cwd(&request.cwd)?;
            let reference = request
                .argv
                .first()
                .and_then(|value| value.strip_prefix("agents/"))
                .ok_or_else(|| "discard requires agents/<ref>".to_owned())?;
            control
                .agent_discard_as(&caller, json!({"agent": reference}))
                .await
        }
        ControlCommand::Git => {
            if request.argv.is_empty() {
                return Err("acyclic git requires a Git-style subcommand".to_owned());
            }
            let (caller, route, root_id) = control.route_root_from_cwd(&request.cwd)?;
            if request.argv.as_slice() == ["merge", "--continue"] {
                return control.agent_merge_transition(&caller, false).await;
            }
            if request.argv.as_slice() == ["merge", "--abort"] {
                return control.agent_merge_transition(&caller, true).await;
            }
            if request.argv.first().is_some_and(|command| command == "add")
                && let Some(operation_id) = control
                    .pending_conflict_for_parent(control.context_for_agent(&caller)?)
                    .await?
            {
                control.sync_agent(&caller).await?;
                let paths = request
                    .argv
                    .iter()
                    .skip(1)
                    .filter(|argument| argument.as_str() != "--")
                    .filter(|argument| !argument.starts_with('-'))
                    .cloned()
                    .collect::<Vec<_>>();
                let paths = if paths.is_empty()
                    && request
                        .argv
                        .iter()
                        .skip(1)
                        .any(|argument| argument == "-A" || argument == "--all")
                {
                    vec![".".to_owned()]
                } else {
                    paths
                };
                if paths.is_empty() {
                    return Err("acyclic git add requires a conflicted path or -A".to_owned());
                }
                control
                    .publication_coordinator()
                    .await?
                    .declare_conflicted_paths(
                        operation_id,
                        control.context_for_agent(&caller)?,
                        root_id,
                        &paths,
                    )
                    .await
                    .map_err(display)?;
                return Ok(json!({"status":"resolved","automaticStaging":true}));
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "merge")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                return control
                    .agent_merge_as(&caller, json!({"agent": reference}))
                    .await;
            }
            if request
                .argv
                .first()
                .is_some_and(|command| command == "diff")
                && let Some(reference) = request
                    .argv
                    .get(1)
                    .and_then(|value| value.strip_prefix("agents/"))
            {
                let mut paths = request.argv.get(2..).unwrap_or_default();
                if paths.first().is_some_and(|argument| argument == "--") {
                    paths = paths.get(1..).unwrap_or_default();
                }
                if paths.len() > 1 {
                    return Err("acyclic git diff agents/<ref> accepts at most one path".to_owned());
                }
                return control
                    .agent_changes_as(
                        &caller,
                        json!({
                            "agent": reference,
                            "path": paths.first(),
                            "_caller_root_id": hex::encode(root_id.into_bytes())
                        }),
                    )
                    .await;
            }
            match route {
                Some(route) => {
                    control
                        .git_tool(&route.agent_id, &route, root_id, request.argv)
                        .await
                }
                None => control.root_git_tool(root_id, request.argv).await,
            }
        }
        ControlCommand::Hook => {
            Err("control command is invalid for a workspace session".to_owned())
        }
    }
}
