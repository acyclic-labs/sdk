//! Pi hook dispatch and bounded session replies.
use super::*;
use crate::pi_selected_host::invalid;

impl PiSession {
    pub(super) async fn dispatch(
        &mut self,
        request: ControlRequest,
        original_native_work: &mut Option<acyclic_fs::NativeMaterializationWork>,
    ) -> acyclic_harness::Result<ControlDispatchResponse> {
        let work = Self::query_work(original_native_work)?;
        let mut issued_caller = None;
        let mut caller_task = None;
        let mut operation_result = None;
        match request.command {
            ControlCommand::Agents => {
                parse_read_only_arguments(&request.argv).map_err(RootError::Invalid)?;
            }
            ControlCommand::Hook => {
                let event = request
                    .name
                    .strip_prefix("pi:")
                    .ok_or_else(|| invalid("Pi session refuses another host's control"))?;
                if matches!(event, "OperatorInspect" | "OperatorApproval") {
                    return self
                        .operator_control(event, &request.arguments, original_native_work)
                        .await;
                }
                let session = request
                    .arguments
                    .get("session_id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| invalid("Pi control requires its original session identity"))?;
                let owner = pi_owner::PiOwnerConfig::load(
                    &self.resources.private_state_root,
                    &self.relative_directory,
                    &work,
                )?;
                self.runtime.verify_owner_with_native_work(&owner, &work)?;
                let caller = request
                    .arguments
                    .get("caller")
                    .map(|value| {
                        acyclic_harness::core::Scope::from_value_with_native_work(value, &work)
                    })
                    .transpose()?;
                let task = if let Some((scope, _)) = &caller {
                    pi_caller::verify(&owner, &self.state, scope, session, &work)?.task_id
                } else {
                    if event != "SessionStart" || session != self.state.root_session_id {
                        return Err(invalid("Pi control requires its original caller proof"));
                    }
                    let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
                        return Err(invalid("Pi original root binding is missing"));
                    };
                    issued_caller = Some(pi_caller::issue(
                        &owner,
                        &self.state.root_session_id,
                        binding,
                        &work,
                    )?);
                    binding.task_id
                };
                caller_task = Some(task);
                match event {
                    "SessionStart" => {
                        if session == self.state.root_session_id {
                            let selected = PathBuf::from(
                                string(&request.arguments, "cwd").map_err(RootError::Invalid)?,
                            )
                            .canonicalize()
                            .map_err(|error| RootError::Storage(error.to_string()))?;
                            if !self.state.roots.values().any(|root| root.path == selected) {
                                return Err(invalid(
                                    "Pi root selection changed after original admission",
                                ));
                            }
                        }
                        if session == self.state.root_session_id || self.roots.is_empty() {
                            self.prepare_root(&work).await?;
                            self.recover_native_children(&work).await?;
                        }
                        if session != self.state.root_session_id {
                            let route = self.state.routes.values().find(|route| {
                                matches!(&route.task_binding, PluginTaskBinding::Pi(binding)
                                    if binding.task_id == task && binding.native_session_id == session)
                            }).ok_or_else(|| invalid("Pi native child has no original created route"))?;
                            let selected = PathBuf::from(
                                string(&request.arguments, "cwd").map_err(RootError::Invalid)?,
                            )
                            .canonicalize()
                            .map_err(|error| RootError::Storage(error.to_string()))?;
                            if !selected.starts_with(&route.mount_path)
                                || !route.lifecycle.needs_mount()
                            {
                                return Err(invalid(
                                    "Pi native child selection differs from its created view",
                                ));
                            }
                        }
                    }
                    "NativeBuiltinRead" | "NativeBuiltinWrite" | "NativeBuiltinEdit" => {
                        return self
                            .native_builtin_control(task, event, &request.arguments, &owner, &work)
                            .await;
                    }
                    "SpawnPrepare" => {
                        operation_result =
                            Some(self.prepare_spawn(task, &request.arguments, &owner, &work)?);
                    }
                    "SpawnCommit" => {
                        let committed = self
                            .spawn_commit(task, &request.arguments, &owner, &work)
                            .await?;
                        let binding = self.binding(task)?;
                        return guarded_scoped_reply(
                            &self.state.root_session_id,
                            &binding.native_session_id,
                            task,
                            committed,
                        );
                    }
                    "TaskInspect" | "TaskDiff" | "TaskImport" | "TaskWriteback" => {
                        let selected = self
                            .selected_control(task, event, &request.arguments, &owner, &work)
                            .await?;
                        let binding = self.binding(task)?;
                        return guarded_scoped_reply(
                            &self.state.root_session_id,
                            &binding.native_session_id,
                            task,
                            selected,
                        );
                    }
                    "TaskWritebackStatus" => {
                        return self.writeback_status(task, &request.arguments, &work).await;
                    }
                    "TaskSend" | "TaskSteer" => {
                        let kind = if event == "TaskSteer" {
                            pi_mail::PiMailKind::Steer
                        } else {
                            pi_mail::PiMailKind::Message
                        };
                        let (value, custody) = self
                            .send_control(task, &request.arguments, &owner, kind, &work)
                            .await?;
                        let binding = self.binding(task)?;
                        return guarded_scoped_reply(
                            &self.state.root_session_id,
                            &binding.native_session_id,
                            task,
                            (
                                PiAcknowledgement {
                                    acknowledgement: value,
                                },
                                custody,
                            ),
                        );
                    }
                    "TaskReceive" => {
                        let after = request
                            .arguments
                            .get("after")
                            .and_then(Value::as_u64)
                            .ok_or_else(|| {
                                invalid("Pi receive requires an exact mailbox cursor")
                            })?;
                        let work = self.mail_work(task, &owner, &work).await?;
                        let binding = self.binding(task)?;
                        let received =
                            pi_mail::receive(&self.runtime.runtime, binding, after, 1, &work)
                                .await?;
                        return guarded_scoped_reply(
                            &self.state.root_session_id,
                            &binding.native_session_id,
                            task,
                            received,
                        );
                    }
                    "TaskWait" => {
                        let target = self.alias_task(
                            &string(&request.arguments, "id").map_err(RootError::Invalid)?,
                        )?;
                        let work = self.mail_work(task, &owner, &work).await?;
                        let binding = self.binding(task)?;
                        let observed = pi_mail::wait(
                            &self.runtime.runtime,
                            binding,
                            target,
                            0,
                            |caller, target| self.authorize_subtree(caller, target),
                            &work,
                        )
                        .await?;
                        return guarded_scoped_reply(
                            &self.state.root_session_id,
                            &binding.native_session_id,
                            task,
                            observed,
                        );
                    }
                    "TaskStatus" => {}
                    "TaskCancel" => {
                        let target: TaskId = serde::Deserialize::deserialize(
                            request.arguments.get("task").ok_or_else(|| {
                                invalid("Pi cancellation requires its original task")
                            })?,
                        )
                        .map_err(|error| RootError::Invalid(error.to_string()))?;
                        self.cancel_native_subtree(task, target, &work).await?;
                    }
                    _ => return Err(invalid("unknown Pi task control operation")),
                }
            }
            _ => {
                return Err(invalid(
                    "Pi workspace operations require their owning task runtime",
                ));
            }
        }
        let observations =
            pi_recovery::observe_pi_session(&self.runtime.runtime, &self.state, &work).await?;
        let PluginTaskBinding::Pi(binding) = &self.state.task_binding else {
            return Err(invalid("Pi original root binding is missing"));
        };
        let native_session = &self
            .binding(caller_task.unwrap_or(binding.task_id))?
            .native_session_id;
        let response = PiStatusResponse {
            session: &self.state.root_session_id,
            native_session,
            tasks: &observations.0,
            caller_task,
            caller: issued_caller.as_ref().map(|reply| &reply.0),
            operation: operation_result.as_ref().map(|reply| &reply.0),
        };
        let encoded = pi_root_provider::guarded_pi_value(
            &response,
            MAXIMUM_CONTROL_MESSAGE_BYTES as u64 * 8,
            observations.1.work(),
        );
        drop(observations.0);
        let mut encoded = encoded?;
        let incoming = [
            Some(observations.1),
            operation_result.map(|(value, custody)| {
                drop(value);
                custody
            }),
            issued_caller.map(|(scope, custody)| {
                drop(scope);
                custody
            }),
        ];
        for mut custody in incoming.into_iter().flatten() {
            encoded.1.absorb(&mut custody)?;
        }
        Ok((encoded.0, Some(encoded.1)))
    }
}
