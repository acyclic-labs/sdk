use super::*;

impl ResumableMachine for PiTaskMachine {
    fn identity(&self) -> &MachineIdentity {
        &self.identity
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn identity_with_native_work<'a>(
        &'a self,
        _work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<&'a MachineIdentity> {
        Ok(&self.identity)
    }

    fn state_schema(&self) -> &Value {
        &self.schema
    }

    fn initialize(&self, _input: &Value) -> acyclic_harness::Result<Value> {
        Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi initialization requires native work",
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn initialize_with_native_work(
        &self,
        input: &Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(Value, acyclic_fs::NativeAllocation)> {
        let mut decoded = crate::pi_recovery::decode_recovery_value::<PiTaskInput>(
            std::borrow::Cow::Borrowed(input),
            acyclic_harness::JsonValueSchema::None,
            crate::MAXIMUM_ADAPTER_STATE_BYTES,
            work,
        )?;
        let state = initial_state(decoded.0, work)?;
        let mut encoded =
            crate::pi_root_provider::guarded_pi_value(&state, NATIVE_METADATA_MAXIMUM, work)?;
        decoded.1.absorb(&mut encoded.1)?;
        Ok((encoded.0, decoded.1))
    }

    fn transition(
        &self,
        _state: &Value,
        _input: &Value,
    ) -> acyclic_harness::Result<MachineTransition> {
        Err(acyclic_harness::Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unsupported,
            reason: "Pi transition requires native work",
        })
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn supports_native_work(&self) -> bool {
        true
    }

    #[cfg(not(target_arch = "wasm32"))]
    fn transition_with_native_work(
        &self,
        state: &Value,
        mut input: Value,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<(MachineTransition, acyclic_fs::NativeAllocation)> {
        use crate::pi_root_provider::guarded_pi_value;
        let mut state_metadata = native_metadata::<PiTaskState>(
            &MetadataValue(state, MetadataRole::State),
            acyclic_harness::JsonValueSchema::Root,
            STATE_LABEL_BYTES,
            work,
        )?;
        let unchanged_hosting =
            matches!(&state_metadata.0, PiTaskState::Hosting { .. }) && input.is_null();
        let mut moved_reply = None;
        let typed_input = match &state_metadata.0 {
            PiTaskState::Hosting { .. } if !input.is_null() => {
                let mut decoded = native_metadata::<PiControlInput>(
                    &input,
                    acyclic_harness::JsonValueSchema::None,
                    CONTROL_INPUT_LABEL_BYTES,
                    work,
                )?;
                state_metadata.1.absorb(&mut decoded.1)?;
                TransitionInput::Control(Box::new(decoded.0))
            }
            PiTaskState::Controlling { .. } => {
                require_single_command(&input)?;
                let mut decoded = native_metadata::<ControlResults>(
                    &MetadataValue(&input, MetadataRole::Results),
                    acyclic_harness::JsonValueSchema::Root,
                    CONTROL_RESULTS_LABEL_BYTES,
                    work,
                )?;
                state_metadata.1.absorb(&mut decoded.1)?;
                moved_reply = Some(take_result_body(&mut input)?);
                TransitionInput::Controls(decoded.0)
            }
            PiTaskState::Allocating { .. } | PiTaskState::Awaiting { .. } => {
                require_single_command(&input)?;
                let mut decoded = native_metadata::<CommandResults>(
                    &input,
                    acyclic_harness::JsonValueSchema::None,
                    COMMAND_RESULTS_LABEL_BYTES,
                    work,
                )?;
                state_metadata.1.absorb(&mut decoded.1)?;
                TransitionInput::Results(decoded.0)
            }
            _ if input.is_null() => TransitionInput::Null,
            _ => TransitionInput::Other,
        };
        let (next, command) = advance(state_metadata.0, typed_input)?;
        let status = match &next {
            PiTaskState::Completed { result } => {
                let mut encoded = guarded_pi_value(result, NATIVE_METADATA_MAXIMUM, work)?;
                state_metadata.1.absorb(&mut encoded.1)?;
                MachineStatus::Completed { value: encoded.0 }
            }
            PiTaskState::Failed { message } => MachineStatus::Failed {
                message: message.clone(),
            },
            _ => MachineStatus::Suspended,
        };
        let mut encoded_state = if unchanged_hosting {
            #[derive(Serialize)]
            struct HostingCheckpoint<'a> {
                state: &'static str,
                allocation: &'a FileRef,
                reply: Option<ReplyCheckpoint<'a>>,
            }
            #[derive(Serialize)]
            struct ReplyCheckpoint<'a> {
                operation_id: OperationId,
                value: &'a Value,
            }
            let PiTaskState::Hosting { allocation, reply } = &next else {
                unreachable!("null hosting transition retains hosting state");
            };
            let reply = match reply {
                Some(reply) => Some(ReplyCheckpoint {
                    operation_id: reply.operation_id,
                    value: state
                        .get("reply")
                        .and_then(|reply| reply.get("value"))
                        .ok_or_else(|| transition_error("Pi control reply has no value"))?,
                }),
                None => None,
            };
            guarded_pi_value(
                &HostingCheckpoint {
                    state: "hosting",
                    allocation,
                    reply,
                },
                work.remaining()?.bytes_encoded,
                work,
            )?
        } else {
            guarded_pi_value(&next, NATIVE_METADATA_MAXIMUM, work)?
        };
        state_metadata.1.absorb(&mut encoded_state.1)?;
        if let Some(body) = moved_reply {
            *encoded_state
                .0
                .get_mut("reply")
                .and_then(|reply| reply.get_mut("value"))
                .ok_or_else(|| transition_error("Pi native reply header is missing"))? = body;
        }
        Ok((
            MachineTransition {
                state: encoded_state.0,
                // The opaque closed-DTO quota is held before this one-slot Vec grows.
                commands: command.into_iter().collect(),
                status,
            },
            state_metadata.1,
        ))
    }
    #[cfg(not(target_arch = "wasm32"))]
    fn validate_transition_with_native_work(
        &self,
        state: &Value,
        input: &Value,
        expected: &MachineTransition,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> acyclic_harness::Result<()> {
        use crate::pi_root_provider::guarded_pi_value;
        let controlling = state.get("state").and_then(Value::as_str) == Some("controlling");
        let retaining_reply =
            state.get("state").and_then(Value::as_str) == Some("hosting") && input.is_null();
        if !controlling && !retaining_reply {
            let input = guarded_pi_value(input, work.remaining()?.bytes_encoded, work)?;
            let (transition, _custody) = self.transition_with_native_work(state, input.0, work)?;
            if &transition != expected {
                return Err(acyclic_harness::Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Conflict,
                    reason: "Pi retained transition differs from its machine",
                });
            }
            return Ok(());
        }
        // Reply bodies are forwarded unchanged. Recompute all metadata through
        // the same transition implementation and compare the borrowed body
        // separately, so verification never copies that body into scratch JSON.
        let state_header = guarded_pi_value(
            &MetadataValue(state, MetadataRole::State),
            NATIVE_METADATA_MAXIMUM,
            work,
        )?;
        let input_header = guarded_pi_value(
            &MetadataValue(input, MetadataRole::Results),
            NATIVE_METADATA_MAXIMUM,
            work,
        )?;
        let (transition, _custody) =
            self.transition_with_native_work(&state_header.0, input_header.0, work)?;
        let expected_header = guarded_pi_value(
            &MetadataValue(&expected.state, MetadataRole::State),
            NATIVE_METADATA_MAXIMUM,
            work,
        )?;
        let body = if controlling {
            require_single_command(input)?;
            input
                .get("commands")
                .and_then(Value::as_array)
                .and_then(|commands| commands.first())
                .and_then(|reply| reply.get("value"))
        } else {
            state.get("reply").and_then(|reply| reply.get("value"))
        };
        let expected_body = expected
            .state
            .get("reply")
            .and_then(|reply| reply.get("value"));
        if transition.state != expected_header.0
            || transition.commands != expected.commands
            || transition.status != expected.status
            || body != expected_body
        {
            return Err(acyclic_harness::Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Conflict,
                reason: "Pi retained transition differs from its machine",
            });
        }
        Ok(())
    }
}
