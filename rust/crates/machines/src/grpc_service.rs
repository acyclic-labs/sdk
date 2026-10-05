//! gRPC projection of the canonical checked Machines provider.
use super::*;
use tonic::{Request, Response, Status};

/// Thin wire adapter. The supplied provider owns lifecycle and idempotency behavior.
/// Authentication is supplied by the server's TLS configuration or interceptor.
pub struct Service<P> {
    provider: Arc<P>,
}
impl<P> Service<P> {
    /// Bind one shared semantic provider to the generated Machines service.
    #[must_use]
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }
}
impl<P> Clone for Service<P> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.provider))
    }
}

fn status(error: ProviderError) -> Status {
    let message = error.to_string();
    match error {
        ProviderError::NotFound(_) => Status::not_found(message),
        ProviderError::Conflict(_) => Status::already_exists(message),
        ProviderError::Unsupported(_) => Status::unimplemented(message),
        ProviderError::Invalid(_) => Status::invalid_argument(message),
        ProviderError::Rejected(_) => Status::failed_precondition(message),
        ProviderError::Unavailable => Status::unavailable(message),
        ProviderError::Cancelled => Status::cancelled(message),
        ProviderError::Indeterminate(_) | ProviderError::OperationIndeterminate(_) => {
            Status::unknown(message)
        }
        ProviderError::Failed => Status::internal(message),
    }
}
fn version(value: Option<&wire::ProtocolVersion>) -> Result<(), Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("protocol is required"))?;
    if value.major != PROTOCOL_MAJOR || value.minor > PROTOCOL_MINOR {
        return Err(Status::failed_precondition(
            "unsupported Machines protocol version",
        ));
    }
    Ok(())
}
fn key(value: Option<&wire::IdempotencyKey>) -> Result<IdempotencyKey, Status> {
    let value = value.ok_or_else(|| Status::invalid_argument("idempotency key is required"))?;
    decode_uuid(&value.value)
        .map(IdempotencyKey)
        .map_err(|error| Status::invalid_argument(error.to_string()))
}
fn wire_operation(value: OperationId) -> wire::OperationId {
    wire::OperationId {
        value: value.as_bytes().to_vec(),
    }
}
fn operation_state(value: OperationObservation) -> wire::OperationState {
    let status = match value.phase {
        OperationPhase::Pending => wire::OperationStatus::Pending,
        OperationPhase::Succeeded => wire::OperationStatus::Succeeded,
        OperationPhase::Cancelled => wire::OperationStatus::Cancelled,
        OperationPhase::Indeterminate => wire::OperationStatus::Indeterminate,
        OperationPhase::Failed => wire::OperationStatus::Failed,
    };
    wire::OperationState {
        operation: Some(wire_operation(value.id)),
        status: status as i32,
    }
}
fn contract(value: &MachineContract) -> Result<wire::MachineContract, Status> {
    Ok(wire::MachineContract {
        image: Some(encode_image(&value.image).map_err(status)?),
        capabilities: value
            .capabilities
            .iter()
            .copied()
            .map(encode_capability)
            .collect(),
        compatibility: Some(encode_compatibility(&value.compatibility).map_err(status)?),
        compatibility_revision: value.compatibility_revision.to_vec(),
        suspension: Some(encode_suspension(value.suspension).map_err(status)?),
        expiration: Some(encode_expiration(value.expiration).map_err(status)?),
        network_policy_digest: value.network_policy_digest.to_vec(),
        budgets: Some(wire::Budgets {
            spend_micros: value.budgets.spend_micros,
            concurrency: value.budgets.concurrency,
        }),
    })
}
fn machine_state(value: MachineObservation) -> Result<wire::MachineState, Status> {
    let state = match value.state {
        MachineState::Starting => wire::MachineStatus::Starting,
        MachineState::Running => wire::MachineStatus::Running,
        MachineState::Suspending => wire::MachineStatus::Suspending,
        MachineState::Suspended => wire::MachineStatus::Suspended,
        MachineState::Waking => wire::MachineStatus::Waking,
        MachineState::Destroying => wire::MachineStatus::Destroying,
        MachineState::Destroyed => wire::MachineStatus::Destroyed,
        MachineState::Failed => wire::MachineStatus::Failed,
        MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
    };
    Ok(wire::MachineState {
        machine: Some(encode_machine(value.id)),
        status: state as i32,
        contract: Some(contract(&value.contract)?),
        endpoints: value
            .endpoints
            .into_iter()
            .map(|value| wire::Endpoint {
                name: value.name,
                uri: value.uri,
            })
            .collect(),
        last_checkpoint: value.last_checkpoint.map(encode_checkpoint),
        created_at_unix_ms: value.created_at_unix_ms,
        changed_at_unix_ms: value.changed_at_unix_ms,
    })
}
fn checkpoint_state(value: CheckpointObservation) -> Result<wire::CheckpointState, Status> {
    Ok(wire::CheckpointState {
        checkpoint: Some(encode_checkpoint(value.id)),
        source: Some(encode_machine(value.source)),
        contract: Some(contract(&value.contract)?),
        forkable: value.forkable,
        created_at_unix_ms: value.created_at_unix_ms,
    })
}
fn admission(
    value: MachineObservation,
    operation: OperationId,
) -> Result<wire::MachineAdmission, Status> {
    Ok(wire::MachineAdmission {
        machine: Some(encode_machine(value.id)),
        operation: Some(wire_operation(operation)),
        contract: Some(contract(&value.contract)?),
    })
}
fn checkpoint_admission(
    value: CheckpointObservation,
    operation: OperationId,
) -> Result<wire::CheckpointAdmission, Status> {
    Ok(wire::CheckpointAdmission {
        checkpoint: Some(encode_checkpoint(value.id)),
        source: Some(encode_machine(value.source)),
        operation: Some(wire_operation(operation)),
        contract: Some(contract(&value.contract)?),
    })
}
fn fork_admission(
    checkpoint: CheckpointId,
    values: Vec<MachineObservation>,
    operation: OperationId,
) -> Result<wire::ForkAdmission, Status> {
    let retained = values
        .first()
        .ok_or_else(|| Status::internal("provider returned an empty fork"))?;
    Ok(wire::ForkAdmission {
        checkpoint: Some(encode_checkpoint(checkpoint)),
        children: values
            .iter()
            .map(|value| encode_machine(value.id))
            .collect(),
        operation: Some(wire_operation(operation)),
        contract: Some(contract(&retained.contract)?),
    })
}
fn live_fork_admission(
    source: MachineId,
    fidelity: ForkFidelity,
    values: Vec<MachineObservation>,
    operation: OperationId,
) -> Result<wire::ForkMachineAdmission, Status> {
    let retained = values
        .first()
        .ok_or_else(|| Status::internal("provider returned an empty live fork"))?;
    Ok(wire::ForkMachineAdmission {
        source: Some(encode_machine(source)),
        children: values
            .iter()
            .map(|value| encode_machine(value.id))
            .collect(),
        operation: Some(wire_operation(operation)),
        contract: Some(contract(&retained.contract)?),
        fidelity: match fidelity {
            ForkFidelity::MemoryAndDisk => wire::ForkFidelity::MemoryAndDisk,
            ForkFidelity::DiskOnly => wire::ForkFidelity::DiskOnly,
        } as i32,
    })
}
fn mutation(
    machine: Option<MachineId>,
    checkpoint: Option<CheckpointId>,
    operation: OperationId,
) -> wire::MutationAdmission {
    wire::MutationAdmission {
        operation: Some(wire_operation(operation)),
        machine: machine.map(encode_machine),
        checkpoint: checkpoint.map(encode_checkpoint),
    }
}
fn event(value: MachineEvent) -> wire::MachineEvent {
    let (kind, state, pressure) = match value.fact {
        EventFact::State(state) => (
            wire::EventKind::State,
            match state {
                MachineState::Starting => wire::MachineStatus::Starting,
                MachineState::Running => wire::MachineStatus::Running,
                MachineState::Suspending => wire::MachineStatus::Suspending,
                MachineState::Suspended => wire::MachineStatus::Suspended,
                MachineState::Waking => wire::MachineStatus::Waking,
                MachineState::Destroying => wire::MachineStatus::Destroying,
                MachineState::Destroyed => wire::MachineStatus::Destroyed,
                MachineState::Failed => wire::MachineStatus::Failed,
                MachineState::Indeterminate => wire::MachineStatus::Indeterminate,
            },
            wire::PressureKind::Unspecified,
        ),
        EventFact::Pressure(value) => (
            wire::EventKind::Pressure,
            wire::MachineStatus::Unspecified,
            match value {
                Pressure::CustomerBudget => wire::PressureKind::CustomerBudget,
                Pressure::MachineLimit => wire::PressureKind::MachineLimit,
                Pressure::ServiceSaturation => wire::PressureKind::ServiceSaturation,
            },
        ),
        EventFact::CapacityChanged => (
            wire::EventKind::Capacity,
            wire::MachineStatus::Unspecified,
            wire::PressureKind::Unspecified,
        ),
    };
    wire::MachineEvent {
        machine: Some(encode_machine(value.machine)),
        sequence: value.sequence,
        observed_at_unix_ms: value.observed_at_unix_ms,
        kind: kind as i32,
        state: state as i32,
        pressure: pressure as i32,
    }
}

#[tonic::async_trait]
impl<P: MachinesProvider + 'static> wire::machines_service_server::MachinesService for Service<P> {
    async fn qualify_image(
        &self,
        request: Request<wire::QualifyImageRequest>,
    ) -> Result<Response<wire::ImageQualification>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let value = self
            .provider
            .qualify_image(decode_image(value.image.as_ref()).map_err(status)?)
            .await
            .map_err(status)?;
        Ok(Response::new(wire::ImageQualification {
            image: Some(encode_image(&value.image).map_err(status)?),
            capabilities: value
                .capabilities
                .into_iter()
                .map(encode_capability)
                .collect(),
            compatibility_revision: value.compatibility_revision.to_vec(),
        }))
    }
    async fn create(
        &self,
        request: Request<wire::CreateMachineRequest>,
    ) -> Result<Response<wire::MachineAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let policy = value
            .compatibility
            .as_ref()
            .ok_or_else(|| Status::invalid_argument("compatibility policy is required"))?;
        let capabilities = policy
            .required
            .iter()
            .map(|value| decode_capability(*value))
            .collect::<Result<BTreeSet<_>, _>>()
            .map_err(status)?;
        if capabilities.len() != policy.required.len() {
            return Err(Status::invalid_argument("duplicate required capability"));
        }
        let budgets = value
            .budgets
            .ok_or_else(|| Status::invalid_argument("budgets are required"))?;
        let request = CreateMachine {
            idempotency_key: id,
            image: decode_image(value.image.as_ref()).map_err(status)?,
            compatibility: decode_compatibility(Some(policy), &capabilities).map_err(status)?,
            suspension: decode_suspension(value.suspension.as_ref()).map_err(status)?,
            expiration: decode_expiration(value.expiration.as_ref()).map_err(status)?,
            network_policy_digest: digest(&value.network_policy_digest, "network policy")
                .map_err(status)?,
            budgets: Budgets {
                spend_micros: budgets.spend_micros,
                concurrency: budgets.concurrency,
            },
        };
        let MutationOutcome::Created(value) =
            self.provider.create(request).await.map_err(status)?
        else {
            return Err(Status::internal("provider returned a non-create outcome"));
        };
        Ok(Response::new(admission(
            value,
            self.provider.recover_operation(id).await.map_err(status)?,
        )?))
    }
    async fn checkpoint(
        &self,
        request: Request<wire::CheckpointMachineRequest>,
    ) -> Result<Response<wire::CheckpointAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let MutationOutcome::Checkpointed(value) = self
            .provider
            .checkpoint(decode_machine(value.machine.as_ref()).map_err(status)?, id)
            .await
            .map_err(status)?
        else {
            return Err(Status::internal(
                "provider returned a non-checkpoint outcome",
            ));
        };
        Ok(Response::new(checkpoint_admission(
            value,
            self.provider.recover_operation(id).await.map_err(status)?,
        )?))
    }
    async fn fork(
        &self,
        request: Request<wire::ForkCheckpointRequest>,
    ) -> Result<Response<wire::ForkAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let checkpoint = decode_checkpoint(value.checkpoint.as_ref()).map_err(status)?;
        let count = NonZeroU32::new(value.count)
            .ok_or_else(|| Status::invalid_argument("fork count must be positive"))?;
        let MutationOutcome::Forked(values) = self
            .provider
            .fork(checkpoint, count, id)
            .await
            .map_err(status)?
        else {
            return Err(Status::internal("provider returned a non-fork outcome"));
        };
        Ok(Response::new(fork_admission(
            checkpoint,
            values,
            self.provider.recover_operation(id).await.map_err(status)?,
        )?))
    }
    async fn fork_machine(
        &self,
        request: Request<wire::ForkMachineRequest>,
    ) -> Result<Response<wire::ForkMachineAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let source = decode_machine(value.machine.as_ref()).map_err(status)?;
        let count = NonZeroU32::new(value.count)
            .ok_or_else(|| Status::invalid_argument("fork count must be positive"))?;
        let MutationOutcome::MachineForked {
            source,
            fidelity,
            children,
        } = self
            .provider
            .fork_machine(source, count, id)
            .await
            .map_err(status)?
        else {
            return Err(Status::internal(
                "provider returned a non-live-fork outcome",
            ));
        };
        Ok(Response::new(live_fork_admission(
            source,
            fidelity,
            children,
            self.provider.recover_operation(id).await.map_err(status)?,
        )?))
    }
    async fn suspend(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let machine = decode_machine(value.machine.as_ref()).map_err(status)?;
        self.provider.suspend(machine, id).await.map_err(status)?;
        Ok(Response::new(mutation(
            Some(machine),
            None,
            self.provider.recover_operation(id).await.map_err(status)?,
        )))
    }
    async fn wake(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let machine = decode_machine(value.machine.as_ref()).map_err(status)?;
        self.provider.wake(machine, id).await.map_err(status)?;
        Ok(Response::new(mutation(
            Some(machine),
            None,
            self.provider.recover_operation(id).await.map_err(status)?,
        )))
    }
    async fn destroy_machine(
        &self,
        request: Request<wire::MachineMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let machine = decode_machine(value.machine.as_ref()).map_err(status)?;
        self.provider
            .destroy_machine(machine, id)
            .await
            .map_err(status)?;
        Ok(Response::new(mutation(
            Some(machine),
            None,
            self.provider.recover_operation(id).await.map_err(status)?,
        )))
    }

    async fn set_suspension_policy(
        &self,
        request: Request<wire::SetSuspensionPolicyRequest>,
    ) -> Result<Response<wire::PolicyAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let machine = decode_machine(value.machine.as_ref()).map_err(status)?;
        let policy = decode_suspension(value.policy.as_ref()).map_err(status)?;
        self.provider
            .set_suspension_policy(machine, policy, id)
            .await
            .map_err(status)?;
        Ok(Response::new(wire::PolicyAdmission {
            machine: Some(encode_machine(machine)),
            operation: Some(wire_operation(
                self.provider.recover_operation(id).await.map_err(status)?,
            )),
            policy: Some(encode_suspension(policy).map_err(status)?),
        }))
    }
    async fn destroy_checkpoint(
        &self,
        request: Request<wire::CheckpointMutationRequest>,
    ) -> Result<Response<wire::MutationAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let checkpoint = decode_checkpoint(value.checkpoint.as_ref()).map_err(status)?;
        self.provider
            .destroy_checkpoint(checkpoint, id)
            .await
            .map_err(status)?;
        Ok(Response::new(mutation(
            None,
            Some(checkpoint),
            self.provider.recover_operation(id).await.map_err(status)?,
        )))
    }
    async fn recover(
        &self,
        request: Request<wire::RecoverRequest>,
    ) -> Result<Response<wire::RecoveredAdmission>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let id = key(value.idempotency_key.as_ref())?;
        let operation = self.provider.recover_operation(id).await.map_err(status)?;
        use wire::recovered_admission::Result as Kind;
        let result = match self.provider.recover(id).await.map_err(status)? {
            MutationOutcome::Created(value) => Kind::Create(admission(value, operation)?),
            MutationOutcome::Checkpointed(value) => {
                Kind::Checkpoint(checkpoint_admission(value, operation)?)
            }
            MutationOutcome::Forked(values) => {
                let retained = values
                    .first()
                    .ok_or_else(|| Status::internal("provider returned empty recovered fork"))?;
                let checkpoint = retained
                    .last_checkpoint
                    .ok_or_else(|| Status::internal("recovered fork lacks checkpoint identity"))?;
                Kind::Fork(fork_admission(checkpoint, values, operation)?)
            }
            MutationOutcome::MachineForked {
                source,
                fidelity,
                children,
            } => Kind::ForkMachine(live_fork_admission(source, fidelity, children, operation)?),
            MutationOutcome::Suspended(machine) => {
                Kind::Suspend(mutation(Some(machine), None, operation))
            }
            MutationOutcome::Woken(machine) => Kind::Wake(mutation(Some(machine), None, operation)),
            MutationOutcome::MachineDestroyed(machine) => {
                Kind::DestroyMachine(mutation(Some(machine), None, operation))
            }
            MutationOutcome::CheckpointDestroyed(checkpoint) => {
                Kind::DestroyCheckpoint(mutation(None, Some(checkpoint), operation))
            }
            MutationOutcome::SuspensionPolicySet(machine, policy) => {
                Kind::SetSuspensionPolicy(wire::PolicyAdmission {
                    machine: Some(encode_machine(machine)),
                    operation: Some(wire_operation(operation)),
                    policy: Some(encode_suspension(policy).map_err(status)?),
                })
            }
        };
        Ok(Response::new(wire::RecoveredAdmission {
            operation: Some(wire_operation(operation)),
            result: Some(result),
        }))
    }
    async fn inspect_machine(
        &self,
        request: Request<wire::InspectMachineRequest>,
    ) -> Result<Response<wire::MachineState>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        Ok(Response::new(machine_state(
            self.provider
                .inspect_machine(decode_machine(value.machine.as_ref()).map_err(status)?)
                .await
                .map_err(status)?,
        )?))
    }
    async fn inspect_checkpoint(
        &self,
        request: Request<wire::InspectCheckpointRequest>,
    ) -> Result<Response<wire::CheckpointState>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        Ok(Response::new(checkpoint_state(
            self.provider
                .inspect_checkpoint(decode_checkpoint(value.checkpoint.as_ref()).map_err(status)?)
                .await
                .map_err(status)?,
        )?))
    }
    async fn list_machines(
        &self,
        request: Request<wire::ListMachinesRequest>,
    ) -> Result<Response<wire::MachinePage>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let after = value
            .after
            .as_ref()
            .map(|value| decode_machine(Some(value)))
            .transpose()
            .map_err(status)?;
        let page = self
            .provider
            .list_machines(after, value.limit)
            .await
            .map_err(status)?;
        Ok(Response::new(wire::MachinePage {
            machines: page
                .machines
                .into_iter()
                .map(machine_state)
                .collect::<Result<_, _>>()?,
            next: page.next.map(encode_machine),
        }))
    }
    async fn events(
        &self,
        request: Request<wire::EventsRequest>,
    ) -> Result<Response<wire::EventPage>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let page = self
            .provider
            .events(
                decode_machine(value.machine.as_ref()).map_err(status)?,
                (value.after_sequence != 0).then_some(value.after_sequence),
                value.limit,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(wire::EventPage {
            events: page.events.into_iter().map(event).collect(),
            next_sequence: page.next_sequence.unwrap_or(0),
        }))
    }
    async fn usage(
        &self,
        request: Request<wire::UsageRequest>,
    ) -> Result<Response<wire::UsageReceipt>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let value = self
            .provider
            .usage(
                decode_machine(value.machine.as_ref()).map_err(status)?,
                value.start_unix_ms,
                value.end_unix_ms,
            )
            .await
            .map_err(status)?;
        Ok(Response::new(wire::UsageReceipt {
            machine: Some(encode_machine(value.machine)),
            start_unix_ms: value.start_unix_ms,
            end_unix_ms: value.end_unix_ms,
            elastic_cpu_ns: value.elastic_cpu_ns,
            dedicated_cpu_ns: value.dedicated_cpu_ns,
            private_resident_byte_seconds: value.private_resident_byte_seconds,
            durable_private_bytes: value.durable_private_bytes,
            lineage_receipt_sha256: value.lineage_receipt_sha256.to_vec(),
            egress_bytes: value.egress_bytes,
            receipt: value.receipt,
        }))
    }
    async fn inspect_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let operation = decode_operation(value.operation.as_ref()).map_err(status)?;
        Ok(Response::new(operation_state(
            self.provider
                .inspect_operation(operation)
                .await
                .map_err(status)?,
        )))
    }
    async fn cancel(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<wire::OperationState>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let operation = decode_operation(value.operation.as_ref()).map_err(status)?;
        Ok(Response::new(operation_state(
            self.provider.cancel(operation).await.map_err(status)?,
        )))
    }

    type WatchOperationStream =
        Pin<Box<dyn futures::Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>>;
    async fn watch_operation(
        &self,
        request: Request<wire::OperationRequest>,
    ) -> Result<Response<Self::WatchOperationStream>, Status> {
        let value = request.into_inner();
        version(value.protocol.as_ref())?;
        let operation = decode_operation(value.operation.as_ref()).map_err(status)?;
        let stream = self
            .provider
            .watch_operation(operation)
            .await
            .map_err(status)?;
        Ok(Response::new(Box::pin(
            stream.map(|value| value.map(operation_state).map_err(status)),
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio_stream::wrappers::TcpListenerStream;

    #[tokio::test]
    async fn canonical_provider_serves_all_nineteen_rpc_operations_with_real_state()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let provider = Arc::new(SimulatedMachines::default());
        let pending_operation = OperationId::new();
        provider.state.lock().await.operations.insert(
            pending_operation,
            OperationObservation {
                id: pending_operation,
                phase: OperationPhase::Pending,
            },
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown, stopped) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(wire::machines_service_server::MachinesServiceServer::new(
                    Service::new(provider),
                ))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = stopped.await;
                })
                .await
        });
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        let client = Machines::grpc(channel.clone());
        let mut raw = wire::machines_service_client::MachinesServiceClient::new(channel);
        assert_eq!(
            client.cancel_operation(pending_operation).await?.phase,
            OperationPhase::Cancelled
        );
        assert_eq!(
            client.inspect_operation(pending_operation).await?.phase,
            OperationPhase::Cancelled
        );
        let mut cancelled = client.watch_operation(pending_operation).await?;
        assert_eq!(
            cancelled.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Cancelled)
        );
        assert!(cancelled.next().await.is_none());
        let image = Image::custom([7; 32])?;
        assert_eq!(client.qualify_image(image.clone()).await?.image, image);
        let create_key = IdempotencyKey::new();
        let request = CreateMachine::new(create_key, image, [8; 32]);
        let machine = client.create(request.clone()).await?;
        assert_eq!(client.create(request.clone()).await?.id(), machine.id());
        let mut changed = request;
        changed.network_policy_digest = [9; 32];
        assert!(client.create(changed).await.is_err());
        assert_eq!(machine.inspect().await?.state, MachineState::Running);
        assert_eq!(client.list(None, 20).await?.machines.len(), 1);
        let operation = client.operation_for(create_key).await?;
        assert_eq!(
            client.inspect_operation(operation).await?.phase,
            OperationPhase::Succeeded
        );
        assert_eq!(
            client.cancel_operation(operation).await?.phase,
            OperationPhase::Succeeded
        );
        let mut observations = client.watch_operation(operation).await?;
        assert_eq!(
            observations.next().await.transpose()?.map(|value| value.id),
            Some(operation)
        );
        assert!(observations.next().await.is_none());
        match client.recover(create_key).await? {
            MutationOutcome::Created(value) => assert_eq!(value.id, machine.id()),
            _ => panic!("recover must retain the actual admitted create"),
        }
        let checkpoint = machine.checkpoint(IdempotencyKey::new()).await?;
        assert_eq!(checkpoint.inspect().await?.id, checkpoint.id());
        let checkpoint_children = checkpoint
            .fork(NonZeroU32::new(2).unwrap(), IdempotencyKey::new())
            .await?;
        assert_eq!(checkpoint_children.len(), 2);
        let live = machine
            .fork(NonZeroU32::new(2).unwrap(), IdempotencyKey::new())
            .await?;
        assert_eq!(live.children.len(), 2);
        assert_eq!(live.fidelity, ForkFidelity::MemoryAndDisk);
        let ids: BTreeSet<_> = checkpoint_children
            .iter()
            .chain(live.children.iter())
            .map(Machine::id)
            .collect();
        assert_eq!(ids.len(), 4);
        assert!(!ids.contains(&machine.id()));
        machine
            .set_suspension_policy(SuspensionPolicy::Manual, IdempotencyKey::new())
            .await?;
        assert_eq!(
            machine.inspect().await?.contract.suspension,
            SuspensionPolicy::Manual
        );
        machine.suspend(IdempotencyKey::new()).await?;
        assert_eq!(machine.inspect().await?.state, MachineState::Suspended);
        machine.wake(IdempotencyKey::new()).await?;
        assert_eq!(machine.inspect().await?.state, MachineState::Running);
        assert!(!machine.events(None, 100).await?.events.is_empty());
        // Process-local metering is projected truthfully, without inventing a hosted signature.
        let receipt = raw
            .usage(wire::UsageRequest {
                protocol: Some(protocol()),
                machine: Some(encode_machine(machine.id())),
                start_unix_ms: 0,
                end_unix_ms: 100,
            })
            .await?
            .into_inner();
        assert_eq!(receipt.machine, Some(encode_machine(machine.id())));
        assert!(receipt.receipt.is_empty());
        assert_eq!(receipt.lineage_receipt_sha256, vec![0; 32]);
        for child in checkpoint_children.iter().chain(live.children.iter()) {
            child.destroy(IdempotencyKey::new()).await?;
            assert_eq!(child.inspect().await?.state, MachineState::Destroyed);
        }
        checkpoint.destroy(IdempotencyKey::new()).await?;
        machine.destroy(IdempotencyKey::new()).await?;
        assert_eq!(machine.inspect().await?.state, MachineState::Destroyed);
        assert_eq!(
            raw.inspect_machine(wire::InspectMachineRequest::default())
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }
}
