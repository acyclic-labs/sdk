use super::*;
use tonic::transport::{
    Certificate, Channel, ClientTlsConfig, Endpoint as TonicEndpoint, Identity,
};

const CONNECT_TIMEOUT: Duration = Duration::from_secs(10);
const RPC_TIMEOUT: Duration = Duration::from_secs(30);
const WATCH_TIMEOUT: Duration = Duration::from_secs(60);
const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

/// Remote mutual-TLS identity. Key material is borrowed and never retained by the client.
pub struct Tls<'a> {
    pub ca: &'a [u8],
    pub certificate: &'a [u8],
    pub private_key: &'a [u8],
}

#[derive(Clone)]
struct GrpcProvider {
    client: wire::machines_service_client::MachinesServiceClient<Channel>,
}

#[cfg(target_os = "linux")]
fn owner_private_socket(
    path: &std::path::Path,
    expected_uid: u32,
) -> std::io::Result<std::fs::Metadata> {
    use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.file_type().is_socket()
        || metadata.uid() != expected_uid
        || metadata.mode() & 0o077 != 0
    {
        return Err(std::io::Error::new(
            std::io::ErrorKind::PermissionDenied,
            "Machines socket is not an owner-private socket",
        ));
    }
    Ok(metadata)
}

impl Machines {
    /// Connects to the endpoint and credentials named by `ACYCLIC_MACHINES_ENDPOINT`,
    /// `ACYCLIC_MACHINES_CA_FILE`, `ACYCLIC_MACHINES_CERT_FILE`, and
    /// `ACYCLIC_MACHINES_KEY_FILE`.
    pub async fn from_env() -> Result<Self, ProviderError> {
        let endpoint = std::env::var("ACYCLIC_MACHINES_ENDPOINT")
            .map_err(|_| ProviderError::Invalid("ACYCLIC_MACHINES_ENDPOINT is required".into()))?;
        if let Some(path) = endpoint.strip_prefix("unix:") {
            return Self::connect_local(std::path::Path::new(path)).await;
        }
        let read = |name: &'static str| async move {
            let path = std::env::var(name)
                .map_err(|_| ProviderError::Invalid(format!("{name} is required")))?;
            tokio::fs::read(path)
                .await
                .map_err(|_| ProviderError::Invalid(format!("{name} is unreadable")))
        };
        let (ca, certificate, private_key) = tokio::try_join!(
            read("ACYCLIC_MACHINES_CA_FILE"),
            read("ACYCLIC_MACHINES_CERT_FILE"),
            read("ACYCLIC_MACHINES_KEY_FILE")
        )?;
        Self::connect(
            &endpoint,
            Tls {
                ca: &ca,
                certificate: &certificate,
                private_key: &private_key,
            },
        )
        .await
    }

    /// Connects to an HTTPS Machines endpoint with mandatory mutual TLS.
    pub async fn connect(uri: &str, tls: Tls<'_>) -> Result<Self, ProviderError> {
        if !uri.starts_with("https://") {
            return Err(ProviderError::Invalid(
                "remote Machines endpoint must use https".into(),
            ));
        }
        let endpoint = TonicEndpoint::from_shared(uri.to_owned())
            .map_err(|_| ProviderError::Invalid("invalid Machines endpoint".into()))?
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .tls_config(
                ClientTlsConfig::new()
                    .ca_certificate(Certificate::from_pem(tls.ca))
                    .identity(Identity::from_pem(tls.certificate, tls.private_key)),
            )
            .map_err(|_| ProviderError::Invalid("invalid Machines TLS identity".into()))?;
        let channel = endpoint
            .connect()
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Self::grpc(channel))
    }

    /// Connects to an owner-confined local Unix socket.
    #[cfg(target_os = "linux")]
    pub async fn connect_local(path: &std::path::Path) -> Result<Self, ProviderError> {
        use std::os::unix::fs::MetadataExt as _;
        use tokio::net::UnixStream;
        use tower::service_fn;

        if !path.is_absolute() {
            return Err(ProviderError::Invalid(
                "local Machines socket must be absolute".into(),
            ));
        }
        let path = path.to_path_buf();
        let expected_uid = rustix::process::geteuid().as_raw();
        let channel = TonicEndpoint::from_static("http://[::]:50051")
            .connect_timeout(CONNECT_TIMEOUT)
            .timeout(RPC_TIMEOUT)
            .connect_with_connector(service_fn(move |_| {
                let path = path.clone();
                async move {
                    let before = owner_private_socket(&path, expected_uid)?;
                    let stream = UnixStream::connect(&path).await?;
                    let peer = stream.peer_cred()?;
                    let after = owner_private_socket(&path, expected_uid)?;
                    if peer.uid() != expected_uid
                        || after.dev() != before.dev()
                        || after.ino() != before.ino()
                    {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::PermissionDenied,
                            "Machines socket identity changed or peer is foreign",
                        ));
                    }
                    Ok(hyper_util::rt::TokioIo::new(stream))
                }
            }))
            .await
            .map_err(|_| ProviderError::Unavailable)?;
        Ok(Self::grpc(channel))
    }

    /// Reports that Unix sockets are unavailable on this platform.
    #[cfg(not(target_os = "linux"))]
    pub async fn connect_local(_path: &std::path::Path) -> Result<Self, ProviderError> {
        Err(ProviderError::Unsupported(
            "local Machines sockets require Linux".into(),
        ))
    }

    fn grpc(channel: Channel) -> Self {
        let client = wire::machines_service_client::MachinesServiceClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        Self::new(Arc::new(GrpcProvider { client }))
    }
}

#[async_trait]
impl MachinesProvider for GrpcProvider {
    fn assurance(&self) -> ProviderAssurance {
        ProviderAssurance::CustomerHosted
    }

    async fn qualify_image(&self, image: Image) -> Result<ImageQualification, ProviderError> {
        let response = self
            .client()
            .qualify_image(wire::QualifyImageRequest {
                protocol: Some(protocol()),
                image: Some(encode_image(&image)?),
            })
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        decode_qualification(response, &image)
    }

    async fn create(&self, request: CreateMachine) -> Result<MutationOutcome, ProviderError> {
        let key = request.idempotency_key;
        let response = self
            .client()
            .create(wire::CreateMachineRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                image: Some(encode_image(&request.image)?),
                compatibility: Some(encode_compatibility(&request.compatibility)?),
                performance: encode_performance(request.performance),
                suspension: Some(encode_suspension(request.suspension)?),
                expiration: Some(encode_expiration(request.expiration)?),
                network_policy_digest: request.network_policy_digest.to_vec(),
                budgets: Some(wire::Budgets {
                    spend_micros: request.budgets.spend_micros,
                    concurrency: request.budgets.concurrency,
                }),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        let operation = decode_operation(response.operation.as_ref())?;
        let machine = decode_machine(response.machine.as_ref())?;
        let _contract = decode_contract(response.contract.as_ref())?;
        self.wait(key, operation).await?;
        self.inspect_machine(machine)
            .await
            .map(MutationOutcome::Created)
    }

    async fn inspect_machine(
        &self,
        machine: MachineId,
    ) -> Result<MachineObservation, ProviderError> {
        self.client()
            .inspect_machine(wire::InspectMachineRequest {
                protocol: Some(protocol()),
                machine: Some(encode_machine(machine)),
            })
            .await
            .map_err(|error| read_error(&error))
            .and_then(|response| decode_machine_observation(response.into_inner(), machine))
    }

    async fn list_machines(
        &self,
        after: Option<MachineId>,
        limit: u32,
    ) -> Result<MachinePage, ProviderError> {
        if limit == 0 || limit > MAX_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "machine page limit must be 1..=256".into(),
            ));
        }
        let page = self
            .client()
            .list_machines(wire::ListMachinesRequest {
                protocol: Some(protocol()),
                after: after.map(encode_machine),
                limit,
            })
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        let mut previous = after;
        let mut machines = Vec::with_capacity(page.machines.len());
        for value in page.machines {
            let expected = decode_machine(value.machine.as_ref())?;
            if previous.is_some_and(|cursor| expected <= cursor) {
                return Err(ProviderError::Rejected(
                    "server returned a noncanonical machine page".into(),
                ));
            }
            machines.push(decode_machine_observation(value, expected)?);
            previous = Some(expected);
        }
        let next = page
            .next
            .as_ref()
            .map(|value| decode_machine(Some(value)))
            .transpose()?;
        if machines.len()
            > usize::try_from(limit)
                .map_err(|_| ProviderError::Invalid("invalid page limit".into()))?
            || next.is_some() && next != machines.last().map(|value| value.id)
        {
            return Err(ProviderError::Rejected(
                "server returned a malformed machine cursor".into(),
            ));
        }
        Ok(MachinePage { machines, next })
    }

    async fn checkpoint(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let value = self
            .client()
            .checkpoint(wire::CheckpointMachineRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                machine: Some(encode_machine(machine)),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        let operation = decode_operation(value.operation.as_ref())?;
        let checkpoint = decode_checkpoint(value.checkpoint.as_ref())?;
        if decode_machine(value.source.as_ref())? != machine {
            return Err(ProviderError::Rejected(
                "checkpoint source was substituted".into(),
            ));
        }
        self.wait(key, operation).await?;
        self.inspect_checkpoint(checkpoint)
            .await
            .map(MutationOutcome::Checkpointed)
    }

    async fn inspect_checkpoint(
        &self,
        checkpoint: CheckpointId,
    ) -> Result<CheckpointObservation, ProviderError> {
        self.client()
            .inspect_checkpoint(wire::InspectCheckpointRequest {
                protocol: Some(protocol()),
                checkpoint: Some(encode_checkpoint(checkpoint)),
            })
            .await
            .map_err(|error| read_error(&error))
            .and_then(|response| decode_checkpoint_observation(&response.into_inner(), checkpoint))
    }

    async fn fork(
        &self,
        checkpoint: CheckpointId,
        count: NonZeroU32,
        performance: Performance,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        if count.get() > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        let value = self
            .client()
            .fork(wire::ForkCheckpointRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                checkpoint: Some(encode_checkpoint(checkpoint)),
                count: count.get(),
                performance: encode_performance(performance),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        if decode_checkpoint(value.checkpoint.as_ref())? != checkpoint
            || value.children.len()
                != usize::try_from(count.get())
                    .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
        {
            return Err(ProviderError::Rejected(
                "fork result was substituted".into(),
            ));
        }
        let operation = decode_operation(value.operation.as_ref())?;
        self.wait(key, operation).await?;
        let mut seen = BTreeSet::new();
        let mut children = Vec::with_capacity(value.children.len());
        for child in value.children {
            let child = decode_machine(Some(&child))?;
            if !seen.insert(child) {
                return Err(ProviderError::Rejected(
                    "fork returned duplicate children".into(),
                ));
            }
            children.push(self.inspect_machine(child).await?);
        }
        Ok(MutationOutcome::Forked(children))
    }

    async fn fork_machine(
        &self,
        machine: MachineId,
        count: NonZeroU32,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        if count.get() > MAX_FORK_CHILDREN {
            return Err(ProviderError::Invalid("fork count exceeds 1024".into()));
        }
        let value = self
            .client()
            .fork_machine(wire::ForkMachineRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                machine: Some(encode_machine(machine)),
                count: count.get(),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        let admitted = decode_fork_machine_admission(&value)?;
        if admitted.source != machine
            || admitted.children.len()
                != usize::try_from(count.get())
                    .map_err(|_| ProviderError::Invalid("invalid fork count".into()))?
        {
            return Err(ProviderError::Rejected(
                "fork result was substituted".into(),
            ));
        }
        self.wait(key, admitted.operation).await?;
        admitted.observe(self).await
    }

    async fn suspend(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.machine_mutation(machine, key, MachineMutation::Suspend)
            .await
    }
    async fn wake(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.machine_mutation(machine, key, MachineMutation::Wake)
            .await
    }
    async fn set_suspension_policy(
        &self,
        machine: MachineId,
        policy: SuspensionPolicy,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let value = self
            .client()
            .set_suspension_policy(wire::SetSuspensionPolicyRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                machine: Some(encode_machine(machine)),
                policy: Some(encode_suspension(policy)?),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        if decode_machine(value.machine.as_ref())? != machine
            || decode_suspension(value.policy.as_ref())? != policy
        {
            return Err(ProviderError::Rejected(
                "policy result was substituted".into(),
            ));
        }
        self.wait(key, decode_operation(value.operation.as_ref())?)
            .await?;
        Ok(MutationOutcome::SuspensionPolicySet(machine, policy))
    }
    async fn destroy_machine(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        self.machine_mutation(machine, key, MachineMutation::Destroy)
            .await
    }
    async fn destroy_checkpoint(
        &self,
        checkpoint: CheckpointId,
        key: IdempotencyKey,
    ) -> Result<MutationOutcome, ProviderError> {
        let value = self
            .client()
            .destroy_checkpoint(wire::CheckpointMutationRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
                checkpoint: Some(encode_checkpoint(checkpoint)),
            })
            .await
            .map_err(|error| mutation_error(key, &error))?
            .into_inner();
        if decode_checkpoint(value.checkpoint.as_ref())? != checkpoint {
            return Err(ProviderError::Rejected(
                "checkpoint result was substituted".into(),
            ));
        }
        self.wait(key, decode_operation(value.operation.as_ref())?)
            .await?;
        Ok(MutationOutcome::CheckpointDestroyed(checkpoint))
    }
    async fn events(
        &self,
        machine: MachineId,
        after_sequence: Option<u64>,
        limit: u32,
    ) -> Result<EventPage, ProviderError> {
        if limit == 0 || limit > MAX_EVENT_PAGE_SIZE {
            return Err(ProviderError::Invalid(
                "event page limit must be 1..=1024".into(),
            ));
        }
        let value = self
            .client()
            .events(wire::EventsRequest {
                protocol: Some(protocol()),
                machine: Some(encode_machine(machine)),
                after_sequence: after_sequence.unwrap_or(0),
                limit,
            })
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        let mut previous = after_sequence.unwrap_or(0);
        let mut events = Vec::with_capacity(value.events.len());
        for item in value.events {
            let event = decode_event(&item, machine)?;
            if event.sequence <= previous {
                return Err(ProviderError::Rejected(
                    "event sequence is not increasing".into(),
                ));
            }
            previous = event.sequence;
            events.push(event);
        }
        let next_sequence = (value.next_sequence != 0).then_some(value.next_sequence);
        if events.len()
            > usize::try_from(limit)
                .map_err(|_| ProviderError::Invalid("invalid event limit".into()))?
            || next_sequence.is_some() && next_sequence != events.last().map(|event| event.sequence)
        {
            return Err(ProviderError::Rejected("event cursor is malformed".into()));
        }
        Ok(EventPage {
            events,
            next_sequence,
        })
    }
    async fn usage(
        &self,
        machine: MachineId,
        start_unix_ms: u64,
        end_unix_ms: u64,
    ) -> Result<UsageReceipt, ProviderError> {
        if start_unix_ms >= end_unix_ms {
            return Err(ProviderError::Invalid(
                "usage interval must be non-empty".into(),
            ));
        }
        let value = self
            .client()
            .usage(wire::UsageRequest {
                protocol: Some(protocol()),
                machine: Some(encode_machine(machine)),
                start_unix_ms,
                end_unix_ms,
            })
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        decode_usage_receipt(value, machine, start_unix_ms, end_unix_ms)
    }
    async fn recover(&self, key: IdempotencyKey) -> Result<MutationOutcome, ProviderError> {
        let (operation, value) = self.recovered_admission(key).await?;
        self.wait(key, operation).await?;
        decode_recovered(self, value).await
    }
    async fn recover_operation(&self, key: IdempotencyKey) -> Result<OperationId, ProviderError> {
        self.recovered_admission(key)
            .await
            .map(|(operation, _)| operation)
    }
    async fn inspect_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationObservation, ProviderError> {
        self.client()
            .inspect_operation(operation_request(operation))
            .await
            .map_err(|error| read_error(&error))
            .and_then(|response| decode_operation_observation(&response.into_inner(), operation))
    }
    async fn cancel(&self, operation: OperationId) -> Result<OperationObservation, ProviderError> {
        self.client()
            .cancel(operation_request(operation))
            .await
            .map_err(|error| read_error(&error))
            .and_then(|response| decode_operation_observation(&response.into_inner(), operation))
    }
    async fn watch_operation(
        &self,
        operation: OperationId,
    ) -> Result<OperationStream, ProviderError> {
        let stream = self
            .client()
            .watch_operation(operation_request(operation))
            .await
            .map_err(|error| read_error(&error))?
            .into_inner();
        let stream = stream::unfold((stream, false), move |(mut stream, done)| async move {
            if done {
                return None;
            }
            match stream.message().await {
                Ok(Some(value)) => {
                    let value = decode_operation_observation(&value, operation);
                    let decode_failed = value.is_err();
                    let terminal = value
                        .as_ref()
                        .is_ok_and(|value| !matches!(value.phase, OperationPhase::Pending));
                    Some((value, (stream, terminal || decode_failed)))
                }
                Ok(None) | Err(_) => Some((Err(operation_watch_error(operation)), (stream, true))),
            }
        });
        Ok(Box::pin(stream))
    }
}

enum MachineMutation {
    Suspend,
    Wake,
    Destroy,
}

impl GrpcProvider {
    fn client(&self) -> wire::machines_service_client::MachinesServiceClient<Channel> {
        self.client.clone()
    }
    async fn recovered_admission(
        &self,
        key: IdempotencyKey,
    ) -> Result<(OperationId, wire::RecoveredAdmission), ProviderError> {
        let value = self
            .client()
            .recover(wire::RecoverRequest {
                protocol: Some(protocol()),
                idempotency_key: Some(encode_key(key)),
            })
            .await
            .map_err(|error| recovery_error(key, &error))?
            .into_inner();
        let operation = validate_recovered_admission(&value)?;
        Ok((operation, value))
    }
    async fn wait(&self, key: IdempotencyKey, operation: OperationId) -> Result<(), ProviderError> {
        let mut stream = self
            .client()
            .watch_operation(operation_request(operation))
            .await
            .map_err(|error| watch_error(key, error))?
            .into_inner();
        loop {
            let next = tokio::time::timeout(WATCH_TIMEOUT, stream.message())
                .await
                .map_err(|_| ProviderError::Indeterminate(key))?
                .map_err(|_| ProviderError::Indeterminate(key))?;
            let Some(value) = next else {
                return Err(ProviderError::Indeterminate(key));
            };
            match decode_operation_observation(&value, operation)?.phase {
                OperationPhase::Pending => {}
                OperationPhase::Succeeded => return Ok(()),
                OperationPhase::Cancelled => return Err(ProviderError::Cancelled),
                OperationPhase::Failed => return Err(ProviderError::Failed),
                OperationPhase::Indeterminate => return Err(ProviderError::Indeterminate(key)),
            }
        }
    }
    async fn machine_mutation(
        &self,
        machine: MachineId,
        key: IdempotencyKey,
        mutation: MachineMutation,
    ) -> Result<MutationOutcome, ProviderError> {
        let request = wire::MachineMutationRequest {
            protocol: Some(protocol()),
            idempotency_key: Some(encode_key(key)),
            machine: Some(encode_machine(machine)),
        };
        let value = match mutation {
            MachineMutation::Suspend => self.client().suspend(request).await,
            MachineMutation::Wake => self.client().wake(request).await,
            MachineMutation::Destroy => self.client().destroy_machine(request).await,
        }
        .map_err(|error| mutation_error(key, &error))?
        .into_inner();
        if decode_machine(value.machine.as_ref())? != machine {
            return Err(ProviderError::Rejected(
                "machine result was substituted".into(),
            ));
        }
        self.wait(key, decode_operation(value.operation.as_ref())?)
            .await?;
        Ok(match mutation {
            MachineMutation::Suspend => MutationOutcome::Suspended(machine),
            MachineMutation::Wake => MutationOutcome::Woken(machine),
            MachineMutation::Destroy => MutationOutcome::MachineDestroyed(machine),
        })
    }
}

async fn decode_recovered(
    provider: &GrpcProvider,
    value: wire::RecoveredAdmission,
) -> Result<MutationOutcome, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    validate_recovered_admission(&value)?;
    match value
        .result
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) => provider
            .inspect_machine(decode_machine(value.machine.as_ref())?)
            .await
            .map(MutationOutcome::Created),
        ResultKind::Checkpoint(value) => provider
            .inspect_checkpoint(decode_checkpoint(value.checkpoint.as_ref())?)
            .await
            .map(MutationOutcome::Checkpointed),
        ResultKind::Fork(value) => {
            if value.children.is_empty()
                || value.children.len()
                    > usize::try_from(MAX_FORK_CHILDREN)
                        .map_err(|_| ProviderError::Invalid("invalid fork bound".into()))?
            {
                return Err(ProviderError::Rejected(
                    "recovered fork child set is outside the public bound".into(),
                ));
            }
            let mut seen = BTreeSet::new();
            let mut children = Vec::with_capacity(value.children.len());
            for item in value.children {
                let child = decode_machine(Some(&item))?;
                if !seen.insert(child) {
                    return Err(ProviderError::Rejected(
                        "recovered fork returned duplicate children".into(),
                    ));
                }
                children.push(child);
            }
            let mut result = Vec::with_capacity(children.len());
            for child in children {
                result.push(provider.inspect_machine(child).await?);
            }
            Ok(MutationOutcome::Forked(result))
        }
        ResultKind::ForkMachine(value) => {
            decode_fork_machine_admission(&value)?
                .observe(provider)
                .await
        }
        ResultKind::Suspend(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::Suspended)
        }
        ResultKind::Wake(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::Woken)
        }
        ResultKind::DestroyMachine(value) => {
            decode_machine(value.machine.as_ref()).map(MutationOutcome::MachineDestroyed)
        }
        ResultKind::SetSuspensionPolicy(value) => Ok(MutationOutcome::SuspensionPolicySet(
            decode_machine(value.machine.as_ref())?,
            decode_suspension(value.policy.as_ref())?,
        )),
        ResultKind::DestroyCheckpoint(value) => {
            decode_checkpoint(value.checkpoint.as_ref()).map(MutationOutcome::CheckpointDestroyed)
        }
    }
}

/// Checked [`wire::ForkMachineAdmission`] before its operation is awaited.
struct ForkMachineAdmitted {
    source: MachineId,
    fidelity: ForkFidelity,
    children: Vec<MachineId>,
    operation: OperationId,
}

impl ForkMachineAdmitted {
    async fn observe(self, provider: &GrpcProvider) -> Result<MutationOutcome, ProviderError> {
        let mut children = Vec::with_capacity(self.children.len());
        for child in self.children {
            children.push(provider.inspect_machine(child).await?);
        }
        Ok(MutationOutcome::MachineForked {
            source: self.source,
            fidelity: self.fidelity,
            children,
        })
    }
}

fn decode_fork_machine_admission(
    value: &wire::ForkMachineAdmission,
) -> Result<ForkMachineAdmitted, ProviderError> {
    let source = decode_machine(value.source.as_ref())?;
    let operation = decode_operation(value.operation.as_ref())?;
    let contract = decode_contract(value.contract.as_ref())?;
    let fidelity = match wire::ForkFidelity::try_from(value.fidelity)
        .map_err(|_| ProviderError::Rejected("fork fidelity is invalid".into()))?
    {
        wire::ForkFidelity::MemoryAndDisk => ForkFidelity::MemoryAndDisk,
        wire::ForkFidelity::DiskOnly => ForkFidelity::DiskOnly,
        wire::ForkFidelity::Unspecified => {
            return Err(ProviderError::Rejected(
                "fork fidelity is unspecified".into(),
            ));
        }
    };
    if contract.fork_fidelity() != Some(fidelity) {
        return Err(ProviderError::Rejected(
            "fork fidelity contradicts the source contract".into(),
        ));
    }
    if value.children.is_empty()
        || value.children.len()
            > usize::try_from(MAX_FORK_CHILDREN)
                .map_err(|_| ProviderError::Invalid("invalid fork bound".into()))?
    {
        return Err(ProviderError::Rejected(
            "fork child set is outside the public bound".into(),
        ));
    }
    let mut seen = BTreeSet::new();
    let mut children = Vec::with_capacity(value.children.len());
    for item in &value.children {
        let child = decode_machine(Some(item))?;
        if child == source || !seen.insert(child) {
            return Err(ProviderError::Rejected(
                "fork returned duplicate children".into(),
            ));
        }
        children.push(child);
    }
    Ok(ForkMachineAdmitted {
        source,
        fidelity,
        children,
        operation,
    })
}

fn validate_recovered_admission(
    value: &wire::RecoveredAdmission,
) -> Result<OperationId, ProviderError> {
    use wire::recovered_admission::Result as ResultKind;
    let outer = decode_operation(value.operation.as_ref())?;
    let nested = match value
        .result
        .as_ref()
        .ok_or_else(|| ProviderError::Rejected("recovery result is missing".into()))?
    {
        ResultKind::Create(value) => value.operation.as_ref(),
        ResultKind::Checkpoint(value) => value.operation.as_ref(),
        ResultKind::Fork(value) => value.operation.as_ref(),
        ResultKind::ForkMachine(value) => value.operation.as_ref(),
        ResultKind::Suspend(value)
        | ResultKind::Wake(value)
        | ResultKind::DestroyMachine(value)
        | ResultKind::DestroyCheckpoint(value) => value.operation.as_ref(),
        ResultKind::SetSuspensionPolicy(value) => value.operation.as_ref(),
    };
    if decode_operation(nested)? == outer {
        Ok(outer)
    } else {
        Err(ProviderError::Rejected(
            "recovery operation identity was substituted".into(),
        ))
    }
}

fn protocol() -> wire::ProtocolVersion {
    wire::ProtocolVersion {
        major: PROTOCOL_MAJOR,
        minor: PROTOCOL_MINOR,
    }
}
fn encode_key(value: IdempotencyKey) -> wire::IdempotencyKey {
    wire::IdempotencyKey {
        value: value.as_bytes().to_vec(),
    }
}
fn encode_machine(value: MachineId) -> wire::MachineId {
    wire::MachineId {
        value: value.as_bytes().to_vec(),
    }
}
fn encode_checkpoint(value: CheckpointId) -> wire::CheckpointId {
    wire::CheckpointId {
        value: value.as_bytes().to_vec(),
    }
}
fn operation_request(value: OperationId) -> wire::OperationRequest {
    wire::OperationRequest {
        protocol: Some(protocol()),
        operation: Some(wire::OperationId {
            value: value.as_bytes().to_vec(),
        }),
    }
}
fn decode_uuid(value: &[u8]) -> Result<Uuid, ProviderError> {
    let bytes: [u8; 16] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected("identity width is invalid".into()))?;
    let value = Uuid::from_bytes(bytes);
    if value.is_nil() {
        Err(ProviderError::Rejected("nil identity is invalid".into()))
    } else {
        Ok(value)
    }
}
fn decode_operation(value: Option<&wire::OperationId>) -> Result<OperationId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("operation identity is missing".into()))?
            .value,
    )
    .map(OperationId)
}
fn decode_machine(value: Option<&wire::MachineId>) -> Result<MachineId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("machine identity is missing".into()))?
            .value,
    )
    .map(MachineId)
}
fn decode_checkpoint(value: Option<&wire::CheckpointId>) -> Result<CheckpointId, ProviderError> {
    decode_uuid(
        &value
            .ok_or_else(|| ProviderError::Rejected("checkpoint identity is missing".into()))?
            .value,
    )
    .map(CheckpointId)
}

fn encode_image(value: &Image) -> Result<wire::Image, ProviderError> {
    use wire::image::ImmutableReference;
    let (kind, immutable_reference) = match value {
        Image::ManagedOci(digest) => (
            wire::ImageKind::ManagedOci,
            ImmutableReference::ManagedDigest(digest.as_bytes().to_vec()),
        ),
        Image::Custom(digest) => (
            wire::ImageKind::Custom,
            ImmutableReference::CustomDigest(digest.as_bytes().to_vec()),
        ),
        Image::Checkpoint(id) => (
            wire::ImageKind::Checkpoint,
            ImmutableReference::Checkpoint(encode_checkpoint(*id)),
        ),
    };
    Ok(wire::Image {
        kind: kind as i32,
        immutable_reference: Some(immutable_reference),
    })
}
fn decode_image(value: Option<&wire::Image>) -> Result<Image, ProviderError> {
    use wire::image::ImmutableReference;
    let value = value.ok_or_else(|| ProviderError::Rejected("image is missing".into()))?;
    match (
        wire::ImageKind::try_from(value.kind)
            .map_err(|_| ProviderError::Rejected("image kind is invalid".into()))?,
        value.immutable_reference.as_ref(),
    ) {
        (wire::ImageKind::ManagedOci, Some(ImmutableReference::ManagedDigest(digest))) => {
            let digest: [u8; 32] = digest.as_slice().try_into().map_err(|_| {
                ProviderError::Rejected("managed image digest width is invalid".into())
            })?;
            Image::managed(digest)
                .map_err(|_| ProviderError::Rejected("managed image digest is invalid".into()))
        }
        (wire::ImageKind::Custom, Some(ImmutableReference::CustomDigest(digest))) => {
            let digest: [u8; 32] = digest.as_slice().try_into().map_err(|_| {
                ProviderError::Rejected("custom image digest width is invalid".into())
            })?;
            if digest == [0; 32] {
                return Err(ProviderError::Rejected(
                    "custom image digest is zero".into(),
                ));
            }
            Image::custom(digest)
                .map_err(|_| ProviderError::Rejected("custom image digest is invalid".into()))
        }
        (wire::ImageKind::Checkpoint, Some(ImmutableReference::Checkpoint(id))) => {
            decode_checkpoint(Some(id)).map(Image::Checkpoint)
        }
        _ => Err(ProviderError::Rejected("image shape is invalid".into())),
    }
}
fn encode_capability(value: Capability) -> i32 {
    (match value {
        Capability::ElasticCpu => wire::Capability::ElasticCpu,
        Capability::ElasticMemory => wire::Capability::ElasticMemory,
        Capability::LiveCheckpoint => wire::Capability::LiveCheckpoint,
        Capability::LiveFork => wire::Capability::LiveFork,
        Capability::SuspendResume => wire::Capability::SuspendResume,
        Capability::LiveMovement => wire::Capability::LiveMovement,
        Capability::DiskFork => wire::Capability::DiskFork,
    }) as i32
}
fn decode_capability(value: i32) -> Result<Capability, ProviderError> {
    match wire::Capability::try_from(value)
        .map_err(|_| ProviderError::Rejected("capability is invalid".into()))?
    {
        wire::Capability::ElasticCpu => Ok(Capability::ElasticCpu),
        wire::Capability::ElasticMemory => Ok(Capability::ElasticMemory),
        wire::Capability::LiveCheckpoint => Ok(Capability::LiveCheckpoint),
        wire::Capability::LiveFork => Ok(Capability::LiveFork),
        wire::Capability::SuspendResume => Ok(Capability::SuspendResume),
        wire::Capability::LiveMovement => Ok(Capability::LiveMovement),
        wire::Capability::DiskFork => Ok(Capability::DiskFork),
        wire::Capability::Unspecified => {
            Err(ProviderError::Rejected("capability is unspecified".into()))
        }
    }
}
fn encode_compatibility(
    value: &CompatibilityPolicy,
) -> Result<wire::CompatibilityPolicy, ProviderError> {
    let (mode, required) = match value {
        CompatibilityPolicy::BestEffort => (wire::CompatibilityMode::BestEffort, Vec::new()),
        CompatibilityPolicy::Require(required) if !required.is_empty() => (
            wire::CompatibilityMode::Require,
            required.iter().copied().map(encode_capability).collect(),
        ),
        CompatibilityPolicy::Require(_) => {
            return Err(ProviderError::Invalid(
                "required capability set cannot be empty".into(),
            ));
        }
    };
    Ok(wire::CompatibilityPolicy {
        mode: mode as i32,
        required,
    })
}
fn decode_compatibility(
    value: Option<&wire::CompatibilityPolicy>,
    capabilities: &BTreeSet<Capability>,
) -> Result<CompatibilityPolicy, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("compatibility policy is missing".into()))?;
    let required = value
        .required
        .iter()
        .map(|item| decode_capability(*item))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if required.len() != value.required.len() {
        return Err(ProviderError::Rejected(
            "capability set contains duplicates".into(),
        ));
    }
    match wire::CompatibilityMode::try_from(value.mode)
        .map_err(|_| ProviderError::Rejected("compatibility mode is invalid".into()))?
    {
        wire::CompatibilityMode::BestEffort if required.is_empty() => {
            Ok(CompatibilityPolicy::BestEffort)
        }
        wire::CompatibilityMode::Require
            if !required.is_empty() && required.is_subset(capabilities) =>
        {
            Ok(CompatibilityPolicy::Require(required))
        }
        _ => Err(ProviderError::Rejected(
            "compatibility policy is contradictory".into(),
        )),
    }
}
fn encode_performance(value: Performance) -> i32 {
    (match value {
        Performance::Elastic => wire::Performance::Elastic,
        Performance::Dedicated => wire::Performance::Dedicated,
    }) as i32
}
fn decode_performance(value: i32) -> Result<Performance, ProviderError> {
    match wire::Performance::try_from(value)
        .map_err(|_| ProviderError::Rejected("performance is invalid".into()))?
    {
        wire::Performance::Elastic => Ok(Performance::Elastic),
        wire::Performance::Dedicated => Ok(Performance::Dedicated),
        wire::Performance::Unspecified => {
            Err(ProviderError::Rejected("performance is unspecified".into()))
        }
    }
}
fn millis(value: Duration) -> Result<u64, ProviderError> {
    u64::try_from(value.as_millis())
        .map_err(|_| ProviderError::Invalid("duration exceeds protocol range".into()))
}
fn encode_suspension(value: SuspensionPolicy) -> Result<wire::SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    let policy = match value {
        SuspensionPolicy::Manual => Policy::Manual(true),
        SuspensionPolicy::AfterIdle(value) if !value.is_zero() => {
            Policy::AfterIdleMs(millis(value)?)
        }
        SuspensionPolicy::AfterIdle(_) => {
            return Err(ProviderError::Invalid(
                "idle duration must be nonzero".into(),
            ));
        }
    };
    Ok(wire::SuspensionPolicy {
        policy: Some(policy),
    })
}
fn decode_suspension(
    value: Option<&wire::SuspensionPolicy>,
) -> Result<SuspensionPolicy, ProviderError> {
    use wire::suspension_policy::Policy;
    match value.and_then(|item| item.policy.as_ref()) {
        Some(Policy::Manual(true)) => Ok(SuspensionPolicy::Manual),
        Some(Policy::AfterIdleMs(value)) if *value != 0 => {
            Ok(SuspensionPolicy::AfterIdle(Duration::from_millis(*value)))
        }
        _ => Err(ProviderError::Rejected(
            "suspension policy is invalid".into(),
        )),
    }
}
fn encode_expiration(value: ExpirationPolicy) -> Result<wire::ExpirationPolicy, ProviderError> {
    let (kind, value_ms) = match value {
        ExpirationPolicy::Never => (wire::ExpirationKind::Never, 0),
        ExpirationPolicy::MaxAge(value) if !value.is_zero() => {
            (wire::ExpirationKind::MaxAge, millis(value)?)
        }
        ExpirationPolicy::AtUnixMs(value) if value != 0 => (wire::ExpirationKind::At, value),
        ExpirationPolicy::Idle(value) if !value.is_zero() => {
            (wire::ExpirationKind::Idle, millis(value)?)
        }
        _ => {
            return Err(ProviderError::Invalid(
                "expiration policy is invalid".into(),
            ));
        }
    };
    Ok(wire::ExpirationPolicy {
        kind: kind as i32,
        value_ms,
    })
}
fn decode_expiration(
    value: Option<&wire::ExpirationPolicy>,
) -> Result<ExpirationPolicy, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("expiration policy is missing".into()))?;
    match wire::ExpirationKind::try_from(value.kind)
        .map_err(|_| ProviderError::Rejected("expiration kind is invalid".into()))?
    {
        wire::ExpirationKind::Never if value.value_ms == 0 => Ok(ExpirationPolicy::Never),
        wire::ExpirationKind::MaxAge if value.value_ms != 0 => Ok(ExpirationPolicy::MaxAge(
            Duration::from_millis(value.value_ms),
        )),
        wire::ExpirationKind::At if value.value_ms != 0 => {
            Ok(ExpirationPolicy::AtUnixMs(value.value_ms))
        }
        wire::ExpirationKind::Idle if value.value_ms != 0 => Ok(ExpirationPolicy::Idle(
            Duration::from_millis(value.value_ms),
        )),
        _ => Err(ProviderError::Rejected(
            "expiration policy is contradictory".into(),
        )),
    }
}

fn decode_qualification(
    value: wire::ImageQualification,
    expected: &Image,
) -> Result<ImageQualification, ProviderError> {
    let image = decode_image(value.image.as_ref())?;
    if &image != expected {
        return Err(ProviderError::Rejected(
            "qualified image was substituted".into(),
        ));
    }
    let capabilities = value
        .capabilities
        .into_iter()
        .map(decode_capability)
        .collect::<Result<BTreeSet<_>, _>>()?;
    let compatibility_revision = digest(&value.compatibility_revision, "compatibility revision")?;
    Ok(ImageQualification {
        image,
        capabilities,
        compatibility_revision,
    })
}
fn decode_contract(
    value: Option<&wire::MachineContract>,
) -> Result<MachineContract, ProviderError> {
    let value =
        value.ok_or_else(|| ProviderError::Rejected("machine contract is missing".into()))?;
    let capabilities = value
        .capabilities
        .iter()
        .map(|item| decode_capability(*item))
        .collect::<Result<BTreeSet<_>, _>>()?;
    if capabilities.len() != value.capabilities.len() {
        return Err(ProviderError::Rejected(
            "capabilities contain duplicates".into(),
        ));
    }
    let budgets = value
        .budgets
        .as_ref()
        .ok_or_else(|| ProviderError::Rejected("budgets are missing".into()))?;
    Ok(MachineContract {
        image: decode_image(value.image.as_ref())?,
        compatibility: decode_compatibility(value.compatibility.as_ref(), &capabilities)?,
        capabilities,
        compatibility_revision: digest(&value.compatibility_revision, "compatibility revision")?,
        performance: decode_performance(value.performance)?,
        suspension: decode_suspension(value.suspension.as_ref())?,
        expiration: decode_expiration(value.expiration.as_ref())?,
        network_policy_digest: digest(&value.network_policy_digest, "network policy")?,
        budgets: Budgets {
            spend_micros: budgets.spend_micros,
            concurrency: budgets.concurrency,
        },
    })
}
fn digest(value: &[u8], name: &str) -> Result<[u8; 32], ProviderError> {
    let value: [u8; 32] = value
        .try_into()
        .map_err(|_| ProviderError::Rejected(format!("{name} digest width is invalid")))?;
    if value == [0; 32] {
        Err(ProviderError::Rejected(format!("{name} digest is zero")))
    } else {
        Ok(value)
    }
}
fn decode_machine_observation(
    value: wire::MachineState,
    expected: MachineId,
) -> Result<MachineObservation, ProviderError> {
    let id = decode_machine(value.machine.as_ref())?;
    if id != expected {
        return Err(ProviderError::Rejected(
            "machine identity was substituted".into(),
        ));
    }
    let state = match wire::MachineStatus::try_from(value.status)
        .map_err(|_| ProviderError::Rejected("machine state is invalid".into()))?
    {
        wire::MachineStatus::Starting => MachineState::Starting,
        wire::MachineStatus::Running => MachineState::Running,
        wire::MachineStatus::Suspending => MachineState::Suspending,
        wire::MachineStatus::Suspended => MachineState::Suspended,
        wire::MachineStatus::Waking => MachineState::Waking,
        wire::MachineStatus::Destroying => MachineState::Destroying,
        wire::MachineStatus::Destroyed => MachineState::Destroyed,
        wire::MachineStatus::Failed => MachineState::Failed,
        wire::MachineStatus::Indeterminate => MachineState::Indeterminate,
        wire::MachineStatus::Unspecified => {
            return Err(ProviderError::Rejected(
                "machine state is unspecified".into(),
            ));
        }
    };
    let mut names = BTreeSet::new();
    let mut endpoints = Vec::with_capacity(value.endpoints.len());
    for endpoint in value.endpoints {
        if endpoint.name.is_empty()
            || endpoint.uri.is_empty()
            || !names.insert(endpoint.name.clone())
        {
            return Err(ProviderError::Rejected(
                "machine endpoints are malformed".into(),
            ));
        }
        endpoints.push(Endpoint {
            name: endpoint.name,
            uri: endpoint.uri,
        });
    }
    if value.created_at_unix_ms == 0 || value.changed_at_unix_ms < value.created_at_unix_ms {
        return Err(ProviderError::Rejected(
            "machine timestamps are malformed".into(),
        ));
    }
    Ok(MachineObservation {
        id,
        state,
        contract: decode_contract(value.contract.as_ref())?,
        endpoints,
        last_checkpoint: value
            .last_checkpoint
            .as_ref()
            .map(|item| decode_checkpoint(Some(item)))
            .transpose()?,
        created_at_unix_ms: value.created_at_unix_ms,
        changed_at_unix_ms: value.changed_at_unix_ms,
    })
}
fn decode_checkpoint_observation(
    value: &wire::CheckpointState,
    expected: CheckpointId,
) -> Result<CheckpointObservation, ProviderError> {
    let id = decode_checkpoint(value.checkpoint.as_ref())?;
    if id != expected || value.created_at_unix_ms == 0 {
        return Err(ProviderError::Rejected(
            "checkpoint observation is malformed".into(),
        ));
    }
    Ok(CheckpointObservation {
        id,
        source: decode_machine(value.source.as_ref())?,
        contract: decode_contract(value.contract.as_ref())?,
        forkable: value.forkable,
        created_at_unix_ms: value.created_at_unix_ms,
    })
}
fn decode_event(
    value: &wire::MachineEvent,
    expected: MachineId,
) -> Result<MachineEvent, ProviderError> {
    let machine = decode_machine(value.machine.as_ref())?;
    if machine != expected || value.sequence == 0 || value.observed_at_unix_ms == 0 {
        return Err(ProviderError::Rejected(
            "event identity is malformed".into(),
        ));
    }
    let fact = match wire::EventKind::try_from(value.kind)
        .map_err(|_| ProviderError::Rejected("event kind is invalid".into()))?
    {
        wire::EventKind::State => EventFact::State(decode_machine_state(value.state)?),
        wire::EventKind::Pressure => EventFact::Pressure(
            match wire::PressureKind::try_from(value.pressure)
                .map_err(|_| ProviderError::Rejected("pressure kind is invalid".into()))?
            {
                wire::PressureKind::CustomerBudget => Pressure::CustomerBudget,
                wire::PressureKind::MachineLimit => Pressure::MachineLimit,
                wire::PressureKind::ServiceSaturation => Pressure::ServiceSaturation,
                wire::PressureKind::Unspecified => {
                    return Err(ProviderError::Rejected("pressure is unspecified".into()));
                }
            },
        ),
        wire::EventKind::Capacity => EventFact::CapacityChanged,
        wire::EventKind::Unspecified => {
            return Err(ProviderError::Rejected("event kind is unspecified".into()));
        }
    };
    Ok(MachineEvent {
        machine,
        sequence: value.sequence,
        observed_at_unix_ms: value.observed_at_unix_ms,
        fact,
    })
}
fn decode_machine_state(value: i32) -> Result<MachineState, ProviderError> {
    match wire::MachineStatus::try_from(value)
        .map_err(|_| ProviderError::Rejected("machine state is invalid".into()))?
    {
        wire::MachineStatus::Starting => Ok(MachineState::Starting),
        wire::MachineStatus::Running => Ok(MachineState::Running),
        wire::MachineStatus::Suspending => Ok(MachineState::Suspending),
        wire::MachineStatus::Suspended => Ok(MachineState::Suspended),
        wire::MachineStatus::Waking => Ok(MachineState::Waking),
        wire::MachineStatus::Destroying => Ok(MachineState::Destroying),
        wire::MachineStatus::Destroyed => Ok(MachineState::Destroyed),
        wire::MachineStatus::Failed => Ok(MachineState::Failed),
        wire::MachineStatus::Indeterminate => Ok(MachineState::Indeterminate),
        wire::MachineStatus::Unspecified => Err(ProviderError::Rejected(
            "machine state is unspecified".into(),
        )),
    }
}
fn decode_operation_observation(
    value: &wire::OperationState,
    expected: OperationId,
) -> Result<OperationObservation, ProviderError> {
    let id = decode_operation(value.operation.as_ref())?;
    if id != expected {
        return Err(ProviderError::Rejected(
            "operation identity was substituted".into(),
        ));
    }
    let phase = match wire::OperationStatus::try_from(value.status)
        .map_err(|_| ProviderError::Rejected("operation status is invalid".into()))?
    {
        wire::OperationStatus::Pending => OperationPhase::Pending,
        wire::OperationStatus::Succeeded => OperationPhase::Succeeded,
        wire::OperationStatus::Cancelled => OperationPhase::Cancelled,
        wire::OperationStatus::Indeterminate => OperationPhase::Indeterminate,
        wire::OperationStatus::Failed => OperationPhase::Failed,
        wire::OperationStatus::Unspecified => {
            return Err(ProviderError::Rejected(
                "operation status is unspecified".into(),
            ));
        }
    };
    Ok(OperationObservation { id, phase })
}
fn mutation_error(key: IdempotencyKey, value: &tonic::Status) -> ProviderError {
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        tonic::Code::Unimplemented => ProviderError::Unsupported(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    }
}
fn watch_error(key: IdempotencyKey, _value: tonic::Status) -> ProviderError {
    ProviderError::Indeterminate(key)
}
fn operation_watch_error(operation: OperationId) -> ProviderError {
    ProviderError::OperationIndeterminate(operation)
}
fn recovery_error(key: IdempotencyKey, value: &tonic::Status) -> ProviderError {
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown
        | tonic::Code::Internal => ProviderError::Indeterminate(key),
        _ => ProviderError::Rejected(value.message().into()),
    }
}
fn read_error(value: &tonic::Status) -> ProviderError {
    match value.code() {
        tonic::Code::Unavailable
        | tonic::Code::DeadlineExceeded
        | tonic::Code::Cancelled
        | tonic::Code::Unknown => ProviderError::Unavailable,
        tonic::Code::NotFound => ProviderError::NotFound(value.message().into()),
        _ => ProviderError::Rejected(value.message().into()),
    }
}

fn decode_usage_receipt(
    value: wire::UsageReceipt,
    machine: MachineId,
    start_unix_ms: u64,
    end_unix_ms: u64,
) -> Result<UsageReceipt, ProviderError> {
    let lineage_receipt_sha256: [u8; 32] = value
        .lineage_receipt_sha256
        .as_slice()
        .try_into()
        .map_err(|_| ProviderError::Rejected("lineage receipt identity is malformed".into()))?;
    if decode_machine(value.machine.as_ref())? != machine
        || value.start_unix_ms != start_unix_ms
        || value.end_unix_ms != end_unix_ms
        || value.receipt.is_empty()
        || lineage_receipt_sha256 == [0; 32]
    {
        return Err(ProviderError::Rejected("usage receipt is malformed".into()));
    }
    Ok(UsageReceipt {
        machine,
        start_unix_ms,
        end_unix_ms,
        elastic_cpu_ns: value.elastic_cpu_ns,
        dedicated_cpu_ns: value.dedicated_cpu_ns,
        private_resident_byte_seconds: value.private_resident_byte_seconds,
        durable_private_bytes: value.durable_private_bytes,
        lineage_receipt_sha256,
        egress_bytes: value.egress_bytes,
        receipt: value.receipt,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::stream;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::TcpListenerStream;
    use tonic::transport::Server;
    use tonic::{Code, Request, Response, Status};
    use wire::machines_service_server::{MachinesService, MachinesServiceServer};

    #[derive(Clone)]
    enum WatchReply {
        Reject(Code),
        Items(Vec<WatchItem>),
    }

    #[derive(Clone)]
    enum WatchItem {
        State(wire::OperationState),
        Error(Code),
    }

    #[derive(Clone)]
    struct OperationService {
        expected_key: IdempotencyKey,
        expected_operation: OperationId,
        recovered: wire::RecoveredAdmission,
        inspected: wire::OperationState,
        cancelled: wire::OperationState,
        watch: WatchReply,
    }

    #[tonic::async_trait]
    impl MachinesService for OperationService {
        async fn qualify_image(
            &self,
            _request: Request<wire::QualifyImageRequest>,
        ) -> Result<Response<wire::ImageQualification>, Status> {
            Err(Status::unimplemented("qualify_image"))
        }
        async fn create(
            &self,
            _request: Request<wire::CreateMachineRequest>,
        ) -> Result<Response<wire::MachineAdmission>, Status> {
            Err(Status::unimplemented("create"))
        }
        async fn checkpoint(
            &self,
            _request: Request<wire::CheckpointMachineRequest>,
        ) -> Result<Response<wire::CheckpointAdmission>, Status> {
            Err(Status::unimplemented("checkpoint"))
        }
        async fn fork(
            &self,
            _request: Request<wire::ForkCheckpointRequest>,
        ) -> Result<Response<wire::ForkAdmission>, Status> {
            Err(Status::unimplemented("fork"))
        }
        async fn fork_machine(
            &self,
            _request: Request<wire::ForkMachineRequest>,
        ) -> Result<Response<wire::ForkMachineAdmission>, Status> {
            Err(Status::unimplemented("fork_machine"))
        }
        async fn suspend(
            &self,
            _request: Request<wire::MachineMutationRequest>,
        ) -> Result<Response<wire::MutationAdmission>, Status> {
            Err(Status::unimplemented("suspend"))
        }
        async fn wake(
            &self,
            _request: Request<wire::MachineMutationRequest>,
        ) -> Result<Response<wire::MutationAdmission>, Status> {
            Err(Status::unimplemented("wake"))
        }
        async fn set_suspension_policy(
            &self,
            _request: Request<wire::SetSuspensionPolicyRequest>,
        ) -> Result<Response<wire::PolicyAdmission>, Status> {
            Err(Status::unimplemented("set_suspension_policy"))
        }
        async fn destroy_machine(
            &self,
            _request: Request<wire::MachineMutationRequest>,
        ) -> Result<Response<wire::MutationAdmission>, Status> {
            Err(Status::unimplemented("destroy_machine"))
        }
        async fn destroy_checkpoint(
            &self,
            _request: Request<wire::CheckpointMutationRequest>,
        ) -> Result<Response<wire::MutationAdmission>, Status> {
            Err(Status::unimplemented("destroy_checkpoint"))
        }

        async fn recover(
            &self,
            request: Request<wire::RecoverRequest>,
        ) -> Result<Response<wire::RecoveredAdmission>, Status> {
            if request
                .into_inner()
                .idempotency_key
                .as_ref()
                .map(|value| value.value.as_slice())
                != Some(self.expected_key.as_bytes().as_slice())
            {
                return Err(Status::invalid_argument("idempotency key was substituted"));
            }
            Ok(Response::new(self.recovered.clone()))
        }

        async fn inspect_machine(
            &self,
            _request: Request<wire::InspectMachineRequest>,
        ) -> Result<Response<wire::MachineState>, Status> {
            Err(Status::unimplemented("inspect_machine"))
        }
        async fn inspect_checkpoint(
            &self,
            _request: Request<wire::InspectCheckpointRequest>,
        ) -> Result<Response<wire::CheckpointState>, Status> {
            Err(Status::unimplemented("inspect_checkpoint"))
        }
        async fn list_machines(
            &self,
            _request: Request<wire::ListMachinesRequest>,
        ) -> Result<Response<wire::MachinePage>, Status> {
            Err(Status::unimplemented("list_machines"))
        }
        async fn events(
            &self,
            _request: Request<wire::EventsRequest>,
        ) -> Result<Response<wire::EventPage>, Status> {
            Err(Status::unimplemented("events"))
        }
        async fn usage(
            &self,
            _request: Request<wire::UsageRequest>,
        ) -> Result<Response<wire::UsageReceipt>, Status> {
            Err(Status::unimplemented("usage"))
        }

        async fn cancel(
            &self,
            request: Request<wire::OperationRequest>,
        ) -> Result<Response<wire::OperationState>, Status> {
            self.require_operation(&request.into_inner())?;
            Ok(Response::new(self.cancelled.clone()))
        }

        async fn inspect_operation(
            &self,
            request: Request<wire::OperationRequest>,
        ) -> Result<Response<wire::OperationState>, Status> {
            self.require_operation(&request.into_inner())?;
            Ok(Response::new(self.inspected.clone()))
        }

        type WatchOperationStream = Pin<
            Box<dyn futures::Stream<Item = Result<wire::OperationState, Status>> + Send + 'static>,
        >;

        async fn watch_operation(
            &self,
            request: Request<wire::OperationRequest>,
        ) -> Result<Response<Self::WatchOperationStream>, Status> {
            self.require_operation(&request.into_inner())?;
            match &self.watch {
                WatchReply::Reject(code) => Err(Status::new(*code, "watch rejected")),
                WatchReply::Items(items) => {
                    let items = items.clone().into_iter().map(|item| match item {
                        WatchItem::State(value) => Ok(value),
                        WatchItem::Error(code) => Err(Status::new(code, "watch interrupted")),
                    });
                    Ok(Response::new(Box::pin(stream::iter(items))))
                }
            }
        }
    }

    impl OperationService {
        fn require_operation(&self, request: &wire::OperationRequest) -> Result<(), Status> {
            if request
                .operation
                .as_ref()
                .map(|value| value.value.as_slice())
                != Some(self.expected_operation.as_bytes().as_slice())
            {
                return Err(Status::invalid_argument("operation was substituted"));
            }
            Ok(())
        }
    }

    async fn serve_operation_service(
        service: OperationService,
    ) -> Result<
        (
            Machines,
            tokio::sync::oneshot::Sender<()>,
            tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
        ),
        Box<dyn std::error::Error + Send + Sync>,
    > {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel();
        let server = tokio::spawn(async move {
            Server::builder()
                .add_service(MachinesServiceServer::new(service))
                .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async {
                    let _ = shutdown_rx.await;
                })
                .await
        });
        let channel = TonicEndpoint::from_shared(format!("http://{address}"))?
            .connect()
            .await?;
        Ok((Machines::grpc(channel), shutdown_tx, server))
    }

    fn operation_state(
        operation: OperationId,
        status: wire::OperationStatus,
    ) -> wire::OperationState {
        wire::OperationState {
            operation: Some(wire::OperationId {
                value: operation.as_bytes().to_vec(),
            }),
            status: status as i32,
        }
    }

    fn recovered_suspend(
        outer: OperationId,
        nested: OperationId,
        machine: MachineId,
    ) -> wire::RecoveredAdmission {
        wire::RecoveredAdmission {
            operation: Some(wire::OperationId {
                value: outer.as_bytes().to_vec(),
            }),
            result: Some(wire::recovered_admission::Result::Suspend(
                wire::MutationAdmission {
                    operation: Some(wire::OperationId {
                        value: nested.as_bytes().to_vec(),
                    }),
                    machine: Some(encode_machine(machine)),
                    checkpoint: None,
                },
            )),
        }
    }

    #[tokio::test]
    async fn remote_transport_rejects_plaintext_before_connecting() {
        let result = Machines::connect(
            "http://example.invalid",
            Tls {
                ca: b"",
                certificate: b"",
                private_key: b"",
            },
        )
        .await;
        assert!(matches!(result, Err(ProviderError::Invalid(_))));
    }

    #[tokio::test]
    async fn operation_for_rejects_a_substituted_nested_admission_operation()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000002")?;
        let substituted = OperationId::parse("00000000-0000-0000-0000-000000000003")?;
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000004")?;
        let service = OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, substituted, machine),
            inspected: operation_state(operation, wire::OperationStatus::Pending),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Items(Vec::new()),
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;

        assert_eq!(
            machines.operation_for(key).await,
            Err(ProviderError::Rejected(
                "recovery operation identity was substituted".into()
            ))
        );

        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn operation_control_crosses_grpc_with_exact_identity_replay_and_terminal_state()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000011")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000012")?;
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000013")?;
        let pending = operation_state(operation, wire::OperationStatus::Pending);
        let succeeded = operation_state(operation, wire::OperationStatus::Succeeded);
        let service = OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: pending.clone(),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Items(vec![
                WatchItem::State(pending.clone()),
                WatchItem::State(pending.clone()),
                WatchItem::State(succeeded),
                WatchItem::State(pending),
            ]),
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;

        assert_eq!(machines.operation_for(key).await?, operation);
        assert_eq!(
            machines.inspect_operation(operation).await?,
            OperationObservation {
                id: operation,
                phase: OperationPhase::Pending,
            }
        );
        assert_eq!(
            machines.cancel_operation(operation).await?,
            OperationObservation {
                id: operation,
                phase: OperationPhase::Cancelled,
            }
        );
        let mut watch = machines.watch_operation(operation).await?;
        assert_eq!(
            watch.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Pending)
        );
        assert_eq!(
            watch.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Pending)
        );
        assert_eq!(
            watch.next().await.transpose()?.map(|value| value.phase),
            Some(OperationPhase::Succeeded)
        );
        assert!(watch.next().await.is_none());

        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn all_operation_observation_paths_reject_substituted_identity()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000021")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000022")?;
        let substituted = OperationId::parse("00000000-0000-0000-0000-000000000023")?;
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000024")?;
        let substituted_state = operation_state(substituted, wire::OperationStatus::Pending);
        let service = OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: substituted_state.clone(),
            cancelled: substituted_state.clone(),
            watch: WatchReply::Items(vec![WatchItem::State(substituted_state)]),
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;

        assert!(matches!(
            machines.inspect_operation(operation).await,
            Err(ProviderError::Rejected(message)) if message.contains("identity was substituted")
        ));
        assert!(matches!(
            machines.cancel_operation(operation).await,
            Err(ProviderError::Rejected(message)) if message.contains("identity was substituted")
        ));
        let mut watch = machines.watch_operation(operation).await?;
        assert!(matches!(
            watch.next().await,
            Some(Err(ProviderError::Rejected(message))) if message.contains("identity was substituted")
        ));

        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }

    #[tokio::test]
    async fn operation_watch_distinguishes_pre_admission_rejection_from_post_admission_ambiguity()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000031")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000032")?;
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000033")?;
        let base = OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: operation_state(operation, wire::OperationStatus::Pending),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Reject(Code::PermissionDenied),
        };
        let (machines, shutdown, server) = serve_operation_service(base.clone()).await?;
        assert!(matches!(
            machines.watch_operation(operation).await,
            Err(ProviderError::Rejected(message)) if message == "watch rejected"
        ));
        let _ = shutdown.send(());
        server.await??;

        for code in [Code::Internal, Code::ResourceExhausted, Code::Unavailable] {
            let service = OperationService {
                watch: WatchReply::Items(vec![
                    WatchItem::State(operation_state(operation, wire::OperationStatus::Pending)),
                    WatchItem::Error(code),
                ]),
                ..base.clone()
            };
            let (machines, shutdown, server) = serve_operation_service(service).await?;
            let mut watch = machines.watch_operation(operation).await?;
            assert!(matches!(
                watch.next().await,
                Some(Ok(OperationObservation { id, phase: OperationPhase::Pending })) if id == operation
            ));
            assert_eq!(
                watch.next().await,
                Some(Err(ProviderError::OperationIndeterminate(operation)))
            );
            assert!(watch.next().await.is_none());
            let _ = shutdown.send(());
            server.await??;
        }

        let service = OperationService {
            watch: WatchReply::Items(vec![WatchItem::State(operation_state(
                operation,
                wire::OperationStatus::Pending,
            ))]),
            ..base
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        let mut watch = machines.watch_operation(operation).await?;
        assert!(matches!(
            watch.next().await,
            Some(Ok(OperationObservation { id, phase: OperationPhase::Pending })) if id == operation
        ));
        assert_eq!(
            watch.next().await,
            Some(Err(ProviderError::OperationIndeterminate(operation)))
        );
        assert!(watch.next().await.is_none());
        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }

    #[test]
    fn wire_decoders_reject_nil_and_duplicate_capabilities() {
        assert!(decode_machine(Some(&wire::MachineId { value: vec![0; 16] })).is_err());
        let capabilities = BTreeSet::from([Capability::ElasticCpu]);
        assert!(
            decode_compatibility(
                Some(&wire::CompatibilityPolicy {
                    mode: wire::CompatibilityMode::Require as i32,
                    required: vec![
                        wire::Capability::ElasticCpu as i32,
                        wire::Capability::ElasticCpu as i32,
                    ],
                }),
                &capabilities,
            )
            .is_err()
        );
    }

    #[test]
    fn all_capability_intents_round_trip_through_the_wire() {
        let capabilities = [
            Capability::ElasticCpu,
            Capability::ElasticMemory,
            Capability::LiveCheckpoint,
            Capability::LiveFork,
            Capability::SuspendResume,
            Capability::LiveMovement,
            Capability::DiskFork,
        ];
        for capability in capabilities {
            assert_eq!(
                decode_capability(encode_capability(capability)),
                Ok(capability)
            );
        }
    }

    #[test]
    fn managed_usage_rejects_missing_lineage_authority() {
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|_| unreachable!());
        let receipt = |lineage_receipt_sha256: Vec<u8>| wire::UsageReceipt {
            machine: Some(encode_machine(machine)),
            start_unix_ms: 1,
            end_unix_ms: 2,
            elastic_cpu_ns: 0,
            dedicated_cpu_ns: 0,
            private_resident_byte_seconds: 0,
            durable_private_bytes: 0,
            lineage_receipt_sha256,
            egress_bytes: 0,
            receipt: vec![1],
            ..Default::default()
        };
        assert!(decode_usage_receipt(receipt(vec![7; 32]), machine, 1, 2).is_ok());
        assert!(decode_usage_receipt(receipt(Vec::new()), machine, 1, 2).is_err());
        assert!(decode_usage_receipt(receipt(vec![0; 32]), machine, 1, 2).is_err());
    }

    #[test]
    fn mutation_errors_preserve_unsupported_capabilities() {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|_| unreachable!());
        assert_eq!(
            mutation_error(key, &tonic::Status::unimplemented("capability unavailable"),),
            ProviderError::Unsupported("capability unavailable".into())
        );
        assert_eq!(
            watch_error(key, tonic::Status::unimplemented("watch unavailable")),
            ProviderError::Indeterminate(key)
        );
        assert_eq!(
            watch_error(
                key,
                tonic::Status::resource_exhausted("watch capacity unavailable"),
            ),
            ProviderError::Indeterminate(key)
        );
    }

    #[tokio::test]
    async fn recovered_forks_reject_empty_and_duplicate_child_sets_before_fanout() {
        let provider = GrpcProvider {
            client: wire::machines_service_client::MachinesServiceClient::new(
                TonicEndpoint::from_static("http://127.0.0.1:1").connect_lazy(),
            ),
        };
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000001")
            .unwrap_or_else(|_| unreachable!());
        let child = MachineId::parse("00000000-0000-0000-0000-000000000002")
            .unwrap_or_else(|_| unreachable!());
        let recovered = |children| wire::RecoveredAdmission {
            operation: Some(wire::OperationId {
                value: operation.as_bytes().to_vec(),
            }),
            result: Some(wire::recovered_admission::Result::Fork(
                wire::ForkAdmission {
                    checkpoint: Some(encode_checkpoint(
                        CheckpointId::parse("00000000-0000-0000-0000-000000000003")
                            .unwrap_or_else(|_| unreachable!()),
                    )),
                    children,
                    operation: Some(wire::OperationId {
                        value: operation.as_bytes().to_vec(),
                    }),
                    contract: None,
                },
            )),
        };
        assert!(
            decode_recovered(&provider, recovered(Vec::new()))
                .await
                .is_err()
        );
        assert!(
            decode_recovered(
                &provider,
                recovered(vec![encode_machine(child), encode_machine(child)])
            )
            .await
            .is_err()
        );
    }

    #[tokio::test]
    async fn servers_without_live_fork_report_unsupported()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let key = IdempotencyKey::parse("00000000-0000-0000-0000-000000000041")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000042")?;
        let machine = MachineId::parse("00000000-0000-0000-0000-000000000043")?;
        let service = OperationService {
            expected_key: key,
            expected_operation: operation,
            recovered: recovered_suspend(operation, operation, machine),
            inspected: operation_state(operation, wire::OperationStatus::Pending),
            cancelled: operation_state(operation, wire::OperationStatus::Cancelled),
            watch: WatchReply::Items(Vec::new()),
        };
        let (machines, shutdown, server) = serve_operation_service(service).await?;
        assert!(matches!(
            machines
                .provider
                .fork_machine(machine, NonZeroU32::MIN, key)
                .await,
            Err(ProviderError::Unsupported(_))
        ));
        let _ = shutdown.send(());
        server.await??;
        Ok(())
    }

    fn wire_contract(capabilities: &[wire::Capability]) -> wire::MachineContract {
        wire::MachineContract {
            image: Some(wire::Image {
                kind: wire::ImageKind::Custom as i32,
                immutable_reference: Some(wire::image::ImmutableReference::CustomDigest(vec![
                    7;
                    32
                ])),
            }),
            capabilities: capabilities.iter().map(|value| *value as i32).collect(),
            compatibility: Some(wire::CompatibilityPolicy {
                mode: wire::CompatibilityMode::BestEffort as i32,
                required: Vec::new(),
            }),
            compatibility_revision: vec![1; 32],
            performance: wire::Performance::Elastic as i32,
            suspension: Some(wire::SuspensionPolicy {
                policy: Some(wire::suspension_policy::Policy::Manual(true)),
            }),
            expiration: Some(wire::ExpirationPolicy {
                kind: wire::ExpirationKind::Never as i32,
                value_ms: 0,
            }),
            network_policy_digest: vec![8; 32],
            budgets: Some(wire::Budgets::default()),
        }
    }

    #[test]
    fn fork_machine_admissions_are_checked_against_the_source_contract()
    -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let source = MachineId::parse("00000000-0000-0000-0000-000000000051")?;
        let child = MachineId::parse("00000000-0000-0000-0000-000000000052")?;
        let other = MachineId::parse("00000000-0000-0000-0000-000000000053")?;
        let operation = OperationId::parse("00000000-0000-0000-0000-000000000054")?;
        let admission =
            |capabilities: &[wire::Capability],
             fidelity: wire::ForkFidelity,
             children: Vec<MachineId>| wire::ForkMachineAdmission {
                source: Some(encode_machine(source)),
                children: children.into_iter().map(encode_machine).collect(),
                operation: Some(wire::OperationId {
                    value: operation.as_bytes().to_vec(),
                }),
                contract: Some(wire_contract(capabilities)),
                fidelity: fidelity as i32,
            };
        let live = [wire::Capability::LiveFork, wire::Capability::DiskFork];
        let disk = [wire::Capability::DiskFork];

        let checked = decode_fork_machine_admission(&admission(
            &live,
            wire::ForkFidelity::MemoryAndDisk,
            vec![child, other],
        ))?;
        assert_eq!(checked.fidelity, ForkFidelity::MemoryAndDisk);
        assert_eq!(checked.children, vec![child, other]);
        assert_eq!(
            decode_fork_machine_admission(&admission(
                &disk,
                wire::ForkFidelity::DiskOnly,
                vec![child]
            ))?
            .fidelity,
            ForkFidelity::DiskOnly
        );
        for rejected in [
            admission(&disk, wire::ForkFidelity::MemoryAndDisk, vec![child]),
            admission(&live, wire::ForkFidelity::DiskOnly, vec![child]),
            admission(&[], wire::ForkFidelity::DiskOnly, vec![child]),
            admission(&live, wire::ForkFidelity::Unspecified, vec![child]),
            admission(&live, wire::ForkFidelity::MemoryAndDisk, Vec::new()),
            admission(&live, wire::ForkFidelity::MemoryAndDisk, vec![child, child]),
            admission(&live, wire::ForkFidelity::MemoryAndDisk, vec![source]),
        ] {
            assert!(decode_fork_machine_admission(&rejected).is_err());
        }
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn local_socket_metadata_must_be_owner_private() -> Result<(), Box<dyn std::error::Error>>
    {
        use std::os::unix::fs::PermissionsExt as _;

        let directory = std::env::temp_dir().join(format!("acyclic-machines-{}", Uuid::new_v4()));
        std::fs::create_dir(&directory)?;
        std::fs::set_permissions(&directory, std::fs::Permissions::from_mode(0o700))?;
        let path = directory.join("service.sock");
        let listener = tokio::net::UnixListener::bind(&path)?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        let uid = rustix::process::geteuid().as_raw();
        assert!(owner_private_socket(&path, uid).is_ok());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o660))?;
        assert!(owner_private_socket(&path, uid).is_err());
        drop(listener);
        std::fs::remove_file(&path)?;
        std::fs::remove_dir(&directory)?;
        Ok(())
    }
}
