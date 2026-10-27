//! Modal Sandbox V2 control plane and native task command router transport.
//! Requires an application and an already built immutable Modal image binding.
mod wire;
use super::instances::Instances;
use super::{
    Allocation, ExecSession, NetworkPolicy, SandboxDriver, SandboxInstance, SandboxSpec,
    duplex::{duplex_exec, pump_stdin},
    http::secret,
    rpc::Rpc,
};
use crate::{ExecCommand, ExecOutput, ExecStream, MachineState, ProviderError};
use async_trait::async_trait;
use futures::{StreamExt, TryStreamExt};
use std::time::Duration;
use tonic::metadata::MetadataMap;

const CONTROL: &str = "modal.client.ModalClient";
const ROUTER: &str = "modal.task_command_router.TaskCommandRouter";

#[derive(Clone)]
struct Guest {
    task: String,
    router: Rpc,
    sandbox_token: String,
}

/// Modal connection with explicit credentials; never reads account config files.
/// V2 Sandboxes have a 24-hour ceiling, retained as an admitted maximum age.
#[derive(Clone)]
pub struct Modal {
    control: Rpc,
    app: String,
    guests: Instances<Guest>,
}

impl Modal {
    /// Binds to the Modal control plane (normally `https://api.modal.com`).
    pub fn new(
        endpoint: &str,
        token_id: &str,
        token_secret: &str,
        app_id: &str,
    ) -> Result<Self, ProviderError> {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("x-modal-token-id", secret(token_id)?);
        headers.insert("x-modal-token-secret", secret(token_secret)?);
        headers.insert(
            "x-modal-client-type",
            reqwest::header::HeaderValue::from_static("1"),
        );
        headers.insert(
            "x-modal-client-version",
            reqwest::header::HeaderValue::from_static("0.11.1"),
        );
        if app_id.is_empty() {
            return Err(ProviderError::Invalid(
                "Modal app admission failed; recovery: configure an existing app ID".into(),
            ));
        }
        Ok(Self {
            control: Rpc::new(endpoint, MetadataMap::from_headers(headers))?,
            app: app_id.into(),
            guests: Instances::default(),
        })
    }

    fn control_for(&self, guest: &Guest) -> Result<Rpc, ProviderError> {
        let mut control = self.control.clone();
        if !guest.sandbox_token.is_empty() {
            let mut headers = control.metadata.clone().into_headers();
            headers.insert("x-modal-sandbox-token", secret(&guest.sandbox_token)?);
            control.metadata = MetadataMap::from_headers(headers);
        }
        Ok(control)
    }

    async fn raw_start(
        guest: &Guest,
        argv: Vec<String>,
        timeout: Duration,
        input: Option<Vec<u8>>,
    ) -> Result<ExecStream, ProviderError> {
        let exec = format!("ex-{}", uuid::Uuid::new_v4().simple());
        let _: wire::Empty = guest
            .router
            .unary(
                ROUTER,
                "TaskExecStart",
                wire::TaskExecStartRequest {
                    task_id: guest.task.clone(),
                    exec_id: exec.clone(),
                    command_args: argv,
                    stdout_config: 1,
                    stderr_config: 1,
                    timeout_secs: Some(
                        u32::try_from(
                            timeout
                                .as_secs()
                                .saturating_add(u64::from(timeout.subsec_nanos() != 0)),
                        )
                        .unwrap_or(u32::MAX),
                    ),
                },
                Duration::from_secs(15),
            )
            .await?;
        // Attach both streams while pumping stdin. Some routers delay response
        // headers until the process emits bytes; awaiting those headers first
        // would deadlock a command which needs input before producing output.
        let attach = |fd| {
            let guest = guest.clone();
            let exec = exec.clone();
            async move {
                guest
                    .router
                    .streaming::<_, wire::TaskExecStdioReadResponse>(
                        ROUTER,
                        "TaskExecStdioRead",
                        wire::TaskExecStdioReadRequest {
                            task_id: guest.task,
                            exec_id: exec,
                            offset: 0,
                            file_descriptor: fd,
                        },
                        timeout + Duration::from_secs(10),
                    )
                    .await
                    .map(|frames| {
                        frames
                            .try_filter_map(move |frame| async move {
                                Ok(if frame.data.is_empty() {
                                    None
                                } else {
                                    Some(if fd == 0 {
                                        ExecOutput::Stdout(frame.data)
                                    } else {
                                        ExecOutput::Stderr(frame.data)
                                    })
                                })
                            })
                            .boxed()
                    })
            }
        };
        let stdout = attach(0);
        let stderr = attach(1);
        let router = guest.router.clone();
        let wait = wire::TaskExecWaitRequest {
            task_id: guest.task.clone(),
            exec_id: exec.clone(),
        };
        let exit = async move {
            let result: wire::TaskExecWaitResponse = router
                .unary(
                    ROUTER,
                    "TaskExecWait",
                    wait,
                    timeout + Duration::from_secs(10),
                )
                .await?;
            match result.exit_status {
                Some(wire::ExitStatus::Code(code)) => Ok(code),
                Some(wire::ExitStatus::Signal(signal)) => Ok(128 + signal),
                None => Err(ProviderError::Unavailable),
            }
        };
        let input_guest = guest.clone();
        let input_exec = exec.clone();
        let input = pump_stdin(input, move |offset, data, eof| {
            let guest = input_guest.clone();
            let exec = input_exec.clone();
            Box::pin(async move {
                let _: wire::Empty = guest
                    .router
                    .unary(
                        ROUTER,
                        "TaskExecStdinWrite",
                        wire::TaskExecStdinWriteRequest {
                            task_id: guest.task,
                            exec_id: exec,
                            offset,
                            data,
                            eof,
                        },
                        Duration::from_secs(10),
                    )
                    .await?;
                Ok(())
            })
        });
        Ok(duplex_exec(stdout, stderr, input, exit))
    }

    fn session(
        &self,
        instance: &SandboxInstance,
        command: &ExecCommand,
        input: Option<Vec<u8>>,
    ) -> Result<ExecSession, ProviderError> {
        let guest = self.guests.get(instance)?;
        Ok(super::process_group_io(
            command,
            input,
            move |argv, timeout, input| {
                let guest = guest.clone();
                Box::pin(async move {
                    Self::raw_start(
                        &guest,
                        argv,
                        Duration::from_millis(u64::from(timeout)),
                        input,
                    )
                    .await
                })
            },
        ))
    }
}

#[async_trait]
impl SandboxDriver for Modal {
    fn name(&self) -> &'static str {
        "modal"
    }
    fn supports_suspend(&self) -> bool {
        false
    }
    fn supports_disk_snapshots(&self) -> bool {
        true
    }
    fn snapshot_retention(&self) -> Option<Duration> {
        Some(Duration::from_secs(30 * 24 * 60 * 60))
    }
    async fn snapshot(
        &self,
        instance: &SandboxInstance,
        request_id: &str,
    ) -> Result<String, ProviderError> {
        let guest = self.guests.get(instance)?;
        let response: wire::ImageId = guest
            .router
            .unary(
                ROUTER,
                "TaskSnapshotFilesystem",
                wire::TaskSnapshotFilesystemRequest {
                    task_id: guest.task,
                    snapshot_id: request_id.into(),
                    ttl_seconds: Some(30 * 24 * 60 * 60),
                },
                Duration::from_secs(55),
            )
            .await?;
        if response.image_id.is_empty() {
            return Err(ProviderError::Unavailable);
        }
        Ok(response.image_id)
    }
    async fn release_snapshot(
        &self,
        snapshot: &super::ProviderSnapshot,
    ) -> Result<super::SnapshotRelease, ProviderError> {
        let _: wire::Empty = self
            .control
            .unary(
                CONTROL,
                "ImageDelete",
                wire::ImageId {
                    image_id: snapshot.handle.clone(),
                },
                Duration::from_secs(10),
            )
            .await?;
        Ok(super::SnapshotRelease::Deleted)
    }
    fn maximum_lifetime(&self) -> Option<Duration> {
        Some(Duration::from_secs(24 * 3600))
    }
    fn validate(&self, spec: &SandboxSpec) -> Result<(), ProviderError> {
        if !spec.env.is_empty() {
            return Err(ProviderError::Unsupported("Modal environment admission failed; recovery: register environment variables in the immutable image".into()));
        }
        spec.lifetime_seconds().map(|_| ())
    }

    async fn create(&self, spec: &SandboxSpec) -> Result<SandboxInstance, ProviderError> {
        let network = match &spec.network {
            NetworkPolicy::BlockAll => wire::NetworkAccess {
                network_access_type: 2,
                allowed_domains: vec![],
            },
            NetworkPolicy::AllowDomains(domains) => wire::NetworkAccess {
                network_access_type: 3,
                allowed_domains: domains.clone(),
            },
        };
        let response: wire::SandboxCreateV2Response = self
            .control
            .unary(
                CONTROL,
                "SandboxCreateV2",
                wire::SandboxCreateV2Request {
                    app_id: self.app.clone(),
                    definition: Some(wire::Sandbox {
                        entrypoint_args: vec!["sleep".into(), "infinity".into()],
                        image_id: spec
                            .snapshot
                            .as_ref()
                            .map_or_else(|| spec.image.clone(), |snapshot| snapshot.handle.clone()),
                        resources: Some(wire::Resources {
                            memory_mb: 1024,
                            milli_cpu: 1000,
                        }),
                        timeout_secs: spec.lifetime_seconds()?,
                        network_access: Some(network),
                        name: Some(spec.name.clone()),
                    }),
                },
                Duration::from_secs(30),
            )
            .await?;
        if response.sandbox_id.is_empty() || response.task_id.is_empty() {
            return Err(ProviderError::Unavailable);
        }
        let access = response
            .command_router_access
            .ok_or(ProviderError::Unavailable)?;
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            reqwest::header::AUTHORIZATION,
            secret(&format!("Bearer {}", access.jwt))?,
        );
        let router = Rpc::new(&access.url, MetadataMap::from_headers(headers))?;
        self.guests.insert(
            response.sandbox_id.clone(),
            Guest {
                task: response.task_id,
                router,
                sandbox_token: response.sandbox_token,
            },
        );
        Ok(SandboxInstance {
            endpoint: format!("modal://{}", response.sandbox_id),
            id: response.sandbox_id,
            allocation: Allocation {
                cpu: 1,
                memory_gib: 1,
                disk_gib: 0,
            },
        })
    }

    async fn state(&self, instance: &SandboxInstance) -> Result<MachineState, ProviderError> {
        let guest = self.guests.get(instance)?;
        let response: wire::SandboxWaitResponse = self
            .control_for(&guest)?
            .unary(
                CONTROL,
                "SandboxWaitV2",
                wire::SandboxWaitRequest {
                    sandbox_id: instance.id.clone(),
                    timeout: 0.0,
                },
                Duration::from_secs(5),
            )
            .await?;
        Ok(
            if response.result.is_some_and(|result| result.status != 0) {
                MachineState::Destroyed
            } else {
                MachineState::Running
            },
        )
    }

    async fn destroy(&self, instance: &SandboxInstance) -> Result<(), ProviderError> {
        let guest = self.guests.get(instance)?;
        let _: wire::Empty = self
            .control_for(&guest)?
            .unary(
                CONTROL,
                "SandboxTerminateV2",
                wire::SandboxRequest {
                    sandbox_id: instance.id.clone(),
                },
                Duration::from_secs(10),
            )
            .await?;
        self.guests.remove(instance);
        Ok(())
    }

    fn start(
        &self,
        instance: &SandboxInstance,
        command: ExecCommand,
    ) -> Result<ExecSession, ProviderError> {
        self.session(instance, &command, None)
    }

    async fn read_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
    ) -> Result<Vec<u8>, ProviderError> {
        super::exec_read_file(instance, path, |command, input| {
            self.session(instance, command, input)
        })
        .await
    }

    async fn write_file(
        &self,
        instance: &SandboxInstance,
        path: &str,
        data: &[u8],
    ) -> Result<(), ProviderError> {
        super::exec_write_file(instance, path, data, |command, input| {
            self.session(instance, command, input)
        })
        .await
    }
}
