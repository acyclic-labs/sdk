//! Explicit Machines placement for registered durable task builds.

use crate::MachinesHost;
use acyclic_harness::{
    Error, Result,
    registry::ComponentIdentity,
    resources::{ArtifactRef, SandboxRef},
    runtime::{
        DurableBatchRequest, ExecutionPlacement, ExecutionProvider, TaskAdmissionRecord,
        TaskSpawner, TaskStateProvider,
    },
    workflow::MachineIdentity,
};
use acyclic_machines::MachineState;
use std::{collections::BTreeMap, future::Future, pin::Pin, sync::Arc};

/// Asynchronous access qualification result without credential material.
pub type AccessFuture<'a> = Pin<Box<dyn Future<Output = Result<[u8; 32]>> + Send + 'a>>;
type PlacementFuture<'a> = Pin<Box<dyn Future<Output = Result<ExecutionPlacement>> + Send + 'a>>;

/// Host-owned preflight for input transfer, mounts, and scoped credential
/// bindings. It returns a nonzero commitment, never credential bytes. The
/// spawner performs/reconciles preparation under the stable operation ID.
pub trait MachinesExecutionAccess: Send + Sync {
    /// Verifies one task's build, transferred inputs, mounts, and secret grants.
    fn task<'a>(&'a self, request: &'a TaskAdmissionRecord) -> AccessFuture<'a>;
    /// Verifies the complete ordered batch before any member is admitted.
    fn batch<'a>(&'a self, request: &'a DurableBatchRequest) -> AccessFuture<'a>;
}

/// One immutable code build and compatible ready sandbox registered for an
/// exact logical task and resumable state machine.
#[derive(Clone)]
pub struct MachinesTaskBuild {
    /// Exact registered logical task.
    pub task: ComponentIdentity,
    /// Exact resumable implementation the remote host has installed.
    pub machine: MachineIdentity,
    /// Immutable task code artifact installed in the environment.
    pub build: ArtifactRef,
    /// Immutable sandbox image used for readiness qualification.
    pub image: ArtifactRef,
    /// Explicit already-created environment; no machine is provisioned implicitly.
    pub environment: SandboxRef,
}

impl MachinesTaskBuild {
    /// Rejects mismatched identities and malformed provider-owned refs.
    pub fn validate(&self) -> Result<()> {
        if self.task.name != self.machine.name
            || self.task.version != self.machine.version
            || self.task.digest == [0; 32]
            || self.machine.digest == [0; 32]
        {
            return Err(Error::Invalid(
                "Machines task build identity is invalid".into(),
            ));
        }
        self.build.validate()?;
        self.image.validate()?;
        self.environment.validate()
    }
}

/// Packages a Machines qualifier with the durable route that actually admits
/// and observes the work. It does not execute an arbitrary local closure or
/// infer that local paths and credentials are accessible in a sandbox.
pub struct MachinesExecution {
    host: MachinesHost,
    identity: ComponentIdentity,
    spawner: Arc<dyn TaskSpawner>,
    state: Arc<dyn TaskStateProvider>,
    access: Arc<dyn MachinesExecutionAccess>,
    builds: BTreeMap<(String, String), MachinesTaskBuild>,
}

impl MachinesExecution {
    /// Binds a task route and explicit access verifier without provisioning.
    pub fn new(
        host: MachinesHost,
        identity: ComponentIdentity,
        spawner: Arc<dyn TaskSpawner>,
        state: Arc<dyn TaskStateProvider>,
        access: Arc<dyn MachinesExecutionAccess>,
    ) -> Result<Self> {
        if identity.digest == [0; 32]
            || spawner.execution_identity().as_ref() != Some(&identity)
            || state.execution_identity().as_ref() != Some(&identity)
        {
            return Err(Error::Conflict(
                "Machines execution route identities differ".into(),
            ));
        }
        Ok(Self {
            host,
            identity,
            spawner,
            state,
            access,
            builds: BTreeMap::new(),
        })
    }

    /// Adds one exact build before the provider is shared with a runtime.
    pub fn with_build(mut self, build: MachinesTaskBuild) -> Result<Self> {
        build.validate()?;
        self.host.image(&build.image)?;
        self.host.machine_id(&build.environment)?;
        let key = (build.task.name.clone(), build.task.version.clone());
        if self.builds.contains_key(&key) {
            return Err(Error::Conflict(
                "Machines task build already registered".into(),
            ));
        }
        self.builds.insert(key, build);
        Ok(self)
    }

    fn registered(
        &self,
        task: &ComponentIdentity,
        machine: &MachineIdentity,
    ) -> Result<&MachinesTaskBuild> {
        let build = self
            .builds
            .get(&(task.name.clone(), task.version.clone()))
            .ok_or_else(|| {
                Error::NotFound(format!(
                    "Machines task build {}@{}",
                    task.name, task.version
                ))
            })?;
        if &build.task != task || &build.machine != machine {
            return Err(Error::Conflict(
                "Machines task build differs from admission".into(),
            ));
        }
        Ok(build)
    }

    async fn qualified(
        &self,
        build: &MachinesTaskBuild,
        access_digest: [u8; 32],
    ) -> Result<ExecutionPlacement> {
        if access_digest == [0; 32] {
            return Err(Error::Invalid(
                "execution access attestation is empty".into(),
            ));
        }
        let image = self.host.qualify(&build.image).await?;
        let environment = self.host.inspect(&build.environment).await?;
        if environment.state != MachineState::Running
            || image.image != environment.contract.image
            || image.compatibility_revision != environment.contract.compatibility_revision
        {
            return Err(Error::Conflict(
                "Machines environment is not ready for the qualified image".into(),
            ));
        }
        let mut digest = blake3::Hasher::new();
        digest.update(b"harness/v2/machines-execution\0");
        digest.update(build.build.as_resource().key());
        digest.update(build.environment.as_resource().key());
        digest.update(&image.compatibility_revision);
        digest.update(&environment.contract.network_policy_digest);
        digest.update(&access_digest);
        let placement = ExecutionPlacement {
            provider: self.identity.clone(),
            build: build.build.clone(),
            environment: Some(build.environment.clone()),
            readiness_revision: *digest.finalize().as_bytes(),
        };
        placement.validate()?;
        Ok(placement)
    }
}

impl ExecutionProvider for MachinesExecution {
    fn identity(&self) -> ComponentIdentity {
        self.identity.clone()
    }
    fn spawner(&self) -> Arc<dyn TaskSpawner> {
        Arc::clone(&self.spawner)
    }
    fn state(&self) -> Arc<dyn TaskStateProvider> {
        Arc::clone(&self.state)
    }
    fn qualify<'a>(&'a self, request: &'a TaskAdmissionRecord) -> PlacementFuture<'a> {
        Box::pin(async move {
            let build = self.registered(&request.task, &request.machine)?;
            let access = self.access.task(request).await?;
            self.qualified(build, access).await
        })
    }
    fn qualify_batch<'a>(&'a self, request: &'a DurableBatchRequest) -> PlacementFuture<'a> {
        Box::pin(async move {
            let build = self.registered(&request.task, &request.machine)?;
            let access = self.access.batch(request).await?;
            self.qualified(build, access).await
        })
    }
}
