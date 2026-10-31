//! The agent's side: the roster of attached devices and the executor that
//! queues a `shell` call on one and waits for its outcome.
use super::records::{
    RECORD_VERSION, Run, RunCommand, RunEvent, RunRecord, TargetPaths, TargetStatus, decode, encode,
};
use crate::{
    Capabilities, Error, Result,
    agent::{Refused, Roster, TargetRefusal, TargetState},
    core::{AuthorityIssuer, AuthorityVerifier, Scope},
    runtime::{RuntimeScope, Target, TargetResolver, ToolContext},
    tool::{Dispatch, ToolExecutor, ToolInvocation, ToolResult},
    tools::ShellInput,
};
use acyclic_stream::{
    BoxProviderFuture, FollowRequest, FollowUpdate, StreamProvider, StreamProviderExt as _,
    WhenAbsent,
};
use futures::StreamExt as _;
use std::collections::BTreeMap;
use std::sync::{Arc, OnceLock};
use tokio::sync::watch;

/// The authority an agent's host attaches to each run it dispatches: a
/// `tool:call:shell` workload it attenuates to one sealed grant per run.
pub struct DeviceGrant {
    /// The agent host's workload; it must grant `tool:call:shell`.
    pub scope: Scope,
    /// Its original parent-bound delegation port; no runtime signing key.
    pub issuer: AuthorityIssuer,
    /// The original owner's current public verification boundary.
    pub verifier: AuthorityVerifier,
}

/// Every device's last presence, in attach order, and the roster they make.
struct Devices {
    order: Vec<String>,
    statuses: BTreeMap<String, TargetStatus>,
    roster: OnceLock<Arc<Roster>>,
    following: bool,
    active: bool,
}

impl Devices {
    fn roster(&self, home: &crate::agent::TargetRecord) -> Arc<Roster> {
        Arc::clone(self.roster.get_or_init(|| {
            let targets = std::iter::once(home.clone())
                .chain(self.order.iter().filter_map(|name| {
                    self.statuses.get(name).map(|status| {
                        let mut target = status.target.clone();
                        if !self.active {
                            target.state = TargetState::Offline;
                        }
                        target
                    })
                }))
                .collect();
            // Each record was validated at the presence boundary; names are indexed once.
            Arc::new(Roster {
                home: home.name.clone(),
                targets,
            })
        }))
    }
}

/// The original follow's lifetime, including cancellation during registration.
struct Presence<'a>(&'a watch::Sender<Devices>);
impl Drop for Presence<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|devices| {
            devices.following = false;
            devices.active = false;
            devices.roster.take();
        });
    }
}

/// The targets of an agent tree: its in-process home plus every attached
/// device under one root, as their presence records list them.
///
/// [`Self::follow`] keeps the roster current from the shared presence
/// stream. The directory retains one agent's original authority; share its
/// resolved executors within that agent's turns.
pub struct TargetDirectory<P: StreamProvider + ?Sized + 'static> {
    provider: Arc<P>,
    store: acyclic_actors::local::Store<P>,
    paths: TargetPaths,
    home: Arc<Target>,
    grant: Arc<DeviceGrant>,
    devices: watch::Sender<Devices>,
}

impl<P: StreamProvider + ?Sized + 'static> TargetDirectory<P> {
    /// The archive hook for this directory's exact original agent grant.
    /// The owner remains host-side; it is never serialized into a device run.
    pub fn lifecycle(
        &self,
        owner: crate::core::AuthorityRoot,
        resources: Option<Arc<dyn crate::agent::AgentLifecycle>>,
    ) -> Result<super::DeviceLifecycle> {
        super::DeviceLifecycle::new(Arc::clone(&self.grant), owner, resources)
    }

    /// The tree whose home is `home` and whose devices live under `paths`.
    ///
    /// # Errors
    /// When the home record is malformed.
    pub fn new(
        store: acyclic_actors::local::Store<P>,
        paths: TargetPaths,
        home: Target,
        grant: DeviceGrant,
    ) -> Result<Self> {
        crate::agent::AgentRecord::check(&home.record).map_err(agent)?;
        let devices = Devices {
            order: Vec::new(),
            statuses: BTreeMap::new(),
            roster: OnceLock::new(),
            following: false,
            active: false,
        };
        Ok(Self {
            provider: Arc::clone(store.provider()),
            store,
            paths,
            home: Arc::new(home),
            grant: Arc::new(grant),
            devices: watch::Sender::new(devices),
        })
    }

    /// Follows every device's presence, keeping the roster current. It
    /// returns only when the stream fails.
    ///
    /// # Errors
    /// When the presence stream fails or carries a malformed record.
    pub async fn follow(&self) -> Result<std::convert::Infallible> {
        if !self.devices.send_if_modified(|devices| {
            if devices.following {
                return false;
            }
            devices.following = true;
            true
        }) {
            return Err(Error::Conflict(
                "device presence already has its original follow".into(),
            ));
        }
        let _presence = Presence(&self.devices);
        let mut request = FollowRequest::new(self.paths.presence()?, 0);
        request.when_absent = WhenAbsent::Wait;
        let mut updates = self.provider.follow(request).await?;
        self.devices.send_modify(|devices| {
            devices.active = true;
            devices.roster.take();
        });
        loop {
            let Some(update) = updates.next().await else {
                return Err(Error::Storage("device presence follow ended".into()));
            };
            let FollowUpdate::Record(record) = update? else {
                continue;
            };
            let status: TargetStatus = decode(&record.value)?;
            crate::agent::AgentRecord::check(&status.target).map_err(agent)?;
            if (status.target.state == TargetState::Online) != status.session.is_some() {
                return Err(Error::Invalid(
                    "device presence has no matching session".into(),
                ));
            }
            let name = status.target.name.clone();
            // A device named as the home is never listed; it cannot shadow it.
            if name == self.home.record.name {
                continue;
            }
            self.devices.send_modify(|devices| {
                if devices.statuses.insert(name.clone(), status).is_none() {
                    devices.order.push(name);
                }
                devices.roster.take();
            });
        }
    }
}

impl<P: StreamProvider + ?Sized + 'static> TargetResolver for TargetDirectory<P> {
    fn roster(&self) -> Arc<Roster> {
        self.devices.borrow().roster(&self.home.record)
    }

    fn resolve(&self, target: Option<&str>) -> Result<Arc<Target>> {
        let roster = self.roster();
        let record = roster.select(target).map_err(Error::Target)?;
        if record.name == self.home.record.name {
            return Ok(Arc::clone(&self.home));
        }
        Ok(Arc::new(Target {
            record: record.clone(),
            executor: Arc::new(DeviceExecutor {
                provider: Arc::clone(&self.provider),
                paths: self.paths.clone(),
                name: record.name.clone(),
                wake: self
                    .store
                    .activation_record(&self.paths.actor(&record.name)?),
                grant: Arc::clone(&self.grant),
                home: self.home.record.clone(),
                devices: self.devices.subscribe(),
            }),
        }))
    }
}

/// Runs `shell` on one device: [`ToolExecutor::dispatch`] queues the run
/// with the call's claim, and execution waits for the run's terminal record.
struct DeviceExecutor<P: StreamProvider + ?Sized + 'static> {
    provider: Arc<P>,
    paths: TargetPaths,
    name: String,
    wake: (acyclic_stream::StreamPath, acyclic_stream::OwnedBytes),
    grant: Arc<DeviceGrant>,
    devices: watch::Receiver<Devices>,
    home: crate::agent::TargetRecord,
}

impl<P: StreamProvider + ?Sized + 'static> DeviceExecutor<P> {
    fn refusal(&self, refused: Refused) -> Error {
        let roster = self.devices.borrow().roster(&self.home);
        Error::Target(TargetRefusal {
            target: self.name.clone(),
            refused,
            online: roster
                .targets
                .iter()
                .filter(|record| record.state == TargetState::Online)
                .map(|record| record.name.clone())
                .collect(),
        })
    }

    /// Original operation events joined with its immutable session end. A
    /// disconnect needs one end marker regardless of the number of commands.
    async fn outcome(
        &self,
        invocation: &ToolInvocation,
        context: &ToolContext,
    ) -> Result<ToolResult> {
        let mut request =
            FollowRequest::new(self.paths.run(&self.name, invocation.operation_id)?, 0);
        request.when_absent = WhenAbsent::Wait;
        let mut events = self.provider.follow(request).await?;
        let mut ended: Option<acyclic_stream::FollowStream> = None;
        let mut output = super::output::Output::new(context);
        let mut session_ended = false;
        loop {
            let update = tokio::select! {
                biased;
                end = async { ended.as_mut().expect("guarded session follow").next().await }, if ended.is_some() => {
                    let end = end.ok_or_else(|| Error::Storage("device session producer ended".into()))??;
                    if let FollowUpdate::Record(record) = end {
                        let RunRecord { event: RunEvent::Lost { .. }, .. } = decode(&record.value)? else {
                            return Err(Error::Invalid("device session has no terminal marker".into()));
                        };
                        // An Exit committed before Close remains recoverable. A
                        // lagging observation can only produce Indeterminate;
                        // it never certifies that the command did not run.
                        let head = self.provider.head(self.paths.run(&self.name, invocation.operation_id)?).await?;
                        if let Some(last) = head.last {
                            let RunRecord { event, .. } = decode(&last.value)?;
                            if let Some(result) = self.terminal(event, invocation) {
                                // Drain the original finite terminal prefix so
                                // every output file is materialized in order.
                                result?;
                                session_ended = true;
                                ended = None;
                                continue;
                            }
                        }
                        return Err(Error::Indeterminate(invocation.operation_id));
                    }
                    continue;
                }
                update = events.next() => update.ok_or_else(|| Error::Storage("device run follow ended".into()))?,
            };
            let FollowUpdate::Record(record) = update? else {
                continue;
            };
            let RunRecord { event, .. } = decode(&record.value)?;
            if let RunEvent::Started { generation, .. } = event {
                if session_ended {
                    continue;
                }
                let mut request =
                    FollowRequest::new(self.paths.session_end(&self.name, generation)?, 0);
                request.when_absent = WhenAbsent::Wait;
                ended = Some(self.provider.follow(request).await?);
            } else if let RunEvent::Output { stderr, data, .. } = event {
                output
                    .append(context, record.sequence, stderr, &data)
                    .await?;
            } else if let Some(result) = self.terminal(event, invocation) {
                return output.finish(result?);
            }
        }
    }

    fn terminal(&self, event: RunEvent, invocation: &ToolInvocation) -> Option<Result<ToolResult>> {
        match event {
            RunEvent::Started { .. } | RunEvent::Output { .. } => None,
            RunEvent::Exit { result } => Some(Ok(ToolResult { value: result })),
            RunEvent::Lost { .. } => Some(Err(Error::Indeterminate(invocation.operation_id))),
            RunEvent::Refused { reason } => Some(Err(self.refusal(Refused::Declined(reason)))),
        }
    }
}

impl<P: StreamProvider + ?Sized + 'static> ToolExecutor for DeviceExecutor<P> {
    fn authorize(
        &self,
        _scope: Option<&RuntimeScope>,
        context: Option<&ToolContext>,
        invocation: &ToolInvocation,
        work: crate::NativeWork<'_>,
    ) -> Result<()> {
        #[cfg(feature = "filesystem")]
        if work.is_some() {
            return Err(Error::NativeBoundary {
                code: crate::wire::ErrorCode::Unsupported,
                reason: "device commands require ordinary task execution",
            });
        }
        let _ = work;
        let context = context.ok_or_else(|| {
            Error::Unauthorized("device shell has no task; bind its original tool context".into())
        })?;
        if invocation.name != crate::tools::shell::PLATFORM.name
            || context.operation_id() != invocation.operation_id
            || context.call_id() != invocation.call_id
            || !context
                .effective_scope()
                .grants()
                .contains(&crate::contract::capability::tool_call("shell"))
        {
            return Err(Error::Unauthorized(
                "device shell differs from its task authority".into(),
            ));
        }
        context.authorize_content_publication(
            &format!(
                "{}/00000000000000000001-stdout.bin",
                super::output::directory(invocation.operation_id)
            ),
            None,
            context.effective_scope(),
            None,
        )?;
        Ok(())
    }

    fn dispatch(
        &self,
        invocation: &ToolInvocation,
        context: Option<&ToolContext>,
    ) -> Result<Option<Dispatch>> {
        let Some(context) = context else {
            return Ok(None);
        };
        let current = self.devices.borrow();
        let connection = current
            .active
            .then(|| current.statuses.get(&self.name))
            .flatten()
            .filter(|status| status.target.state == TargetState::Online)
            .and_then(|status| {
                status
                    .session
                    .clone()
                    .map(|session| (session, status.generation))
            });
        drop(current);
        let Some((session, generation)) = connection else {
            return Err(self.refusal(Refused::Offline));
        };
        let (agent, volumes) = context.agent().ok_or_else(|| {
            Error::Unauthorized("device dispatch has no original agent binding".into())
        })?;
        let volume = volumes
            .iter()
            .find(|binding| binding.name == "project")
            .ok_or_else(|| {
                Error::Unauthorized("device dispatch has no original project volume".into())
            })?
            .volume
            .clone();
        if self.grant.scope.agent().map(|id| id.into_bytes()) != Some(agent.id().to_bytes()) {
            return Err(Error::Unauthorized(
                "device grant belongs to another agent".into(),
            ));
        }
        let command = RunCommand {
            op: invocation.operation_id,
            call_id: invocation.call_id.clone(),
            agent: agent.clone(),
            volume,
            actor: *blake3::hash(&self.paths.actor(&self.name)?.encode()).as_bytes(),
            target: self.name.clone(),
            session,
            generation,
            deadline_epoch_ms: context.effective_scope().run_limits().deadline_epoch_ms,
            arguments: invocation.arguments.clone(),
        };
        // A malformed call is refused by execution before any run is queued.
        if command.input().is_err() {
            return Ok(None);
        }
        self.grant.verifier.verify(&self.grant.scope, &mut None)?;
        let grant = self
            .grant
            .issuer
            .issue(
                &self.grant.scope,
                command.grant_id()?,
                &Capabilities::new([crate::contract::capability::tool_call("shell")]),
            )?
            .seal(&self.grant.verifier)?;
        let record = encode(&Run {
            v: RECORD_VERSION,
            command,
            grant,
        })?;
        if record.len() > acyclic_stream::MAX_RECORD_BYTES {
            return Err(Error::Invalid(
                "the command does not fit one device run record".into(),
            ));
        }
        Ok(Some(Dispatch {
            records: vec![
                (self.paths.queue(&self.name)?, record.into()),
                self.wake.clone(),
            ],
        }))
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let input: ShellInput = serde_json::from_value(invocation.arguments.clone())
                .map_err(|error| Error::Invalid(format!("shell input decode failed: {error}")))?;
            input.validate()?;
            let context = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("device shell has no original task".into()))?;
            self.outcome(&invocation, context).await
        })
    }

    fn reconcile<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<Option<ToolResult>>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("device shell has no original task".into()))?;
            self.outcome(&invocation, context).await.map(Some)
        })
    }
}

fn agent(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}
