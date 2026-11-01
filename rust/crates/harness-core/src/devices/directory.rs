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
use std::sync::Arc;
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
    statuses: BTreeMap<String, TargetStatus>,
    roster: Arc<Roster>,
    offline: Arc<Roster>,
    following: bool,
    active: bool,
}

impl Devices {
    fn roster(&self) -> Arc<Roster> {
        Arc::clone(if self.active {
            &self.roster
        } else {
            &self.offline
        })
    }

    fn changed_roster(
        &self,
        home: &crate::agent::TargetRecord,
        status: &TargetStatus,
    ) -> Result<Option<(Arc<Roster>, Arc<Roster>)>> {
        if self
            .statuses
            .get(&status.target.name)
            .is_some_and(|prior| prior.target == status.target)
        {
            return Ok(None);
        }
        let mut targets = self.roster.targets().to_vec();
        if let Some(prior) = targets
            .iter_mut()
            .find(|target| target.name == status.target.name)
        {
            *prior = status.target.clone();
        } else {
            targets.push(status.target.clone());
        }
        let roster = Arc::new(Roster::new(&home.name, targets.clone()).map_err(agent)?);
        // Prepare producer-loss disclosure at this same admitted change. Drop
        // only selects a retained immutable snapshot; it cannot allocate/fail.
        for target in targets.iter_mut().skip(1) {
            target.state = TargetState::Offline;
        }
        let offline = Arc::new(Roster::new(&home.name, targets).map_err(agent)?);
        Ok(Some((roster, offline)))
    }
}

/// The original follow's lifetime, including cancellation during registration.
struct Presence<'a>(&'a watch::Sender<Devices>);
impl Drop for Presence<'_> {
    fn drop(&mut self) {
        self.0.send_modify(|devices| {
            devices.following = false;
            devices.active = false;
        });
    }
}

/// The targets of an agent tree: its in-process home plus every attached
/// device under one root, as their presence records list them.
///
/// [`Self::follow`] keeps the roster current from the shared presence
/// stream; [`Self::for_agent`] shares that same subscription and roster while
/// retaining each agent's own grant and home workspace.
pub struct TargetDirectory<P: StreamProvider + ?Sized + 'static> {
    provider: Arc<P>,
    sockets: Arc<dyn acyclic_actors::guest::SocketHost>,
    store: Arc<acyclic_actors::local::Store<P>>,
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
        sockets: Arc<dyn acyclic_actors::guest::SocketHost>,
        paths: TargetPaths,
        home: Target,
        grant: DeviceGrant,
    ) -> Result<Self> {
        crate::agent::AgentRecord::check(&home.record).map_err(agent)?;
        let roster =
            Arc::new(Roster::new(&home.record.name, vec![home.record.clone()]).map_err(agent)?);
        let devices = Devices {
            statuses: BTreeMap::new(),
            offline: Arc::clone(&roster),
            roster,
            following: false,
            active: false,
        };
        Ok(Self {
            provider: Arc::clone(store.provider()),
            store: Arc::new(store),
            sockets,
            paths,
            home: Arc::new(home),
            grant: Arc::new(grant),
            devices: watch::Sender::new(devices),
        })
    }

    /// Binds another agent without another presence follow or roster copy.
    /// The host supplies that agent's original home workspace and grant; no
    /// child inherits the parent's selected volume or revocation identity.
    #[must_use]
    pub fn for_agent(&self, grant: DeviceGrant, home: Arc<dyn ToolExecutor>) -> Self {
        Self {
            provider: Arc::clone(&self.provider),
            store: Arc::clone(&self.store),
            sockets: Arc::clone(&self.sockets),
            paths: self.paths.clone(),
            home: Arc::new(Target {
                record: self.home.record.clone(),
                executor: home,
            }),
            grant: Arc::new(grant),
            devices: self.devices.clone(),
        }
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
            if (status.target.state == TargetState::Online) != status.session.is_some()
                || status.session.is_some() != status.socket.is_some()
            {
                return Err(Error::Invalid(
                    "device presence has no matching session".into(),
                ));
            }
            let name = status.target.name.clone();
            // A device named as the home is never listed; it cannot shadow it.
            if name == self.home.record.name {
                continue;
            }
            let changed = self
                .devices
                .borrow()
                .changed_roster(&self.home.record, &status)?;
            self.devices.send_modify(|devices| {
                devices.statuses.insert(name, status);
                if let Some((roster, offline)) = changed {
                    devices.roster = roster;
                    devices.offline = offline;
                }
            });
        }
    }
}

impl<P: StreamProvider + ?Sized + 'static> TargetResolver for TargetDirectory<P> {
    fn roster(&self) -> Arc<Roster> {
        self.devices.borrow().roster()
    }

    fn resolve(&self, target: Option<&str>) -> Result<Arc<Target>> {
        let roster = self.roster();
        let record = roster.select(target).map_err(Error::Target)?;
        if record.name == self.home.record.name {
            return Ok(Arc::clone(&self.home));
        }
        let connection = self
            .devices
            .borrow()
            .statuses
            .get(&record.name)
            .cloned()
            .ok_or_else(|| Error::Invalid("listed device has no original connection".into()))?;
        let socket = connection
            .socket
            .clone()
            .ok_or_else(|| Error::Invalid("online device has no original socket".into()))?;
        let terminal = self
            .sockets
            .web_socket_terminal(
                &self.paths.actor(&record.name)?,
                &acyclic_actors::guest::WebSocketId(socket),
            )
            .map_err(|_| {
                Error::Target(TargetRefusal {
                    target: record.name.clone(),
                    refused: Refused::Offline,
                    online: roster
                        .targets()
                        .iter()
                        .filter(|target| {
                            target.state == TargetState::Online && target.name != record.name
                        })
                        .map(|target| target.name.clone())
                        .collect(),
                })
            })?;
        Ok(Arc::new(Target {
            record: record.clone(),
            executor: Arc::new(DeviceExecutor {
                provider: Arc::clone(&self.provider),
                paths: self.paths.clone(),
                name: record.name.clone(),
                sockets: Arc::clone(&self.sockets),
                connection,
                terminal,
                wake: self
                    .store
                    .activation_record(&self.paths.actor(&record.name)?),
                grant: Arc::clone(&self.grant),
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
    sockets: Arc<dyn acyclic_actors::guest::SocketHost>,
    connection: TargetStatus,
    terminal: Arc<dyn acyclic_actors::guest::SocketTerminal>,
    wake: (acyclic_stream::StreamPath, acyclic_stream::OwnedBytes),
    grant: Arc<DeviceGrant>,
    devices: watch::Receiver<Devices>,
}

impl<P: StreamProvider + ?Sized + 'static> DeviceExecutor<P> {
    fn refusal(&self, refused: Refused) -> Error {
        let roster = self.devices.borrow().roster();
        Error::Target(TargetRefusal {
            target: self.name.clone(),
            refused,
            online: roster
                .targets()
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
        reconcile: bool,
    ) -> Result<ToolResult> {
        // Dispatch publishes no Run for malformed input. Both first execution
        // and recovery must return that verdict before following its outcome.
        let input: ShellInput = serde_json::from_value(invocation.arguments.clone())
            .map_err(|error| Error::Invalid(format!("shell input decode failed: {error}")))?;
        input.validate()?;
        let mut request =
            FollowRequest::new(self.paths.run(&self.name, invocation.operation_id)?, 0);
        if !reconcile {
            request.when_absent = WhenAbsent::Wait;
        }
        let mut socket_end = (!reconcile).then(|| Arc::clone(&self.terminal).completion());
        let mut closed_at = None;
        let opening = self.provider.follow(request);
        tokio::pin!(opening);
        let mut events = tokio::select! {
            result = &mut opening => result.map_err(|error| match error {
                acyclic_stream::StreamError::NotFound if reconcile => Error::Indeterminate(invocation.operation_id),
                cause => cause.into(),
            })?,
            _ = futures::future::OptionFuture::from(socket_end.as_mut()), if socket_end.is_some() => {
                socket_end = None;
                let tail = self.accepted_tail(invocation.operation_id).await?;
                if tail == 0 { return Err(Error::Indeterminate(invocation.operation_id)); }
                closed_at = Some(tail);
                opening.await?
            }
        };
        let mut ended: Option<acyclic_stream::FollowStream> = None;
        let mut output = super::output::Output::new(context);
        let mut next = 0;
        loop {
            if closed_at.is_some_and(|tail| next >= tail) {
                return Err(Error::Indeterminate(invocation.operation_id));
            }
            let update = tokio::select! {
                biased;
                _ = futures::future::OptionFuture::from(socket_end.as_mut()), if socket_end.is_some() => {
                    // The same socket's physical publication producer ended.
                    // Inspect one accepted cut, drain its output/Exit, and never
                    // wait for a marker a refused Close could not publish.
                    socket_end = None;
                    ended = None;
                    closed_at = Some(self.accepted_tail(invocation.operation_id).await?);
                    continue;
                }

                end = futures::future::OptionFuture::from(ended.as_mut().map(futures::StreamExt::next)), if ended.is_some() => {
                    let end = end.flatten().ok_or_else(|| Error::Storage("device session producer ended".into()))??;
                    if let FollowUpdate::Record(record) = end {
                        let RunRecord { event: RunEvent::Lost { .. }, .. } = decode(&record.value)? else {
                            return Err(Error::Invalid("device session has no terminal marker".into()));
                        };
                        // Drain only the accepted prefix observed at session end,
                        // including output preceding Lost or an absent terminal.
                        // An Exit within that cut still wins; exhaustion remains
                        // uncertainty and never waits for another incarnation.
                        closed_at = Some(self.provider.head(self.paths.run(&self.name, invocation.operation_id)?).await?.tail);
                        ended = None;
                    }
                    continue;
                }
                update = events.next() => update.ok_or_else(|| Error::Storage("device run follow ended".into()))?,
            };
            let FollowUpdate::Record(record) = update? else {
                continue;
            };
            if closed_at.is_some_and(|tail| record.sequence >= tail) {
                return Err(Error::Indeterminate(invocation.operation_id));
            }
            next = record
                .sequence
                .checked_add(1)
                .ok_or_else(|| Error::Invalid("device run sequence overflows".into()))?;
            let RunRecord { event, .. } = decode(&record.value)?;
            if let RunEvent::Started {
                socket, generation, ..
            } = event
            {
                if closed_at.is_some() {
                    continue;
                }
                if reconcile {
                    let pin = if self.connection.socket.as_deref() == Some(socket.as_str())
                        && self.connection.generation == generation
                    {
                        Some(Arc::clone(&self.terminal))
                    } else {
                        self.sockets
                            .web_socket_terminal(
                                &self.paths.actor(&self.name)?,
                                &acyclic_actors::guest::WebSocketId(socket),
                            )
                            .ok()
                    };
                    match pin {
                        Some(pin) => socket_end = Some(pin.completion()),
                        None => {
                            closed_at = Some(self.accepted_tail(invocation.operation_id).await?);
                            continue;
                        }
                    }
                }
                let mut request =
                    FollowRequest::new(self.paths.session_end(&self.name, generation)?, 0);
                request.when_absent = WhenAbsent::Wait;
                tokio::select! {
                    result = self.provider.follow(request) => ended = Some(result?),
                    _ = futures::future::OptionFuture::from(socket_end.as_mut()), if socket_end.is_some() => {
                        socket_end = None;
                        closed_at = Some(self.accepted_tail(invocation.operation_id).await?);
                    }
                }
            } else if let RunEvent::Output { stderr, data, .. } = event {
                output
                    .append(context, record.sequence, stderr, &data)
                    .await?;
            } else if let Some(result) = self.terminal(event, invocation) {
                return output.finish(result?);
            }
        }
    }

    async fn accepted_tail(&self, operation: crate::OperationId) -> Result<u64> {
        match self
            .provider
            .head(self.paths.run(&self.name, operation)?)
            .await
        {
            Ok(head) => Ok(head.tail),
            Err(acyclic_stream::StreamError::NotFound) => Ok(0),
            Err(_) => Err(Error::Indeterminate(operation)),
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
            .filter(|status| {
                status.target.state == TargetState::Online
                    && status.session == self.connection.session
                    && status.socket == self.connection.socket
                    && status.generation == self.connection.generation
            })
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
            delivery: (self.paths.queue(&self.name)?, record.into()),
            activation: self.wake.clone(),
        }))
    }

    fn execute<'a>(
        &'a self,
        invocation: ToolInvocation,
        context: Option<ToolContext>,
    ) -> BoxProviderFuture<'a, Result<ToolResult>> {
        Box::pin(async move {
            self.authorize(None, context.as_ref(), &invocation, None)?;
            let context = context
                .as_ref()
                .ok_or_else(|| Error::Unauthorized("device shell has no original task".into()))?;
            self.outcome(&invocation, context, false).await
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
            self.outcome(&invocation, context, true).await.map(Some)
        })
    }
}

fn agent(error: crate::agent::Error) -> Error {
    Error::Invalid(error.to_string())
}
