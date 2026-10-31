//! The device's side: one socket to its actor, over which it is handed the
//! runs it executes and reports how each ended.
use super::actor::target_actor;
use super::records::{
    DeviceMessage, HandoverRecord, MessageRecord, RECORD_VERSION, Run, RunCommand, TargetPaths,
    decode, encode,
};
use crate::{
    Error, OperationId, Result,
    agent::TargetRecord,
    approvals::{ApprovalPolicy, ApprovalRule, Approver, RuleVerdict},
    core::AuthorityVerifier,
    interaction::ApprovalTarget,
    tools::ShellInput,
};
use acyclic_actors::{
    guest::SocketPayload,
    sockets::{SocketFrame, SocketOpen},
};
use acyclic_stream::{BoxProviderFuture, ProviderPlatform};
use futures::{StreamExt as _, stream::FuturesUnordered};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

/// One duplex socket to the device's actor, carrying encoded
/// [`SocketFrame`]s.
pub trait DeviceLink: Send {
    /// Sends one frame, in order with every earlier one.
    fn send(&self, frame: bytes::Bytes) -> BoxProviderFuture<'static, Result<()>>;
    /// The next frame; `None` once the actor's host ended the socket. It is
    /// cancel-safe: dropping it loses no frame.
    fn next(&mut self) -> BoxProviderFuture<'_, Result<Option<bytes::Bytes>>>;
}

#[cfg(feature = "filesystem-local")]
impl DeviceLink for acyclic_stream::session::ActorSocket {
    fn send(&self, frame: bytes::Bytes) -> BoxProviderFuture<'static, Result<()>> {
        let sender = self.sender();
        Box::pin(async move { Ok(sender.send(frame).await?) })
    }

    fn next(&mut self) -> BoxProviderFuture<'_, Result<Option<bytes::Bytes>>> {
        Box::pin(async move { Ok(Self::next(self).await?) })
    }
}

/// Runs one `shell` command on the device: the same native process runner a
/// local `shell` call uses.
pub trait DeviceShell: ProviderPlatform {
    /// Opens the original Machine output stream. Polling follows output credit;
    /// dropping it retains the Machine provider's cancellation and cleanup owner.
    fn start(
        &self,
        op: OperationId,
        input: ShellInput,
        deadline_epoch_ms: Option<u64>,
    ) -> BoxProviderFuture<'_, Result<acyclic_machines::ExecStream>>;
}

#[cfg(feature = "machines")]
impl DeviceShell for crate::machines::MachinesWorkspace {
    fn start(
        &self,
        _op: OperationId,
        input: ShellInput,
        deadline_epoch_ms: Option<u64>,
    ) -> BoxProviderFuture<'_, Result<acyclic_machines::ExecStream>> {
        Box::pin(self.shell_until(input, None, deadline_epoch_ms))
    }
}

/// The device owner's independently authenticated approval machinery.
pub trait DeviceApprover: ProviderPlatform {
    /// Evaluates this exact stage of the owner's policy, using its own human,
    /// model or retained parent. An agent-host approval is not an owner verdict.
    fn decide<'a>(
        &'a self,
        stage: &'a Approver,
        run: &'a RunCommand,
    ) -> BoxProviderFuture<'a, Result<RuleVerdict>>;
}

/// Public trust that verifies each run's grant on the device.
pub struct DeviceAuthority {
    /// Original public authority, including its current trust and producer guard.
    /// A detached snapshot cannot stand in for the owner's validity boundary.
    pub verifier: AuthorityVerifier,
    /// Original attaching invocation's clock and stop, retained by its host.
    /// Event-only contexts refuse deadline narrowing before dispatch.
    pub context: acyclic_auth::operation::Context,
}

/// One connection's identity: the session its socket opens with.
pub struct Attachment {
    session: String,
    open: SocketOpen,
}

impl Attachment {
    /// The encoded open request for the device's socket.
    #[must_use]
    pub fn open(&self) -> bytes::Bytes {
        self.open.encode()
    }

    /// The session this connection runs commands for.
    #[must_use]
    pub fn session(&self) -> &str {
        &self.session
    }
}

/// An attached device: it executes the runs its actor hands it, after
/// checking each run's grant and its owner's execution policy.
pub struct DeviceRunner {
    paths: TargetPaths,
    target: TargetRecord,
    shell: Arc<dyn DeviceShell>,
    authority: DeviceAuthority,
    policy: ApprovalPolicy,
    approver: Option<Arc<dyn DeviceApprover>>,
}

impl DeviceRunner {
    /// Device `target` under `paths`, running commands with `shell` under
    /// its owner's `policy`.
    #[must_use]
    pub fn new(
        paths: TargetPaths,
        target: TargetRecord,
        shell: Arc<dyn DeviceShell>,
        authority: DeviceAuthority,
        policy: ApprovalPolicy,
    ) -> Self {
        Self {
            paths,
            target,
            shell,
            authority,
            policy,
            approver: None,
        }
    }

    /// Uses the owner's approver for human, model and parent stages.
    #[must_use]
    pub fn with_approver(mut self, approver: Arc<dyn DeviceApprover>) -> Self {
        self.approver = Some(approver);
        self
    }

    /// A fresh connection: its socket open request and session. Each
    /// connection has its own session; a run queued for another never runs.
    ///
    /// # Errors
    /// When the device's name is not an actor identity.
    pub fn attach(&self) -> Result<Attachment> {
        let session = OperationId::new()
            .into_bytes()
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        Ok(Attachment {
            open: SocketOpen {
                actor: target_actor(&self.paths, &self.target.name)?,
                subscription: "inbox".into(),
                app_channel: Some(session.clone()),
                subprotocols: Vec::new(),
                headers: Vec::new(),
                resume_after: 0,
                reconnected: false,
            },
            session,
        })
    }

    /// Serves one connection opened with `attachment` until its socket ends.
    /// Runs still in flight then are abandoned: the actor has recorded them
    /// lost, and no run is ever resumed on another connection.
    ///
    /// # Errors
    /// When the actor refuses the socket or the link fails.
    pub async fn serve(&self, attachment: &Attachment, link: &mut dyn DeviceLink) -> Result<()> {
        match link.next().await?.map(|frame| SocketFrame::decode(&frame)) {
            Some(Ok(SocketFrame::Accepted { .. })) => {}
            Some(Ok(SocketFrame::Rejected { status, reason })) => {
                return Err(Error::Unauthorized(format!(
                    "the device's actor refused its socket ({status}): {reason}"
                )));
            }
            Some(Ok(SocketFrame::Close { .. })) | None => return Ok(()),
            Some(_) => return Err(Error::Invalid("device socket opened out of order".into())),
        }
        let mut running = FuturesUnordered::new();
        // One pending report across the connection, rather than a growing
        // outbox or one collector per command. The original stream stops
        // being pulled while this slot and socket upload credit are occupied.
        let (reports, mut reported) = tokio::sync::mpsc::channel(1);
        let mut sending: Option<BoxProviderFuture<'static, Result<()>>> = None;
        // Socket frames preserve queue order. One original sequence fences
        // replay without retaining operation history for the connection.
        let mut handed = None;
        loop {
            tokio::select! {
                frame = link.next() => {
                    let Some(frame) = frame? else {
                        return Ok(());
                    };
                    match SocketFrame::decode(&frame) {
                        Ok(SocketFrame::Message(SocketPayload::Text { text })) => {
                            // Only the actor writes here; anything but a run is no command.
                            let Ok(HandoverRecord { sequence, run, .. }) = decode(text.as_bytes()) else {
                                continue;
                            };
                            if handed.is_none_or(|previous| sequence > previous) {
                                handed = Some(sequence);
                                let reports = reports.clone();
                                running.push(async move {
                                    if let Some(report) = self.handle(attachment.session(), run, &reports).await? {
                                        reports.send(report).await.map_err(|_| Error::Storage("device report consumer ended".into()))?;
                                    }
                                    Ok::<_, Error>(())
                                });
                            }
                        }
                        Ok(SocketFrame::Close { .. }) => return Ok(()),
                        _ => {}
                    }
                }
                result = async { sending.as_mut().expect("guarded pending send").await }, if sending.is_some() => {
                    result?;
                    sending = None;
                }
                Some(message) = reported.recv(), if sending.is_none() => {
                        let text = String::from_utf8(encode(&MessageRecord {
                            v: RECORD_VERSION,
                            message,
                        })?)
                        .map_err(|error| Error::Invalid(error.to_string()))?;
                        sending = Some(link.send(SocketFrame::Message(SocketPayload::Text { text }).encode()));
                }
                Some(result) = running.next(), if !running.is_empty() => {
                    result?;
                }
            }
        }
    }

    /// One handed run: refused, or run once its hand-over is recorded.
    async fn handle(
        &self,
        session: &str,
        run: Run,
        reports: &tokio::sync::mpsc::Sender<DeviceMessage>,
    ) -> Result<Option<DeviceMessage>> {
        let started = AtomicBool::new(false);
        let input = match self.admit(session, &run) {
            Ok(input) => input,
            Err(reason) => {
                return Ok(Some(DeviceMessage::Refused {
                    op: run.command.op,
                    reason,
                }));
            }
        };
        let expiry = self
            .authority
            .verifier
            .expiry(&run.grant)?
            .checked_mul(1000)
            .ok_or_else(|| Error::Invalid("device grant expiry overflows".into()))?;
        let expiry = run
            .command
            .deadline_epoch_ms
            .map_or(expiry, |task| task.min(expiry));
        let budget = match self.authority.context.narrow(
            Some(std::time::Duration::from_millis(u64::from(
                input.timeout_ms()?,
            ))),
            Some(expiry),
        ) {
            Ok(budget) => budget,
            Err(error) => {
                return Ok(Some(DeviceMessage::Refused {
                    op: run.command.op,
                    reason: error.to_string(),
                }));
            }
        };
        let changes = self.authority.verifier.trust_changes();
        let work = budget.run(self.handle_current(
            &run,
            input,
            &started,
            reports,
            budget.deadline_unix_millis(),
        ));
        tokio::pin!(work);
        loop {
            if let Err(error) = budget.check() {
                return Ok(Some(interrupted(
                    run.command.op,
                    &started,
                    error.to_string(),
                )));
            }
            // Registration precedes verification, including after each wake.
            let changed = changes.changed();
            let current = self.authority.verifier.verify(&run.grant, &mut None);
            if let Err(error) = current {
                return Ok(Some(interrupted(
                    run.command.op,
                    &started,
                    error.to_string(),
                )));
            }
            tokio::select! {
                biased;
                change = changed => {
                    if let Err(error) = change {
                        return Ok(Some(interrupted(run.command.op, &started, error.to_string())));
                    }
                }
                result = &mut work => {
                    let result = match result {
                        Ok(result) => result,
                        Err(error) => return Ok(Some(interrupted(run.command.op, &started, error.to_string()))),
                    };
                    // A stop ready in this completing poll wins over the result.
                    return match self.authority.verifier.verify(&run.grant, &mut None) {
                        Ok(()) => result,
                        Err(error) => Ok(Some(interrupted(run.command.op, &started, error.to_string()))),
                    };
                }
            }
        }
    }

    async fn handle_current(
        &self,
        run: &Run,
        input: ShellInput,
        started: &AtomicBool,
        reports: &tokio::sync::mpsc::Sender<DeviceMessage>,
        deadline: u64,
    ) -> Result<Option<DeviceMessage>> {
        let op = run.command.op;
        match self.decide(&run.command).await {
            Ok(()) => {}
            Err(reason) => return Ok(Some(DeviceMessage::Refused { op, reason })),
        }
        // The original actor composition sent this only after Started committed.
        // Approval can wait: recheck the owner immediately before dispatch.
        if let Err(error) = self.authority.verifier.verify(&run.grant, &mut None) {
            return Ok(Some(DeviceMessage::Refused {
                op,
                reason: format!("its grant is no longer current: {error}"),
            }));
        }
        started.store(true, Ordering::Release);
        let mut stream = match self.shell.start(op, input, Some(deadline)).await {
            Ok(stream) => stream,
            Err(error) => {
                return Ok(Some(DeviceMessage::Lost {
                    op,
                    reason: error.to_string(),
                }));
            }
        };
        while let Some(output) = stream.next().await {
            use acyclic_machines::ExecOutput;
            match output {
                Ok(output @ (ExecOutput::Data(_) | ExecOutput::Stderr(_))) => {
                    let (stderr, data) = match output {
                        ExecOutput::Data(data) => (false, data),
                        ExecOutput::Stderr(data) => (true, data),
                        _ => unreachable!("matched channel bytes"),
                    };
                    for chunk in data.chunks(super::records::OUTPUT_CHUNK_BYTES) {
                        reports
                            .send(DeviceMessage::Output {
                                op,
                                stderr,
                                data: chunk.to_vec(),
                            })
                            .await
                            .map_err(|_| Error::Storage("device report consumer ended".into()))?;
                    }
                }
                Ok(ExecOutput::ExitCode(code)) => {
                    return Ok(Some(DeviceMessage::Exit {
                        op,
                        result: serde_json::json!({"success": code == 0, "exit_code": code, "stop": null}),
                    }));
                }
                Ok(ExecOutput::Stopped(stop)) if stop.cleanup_completed => {
                    return Ok(Some(DeviceMessage::Exit {
                        op,
                        result: serde_json::json!({"success": false, "exit_code": null, "stop": crate::machines::MachinesWorkspace::stop_result(stop)}),
                    }));
                }
                Ok(ExecOutput::Stopped(stop)) => {
                    return Ok(Some(DeviceMessage::Lost {
                        op,
                        reason: stop.message,
                    }));
                }
                Err(error) => {
                    return Ok(Some(DeviceMessage::Lost {
                        op,
                        reason: error.to_string(),
                    }));
                }
            }
        }
        Ok(Some(DeviceMessage::Lost {
            op,
            reason: "original Machine output producer ended without a terminal receipt".into(),
        }))
    }

    /// The run's checks on this device: its session, its grant, and the
    /// owner's policy. A refusal's reason is what the model reads.
    fn admit(&self, session: &str, run: &Run) -> std::result::Result<ShellInput, String> {
        let command = &run.command;
        if command.target != self.target.name
            || command.session != session
            || command.actor
                != *blake3::hash(
                    &self
                        .paths
                        .actor(&self.target.name)
                        .map_err(|error| error.to_string())?
                        .encode(),
                )
                .as_bytes()
            || run.grant.agent().map(|id| id.into_bytes()) != Some(command.agent.id().to_bytes())
        {
            return Err("the command was sent to another connection of the device".into());
        }
        let input = command.input().map_err(|error| error.to_string())?;
        let id = command.grant_id().map_err(|error| error.to_string())?;
        self.authority
            .verifier
            .verify(&run.grant, &mut None)
            .map_err(|error| format!("its grant did not verify: {error}"))?;
        if run.grant.id() != id
            || !run
                .grant
                .capabilities()
                .contains(&crate::contract::capability::tool_call("shell"))
        {
            return Err("its grant does not authorize this command".into());
        }
        Ok(input)
    }

    /// The owner's policy, evaluated here. A model or parent decision in the
    /// agent's tree does not substitute for an owner-side decision.
    async fn decide(&self, command: &RunCommand) -> std::result::Result<(), String> {
        let target = ApprovalTarget {
            device: self.target.name.clone(),
            owner: self.target.owner.clone(),
        };
        for approver in &self.policy.approvers {
            match approver {
                Approver::Rules {
                    principal,
                    rules,
                    fallback,
                } => match ApprovalRule::evaluate_target(
                    rules,
                    *fallback,
                    (crate::tools::shell::PLATFORM.name, &command.arguments),
                    Some(&target),
                ) {
                    RuleVerdict::Allow => return Ok(()),
                    RuleVerdict::Deny => return Err(format!("refused by {principal}")),
                    RuleVerdict::Ask => {}
                },
                stage @ (Approver::Human { .. } | Approver::Llm { .. } | Approver::Parent) => {
                    let Some(owner) = &self.approver else {
                        continue;
                    };
                    match owner.decide(stage, command).await {
                        Ok(RuleVerdict::Allow) => return Ok(()),
                        Ok(RuleVerdict::Deny) => return Err("refused by the device's owner".into()),
                        Ok(RuleVerdict::Ask) => {}
                        Err(error) => {
                            return Err(format!("the device's owner could not decide: {error}"));
                        }
                    }
                }
            }
        }
        Err("no approver of the device's owner accepted it".into())
    }
}

fn interrupted(op: OperationId, started: &AtomicBool, reason: String) -> DeviceMessage {
    if started.load(Ordering::Acquire) {
        DeviceMessage::Lost { op, reason }
    } else {
        DeviceMessage::Refused { op, reason }
    }
}
