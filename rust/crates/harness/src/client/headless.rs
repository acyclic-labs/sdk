//! Headless policy and stdin dispatch belong to the harness, not its CLI wrappers.

use super::{ClientStateStore, HarnessClient, jsonl};
use crate::{Error, Result, wire};
use acyclic_stream::BoxProviderStream;
use futures::{FutureExt, Stream, StreamExt};
use std::io::Write;

/// Settled task outcome with stable process exit codes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HeadlessOutcome {
    /// The task completed, or is idle awaiting another prompt.
    Completed,
    /// Execution failed.
    Failed,
    /// Cancellation settled.
    Cancelled,
    /// Execution needs external input or approval.
    Blocked,
    /// The service cannot establish whether an operation completed.
    Indeterminate,
}

impl HeadlessOutcome {
    /// Stable CLI contract: completed=0, failed=1, cancelled=2, blocked=3, indeterminate=4.
    #[must_use]
    pub const fn exit_code(self) -> u8 {
        match self {
            Self::Completed => 0,
            Self::Failed => 1,
            Self::Cancelled => 2,
            Self::Blocked => 3,
            Self::Indeterminate => 4,
        }
    }
}

fn emit(output: &mut impl Write, frame: wire::server_frame::Frame) -> Result<()> {
    emit_frame(output, &wire::ServerFrame { frame: Some(frame) })
}

fn emit_frame(output: &mut impl Write, frame: &wire::ServerFrame) -> Result<()> {
    let mut line = jsonl::server_frame(frame)?;
    line.push('\n');
    output
        .write_all(line.as_bytes())
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))
}

fn completion(status: &wire::OperationStatus) -> Option<HeadlessOutcome> {
    match wire::CompletionState::try_from(status.state).ok()? {
        wire::CompletionState::Succeeded => Some(HeadlessOutcome::Completed),
        wire::CompletionState::Failed => Some(HeadlessOutcome::Failed),
        wire::CompletionState::Cancelled => Some(HeadlessOutcome::Cancelled),
        wire::CompletionState::Indeterminate => Some(HeadlessOutcome::Indeterminate),
        _ => None,
    }
}

/// Submits and streams every response as service JSONL until this task settles.
pub async fn run_headless<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    command: wire::CommandEnvelope,
    target: wire::ObserveRequest,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    crate::wire_api::validate_observe_request(&target)?;
    if command
        .operation
        .as_ref()
        .is_none_or(|identity| identity.operation_id != target.operation_id)
        || command.authority != target.owner
    {
        return Err(Error::Invalid(
            "headless command and observe target differ".into(),
        ));
    }
    emit(
        output,
        wire::server_frame::Frame::Handshake(client.handshake().clone()),
    )?;
    client.queue(command)?;
    let mut outcome = None;
    client
        .flush_outbox(|admission| {
            let target_admission = admission
                .operation
                .as_ref()
                .is_some_and(|identity| identity.operation_id == target.operation_id);
            let state = admission.state;
            emit(output, wire::server_frame::Frame::Admission(admission))?;
            if target_admission || state == wire::AdmissionState::Indeterminate as i32 {
                outcome = admission_outcome(state);
            }
            Ok(())
        })
        .await?;
    if let Some(outcome) = outcome {
        return Ok(outcome);
    }
    client.replay().await?;
    follow(client, &target, futures::stream::empty(), output, true).await
}

/// Reads wire command frames from JSONL stdin while continuing to stream replay.
/// EOF closes command input; it does not cancel a running task.
pub async fn run_jsonl<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    target: wire::ObserveRequest,
    input: impl Stream<Item = Result<String>> + Unpin,
    output: &mut impl Write,
) -> Result<HeadlessOutcome> {
    crate::wire_api::validate_observe_request(&target)?;
    emit(
        output,
        wire::server_frame::Frame::Handshake(client.handshake().clone()),
    )?;
    let mut admitted = false;
    let mut outcome = None;
    client
        .flush_outbox(|admission| {
            if admission
                .operation
                .as_ref()
                .is_some_and(|identity| identity.operation_id == target.operation_id)
            {
                admitted = admission.state == wire::AdmissionState::Accepted as i32;
                outcome = admission_outcome(admission.state);
            }
            if admission.state == wire::AdmissionState::Indeterminate as i32 {
                outcome = Some(HeadlessOutcome::Indeterminate);
            }
            emit(output, wire::server_frame::Frame::Admission(admission))
        })
        .await?;
    if let Some(outcome) = outcome {
        return Ok(outcome);
    }
    client.replay().await?;
    follow(client, &target, input, output, admitted).await
}

fn admission_outcome(state: i32) -> Option<HeadlessOutcome> {
    match wire::AdmissionState::try_from(state).ok()? {
        wire::AdmissionState::Rejected => Some(HeadlessOutcome::Failed),
        wire::AdmissionState::Indeterminate => Some(HeadlessOutcome::Indeterminate),
        _ => None,
    }
}

enum Incoming {
    Command(Option<Result<String>>),
    Delivery(Result<Option<wire::Delivery>>),
    Status(Option<Result<wire::OperationStatus>>),
    Settled(Result<HeadlessOutcome>),
}

async fn follow<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    target: &wire::ObserveRequest,
    mut input: impl Stream<Item = Result<String>> + Unpin,
    output: &mut impl Write,
    mut admitted: bool,
) -> Result<HeadlessOutcome> {
    let mut input_open = true;
    let mut replay_ended = false;
    let mut replay_attempt = None;
    let mut status = None;
    let mut updates = subscribe(client, target, admitted).await?;
    loop {
        let settled = settlement(
            client,
            target,
            status.as_ref(),
            updates.is_none(),
            input_open,
            admitted,
            replay_ended,
        );
        let incoming = next_incoming(
            client,
            &mut updates,
            &mut input,
            input_open,
            replay_ended,
            settled,
        )
        .await;
        match incoming {
            Incoming::Settled(result) => return result,
            Incoming::Status(Some(change)) => {
                let change = change?;
                let frame = wire::ServerFrame {
                    frame: Some(wire::server_frame::Frame::Status(change)),
                };
                emit_frame(output, &frame)?;
                if let Some(wire::server_frame::Frame::Status(change)) = frame.frame {
                    status = Some(change);
                }
            }
            Incoming::Status(None) => updates = None,
            Incoming::Command(Some(line)) => {
                let frame = jsonl::parse_client_frame(&line?)?;
                let resume = matches!(frame.frame, Some(wire::client_frame::Frame::Resume(_)));
                if let Some(state) = dispatch(client, frame, target, output).await? {
                    match state {
                        wire::AdmissionState::Rejected => return Ok(HeadlessOutcome::Failed),
                        wire::AdmissionState::Indeterminate => {
                            return Ok(HeadlessOutcome::Indeterminate);
                        }
                        wire::AdmissionState::Accepted => {
                            admitted = true;
                            // Admission invalidates a pre-submission snapshot even before EOF.
                            client.replay().await?;
                            replay_ended = false;
                            replay_attempt = None;
                        }
                        wire::AdmissionState::Unspecified => {
                            return Err(Error::Conflict("unspecified admission".into()));
                        }
                    }
                    if updates.is_none() {
                        updates = subscribe(client, target, admitted).await?;
                    }
                }
                if resume {
                    replay_ended = false;
                    replay_attempt = None;
                }
            }
            Incoming::Command(None) => input_open = false,
            Incoming::Delivery(delivery) => match delivery? {
                Some(delivery) => {
                    // A complete validated frame reaches stdout before its cursor commits.
                    // Neither stdin EOF nor a status update can cancel commit/acknowledgement.
                    client
                        .publish_delivery(delivery, |frame| emit_frame(output, frame))
                        .await?;
                }
                None => replay_ended = true,
            },
        }
        continue_replay(
            client,
            status.as_ref(),
            &mut replay_ended,
            &mut replay_attempt,
        )
        .await?;
    }
}

async fn continue_replay<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    status: Option<&wire::OperationStatus>,
    ended: &mut bool,
    attempt: &mut Option<(u64, u64)>,
) -> Result<()> {
    if *ended && let Some(status) = status {
        let position = replay_position(client, status)?;
        // A finite page may end before the observed cut. Retry only after cursor
        // progress or a newer cut, never by repeatedly fetching the same empty page.
        if position.0 < position.1 && *attempt != Some(position) {
            *attempt = Some(position);
            client.replay().await?;
            *ended = false;
        }
    }
    Ok(())
}

fn settlement<S: ClientStateStore>(
    client: &HarnessClient<S>,
    target: &wire::ObserveRequest,
    status: Option<&wire::OperationStatus>,
    updates_ended: bool,
    input_open: bool,
    admitted: bool,
    replay_ended: bool,
) -> Result<Option<HeadlessOutcome>> {
    if let Some(current) = status {
        let caught_up = reached(client, current)?;
        if caught_up && !client.has_buffered_delivery() {
            if let Some(outcome) = completion(current) {
                return Ok(Some(outcome));
            }
            if target
                .owner
                .as_ref()
                .is_some_and(|owner| client.session().waiting(owner, &target.operation_id))
            {
                return Ok(Some(HeadlessOutcome::Blocked));
            }
            if updates_ended {
                return Err(indeterminate(target)?);
            }
        }
        if !caught_up && replay_ended && !client.has_buffered_delivery() {
            return Err(indeterminate(target)?);
        }
    } else if updates_ended && !input_open {
        return Err(if admitted {
            indeterminate(target)?
        } else {
            Error::NotFound("operation was not submitted".into())
        });
    }
    Ok(None)
}

async fn next_incoming<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    updates: &mut Option<BoxProviderStream<'static, Result<wire::OperationStatus>>>,
    input: &mut (impl Stream<Item = Result<String>> + Unpin),
    input_open: bool,
    replay_ended: bool,
    settled: Result<Option<HeadlessOutcome>>,
) -> Incoming {
    let change = async {
        match updates.as_mut() {
            Some(stream) => stream.next().await,
            None => futures::future::pending().await,
        }
    }
    .fuse();
    let command = async {
        if input_open {
            input.next().await
        } else {
            futures::future::pending().await
        }
    }
    .fuse();
    let delivery = async {
        if !replay_ended || client.has_buffered_delivery() {
            client.read_delivery().await
        } else {
            futures::future::pending().await
        }
    }
    .fuse();
    let settled = async {
        match settled {
            Ok(Some(outcome)) => Ok(outcome),
            Err(error) => Err(error),
            Ok(None) => futures::future::pending().await,
        }
    }
    .fuse();
    futures::pin_mut!(change, command, delivery, settled);
    futures::select_biased! {
        status = change => Incoming::Status(status),
        line = command => Incoming::Command(line),
        frame = delivery => Incoming::Delivery(frame),
        // Drain every ready source before accepting a settlement boundary.
        result = settled => Incoming::Settled(result),
    }
}

async fn subscribe<S: ClientStateStore>(
    client: &HarnessClient<S>,
    target: &wire::ObserveRequest,
    admitted: bool,
) -> Result<Option<BoxProviderStream<'static, Result<wire::OperationStatus>>>> {
    match client.operation_updates(target.clone()).await {
        Ok(stream) => Ok(Some(stream)),
        Err(Error::NotFound(_)) if !admitted => Ok(None),
        Err(error) => Err(error),
    }
}

fn indeterminate(target: &wire::ObserveRequest) -> Result<Error> {
    Ok(Error::Indeterminate(crate::OperationId::parse(
        &target.operation_id,
    )?))
}

fn reached<S: ClientStateStore>(
    client: &HarnessClient<S>,
    status: &wire::OperationStatus,
) -> Result<bool> {
    let (cursor, through) = replay_position(client, status)?;
    Ok(cursor >= through)
}

fn replay_position<S: ClientStateStore>(
    client: &HarnessClient<S>,
    status: &wire::OperationStatus,
) -> Result<(u64, u64)> {
    let through = status
        .replay_through
        .as_ref()
        .ok_or_else(|| Error::Conflict("operation status lacks a replay boundary".into()))?;
    let authority = through
        .authority
        .as_ref()
        .ok_or_else(|| Error::Conflict("operation replay boundary lacks an authority".into()))?;
    let cursor = client
        .session()
        .cursor(authority)
        .ok_or_else(|| Error::Conflict("operation replay boundary is not tracked".into()))?;
    if through.authority != status.owner || through.generation != cursor.generation {
        return Err(Error::Conflict(
            "operation replay boundary identity mismatch".into(),
        ));
    }
    Ok((
        client.session().replay_revision(authority),
        through.revision,
    ))
}

async fn dispatch<S: ClientStateStore>(
    client: &mut HarnessClient<S>,
    frame: wire::ClientFrame,
    target: &wire::ObserveRequest,
    output: &mut impl Write,
) -> Result<Option<wire::AdmissionState>> {
    use wire::client_frame::Frame;
    let mut admission = None;
    let response = match frame
        .frame
        .ok_or_else(|| Error::Invalid("empty input frame".into()))?
    {
        Frame::Command(command) => {
            let is_target = command
                .operation
                .as_ref()
                .is_some_and(|identity| identity.operation_id == target.operation_id);
            if is_target && command.authority != target.owner {
                return Err(Error::Invalid("headless command owner differs".into()));
            }
            client
                .submit(command, |response| {
                    if response
                        .operation
                        .as_ref()
                        .is_some_and(|operation| operation.operation_id == target.operation_id)
                    {
                        admission = Some(
                            wire::AdmissionState::try_from(response.state)
                                .map_err(|error| Error::Conflict(error.to_string()))?,
                        );
                    }
                    emit(output, wire::server_frame::Frame::Admission(response))
                })
                .await?;
            return Ok(admission);
        }
        Frame::Observe(request) => {
            wire::server_frame::Frame::Status(client.observe(request).await?)
        }
        Frame::Cancel(request) => {
            wire::server_frame::Frame::Cancellation(client.cancel(request).await?)
        }
        Frame::Handshake(request) => {
            crate::wire_api::negotiate(
                &request,
                client
                    .handshake()
                    .supported
                    .as_ref()
                    .ok_or_else(|| Error::Conflict("missing handshake capabilities".into()))?,
            )?;
            wire::server_frame::Frame::Handshake(client.handshake().clone())
        }
        Frame::Resume(request) => {
            crate::wire_api::validate_resume_protocol(&request)?;
            for cursor in request.cursors {
                let existing = client
                    .session()
                    .resume()
                    .cursors
                    .into_iter()
                    .find(|saved| saved.authority == cursor.authority);
                if let Some(saved) = existing {
                    if saved != cursor {
                        return Err(Error::Conflict(
                            "stdin resume differs from durable cursor".into(),
                        ));
                    }
                } else {
                    client.track(cursor)?;
                }
            }
            client.replay().await?;
            return Ok(None);
        }
        Frame::Acknowledge(ack) => {
            if !client.session().resume().cursors.iter().any(|cursor| {
                cursor.authority == ack.authority
                    && cursor.generation == ack.generation
                    && cursor.revision >= ack.through_revision
            }) {
                return Err(Error::Conflict(
                    "stdin acknowledgement exceeds durable cursor".into(),
                ));
            }
            return Ok(None);
        }
    };
    emit(output, response)?;
    Ok(admission)
}
