//! Local headless clients use the same request files and atomic query follow.
use super::{
    ClientQuery, CommandRequest, CommandResolution, HeadlessClient, HeadlessOutcome,
    QueryViewLimits,
};
use crate::{
    Error, OperationId, Result,
    local::LocalHarness,
    task_volume::{Change, Component, FileKey, FileValue, Generation, Query, TaskFile},
};
use futures::{FutureExt, Stream, StreamExt};
use std::{future::Future, io::Write, pin::Pin, sync::Arc};

const MAXIMUM_BYTES: usize = super::jsonl::MAXIMUM_REQUEST_BYTES;
const MAXIMUM_FILES: usize = 4096;
// Request size is a per-file admission bound, independent of retained display.
const OBSERVATION_BYTES: u64 = 4 * 1024 * 1024;
mod approvals;

fn session(operation: OperationId) -> Result<Component> {
    Ok(Component::new(
        uuid::Uuid::from_bytes(operation.into_bytes()).to_string(),
    )?)
}
fn selection(operation: OperationId) -> Result<Query> {
    Ok(Query::TaskSession {
        task: Component::new(crate::runtime::task_id_for_admission(operation).to_string())?,
        session: session(operation)?,
    })
}
fn key(operation: OperationId, make: fn(Component) -> TaskFile) -> Result<FileKey> {
    Ok(FileKey::Session {
        session: session(operation)?,
        file: make(Component::new(operation.to_string())?),
    })
}

/// Builds an immutable prompt request without admitting or executing it.
pub fn prepare_local_prompt(operation: OperationId, prompt: &str) -> Result<FileValue> {
    if prompt.is_empty() {
        return Err(Error::Invalid("empty local prompt".into()));
    }
    let bytes = serde_json::to_vec(&CommandResolution::Prompt {
        text: prompt.into(),
        source: None,
        mentions: Vec::new(),
    })
    .map_err(|error| Error::Invalid(error.to_string()))?;
    let file = FileValue::new(key(operation, TaskFile::Request)?, bytes);
    validate(&file, operation)?;
    Ok(file)
}

fn validate(file: &FileValue, operation: OperationId) -> Result<()> {
    if file.channel.is_some() || file.requester.is_some() || file.publication.is_some() {
        return Err(Error::Unauthorized(
            "stdin cannot supply owner metadata".into(),
        ));
    }
    if let FileKey::Task {
        task,
        file: TaskFile::ApprovalDecision { .. },
    } = &file.key
    {
        uuid::Uuid::parse_str(task.as_str()).map_err(|error| Error::Invalid(error.to_string()))?;
        crate::interaction::ApprovalDecisionFile::decode(&file.bytes, MAXIMUM_BYTES as u64)?;
        return Ok(());
    }
    if file.key != key(operation, TaskFile::Request)? {
        return Err(Error::Unauthorized(
            "stdin request targets another session or execution owner".into(),
        ));
    }
    let mut claim = file.clone();
    claim.key = key(operation, TaskFile::ExecutionClaim)?;
    if file.bytes.len() > MAXIMUM_BYTES
        || crate::task_volume::record_bytes(file)? > acyclic_stream::MAX_RECORD_BYTES
        || crate::task_volume::record_bytes(&claim)? > acyclic_stream::MAX_RECORD_BYTES
    {
        return Err(Error::Invalid("local request exceeds byte bound".into()));
    }
    serde_json::from_slice::<CommandResolution>(&file.bytes)
        .or_else(|_| {
            serde_json::from_slice::<CommandRequest>(&file.bytes)
                .map(|request| CommandResolution::Submit { request })
        })
        .map_err(|error| Error::Invalid(error.to_string()))?;
    Ok(())
}

/// Decodes an existing task-file value, retaining its exact retry identity.
/// Only this connection's caller-owned request file is admissible.
pub fn parse_local_request(line: &str, operation: OperationId) -> Result<FileValue> {
    if line.len() > MAXIMUM_BYTES {
        return Err(Error::Invalid("stdin line exceeds byte bound".into()));
    }
    let file = super::jsonl::parse_request(line)?;
    validate(&file, operation)?;
    Ok(file)
}

/// Local backend for the shared headless runner. Requests themselves are the
/// durable outbox. Views and pending deliveries are disposable provider folds.
pub struct LocalFileClient {
    local: Arc<LocalHarness>,
    operation: OperationId,
    query: ClientQuery,
    approvals: approvals::Events,
    initial: bool,
    submitted: bool,
    host: Pin<Box<dyn Future<Output = Result<()>>>>,
    attachment: Option<futures::channel::oneshot::Sender<()>>,
    host_done: bool,
    host_error: Option<Error>,
    drain_through: u64,
}

impl LocalFileClient {
    // Task metadata can need a coordinator guard held by the paused driver.
    // Poll both futures so that its IO can release that guard.
    async fn while_driving<T>(
        &mut self,
        work: impl Future<Output = T>,
        output: &mut impl Write,
    ) -> Result<T> {
        let host = &mut self.host;
        let host_done = &mut self.host_done;
        let host_error = &mut self.host_error;
        let drain_through = &mut self.drain_through;
        let local = self.local.clone();
        let operation = self.operation;
        self.approvals
            .during(
                async {
                    if *host_done {
                        return Ok(work.await);
                    }
                    futures::pin_mut!(work);
                    match futures::future::select(work, host.as_mut()).await {
                        futures::future::Either::Left((result, _)) => Ok(result),
                        futures::future::Either::Right((result, work)) => {
                            *host_done = true;
                            *host_error = result.err();
                            let result = work.await;
                            *drain_through = local
                                .task_files()
                                .inner
                                .snapshot(&selection(operation)?, MAXIMUM_FILES)
                                .await?
                                .cursor
                                .position;
                            Ok(result)
                        }
                    }
                },
                output,
            )
            .await?
    }

    pub(super) async fn open(local: Arc<LocalHarness>, operation: OperationId) -> Result<Self> {
        let query = ClientQuery::open(
            &local.task_files().inner,
            selection(operation)?,
            None,
            QueryViewLimits {
                files: MAXIMUM_FILES,
                segments: MAXIMUM_FILES,
                bytes: OBSERVATION_BYTES,
            },
        )
        .await?;
        let mut subscription = crate::package::harness::CommandSubscription::start(
            local.clone(),
            &acyclic_stream::runtime::PlatformExecutor,
            operation,
            true,
        );
        let attachment = subscription.take_attachment();
        Ok(Self {
            approvals: approvals::follow(
                local.clone(),
                crate::runtime::task_id_for_admission(operation),
            ),
            host: Box::pin(subscription),
            attachment,
            local,
            operation,
            query,
            initial: true,
            submitted: false,
            host_done: false,
            host_error: None,
            drain_through: 0,
        })
    }

    /// Drain admitted execution before retiring its native mounts, including on
    /// malformed input or a failed output sink. Dropping an awaiting future
    /// cannot join a native capture worker.
    pub async fn finish(mut self, outcome: Result<HeadlessOutcome>) -> Result<HeadlessOutcome> {
        let mut output = std::io::sink();
        let requested = self.submitted
            || self
                .query
                .file(&key(self.operation, TaskFile::Request)?)
                .is_some();
        let mut settlement = Ok(());
        if requested && !self.host_done && self.outcome()?.is_none() && outcome.is_err() {
            // Refused input must still retire a live invocation. Cancellation
            // uses the existing task owner and publishes status to its follows.
            let task = crate::runtime::task_id_for_admission(self.operation);
            let cancel = self.local.dispatch_command(
                OperationId::new(),
                None,
                CommandResolution::Submit {
                    request: CommandRequest::Cancel { task },
                },
            );
            // The driver may hold a coordinator guard across pending IO. Keep
            // polling it while cancellation acquires and updates that owner.
            match self
                .approvals
                .during(
                    futures::future::select(Box::pin(cancel), self.host.as_mut()),
                    &mut output,
                )
                .await?
            {
                futures::future::Either::Left((result, _)) => {
                    if matches!(result, Err(Error::NotFound(_))) {
                        // Admission can still be queued on the runtime owner.
                        // Stop its attachment and join below even without a
                        // task to cancel; an observation is never settlement.
                        drop(self.attachment.take());
                    } else {
                        result?;
                        let _ = self
                            .approvals
                            .during(self.host.as_mut(), &mut output)
                            .await?;
                        self.host_done = true;
                    }
                }
                futures::future::Either::Right((_, cancel)) => {
                    self.host_done = true;
                    if let Err(error) = self.approvals.during(cancel, &mut output).await?
                        && !matches!(error, Error::NotFound(_))
                    {
                        return Err(error);
                    }
                }
            }
        }
        if requested
            && !self.host_done
            && self.outcome()?.is_none()
            && !self.awaiting_approval(&mut output).await?
        {
            // The one-request headless owner returns after saving its outcome.
            // Keep polling it until native work and publication have settled.
            settlement = self
                .approvals
                .during(self.host.as_mut(), &mut output)
                .await?;
            self.host_done = true;
        }
        // Even a published outcome precedes worker teardown. Stop idle follow
        // and join before the process can shut down its executor.
        drop(self.attachment.take());
        if !self.host_done {
            settlement = self
                .approvals
                .during(self.host.as_mut(), &mut output)
                .await?;
        }
        let Self {
            local,
            host,
            query,
            approvals,
            ..
        } = self;
        drop(host);
        drop(query);
        drop(approvals);
        super::disconnect_local(local).await?;
        match outcome {
            Err(error) => Err(error),
            Ok(HeadlessOutcome::Indeterminate)
                if matches!(settlement, Err(Error::Indeterminate(_))) =>
            {
                Ok(HeadlessOutcome::Indeterminate)
            }
            Ok(value) => settlement.map(|()| value),
        }
    }

    fn initial(&mut self, output: &mut impl Write) -> Result<()> {
        if self.initial {
            let snapshot = self.query.snapshot();
            for file in &snapshot.files {
                if matches!(
                    file.key,
                    FileKey::Session {
                        file: TaskFile::ExecutionResult(_),
                        ..
                    }
                ) {
                    let bytes = file
                        .segments
                        .first()
                        .ok_or_else(|| Error::Invalid("outcome has no segment".into()))?;
                    super::command_output::completion(bytes)?;
                }
            }
            emit(
                output,
                super::jsonl::snapshot(&selection(self.operation)?, &snapshot)?,
            )?;
            self.initial = false;
        }
        Ok(())
    }

    #[inline(never)]
    fn submit<'a>(
        &'a mut self,
        file: &'a FileValue,
        output: &'a mut impl Write,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + 'a>> {
        Box::pin(async move {
            validate(file, self.operation)?;
            if matches!(
                file.key,
                FileKey::Task {
                    file: TaskFile::ApprovalDecision { .. },
                    ..
                }
            ) {
                let local = self.local.clone();
                return self
                    .while_driving(
                        local.submit_operation_approval(
                            crate::runtime::task_id_for_admission(self.operation),
                            file,
                        ),
                        output,
                    )
                    .await?;
            }
            // Both -p and JSONL enter through this boundary. Already pinned prompt
            // resolutions are data, so template expansion is never parsed twice.
            let normalized;
            let file = match serde_json::from_slice::<CommandResolution>(&file.bytes) {
                Ok(CommandResolution::Prompt {
                    text,
                    source: None,
                    mentions,
                }) if mentions.is_empty() => {
                    let local = self.local.clone();
                    let resolution = self
                        .while_driving(local.resolve_input(self.operation, &text), output)
                        .await??;
                    let mut captured = file.clone();
                    captured.bytes = serde_json::to_vec(&resolution)
                        .map_err(|error| Error::Invalid(error.to_string()))?
                        .into();
                    normalized = captured;
                    validate(&normalized, self.operation)?;
                    &normalized
                }
                _ => file,
            };
            let resolution = serde_json::from_slice::<CommandResolution>(&file.bytes)
                .or_else(|_| {
                    serde_json::from_slice::<CommandRequest>(&file.bytes)
                        .map(|request| CommandResolution::Submit { request })
                })
                .map_err(|error| Error::Invalid(error.to_string()))?;
            match resolution {
                CommandResolution::Select { .. }
                | CommandResolution::Confirm { .. }
                | CommandResolution::Sdk { .. }
                | CommandResolution::Submit {
                    request:
                        CommandRequest::Quit | CommandRequest::Theme { .. } | CommandRequest::CycleMode,
                } => {
                    return Err(Error::Unsupported(
                        "command requires an interactive client".into(),
                    ));
                }
                _ => {}
            }
            let cursor = self.query.cursor();
            let mut expected = Generation {
                volume: cursor.volume,
                epoch: cursor.epoch,
                position: cursor.position,
                commit: cursor.commit,
            };
            loop {
                match self.local.task_files().write(expected, file).await {
                    Ok(_) => {
                        self.submitted = true;
                        return Ok(());
                    }
                    Err(crate::task_volume::Error::Conflict(next))
                        if next.position > expected.position =>
                    {
                        expected = next
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        })
    }

    async fn awaiting_approval(&mut self, output: &mut impl Write) -> Result<bool> {
        let tasks = self.approvals.tasks()?;
        let local = self.local.clone();
        self.while_driving(
            async move {
                for task in tasks {
                    if local.awaiting_human_approval(task).await? {
                        return Ok(true);
                    }
                }
                Ok(false)
            },
            output,
        )
        .await?
    }

    fn outcome(&self) -> Result<Option<HeadlessOutcome>> {
        self.query
            .file(&key(self.operation, TaskFile::ExecutionResult)?)
            .map(|file| {
                let bytes = file
                    .segments
                    .first()
                    .ok_or_else(|| Error::Invalid("outcome has no segment".into()))?;
                super::command_output::completion(bytes)
            })
            .transpose()
    }

    #[inline(never)]
    fn run<'a>(
        &'a mut self,
        operation: OperationId,
        mut input: impl Stream<Item = Result<String>> + Unpin + 'a,
        mut input_open: bool,
        output: &'a mut impl Write,
    ) -> Pin<Box<dyn Future<Output = Result<HeadlessOutcome>> + 'a>> {
        Box::pin(async move {
            if operation != self.operation {
                return Err(Error::Unauthorized("headless operation differs".into()));
            }
            self.initial(output)?;
            loop {
                let settled = self.outcome()?;
                if settled.is_none() && !input_open && self.awaiting_approval(output).await? {
                    return Ok(HeadlessOutcome::Indeterminate);
                }
                if settled.is_none()
                    && self.host_done
                    && self.query.cursor().position >= self.drain_through
                {
                    let error = self
                        .host_error
                        .take()
                        .unwrap_or(Error::Indeterminate(operation));
                    return if HeadlessOutcome::from_error(&error) == HeadlessOutcome::Indeterminate
                    {
                        Ok(HeadlessOutcome::Indeterminate)
                    } else {
                        Err(error)
                    };
                }
                if let Some(outcome) = settled {
                    if input_open {
                        match input.next().now_or_never() {
                            Some(Some(line)) => {
                                self.submit(&parse_local_request(&line?, operation)?, output)
                                    .await?;
                                continue;
                            }
                            Some(None) | None => {}
                        }
                    }
                    return Ok(outcome);
                }
                let submitted = self.submitted
                    || self
                        .query
                        .file(&key(operation, TaskFile::Request)?)
                        .is_some();
                enum Next {
                    Input(Option<Result<String>>),
                    File(Result<()>),
                    Host(Result<()>),
                    Approval(Option<Result<Box<approvals::Event>>>),
                }
                let next = tokio::select! {
                    biased;
                    line = input.next(), if input_open => Next::Input(line),
                    change = self.query.publish_next(async |change| publish(output, change)) => Next::File(change),
                    approval = self.approvals.next() => Next::Approval(approval),
                    result = &mut self.host, if !self.host_done => Next::Host(result),
                };
                match next {
                    Next::Input(Some(line)) => {
                        self.submit(&parse_local_request(&line?, operation)?, output)
                            .await?
                    }
                    Next::Input(None) => {
                        input_open = false;
                        if !submitted {
                            return Err(Error::NotFound("operation was not submitted".into()));
                        }
                    }
                    Next::File(result) => {
                        result?;
                    }
                    Next::Approval(Some(event)) => (*event?).publish(output)?,
                    Next::Approval(None) => {
                        return Err(Error::Storage("approval tree follow ended".into()));
                    }
                    Next::Host(result) => {
                        self.host_done = true;
                        self.host_error = result.err();
                        let local = self.local.clone();
                        self.drain_through = self
                            .while_driving(
                                local
                                    .task_files()
                                    .inner
                                    .snapshot(&selection(operation)?, MAXIMUM_FILES),
                                output,
                            )
                            .await??
                            .cursor
                            .position;
                    }
                }
            }
        })
    }
}

fn emit(output: &mut impl Write, line: String) -> Result<()> {
    let mut bytes = line.into_bytes();
    bytes.push(b'\n');
    output
        .write_all(&bytes)
        .and_then(|()| output.flush())
        .map_err(|error| Error::Storage(error.to_string()))
}
fn publish(output: &mut impl Write, change: &Change) -> Result<()> {
    for file in change.file.iter().chain(&change.files) {
        if matches!(
            file.key,
            FileKey::Session {
                file: TaskFile::ExecutionResult(_),
                ..
            }
        ) {
            super::command_output::completion(&file.bytes)?;
        }
    }
    emit(output, super::jsonl::change(change)?)
}

impl HeadlessClient for LocalFileClient {
    async fn prompt<W: Write>(
        &mut self,
        command: FileValue,
        operation: OperationId,
        output: &mut W,
    ) -> Result<HeadlessOutcome> {
        if operation != self.operation {
            return Err(Error::Unauthorized("headless operation differs".into()));
        }
        validate(&command, operation)?;
        self.initial(output)?;
        self.submit(&command, output).await?;
        self.run(operation, futures::stream::empty(), false, output)
            .await
    }
    async fn input<W: Write>(
        &mut self,
        operation: OperationId,
        input: impl Stream<Item = Result<String>> + Unpin,
        output: &mut W,
    ) -> Result<HeadlessOutcome> {
        self.run(operation, input, true, output).await
    }
}

#[cfg(test)]
#[cfg(not(acyclic_harness_loom))]
mod tests;
