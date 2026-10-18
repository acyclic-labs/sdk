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

pub(super) const MAXIMUM_BYTES: usize = super::jsonl::MAXIMUM_REQUEST_BYTES;
const MAXIMUM_FILES: usize = 4096;
// Request size is a per-file admission bound, independent of retained display.
const OBSERVATION_BYTES: u64 = 4 * 1024 * 1024;

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
    prepare_local_resolution(
        operation,
        &CommandResolution::Prompt {
            text: prompt.into(),
            source: None,
            mentions: Vec::new(),
        },
    )
}

/// Builds this session's immutable request file for an already resolved input.
pub(super) fn prepare_local_resolution(
    operation: OperationId,
    resolution: &CommandResolution,
) -> Result<FileValue> {
    let bytes =
        serde_json::to_vec(resolution).map_err(|error| Error::Invalid(error.to_string()))?;
    let file = FileValue::new(key(operation, TaskFile::Request)?, bytes);
    validate(&file, operation)?;
    Ok(file)
}

fn validate(file: &FileValue, operation: OperationId) -> Result<()> {
    if file.key != key(operation, TaskFile::Request)? || file.channel.is_some() {
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
    initial: bool,
    submitted: bool,
    host: Pin<Box<dyn Future<Output = Result<()>>>>,
    host_done: bool,
    host_error: Option<Error>,
    drain_through: u64,
}

impl LocalFileClient {
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
        Ok(Self {
            host: Box::pin(super::commands::run_local_command_files(
                local.clone(),
                operation,
                true,
            )),
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
        let requested = self.submitted
            || self
                .query
                .file(&key(self.operation, TaskFile::Request)?)
                .is_some();
        if requested && !self.host_done && self.outcome()?.is_none() {
            // The one-request headless owner returns after saving its outcome.
            // Keep polling it until native work and publication have settled.
            let _ = self.host.as_mut().await;
        }
        let Self {
            local, host, query, ..
        } = self;
        drop(host);
        drop(query);
        super::disconnect_local(local).await?;
        outcome
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

    async fn submit(&mut self, file: &FileValue) -> Result<()> {
        validate(file, self.operation)?;
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

    async fn run(
        &mut self,
        operation: OperationId,
        mut input: impl Stream<Item = Result<String>> + Unpin,
        mut input_open: bool,
        output: &mut impl Write,
    ) -> Result<HeadlessOutcome> {
        if operation != self.operation {
            return Err(Error::Unauthorized("headless operation differs".into()));
        }
        self.initial(output)?;
        loop {
            let settled = self.outcome()?;
            if settled.is_none()
                && self.host_done
                && self.query.cursor().position >= self.drain_through
            {
                let error = self
                    .host_error
                    .take()
                    .unwrap_or(Error::Indeterminate(operation));
                return if HeadlessOutcome::from_error(&error) == HeadlessOutcome::Indeterminate {
                    Ok(HeadlessOutcome::Indeterminate)
                } else {
                    Err(error)
                };
            }
            if let Some(outcome) = settled {
                if input_open {
                    match input.next().now_or_never() {
                        Some(Some(line)) => {
                            self.submit(&parse_local_request(&line?, operation)?)
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
            }
            let next = tokio::select! {
                biased;
                line = input.next(), if input_open => Next::Input(line),
                change = self.query.publish_next(async |change| publish(output, change)) => Next::File(change),
                result = &mut self.host, if !self.host_done => Next::Host(result),
            };
            match next {
                Next::Input(Some(line)) => {
                    self.submit(&parse_local_request(&line?, operation)?)
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
                Next::Host(result) => {
                    self.host_done = true;
                    self.host_error = result.err();
                    self.drain_through = self
                        .local
                        .task_files()
                        .inner
                        .snapshot(&selection(operation)?, MAXIMUM_FILES)
                        .await?
                        .cursor
                        .position;
                }
            }
        }
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
        self.submit(&command).await?;
        Box::pin(self.run(operation, futures::stream::empty(), false, output)).await
    }
    async fn input<W: Write>(
        &mut self,
        operation: OperationId,
        input: impl Stream<Item = Result<String>> + Unpin,
        output: &mut W,
    ) -> Result<HeadlessOutcome> {
        Box::pin(self.run(operation, input, true, output)).await
    }
}

#[cfg(all(test, not(acyclic_harness_loom)))]
mod tests;
