//! Live per-agent files over the original membership and execution journals.
use super::*;
use acyclic_harness_client::{EventProjection, ExecutionPosition};
use acyclic_stream::{FollowRequest, RecordStream, records_only};
use futures::{FutureExt as _, StreamExt as _, TryStreamExt as _, future::LocalBoxFuture};
use std::{
    future::Future,
    io::{BufWriter, Write as _},
    path::Path,
};
use tokio::sync::watch;

#[cfg(all(test, feature = "filesystem-local"))]
#[path = "transcripts_tests.rs"]
mod tests;

impl Conversation {
    /// Run while projecting each agent's own journal to `<id>.jsonl` in
    /// `directory`. Files are replaced on each invocation and flushed per
    /// record/chunk, using the same version-one envelopes and authorized
    /// payload reads as root event output. No inherited calls are duplicated.
    /// `agents.jsonl` is replaced on each invocation, then appended and flushed
    /// on discovery: version 1, file-stem `id`, usage `agent_path`, parent id
    /// (null for a root).
    /// Membership is followed from creation; each agent retains one bounded
    /// execution page and one stream continuation, with no threads or scans.
    /// After `run` ends, drain the committed frontier of every discovered agent.
    ///
    /// # Errors
    /// Original session, journal, encoding or destination I/O failure.
    pub async fn with_transcripts<T>(
        self: &Arc<Self>,
        directory: &Path,
        operation: &str,
        run: impl Future<Output = T>,
    ) -> Result<T> {
        std::fs::create_dir_all(directory).map_err(failed)?;
        let connection = self.connection.observation().await?;
        let (finished, finish) = watch::channel(false);
        let export = export(connection, directory, operation, finish);
        let work = async {
            let result = run.await;
            finished.send_replace(true);
            result
        };
        let ((), result) = futures::future::try_join(export, async { Ok(work.await) }).await?;
        Ok(result)
    }
}

type Update = Option<(AgentPath, MembershipFollow)>;
type Job<'a> = LocalBoxFuture<'a, Result<Update>>;

async fn export(
    connection: Connection,
    directory: &Path,
    operation: &str,
    finish: watch::Receiver<bool>,
) -> Result<()> {
    let mut agents =
        BufWriter::new(std::fs::File::create(directory.join("agents.jsonl")).map_err(failed)?);
    let jobs = futures::stream::FuturesUnordered::new();
    add(
        &jobs,
        connection.clone(),
        directory,
        &mut agents,
        operation,
        finish.clone(),
    )
    .await?;
    let mut jobs = jobs;
    while let Some(update) = jobs.next().await {
        if let Some((child, follow)) = update? {
            jobs.push(follow.next(finish.clone()).boxed_local());
            let selected = if let Some(host) = &connection._host {
                host.attach(Some(child), false).await?
            } else {
                connection.select(child).await?
            };
            add(
                &jobs,
                selected,
                directory,
                &mut agents,
                operation,
                finish.clone(),
            )
            .await?;
        }
    }
    Ok(())
}

async fn add<'a>(
    jobs: &futures::stream::FuturesUnordered<Job<'a>>,
    connection: Connection,
    directory: &Path,
    agents: &mut BufWriter<std::fs::File>,
    operation: &'a str,
    finish: watch::Receiver<bool>,
) -> Result<()> {
    let path = directory.join(format!("{}.jsonl", connection.root().id()));
    let file = std::fs::File::create(path).map_err(failed)?;
    line(
        agents,
        &serde_json::json!({
            "version": 1,
            "id": connection.root().id(),
            "agent_path": connection.root().as_str(),
            "parent": connection.root().parent().map(|parent| parent.id()),
        }),
    )?;
    let bounded = *finish.borrow();
    let follow = MembershipFollow::open(connection.clone(), 0, bounded).await?;
    jobs.push(follow.next(finish.clone()).boxed_local());
    let conversation = Conversation::open(connection);
    jobs.push(
        async move {
            journal(conversation, file, operation, finish).await?;
            Ok(None)
        }
        .boxed_local(),
    );
    Ok(())
}

struct MembershipFollow {
    connection: Connection,
    records: RecordStream,
    next: u64,
    bounded: bool,
}

impl MembershipFollow {
    async fn open(connection: Connection, next: u64, bounded: bool) -> Result<Self> {
        let mut request = FollowRequest::new(connection.root().membership().map_err(failed)?, next);
        request.until = bounded.then_some(u64::MAX);
        let records = records_only(connection.session.follow(request).await.map_err(failed)?);
        Ok(Self {
            connection,
            records,
            next,
            bounded,
        })
    }

    async fn next(mut self, mut finish: watch::Receiver<bool>) -> Result<Update> {
        loop {
            if !self.bounded && *finish.borrow() {
                self = Self::open(self.connection, self.next, true).await?;
            }
            tokio::select! {
                biased;
                changed = finish.changed(), if !self.bounded => {
                    changed.map_err(failed)?;
                    self = Self::open(self.connection, self.next, true).await?;
                }
                record = self.records.try_next() => {
                    let Some(record) = record.map_err(failed)? else {
                        return if self.bounded {
                            Ok(None)
                        } else {
                            Err(Error::Storage("membership follow ended".into()))
                        };
                    };
                    self.next = record.sequence + 1;
                    let member: crate::agent::Membership =
                        serde_json::from_slice(&record.value).map_err(failed)?;
                    if member.version != 1 || !self.connection.root().contains(&member.agent) {
                        return Err(Error::Invalid("invalid agent membership".into()));
                    }
                    if &member.agent != self.connection.root() {
                        return Ok(Some((member.agent, self)));
                    }
                }
            }
        }
    }
}

async fn journal(
    conversation: Arc<Conversation>,
    file: impl std::io::Write,
    operation: &str,
    mut finish: watch::Receiver<bool>,
) -> Result<()> {
    let mut file = BufWriter::new(file);
    let projection = EventProjection::new(operation.to_owned());
    let mut position = ExecutionPosition::default();
    let mut live = false;
    let mut cut = None;
    loop {
        let finished = *finish.borrow();
        let query = tokio::select! {
            biased;
            changed = finish.changed(), if !finished => {
                changed.map_err(failed)?;
                continue;
            }
            query = conversation.execution_page(position.input, position.after, live && !finished) => query?,
        };
        if finished {
            let end = *cut.get_or_insert(query.inbox_tail);
            if query.input >= end {
                return Ok(());
            }
        }
        let next = query.continuation().map_err(failed)?;
        if let Some(page) = &query.page {
            for record in &page.items {
                let after = if record.sequence == page.end {
                    next
                } else {
                    ExecutionPosition {
                        input: query.input,
                        after: record.sequence,
                    }
                };
                let before = ExecutionPosition {
                    input: query.input,
                    after: record.sequence - 1,
                };
                let fields = crate::executor::ExecutionPayloadField::fields(&record.event);
                let count = crate::executor::ExecutionPayloadField::fields(&record.event).count();
                let complete = if count > 0 { before } else { after };
                line(
                    &mut file,
                    &projection.execution_event(conversation.path(), query.input, record, complete),
                )?;
                for (index, (field, reference)) in fields.enumerate() {
                    let mut offset = 0;
                    loop {
                        let (bytes, length) = conversation
                            .execution_payload(query.input, record.sequence, field, offset)
                            .await?;
                        if length != reference.descriptor().byte_length() {
                            return Err(Error::Invalid("execution payload changed bounds".into()));
                        }
                        let end = offset.checked_add(bytes.len() as u64).ok_or_else(|| {
                            Error::Invalid("execution payload offset overflows".into())
                        })?;
                        let complete = if end == length && index + 1 == count {
                            after
                        } else {
                            before
                        };
                        let event = projection
                            .execution_payload_event(
                                conversation.path(),
                                query.input,
                                record,
                                field,
                                offset,
                                &bytes,
                                complete,
                            )
                            .map_err(failed)?;
                        line(&mut file, &event)?;
                        if end == length {
                            break;
                        }
                        offset = end;
                    }
                }
            }
        }
        position = next;
        if query.page.is_none() && query.settled {
            return Ok(());
        }
        if query.page.is_none()
            || (!query.settled && query.page.as_ref().is_some_and(|page| page.next.is_none()))
        {
            // Only a page fetched after completion can exhaust the final
            // frontier: payload reads above may have yielded while it grew.
            if finished {
                return Ok(());
            }
            live = true;
        }
    }
}

fn line(file: &mut BufWriter<impl std::io::Write>, event: &impl serde::Serialize) -> Result<()> {
    serde_json::to_writer(&mut *file, event).map_err(failed)?;
    file.write_all(b"\n").map_err(failed)?;
    file.flush().map_err(failed)
}
