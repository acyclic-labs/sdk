//! One ordered replica over a conditional command journal.
use crate::{
    journal::{decode_command, replay},
    *,
};
use std::sync::{Arc, Mutex};

#[derive(Default)]
pub(crate) struct ReplicaClock(Mutex<u64>);

impl ReplicaClock {
    pub(crate) fn set(&self, millis: u64) {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = millis;
    }
}
impl UnixMillisClock for ReplicaClock {
    fn now_unix_millis(&self) -> u64 {
        *self
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

pub(crate) struct JournalEntry {
    /// Exclusive storage cursor after this entry; strictly increasing.
    pub(crate) sequence: u64,
    pub(crate) at_millis: u64,
    pub(crate) command: OwnedBytes,
}

pub(crate) struct JournalPage {
    pub(crate) entries: Vec<JournalEntry>,
    /// Head and clock sampled by the same storage read.
    pub(crate) head: u64,
    pub(crate) at_millis: u64,
}

pub(crate) struct JournalFrame<'a> {
    pub(crate) command: &'a WriteRequest,
    /// Trusted proposal clock sampled by the journal read. Installation and
    /// recovery use this same stamp; the port checks deadlines at acceptance.
    pub(crate) at_millis: u64,
    pub(crate) deadline: Option<u64>,
    #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
    pub(crate) changes: Vec<(StreamPath, bool)>,
}

pub(crate) enum AppendIf {
    Accepted {
        sequence: u64,
        at_millis: u64,
    },
    Stale {
        head: u64,
    },
    /// Proven refusal before any durable edit.
    Refused(StreamError),
}

pub(crate) struct Execution {
    pub(crate) outcome: WriteOutcome,
    pub(crate) allocation: crate::memory::Allocation,
}

#[derive(Default)]
struct Attempt {
    started: bool,
    appended: Option<AppendIf>,
}

impl Attempt {
    async fn persist(
        &mut self,
        journal: &mut impl CommandJournal,
        expected: u64,
        frame: JournalFrame<'_>,
    ) -> Result<(), StreamError> {
        self.started = true;
        let stamp = frame.at_millis;
        let result = journal.append_if(expected, &[frame]).await?;
        let outcome = match &result {
            AppendIf::Accepted {
                sequence,
                at_millis,
            } => {
                if *sequence <= expected || *at_millis != stamp {
                    return Err(StreamError::InvalidResponse);
                }
                Ok(())
            }
            AppendIf::Stale { .. } => Err(StreamError::Unavailable),
            AppendIf::Refused(error) => Err(error.clone()),
        };
        self.appended = Some(result);
        outcome
    }
}

/// Storage owns physical ordering, durability, its acceptance clock and events.
/// It never evaluates Stream conditions or invents a replacement retry identity.
pub(crate) trait CommandJournal: ProviderPlatform {
    fn append_if(
        &mut self,
        expected: u64,
        frames: &[JournalFrame<'_>],
    ) -> impl Future<Output = Result<AppendIf, StreamError>> + ProviderTask;
    fn read(
        &mut self,
        from: u64,
        page: usize,
    ) -> impl Future<Output = Result<JournalPage, StreamError>> + ProviderTask;
    /// Registers atomically against `after`; an already advanced head is ready.
    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>>;
}

pub(crate) trait ReplayObserver: ProviderPlatform {
    fn observe<'a>(
        &'a mut self,
        provider: &'a MemoryStream,
        command: &'a WriteRequest,
        after: bool,
    ) -> impl Future<Output = Result<(), StreamError>> + ProviderTask + 'a;
}
struct NoopObserver;
impl ReplayObserver for NoopObserver {
    fn observe<'a>(
        &'a mut self,
        _: &'a MemoryStream,
        _: &'a WriteRequest,
        _: bool,
    ) -> impl Future<Output = Result<(), StreamError>> + ProviderTask + 'a {
        std::future::ready(Ok(()))
    }
}

pub(crate) struct Replicated<J> {
    pub(crate) journal: J,
    pub(crate) provider: MemoryStream,
    pub(crate) cursor: u64,
    clock: Arc<ReplicaClock>,
    failed: bool,
}

impl<J: CommandJournal> Replicated<J> {
    pub(crate) fn new(
        journal: J,
        provider: MemoryStream,
        clock: Arc<ReplicaClock>,
        cursor: u64,
    ) -> Self {
        Self {
            journal,
            provider,
            clock,
            cursor,
            failed: false,
        }
    }

    pub(crate) fn is_failed(&self) -> bool {
        self.failed
    }

    fn check(&self) -> Result<(), StreamError> {
        if self.failed {
            Err(StreamError::Unavailable)
        } else {
            Ok(())
        }
    }

    fn fail(&mut self) {
        self.failed = true;
        self.provider.revisions.close();
    }

    /// Returns false only for a proven losing proposal that can be reconsidered.
    fn complete<T>(&mut self, attempt: Attempt, result: &Result<T, StreamError>) -> bool {
        match attempt.appended {
            Some(AppendIf::Stale { head }) if head > self.cursor => return false,
            Some(AppendIf::Accepted { sequence, .. }) if result.is_ok() => self.cursor = sequence,
            Some(AppendIf::Refused(_)) => {}
            Some(_) => self.fail(),
            None if attempt.started
                && matches!(
                    result,
                    Err(StreamError::Unavailable
                        | StreamError::InvalidResponse
                        | StreamError::Unsupported)
                ) =>
            {
                self.fail()
            }
            None => {}
        }
        true
    }

    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    pub(crate) async fn execute_prepared(
        &mut self,
        command: &WriteRequest,
        plan: crate::memory::PreparedCommit,
        body: crate::AppendRecord,
    ) -> Result<Execution, StreamError> {
        self.refresh(1).await?;
        let expected = self.cursor;
        let WriteRequest::Commit(request) = command else {
            return Err(StreamError::InvalidArgument);
        };
        let frame = JournalFrame {
            command,
            at_millis: self.clock.now_unix_millis(),
            deadline: request.deadline_unix_millis,
        };
        let mut attempt = Attempt::default();
        let result = plan
            .with_commit_clock(Arc::clone(&self.clock) as _)
            .settle_durable(body, attempt.persist(&mut self.journal, expected, frame))
            .await
            .map(|(outcome, allocation)| Execution {
                outcome: WriteOutcome::Commit(outcome),
                allocation,
            });
        if !self.complete(attempt, &result) {
            return Err(StreamError::PrefixNotRetained);
        }
        result
    }

    pub(crate) async fn refresh(&mut self, page: usize) -> Result<(), StreamError> {
        self.refresh_with(page, &mut NoopObserver).await
    }

    /// The observer only derives delivery notifications around canonical replay.
    /// It cannot alter a command, its ordering or its applied cursor.
    pub(crate) async fn refresh_with(
        &mut self,
        page: usize,
        observe: &mut impl ReplayObserver,
    ) -> Result<(), StreamError> {
        self.check()?;
        if page == 0 {
            return Err(StreamError::InvalidArgument);
        }
        let result = async {
            let mut target = None;
            loop {
                let received = self.journal.read(self.cursor, page).await?;
                if received.entries.len() > page || received.head < self.cursor {
                    return Err(StreamError::InvalidResponse);
                }
                let target = *target.get_or_insert(received.head);
                if received.head < target {
                    return Err(StreamError::InvalidResponse);
                }
                if self.cursor == target {
                    self.clock.set(received.at_millis);
                    return Ok(());
                }
                if received.entries.is_empty() {
                    if received.head != self.cursor {
                        return Err(StreamError::InvalidResponse);
                    }
                    self.clock.set(received.at_millis);
                    return Ok(());
                }
                for entry in received.entries {
                    if entry.sequence <= self.cursor
                        || entry.sequence > received.head
                        || entry.sequence > target
                    {
                        return Err(StreamError::InvalidResponse);
                    }
                    let command = decode_command(&entry.command)?;
                    self.clock.set(entry.at_millis);
                    observe.observe(&self.provider, &command, false).await?;
                    // Only accepted commands are journal entries. Any refusal
                    // during replay contradicts that ordered durable history.
                    replay(&self.provider, command.clone()).await?;
                    observe.observe(&self.provider, &command, true).await?;
                    self.cursor = entry.sequence;
                    if self.cursor == target {
                        self.clock.set(received.at_millis);
                        return Ok(());
                    }
                    if self.cursor > target {
                        return Err(StreamError::InvalidResponse);
                    }
                }
            }
        }
        .await;
        if result.is_err() {
            self.fail();
        }
        result
    }

    pub(crate) fn changed(
        &mut self,
    ) -> Result<BoxProviderFuture<'static, Result<(), StreamError>>, StreamError> {
        self.check()?;
        Ok(self.journal.changed(self.cursor))
    }

    pub(crate) async fn execute(
        &mut self,
        command: WriteRequest,
        deadline: Option<u64>,
        page: usize,
    ) -> Result<Execution, StreamError> {
        self.execute_with(command, deadline, page, &mut NoopObserver)
            .await
    }

    pub(crate) async fn execute_with(
        &mut self,
        command: WriteRequest,
        deadline: Option<u64>,
        page: usize,
        observe: &mut impl ReplayObserver,
    ) -> Result<Execution, StreamError> {
        let canonical = self.provider.canonicalize(command)?;
        let command = &canonical.command;
        let deadline = deadline
            .into_iter()
            .chain(match command {
                WriteRequest::Commit(request) => request.deadline_unix_millis,
                _ => None,
            })
            .min();
        #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
        let changes = match command {
            WriteRequest::Append(request) => vec![(request.path.clone(), false)],
            WriteRequest::Fork(request) => vec![(request.destination.clone(), true)],
            WriteRequest::Commit(request) => request
                .mutations
                .iter()
                .map(|mutation| {
                    (
                        crate::request::mutation_path(mutation).clone(),
                        matches!(mutation, CommitMutation::Fork { .. }),
                    )
                })
                .collect(),
        };
        loop {
            self.refresh_with(page, observe).await?;
            observe.observe(&self.provider, command, false).await?;
            let expected = self.cursor;
            let mut attempt = Attempt::default();
            let frame = JournalFrame {
                command,
                at_millis: self.clock.now_unix_millis(),
                deadline,
                #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
                changes: changes.clone(),
            };
            let result = self
                .provider
                .apply_durable(
                    &canonical,
                    deadline,
                    attempt.persist(&mut self.journal, expected, frame),
                )
                .await;
            if !self.complete(attempt, &result) {
                // This is the conditional journal's losing proposal. Its
                // uninstalled plan was dropped; catch up and decide again
                // under the same exact request identity and deadline.
                continue;
            }
            if self.cursor > expected {
                observe.observe(&self.provider, command, true).await?;
            }
            return result;
        }
    }
}
