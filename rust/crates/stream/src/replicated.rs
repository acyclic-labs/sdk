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
    /// Trusted proposal clock. A journal that owns the acceptance clock may
    /// replace it; installation binds that returned stamp to the original plan.
    #[cfg(any(
        all(feature = "local", not(target_arch = "wasm32")),
        all(
            any(feature = "wasm", feature = "wasm-journal"),
            target_arch = "wasm32"
        )
    ))]
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
    #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
    Refused(StreamError),
}

pub(crate) struct Execution {
    pub(crate) outcome: WriteOutcome,
    // Retain the original reply claim even when an adapter only takes its outcome.
    pub(crate) _allocation: crate::memory::Allocation,
}

#[derive(Default)]
struct AttemptResult {
    started: bool,
    appended: Option<AppendIf>,
}

struct Attempt<'a> {
    result: AttemptResult,
    failed: &'a mut bool,
    revisions: &'a AppliedRevisions,
}

impl<'a> Attempt<'a> {
    fn new(failed: &'a mut bool, revisions: &'a AppliedRevisions) -> Self {
        Self {
            result: AttemptResult::default(),
            failed,
            revisions,
        }
    }

    fn finish(mut self) -> AttemptResult {
        std::mem::take(&mut self.result)
    }

    fn persist(
        &mut self,
        journal: &mut impl CommandJournal,
        expected: u64,
        frame: JournalFrame<'_>,
    ) -> impl Future<Output = Result<Option<u64>, StreamError>> {
        async move {
            self.result.started = true;
            let result = journal.append_if(expected, &[frame]).await?;
            let outcome = match &result {
                AppendIf::Accepted {
                    sequence,
                    at_millis,
                } => {
                    if *sequence <= expected {
                        return Err(StreamError::InvalidResponse);
                    }
                    Ok(Some(*at_millis))
                }
                AppendIf::Stale { .. } => Err(StreamError::Unavailable),
                #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
                AppendIf::Refused(error) => Err(error.clone()),
            };
            self.result.appended = Some(result);
            outcome
        }
    }
}

impl Drop for Attempt<'_> {
    fn drop(&mut self) {
        // A dropped durability waiter proves neither refusal nor installation.
        // Retire its original authority before another observer can use its cut.
        if self.result.started {
            *self.failed = true;
            self.revisions.close();
        }
    }
}

/// Storage owns physical ordering, durability and its acceptance clock.
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
}

/// Journal notifications for replicas refreshed by an independent writer.
#[cfg(all(feature = "postgres", not(target_arch = "wasm32")))]
pub(crate) trait JournalNotifications: CommandJournal {
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
impl<T: ReplayObserver + ?Sized> ReplayObserver for &mut T {
    fn observe<'a>(
        &'a mut self,
        provider: &'a MemoryStream,
        command: &'a WriteRequest,
        after: bool,
    ) -> impl Future<Output = Result<(), StreamError>> + ProviderTask + 'a {
        (**self).observe(provider, command, after)
    }
}
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-journal"))]
struct NoopObserver;
#[cfg(any(not(target_arch = "wasm32"), feature = "wasm-journal"))]
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
    stale: bool,
    acceptance_clock: bool,
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
            stale: false,
            acceptance_clock: false,
        }
    }

    /// The port samples and persists its own clock under the append CAS. Its
    /// accepted stamp can bind an original plan without a preceding clock read.
    #[cfg(any(
        feature = "postgres",
        all(test, feature = "local", not(target_arch = "wasm32"))
    ))]
    pub(crate) fn with_acceptance_clock(mut self) -> Self {
        self.acceptance_clock = true;
        self
    }

    #[cfg(any(feature = "local", feature = "postgres"))]
    pub(crate) fn is_failed(&self) -> bool {
        self.failed
    }

    #[cfg(all(
        test,
        any(feature = "local", feature = "postgres"),
        not(target_arch = "wasm32")
    ))]
    pub(crate) fn set_proposal_clock(&self, millis: u64) {
        self.clock.set(millis);
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

    /// A proven stale cut refuses this proposal without invalidating the replica.
    fn complete<T>(&mut self, attempt: AttemptResult, result: &Result<T, StreamError>) -> bool {
        match attempt.appended {
            Some(AppendIf::Stale { head }) if head > self.cursor => {
                self.stale = true;
                return false;
            }
            Some(AppendIf::Accepted {
                sequence,
                at_millis,
            }) if result.is_ok() => {
                self.cursor = sequence;
                self.clock.set(at_millis);
            }
            #[cfg(all(feature = "local", not(target_arch = "wasm32")))]
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
        let mut attempt = Attempt::new(&mut self.failed, &self.provider.revisions);
        let result = plan
            .with_commit_clock(Arc::clone(&self.clock) as _)
            .settle_durable(body, attempt.persist(&mut self.journal, expected, frame))
            .await
            .map(|(outcome, allocation)| Execution {
                outcome: WriteOutcome::Commit(outcome),
                _allocation: allocation,
            });
        let attempt = attempt.finish();
        if !self.complete(attempt, &result) {
            return Err(StreamError::PrefixNotRetained);
        }
        result
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm-journal"))]
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
        } else {
            self.stale = false;
        }
        result
    }

    #[cfg(all(feature = "postgres", not(target_arch = "wasm32")))]
    pub(crate) fn changed(
        &mut self,
    ) -> Result<BoxProviderFuture<'static, Result<(), StreamError>>, StreamError>
    where
        J: JournalNotifications,
    {
        self.check()?;
        Ok(self.journal.changed(self.cursor))
    }

    #[cfg(any(not(target_arch = "wasm32"), feature = "wasm-journal"))]
    pub(crate) fn execute(
        &mut self,
        command: WriteRequest,
        deadline: Option<u64>,
        page: usize,
    ) -> impl Future<Output = Result<Execution, StreamError>> {
        self.execute_with(command, deadline, page, NoopObserver)
    }

    pub(crate) fn execute_with(
        &mut self,
        command: WriteRequest,
        deadline: Option<u64>,
        page: usize,
        mut observe: impl ReplayObserver,
    ) -> impl Future<Output = Result<Execution, StreamError>> {
        async move {
            self.check()?;
            if page == 0 {
                return Err(StreamError::InvalidArgument);
            }
            let canonical = self.provider.canonicalize(command)?;
            let command = &canonical.command;
            let deadline = deadline
                .into_iter()
                .chain(match command {
                    WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => {
                        request.deadline_unix_millis
                    }
                    _ => None,
                })
                .min();
            #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
            let changes = match command {
                WriteRequest::Append(request) => vec![(request.path.clone(), false)],
                WriteRequest::Fork(request) => vec![(request.destination.clone(), true)],
                WriteRequest::Commit(request) | WriteRequest::CommitAccepted(request) => request
                    .mutations
                    .iter()
                    .map(|mutation| {
                        (
                            crate::CommitMutation::path(mutation).clone(),
                            matches!(mutation, CommitMutation::Fork { .. }),
                        )
                    })
                    .collect(),
            };
            let fresh = !self.acceptance_clock || self.stale;
            if fresh {
                self.refresh_with(page, &mut observe).await?;
            }
            observe.observe(&self.provider, command, false).await?;
            let expected = self.cursor;
            let mut attempt = Attempt::new(&mut self.failed, &self.provider.revisions);
            let frame = JournalFrame {
                command,
                #[cfg(any(
                    all(feature = "local", not(target_arch = "wasm32")),
                    all(
                        any(feature = "wasm", feature = "wasm-journal"),
                        target_arch = "wasm32"
                    )
                ))]
                at_millis: self.clock.now_unix_millis(),
                deadline,
                #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
                changes,
            };
            let result = {
                let persist = std::pin::pin!(attempt.persist(&mut self.journal, expected, frame));
                self.provider
                    .apply_durable(&canonical, deadline, persist)
                    .await
            };
            let attempt = attempt.finish();
            // Exact accepted identities and mismatched immutable bindings are
            // authoritative at the retained cut. A local refusal needs one
            // current observation before it can be returned as current state.
            let immutable = !attempt.started
                && matches!(
                    &result,
                    Ok(Execution {
                        outcome: WriteOutcome::Append(AppendOutcome::Committed(_))
                            | WriteOutcome::Fork(_)
                            | WriteOutcome::Commit(CommitOutcome::Committed(_))
                            | WriteOutcome::CommitAccepted(
                                crate::AcceptedCommitOutcome::Committed { .. }
                            ),
                        ..
                    }) | Err(StreamError::IdempotencyMismatch)
                );
            if !attempt.started && !fresh && !immutable {
                self.refresh_with(page, &mut observe).await?;
                if deadline.is_some_and(|deadline| self.clock.now_unix_millis() >= deadline) {
                    return Err(StreamError::DeadlineElapsed);
                }
                if self.cursor != expected {
                    return Err(StreamError::PrefixNotRetained);
                }
            }
            if !self.complete(attempt, &result) {
                return Err(StreamError::PrefixNotRetained);
            }
            if self.cursor > expected {
                observe.observe(&self.provider, command, true).await?;
            }
            result
        }
    }
}
