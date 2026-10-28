//! One ordered replica over a conditional command journal.
use crate::{
    journal::{Command, Outcome, decode_command, replay},
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

pub(crate) struct JournalFrame {
    pub(crate) command: OwnedBytes,
    pub(crate) deadline: Option<u64>,
    #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
    pub(crate) changes: Vec<(StreamPath, bool)>,
}

pub(crate) enum AppendIf {
    Accepted { sequence: u64, at_millis: u64 },
    Stale { head: u64 },
}

/// Storage owns physical ordering, durability, its acceptance clock and events.
/// It never evaluates Stream conditions or invents a replacement retry identity.
#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]
#[cfg_attr(not(target_arch = "wasm32"), async_trait)]
pub(crate) trait CommandJournal: ProviderPlatform {
    async fn append_if(
        &mut self,
        expected: u64,
        frames: Vec<JournalFrame>,
    ) -> Result<AppendIf, StreamError>;
    async fn read(&mut self, from: u64, page: usize) -> Result<JournalPage, StreamError>;
    /// Registers atomically against `after`; an already advanced head is ready.
    fn changed(&mut self, after: u64) -> BoxProviderFuture<'static, Result<(), StreamError>>;
}

pub(crate) trait ReplayObserver: ProviderPlatform {
    fn observe<'a>(
        &'a mut self,
        provider: &'a MemoryStream,
        command: &'a Command,
        after: bool,
    ) -> impl Future<Output = Result<(), StreamError>> + ProviderTask + 'a;
}
struct NoopObserver;
impl ReplayObserver for NoopObserver {
    fn observe<'a>(
        &'a mut self,
        _: &'a MemoryStream,
        _: &'a Command,
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
        command: Command,
        deadline: Option<u64>,
        page: usize,
    ) -> Result<Outcome, StreamError> {
        self.execute_with(command, deadline, page, &mut NoopObserver)
            .await
    }

    pub(crate) async fn execute_with(
        &mut self,
        command: Command,
        deadline: Option<u64>,
        page: usize,
        observe: &mut impl ReplayObserver,
    ) -> Result<Outcome, StreamError> {
        let command = command.validated()?;
        let deadline = deadline
            .into_iter()
            .chain(match &command {
                Command::Commit(request) => request.deadline_unix_millis,
                _ => None,
            })
            .min();
        let encoded = OwnedBytes::from(command.encode_to_vec());
        #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
        let changes = match &command {
            Command::Append(request) => vec![(request.path.clone(), false)],
            Command::Fork(request) => vec![(request.destination.clone(), true)],
            Command::Commit(request) => request
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
            observe.observe(&self.provider, &command, false).await?;
            let expected = self.cursor;
            let mut appended = None;
            let clock = &self.clock;
            let journal = &mut self.journal;
            let frame = JournalFrame {
                command: encoded.clone(),
                deadline,
                #[cfg(all(feature = "wasm-journal", target_arch = "wasm32"))]
                changes: changes.clone(),
            };
            let result = self
                .provider
                .apply_durable(command.clone(), deadline, async {
                    let result = journal.append_if(expected, vec![frame]).await?;
                    match result {
                        AppendIf::Accepted {
                            sequence,
                            at_millis,
                        } => {
                            if sequence <= expected {
                                return Err(StreamError::InvalidResponse);
                            }
                            clock.set(at_millis);
                            appended = Some(AppendIf::Accepted {
                                sequence,
                                at_millis,
                            });
                            Ok(())
                        }
                        AppendIf::Stale { head } => {
                            appended = Some(AppendIf::Stale { head });
                            Err(StreamError::Unavailable)
                        }
                    }
                })
                .await;
            match appended {
                Some(AppendIf::Stale { head }) if head > expected => {
                    // This is the conditional journal's losing proposal. Its
                    // uninstalled plan was dropped; catch up and decide again
                    // under the same exact request identity and deadline.
                    continue;
                }
                Some(AppendIf::Accepted { sequence, .. }) if result.is_ok() => {
                    self.cursor = sequence;
                    observe.observe(&self.provider, &command, true).await?;
                }
                Some(_) => self.fail(),
                None if matches!(
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
            return result;
        }
    }
}
