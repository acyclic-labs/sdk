//! One original-input query and wait mechanism for journal projections.
use super::*;
use crate::executor::{ExecutionEventPage, ExecutionJournal, ModelUsagePage};

trait Projection {
    type Page: acyclic_stream::ProviderTask;

    fn empty(after: u64) -> Self::Page;
    fn end(page: &Self::Page) -> u64;
    fn read<'a>(
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        agent: &'a str,
        after: u64,
    ) -> impl std::future::Future<Output = crate::Result<Self::Page>> + acyclic_stream::ProviderTask + 'a;
}

struct Usage;

impl Projection for Usage {
    type Page = ModelUsagePage;

    fn empty(after: u64) -> Self::Page {
        ModelUsagePage {
            items: Vec::new(),
            end: after,
            next: None,
        }
    }

    fn end(page: &Self::Page) -> u64 {
        page.end
    }

    fn read<'a>(
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        agent: &'a str,
        after: u64,
    ) -> impl std::future::Future<Output = crate::Result<Self::Page>> + acyclic_stream::ProviderTask + 'a
    {
        crate::executor::usage_page(journal, operation, agent, after)
    }
}

struct Events;

impl Projection for Events {
    type Page = ExecutionEventPage;

    fn empty(after: u64) -> Self::Page {
        ExecutionEventPage::empty(after)
    }
    fn end(page: &Self::Page) -> u64 {
        page.end
    }

    fn read<'a>(
        journal: &'a dyn ExecutionJournal,
        operation: OperationId,
        _agent: &'a str,
        after: u64,
    ) -> impl std::future::Future<Output = crate::Result<Self::Page>> + acyclic_stream::ProviderTask + 'a
    {
        crate::executor::event_page(journal, operation, after)
    }
}

impl<P, O> Bound<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// Bounded call admissions and completions from the original private journal.
    /// Quiet pages wait on the same inbox, journal and actor producers as events.
    pub async fn usage_page(
        &self,
        agent: &AgentPath,
        input: u64,
        after: u64,
        wait: bool,
    ) -> Result<(u64, Option<ModelUsagePage>, bool), Error> {
        let (input, _, page, settled) = self
            .journal_page::<Usage>(agent, input, after, wait)
            .await?;
        Ok((input, page, settled))
    }

    /// Bounded original model/tool observations, retaining immutable payload refs.
    /// A short page is not final: `settled` is the original actor's consumed-input
    /// fence with no retained work at or before this input. No Stream grant or
    /// second event producer is exported by this query.
    pub async fn event_page(
        &self,
        agent: &AgentPath,
        input: u64,
        after: u64,
        wait: bool,
    ) -> Result<(u64, u64, Option<ExecutionEventPage>, bool), Error> {
        self.journal_page::<Events>(agent, input, after, wait).await
    }

    /// Reads one bounded private payload selected by its original observation.
    /// Inherited inputs, absent observations and fields fail before disclosure;
    /// the immutable range reader retains the original private grant and policy.
    pub async fn read_event_payload(
        &self,
        agent: &AgentPath,
        input: u64,
        sequence: u64,
        field: crate::executor::ExecutionPayloadField,
        offset: u64,
        maximum: u64,
    ) -> Result<(acyclic_stream::OwnedBytes, u64), Error> {
        let after = sequence
            .checked_sub(1)
            .ok_or(Error::Record("journal observations are one-based"))?;
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let (selected, _, journal) = self.journal_input(&state, input, after, false).await?;
        if selected != input {
            return Err(Error::Record(
                "payload input precedes this agent's adoption",
            ));
        }
        let (journal, operation) =
            journal.ok_or(Error::Record("payload input has no original turn"))?;
        let records = journal
            .replay(operation, after, 1, None)
            .map_err(turn)?
            .await
            .map_err(turn)?;
        let record = records
            .first()
            .filter(|record| record.operation_id == operation && record.sequence == sequence)
            .ok_or(Error::Record("payload observation is absent"))?;
        let reference = field.select(&record.event).ok_or(Error::Record(
            "payload field is absent from this observation",
        ))?;
        let length = reference.descriptor().byte_length();
        let remaining = length.checked_sub(offset).ok_or(Error::Record(
            "payload offset is past the immutable file's end",
        ))?;
        let bytes = journal
            .load_range(
                reference,
                acyclic_fs::ByteRange {
                    offset,
                    length: remaining.min(maximum),
                },
                None,
            )
            .map_err(turn)?
            .await
            .map_err(turn)?;
        Ok((bytes, length))
    }

    /// Queries one original inbox input and a bounded execution journal page.
    /// Adoption's first status supplies the fork cut; inherited inputs are skipped.
    /// Failed turns remain discoverable without another index or publication.
    /// `wait` parks a quiet query on the original inbox/journal/actor producers.
    /// The final flag requires the actor's committed consumed-input fence and
    /// no retained turn, queue or overflow work at or before that input.
    async fn journal_page<Q: Projection>(
        &self,
        agent: &AgentPath,
        input: u64,
        after: u64,
        wait: bool,
    ) -> Result<(u64, u64, Option<Q::Page>, bool), Error> {
        self.admit_path(agent)?;
        let state = self.agents.state(agent).await?;
        let (input, tail, journal) = self.journal_input(&state, input, after, wait).await?;
        if input == tail {
            return Ok((input, tail, None, false));
        }
        loop {
            // Observe settlement before the journal page: a settled actor can
            // only name an execution whose original journal writes preceded it.
            let (settled, actor_cut) = self.agents.input_progress(agent, input).await?;
            let page = if let Some((journal, operation)) = &journal {
                Q::read(journal, *operation, agent.as_str(), after)
                    .await
                    .map_err(turn)?
            } else {
                Q::empty(after)
            };
            if !wait || settled || Q::end(&page) > after {
                return Ok((input, tail, Some(page), settled));
            }
            // Both continuations start at their original observations, so an
            // append between the page and this wait remains visible. No task,
            // timer or status recheck produces readiness.
            let actor_changed = self.journal_record(actor_cut);
            if let Some((journal, operation)) = &journal {
                let journal_changed =
                    self.journal_record(journal.follow(*operation, Q::end(&page)).map_err(turn)?);
                tokio::select! {
                    result = journal_changed => result?,
                    result = actor_changed => result?,
                };
            } else {
                actor_changed.await?;
            }
        }
    }

    /// Resolves the original inbox owner once for every journal projection.
    /// The same adoption, input, turn and private-grant proof also governs
    /// bounded payload reads; no caller-supplied file or turn is authority.
    async fn journal_input(
        &self,
        state: &crate::agent::AgentState,
        input: u64,
        after: u64,
        wait: bool,
    ) -> Result<
        (
            u64,
            u64,
            Option<(
                super::super::FilesystemExecutionJournal<P, StreamAuthorityStore<P>, O>,
                OperationId,
            )>,
        ),
        Error,
    > {
        let agent = &state.path;
        use crate::agent::AgentRecord as _;
        use futures::TryStreamExt as _;
        let provider = self.agents.provider();
        let mut statuses = provider
            .read(acyclic_stream::ReadRequest {
                key: None,
                path: agent.status()?,
                from: 0,
                limit: 1,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let status = statuses
            .try_next()
            .await
            .map_err(Error::Stream)?
            .filter(|record| record.sequence == 0)
            .ok_or(Error::Record("journal adoption status is missing"))?;
        let adopted = crate::agent::StatusRecord::decode(&status.value)?.cursor;
        if input < adopted && after != 0 {
            return Err(Error::Record(
                "journal cursor precedes this agent's adoption",
            ));
        }
        let input = input.max(adopted);
        let tail = crate::agent::inbox_tail(&**provider, agent).await?;
        if input > tail {
            return Err(Error::Record(
                "journal input cursor is past the committed tail",
            ));
        }
        let record = if input == tail {
            if after != 0 {
                return Err(Error::Record("journal cursor names an absent input"));
            }
            if !wait {
                return Ok((input, tail, None));
            }
            let mut request = acyclic_stream::FollowRequest::new(agent.inbox()?, input);
            request.when_absent = acyclic_stream::WhenAbsent::Wait;
            loop {
                let (_, actor_cut) = self.agents.input_progress(agent, input).await?;
                tokio::select! {
                    record = self.journal_record(request.clone()) => break Some(record?),
                    changed = self.journal_record(actor_cut) => { changed?; }
                }
            }
        } else {
            let mut records = provider
                .read(acyclic_stream::ReadRequest {
                    key: None,
                    path: agent.inbox()?,
                    from: input,
                    limit: 1,
                    at_least: Default::default(),
                })
                .await
                .map_err(Error::Stream)?;
            records.try_next().await.map_err(Error::Stream)?
        };
        let record = record
            .filter(|record| record.sequence == input)
            .ok_or(Error::Record("committed journal input is missing"))?;
        // A wait at the former tail may have admitted its next original input.
        // Retain this opening cut without another head request.
        let tail = tail.max(
            input
                .checked_add(1)
                .ok_or(Error::Record("journal input overflows"))?,
        );
        let record = crate::agent::InboxRecord::decode(&record.value)?;
        let queued = match record {
            crate::agent::InboxRecord::ForkAdopted(adoption) => {
                if input != adopted
                    || adoption.forked_at != input
                    || adoption.agent != agent.id()
                    || adoption.parent != agent.parent()
                {
                    return Err(Error::Record("journal adoption differs"));
                }
                adoption.brief.map(|brief| crate::agent::Queued {
                    inbox: input,
                    input: crate::agent::TurnInput::Brief {
                        brief: Box::new(brief),
                    },
                })
            }
            record => crate::agent::turn_input(input, record),
        };
        let journal = if let Some(queued) = queued {
            Some((
                self.turns.query_journal(state).await.map_err(turn)?,
                crate::agent::turn_id(state, &queued.input)?,
            ))
        } else {
            if after != 0 {
                return Err(Error::Record(
                    "journal cursor names an input without a turn",
                ));
            }
            None
        };
        Ok((input, tail, journal))
    }

    /// Every query wait names an original producer. A closed producer cannot
    /// become an empty successful page or leave its consumer hanging.
    async fn journal_record(
        &self,
        request: acyclic_stream::FollowRequest,
    ) -> Result<acyclic_stream::Record, Error> {
        use futures::TryStreamExt as _;
        let updates = self
            .agents
            .provider()
            .follow(request)
            .await
            .map_err(Error::Stream)?;
        acyclic_stream::records_only(updates)
            .try_next()
            .await
            .map_err(Error::Stream)?
            .ok_or(Error::Stream(acyclic_stream::StreamError::Unavailable))
    }
}
