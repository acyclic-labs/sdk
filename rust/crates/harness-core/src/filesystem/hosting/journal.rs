//! One original-input query and wait mechanism for journal projections.
use super::*;
use crate::executor::{ExecutionEventPage, ExecutionJournal, ModelUsagePage};

type OriginalInput<P, O> = (
    u64,
    u64,
    Option<(
        super::super::FilesystemExecutionJournal<P, StreamAuthorityStore<P>, O>,
        OperationId,
        Option<crate::agent::IntentId>,
    )>,
    bool,
    u64,
);

#[cfg(all(test, not(target_arch = "wasm32")))]
#[path = "journal_tests.rs"]
pub(in crate::filesystem) mod tests;

trait Projection {
    type Page: acyclic_stream::ProviderTask;
    const CONTEXT_WINDOW: bool = false;

    fn empty(after: u64) -> Self::Page;
    fn end(page: &Self::Page) -> u64;
    fn context_changed(_page: &mut Self::Page) {}
    fn metadata_changed(_observed: Option<u64>, _open_children: u64) -> bool {
        false
    }
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
    const CONTEXT_WINDOW: bool = true;

    fn empty(after: u64) -> Self::Page {
        ModelUsagePage {
            items: Vec::new(),
            tools: Default::default(),
            waits: Default::default(),
            context: None,
            end: after,
            next: None,
        }
    }

    fn metadata_changed(observed: Option<u64>, open_children: u64) -> bool {
        observed != Some(open_children)
    }

    fn end(page: &Self::Page) -> u64 {
        page.end
    }

    fn context_changed(page: &mut Self::Page) {
        // A settled manual rewrite has no later worker admission to measure.
        // Retire its old occupancy until the next request verifies a new bound.
        page.context = Some(crate::executor::ContextUsage::Unavailable);
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
    /// Page completion measurements referenced by an exact original status.
    /// Rechecks tree admission before reading the original completion range.
    pub async fn child_wakes(
        &self,
        agent: &AgentPath,
        status: u64,
        after: Option<u64>,
        limit: usize,
    ) -> Result<Vec<crate::agent::ChildWake>, Error> {
        use crate::agent::AgentRecord as _;
        use futures::TryStreamExt as _;
        self.admit_path(agent)?;
        let provider = self.agents.provider();
        let mut records = provider
            .read(acyclic_stream::ReadRequest {
                path: agent.status()?,
                from: status,
                limit: 1,
                key: None,
                at_least: Default::default(),
            })
            .await
            .map_err(Error::Stream)?;
        let record = records
            .try_next()
            .await
            .map_err(Error::Stream)?
            .filter(|record| record.sequence == status)
            .ok_or(Error::Record("status measurement cut is missing"))?;
        match crate::agent::StatusRecord::decode(&record.value)?.child_wakes {
            Some(wakes) if status.checked_add(1).is_some_and(|end| wakes.until <= end) => {
                wakes.page(provider.clone(), agent, after, limit).await
            }
            Some(_) => Err(Error::Record(
                "completion range exceeds its original status",
            )),
            None => Ok(Vec::new()),
        }
    }

    /// Bounded call admissions and completions from the original private journal.
    /// Quiet pages wait on the same inbox, journal and actor producers as events.
    pub async fn usage_page(
        &self,
        agent: &AgentPath,
        input: u64,
        after: u64,
        wait: bool,
        observed_children: Option<u64>,
    ) -> Result<(u64, Option<ModelUsagePage>, bool, u64), Error> {
        let (input, _, page, settled, open_children) = self
            .journal_page::<Usage>(agent, input, after, wait, observed_children)
            .await?;
        Ok((input, page, settled, open_children))
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
        let (input, tail, page, settled, _) = self
            .journal_page::<Events>(agent, input, after, wait, None)
            .await?;
        Ok((input, tail, page, settled))
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
        let (selected, _, journal, _, _) = self
            .journal_input::<Events>(&state, input, after, false, None)
            .await?;
        if selected != input {
            return Err(Error::Record(
                "payload input precedes this agent's adoption",
            ));
        }
        let (journal, operation, _) =
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
        if field == crate::executor::ExecutionPayloadField::AdmittedRequest {
            // Reconstructed and verified against its digest on each read;
            // paging re-reads the request's segments per page.
            let request =
                crate::executor::ExecutionPayloadField::admitted_request(&journal, &record.event)
                    .await
                    .map_err(turn)?;
            let bytes = request.bytes();
            let page = usize::try_from(offset)
                .ok()
                .and_then(|start| bytes.get(start..))
                .ok_or(Error::Record("payload offset is past the request's end"))?;
            let take =
                usize::try_from(maximum).map_or(page.len(), |maximum| maximum.min(page.len()));
            return Ok((page.get(..take).unwrap_or(page).into(), bytes.len() as u64));
        }
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
        observed_children: Option<u64>,
    ) -> Result<(u64, u64, Option<Q::Page>, bool, u64), Error> {
        self.admit_path(agent)?;
        loop {
            let state = self.agents.state(agent).await?;
            let (input, tail, journal, closed, open_children) = self
                .journal_input::<Q>(&state, input, after, wait, observed_children)
                .await?;
            if input == tail {
                return Ok((input, tail, None, closed, open_children));
            }
            // Observe settlement before the journal page: a settled actor can
            // only name an execution whose original journal writes preceded it.
            let progress = self.agents.input_progress(agent, input).await?;
            let settled = progress.settled;
            let open_children = progress.open_children;
            let metadata_changed = Q::metadata_changed(observed_children, open_children);
            let mut page = if let Some((journal, operation, _)) = &journal {
                Q::read(journal, *operation, agent.as_str(), after)
                    .await
                    .map_err(turn)?
            } else {
                Q::empty(after)
            };
            if Q::CONTEXT_WINDOW
                && settled
                && let Some((_, _, Some(selection))) = &journal
                && let crate::agent::Reply::Answered { entry, .. } =
                    self.agents.reply(agent, selection, input).await?
                && entry.checkpoint().is_some()
            {
                Q::context_changed(&mut page);
            }
            if !wait || settled || Q::end(&page) > after || metadata_changed {
                return Ok((input, tail, Some(page), settled, open_children));
            }
            // Both continuations start at their original observations, so an
            // append between the page and this wait remains visible. No task,
            // timer or status recheck produces readiness.
            let actor_changed = self.journal_record(progress.follow);
            if let Some((journal, operation, _)) = &journal {
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
    async fn journal_input<Q: Projection>(
        &self,
        state: &crate::agent::AgentState,
        input: u64,
        after: u64,
        wait: bool,
        observed_children: Option<u64>,
    ) -> Result<OriginalInput<P, O>, Error> {
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
        self.journal_from_input::<Q>(
            state,
            input,
            tail,
            adopted,
            after,
            (wait, observed_children),
        )
        .await
    }

    /// Consume the selected input after its original inbox cut. Settlement
    /// and archive may advance while that cut is retained across an await.
    async fn journal_from_input<Q: Projection>(
        &self,
        state: &crate::agent::AgentState,
        input: u64,
        mut tail: u64,
        adopted: u64,
        after: u64,
        (wait, observed_children): (bool, Option<u64>),
    ) -> Result<OriginalInput<P, O>, Error> {
        use crate::agent::AgentRecord as _;
        use futures::TryStreamExt as _;
        let agent = &state.path;
        let provider = self.agents.provider();
        let record = if input == tail {
            if after != 0 {
                return Err(Error::Record("journal cursor names an absent input"));
            }
            let open_children = state.open_children.len() as u64;
            if !wait || state.closed || Q::metadata_changed(observed_children, open_children) {
                // The actor observation preceded the inbox cut. If archived,
                // its closing fence excludes every later input publication.
                return Ok((input, tail, None, state.closed, open_children));
            }
            let mut request = acyclic_stream::FollowRequest::new(agent.inbox()?, input);
            request.when_absent = acyclic_stream::WhenAbsent::Wait;
            loop {
                let progress = self.agents.input_progress(agent, input).await?;
                if progress.closed {
                    // Closure may follow both a new input and its settlement.
                    // Re-read after that fence before certifying an empty tail.
                    tail = crate::agent::inbox_tail(&**provider, agent).await?;
                    if input == tail {
                        return Ok((input, tail, None, true, progress.open_children));
                    }
                    break Some(self.journal_record(request.clone()).await?);
                }
                if Q::metadata_changed(observed_children, progress.open_children) {
                    return Ok((input, tail, None, false, progress.open_children));
                }
                tokio::select! {
                    record = self.journal_record(request.clone()) => break Some(record?),
                    changed = self.journal_record(progress.follow) => { changed?; }
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
                    steering: false,
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
                match &queued.input {
                    crate::agent::TurnInput::Context { id, action, .. } if !matches!(action, crate::agent::ContextAction::Restore { mode, .. } if !mode.context()) => {
                        Some(id.clone())
                    }
                    _ => None,
                },
            ))
        } else {
            if after != 0 {
                return Err(Error::Record(
                    "journal cursor names an input without a turn",
                ));
            }
            None
        };
        Ok((
            input,
            tail,
            journal,
            false,
            state.open_children.len() as u64,
        ))
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
