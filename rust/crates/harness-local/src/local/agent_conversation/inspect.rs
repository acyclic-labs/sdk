//! Read-only inspection of one visible agent, over the shared live queries.
use super::{Conversation, UsageQuery};
use crate::{
    Result,
    agent::{AgentPath, AgentRecord as _, Phase, StatusRecord},
    executor::{ContextUsage, ModelUsageTotals, ToolUsage, UsageTotal, WaitUsage},
};
use acyclic_harness_client::{AgentCommand, AgentPageRequest, AgentReply};
use acyclic_stream::{FollowFilter, FollowRequest, RecordStream, StreamError, wire};
use futures::TryStreamExt as _;
use serde::Serialize;
use std::{collections::BTreeMap, sync::Arc};

/// One selected agent's live inspection. No subtree is traversed. Usage retains
/// its original bounded frontier; status follows retain no transcript payloads.
pub struct InspectQuery {
    usage: UsageQuery,
    statuses: RecordStream,
    phase: Phase,
}

/// Version-one live inspection projection. Unknown usage stays unknown, and
/// synchronization describes the usage frontier, not an atomic cross-stream cut.
#[derive(Serialize)]
pub struct Inspection<'a> {
    /// Projection schema version.
    pub version: u32,
    /// Exact authenticated selection.
    pub agent: &'a AgentPath,
    /// Last observed phase; running alone does not establish a stuck agent.
    pub phase: &'a Phase,
    /// Original committed usage, including unfinished calls with unknown counts.
    pub usage: &'a ModelUsageTotals,
    /// Same original contributions by admitted provider. Counter conventions
    /// remain provider-defined, as in the original usage record.
    pub providers: &'a BTreeMap<String, ModelUsageTotals>,
    /// Monotonic durations of completed calls, with unknown coverage explicit.
    pub model_time_ns: UsageTotal,
    /// Tool admissions, successful settlements, failure classes and original
    /// publication intervals, consumed at the same usage frontier.
    pub tools: &'a ToolUsage,
    /// Original recovery waits from the same bounded usage frontier. Elapsed
    /// time covers completed timers; unfinished starts are explicit.
    pub waits: WaitUsage,
    /// Whether bounded initial usage replay has reached its live frontier.
    pub synchronized: bool,
    /// Direct open-child count at the usage query's actor cut.
    pub open_children: Option<u64>,
    /// Original context occupancy, without inferring it from token totals.
    pub context: Option<ContextUsage>,
    /// Latest admitted response provider and model, if any.
    pub model: Option<(&'a str, &'a str)>,
}

impl Conversation {
    /// Lazily enumerate this tree through the shared children-page walk.
    /// Membership is a finite traversal, not an atomic tree cut. Each yielded
    /// query remains live independently; retain only the agents being displayed.
    pub fn inspect_tree(self: &Arc<Self>) -> impl futures::Stream<Item = Result<InspectQuery>> {
        let connection = self.connection().clone();
        let tree = super::tree::TreeWalk::new(self.agent());
        futures::stream::try_unfold((connection, tree), |(connection, mut tree)| async move {
            let Some(agent) = tree.next(&connection).await? else {
                return Ok(None);
            };
            let selected = Conversation::open(connection.select(agent).await?);
            let query = selected.inspect().await?;
            Ok(Some((query, (connection, tree))))
        })
    }

    /// Open a read-only query for this agent. The opening status and subsequent
    /// status-only follow share a frontier, so an update during opening is retained.
    /// Conversation entries and tool-output history are never subscribed.
    ///
    /// # Errors
    /// Original source, authorization or observation failure.
    pub async fn inspect(self: &Arc<Self>) -> Result<InspectQuery> {
        let (_, statuses, phase) = self.settlement(false).await?;
        let mut request =
            FollowRequest::new(self.agent().status().map_err(super::failed)?, statuses);
        request.filter = FollowFilter::new(wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: b"status".as_slice().into(),
                end: b"status\0".as_slice().into(),
            }],
            ..Default::default()
        })
        .map_err(super::failed)?;
        let statuses = self
            .connection
            .session
            .follow_records(request)
            .await
            .map_err(|error| self.connection.stream_failure(error))?;
        Ok(InspectQuery {
            usage: self.usage_query(),
            statuses,
            phase,
        })
    }

    /// Read just the explicitly requested direct-child page. This never reads
    /// child usage or enumerates descendants; select a visible child to inspect it.
    ///
    /// # Errors
    /// Original source, authorization or page-validation failure.
    pub async fn children_page(&self, page: AgentPageRequest) -> Result<AgentReply> {
        self.connection()
            .execute(&AgentCommand::Children {
                agent: self.agent().clone(),
                page,
            })
            .await
    }
}

impl InspectQuery {
    /// Borrow the current view without reading storage or allocating.
    #[must_use]
    pub fn snapshot(&self) -> Inspection<'_> {
        Inspection {
            version: 1,
            agent: self.usage.conversation.agent(),
            phase: &self.phase,
            usage: self.usage.totals(),
            providers: self.usage.providers(),
            model_time_ns: self.usage.model_time_ns(),
            tools: self.usage.tools(),
            waits: self.usage.waits(),
            synchronized: self.usage.synchronized(),
            open_children: self.usage.open_children(),
            context: self.usage.context(),
            model: self.usage.model(),
        }
    }

    /// Admit the next bounded usage page or status event. Once caught up, both
    /// sources wait on their original stream events. Cancellation preserves
    /// their retained frontiers; no timer or readiness recheck is introduced.
    ///
    /// # Errors
    /// Original source, authorization or contribution failure.
    pub async fn advance(&mut self) -> Result<bool> {
        if !self.usage.synchronized() {
            return self.usage.advance().await;
        }
        tokio::select! {
            result = self.usage.advance() => result,
            record = self.statuses.try_next() => {
                let record = record
                    .map_err(|error| self.usage.conversation.connection.stream_failure(error))?
                    .ok_or_else(|| self.usage.conversation.connection.stream_failure(StreamError::Unavailable))?;
                let status = StatusRecord::decode(&record.value).map_err(super::failed)?;
                let changed = self.phase != status.phase;
                self.phase = status.phase;
                Ok(changed)
            }
        }
    }
}
