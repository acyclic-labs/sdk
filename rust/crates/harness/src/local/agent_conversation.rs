//! A typed client for one agent tree's root conversation, which fans out
//! through its spawn, send and archive tools. Prompts, cancels and approval
//! decisions are commands on the tree's [`super::Connection`], in process or
//! remote alike; observation is one [`super::Observer`] of its status, its
//! settled conversation entries and its tool output. Frontends render its
//! events; it holds no frontend state.
//!
//! [`Conversation::compact`], [`Conversation::rewind`] and
//! [`Conversation::clear_all`] select the model context of later turns. Each
//! settles like a turn, as one conversation entry, a checkpoint, which the
//! observer reports as [`super::Event::Turn`] whose input [`checkpoint`] names:
//! later turns see that checkpoint's summary, then the turns after it.
use super::Connection;
mod client_files;
pub use client_files::VisibleFiles;
mod directory;
mod events;
mod history;
mod inspect;
#[cfg(all(test, not(target_arch = "wasm32")))]
mod pending_tests;
mod recording;
mod run;
pub use run::RunOutcome;
mod transcripts;
mod tree;
mod tree_feed;
pub use tree_feed::{RootCut, SupervisionFeed, TreeFeed, TreeRecord};
mod usage;
mod usage_query;
use crate::{
    Error, Result,
    agent::{
        AgentPath, ContextAction, ConversationEntry, Delivered, IntentId, Phase, Reply,
        StatusRecord, TurnInput,
    },
    conversation::FileRef,
    executor::TurnOutput,
    interaction::{ApprovalBinding, ApprovalVerdict, Interaction},
    model::ModelContent,
};
pub use acyclic_harness_client::ExecutionQuery;
use acyclic_harness_client::{
    AgentCommand, AgentControlFailure, AgentExchangeError, AgentPageRequest, AgentReply, ReadTarget,
};
pub use history::{HistoryPage, PreparedHistoryAction};
pub use inspect::{InspectQuery, Inspection};
use std::sync::Arc;
pub use usage_query::UsageQuery;

/// One captured input fence, including the conversation frontier committed
/// with its phase. Frontends can paint through this cut before acknowledging it.
#[derive(Clone, Debug)]
pub struct InputSettlement {
    /// Exclusive conversation tail at the captured settlement.
    pub conversation: u64,
    /// Phase at that same actor checkpoint.
    pub phase: Phase,
}

fn failed(error: impl std::fmt::Display) -> Error {
    Error::Storage(error.to_string())
}

fn intent(id: &str) -> Result<IntentId> {
    IntentId::try_from(id).map_err(failed)
}

fn unexpected() -> Error {
    Error::Conflict("the endpoint answered another command".into())
}

/// A settled turn as a view shows it.
#[derive(Debug, PartialEq, Eq)]
pub struct Turn {
    /// The prompt that started it; none for a turn no message started.
    pub prompt: Option<String>,
    /// Its tool calls in request order, collapsed.
    pub tools: Vec<crate::executor::ToolCard>,
    /// Its answer text, partial for a cancelled turn.
    pub answer: String,
}

/// Where a sent message stands. Only a refusal is settled: an unknown
/// outcome may have committed, so it is resent exactly, never reported.
#[derive(Debug, PartialEq, Eq)]
pub enum Sent {
    /// Committed at this inbox sequence.
    Accepted(u64),
    /// Definitively not committed, for this reason.
    Refused(String),
    /// Neither known to have committed nor refused: resending the same id
    /// and prompt answers it, and never commits it twice.
    Unknown,
}

/// An immutable peer message prepared on an authenticated sender connection.
/// Keep this value until delivery is acknowledged; rebuilding it after an
/// uncertain result would select different opt-out guards.
pub struct PreparedPeerMessage {
    sender: AgentPath,
    command: AgentCommand,
}

/// A prompt's exchange as [`Sent`]: only the host's decoded refusal, or a
/// closed conversation, is a refusal; any other failure after the request
/// left may have committed it.
fn sent(exchange: super::endpoint::Exchange) -> Result<Sent> {
    // A command that never left, such as one over the command limit,
    // committed nothing: it is refused.
    let exchange = match exchange {
        Ok(exchange) => exchange,
        Err(unsent) => return Ok(Sent::Refused(unsent.to_string())),
    };
    match exchange {
        Ok(AgentReply::Delivered {
            delivery: Delivered::Committed { inbox },
        }) => Ok(Sent::Accepted(inbox)),
        Ok(AgentReply::Delivered {
            delivery: Delivered::Closed,
        }) => Ok(Sent::Refused("conversation is archived".into())),
        Ok(_) => Err(unexpected()),
        Err(AgentExchangeError::Control(refused @ AgentControlFailure::Refused { .. })) => {
            Ok(Sent::Refused(refused.to_string()))
        }
        Err(_) => Ok(Sent::Unknown),
    }
}

/// One agent tree's root conversation, driven by its connection's principal.
pub struct Conversation {
    connection: Connection,
}

/// An owned diff query retains its original connection, including an embedded
/// host's lifetime, until the client closes the query.
pub struct WorkspaceDiff {
    query: crate::client::DiffQuery,
    _connection: Connection,
}

/// The original input query retains the same embedded-host lifetime.
pub struct ConversationInput {
    query: crate::client::InputQuery,
    _connection: Connection,
}

impl ConversationInput {
    /// Receive one original credited step or typed failure.
    pub async fn next(
        &mut self,
    ) -> std::result::Result<Option<crate::client::InputPreviewStep>, AgentExchangeError> {
        self.query.next().await
    }
}

impl WorkspaceDiff {
    /// Receive the next original credited event, or the original failure.
    pub async fn next(
        &mut self,
    ) -> std::result::Result<Option<crate::client::DiffQueryEvent>, AgentExchangeError> {
        self.query.next().await
    }
}

impl Conversation {
    /// Recover every accepted question still pending in this conversation.
    /// One host-selected input walk streams credited original text ranges;
    /// neither client persistence nor a file grant is required to reconnect.
    pub async fn pending_inputs(&self) -> Result<Vec<(u64, String)>> {
        let mut query = self
            .connection
            .controls
            .pending_input_query(
                self.agent().clone(),
                AgentPageRequest::new(1, crate::client::MAX_READ_BYTES as u32, None)
                    .map_err(failed)?,
            )
            .await
            .map_err(failed)?;
        let mut inputs: Vec<(u64, String)> = Vec::new();
        let mut total = 0;
        while let Some(step) = query.next().await.map_err(failed)? {
            match step {
                crate::client::InputPreviewStep::Progress => {}
                crate::client::InputPreviewStep::Pending {
                    inbox,
                    offset,
                    preview,
                } => {
                    if offset == 0 {
                        if inputs
                            .last()
                            .is_some_and(|(_, text)| text.len() as u64 != total)
                        {
                            return Err(Error::Invalid(
                                "pending input ended before its complete body".into(),
                            ));
                        }
                        inputs.push((inbox, String::new()));
                        total = preview.bytes;
                    }
                    let (selected, text) = inputs.last_mut().ok_or_else(|| {
                        Error::Invalid("pending range has no original input".into())
                    })?;
                    if *selected != inbox || text.len() as u64 != offset || total != preview.bytes {
                        return Err(Error::Invalid(
                            "pending input ranges are not contiguous".into(),
                        ));
                    }
                    text.push_str(&preview.text);
                }
                crate::client::InputPreviewStep::Selected(None) => {
                    if inputs
                        .last()
                        .is_some_and(|(_, text)| text.len() as u64 != total)
                    {
                        return Err(Error::Invalid(
                            "pending input ended before its complete body".into(),
                        ));
                    }
                    inputs.reverse();
                    return Ok(inputs);
                }
                crate::client::InputPreviewStep::Selected(Some(_)) => {
                    return Err(Error::Invalid("pending query selected a preview".into()));
                }
            }
        }
        Err(Error::Invalid(
            "input query ended without a selection".into(),
        ))
    }

    /// Open the selected context's bounded last-input query.
    pub async fn input_query(
        &self,
        page: AgentPageRequest,
    ) -> std::result::Result<ConversationInput, AgentExchangeError> {
        let query = self
            .connection
            .controls
            .input_query(self.agent().clone(), page)
            .await?;
        Ok(ConversationInput {
            query,
            _connection: self.connection.clone(),
        })
    }

    /// Prepare a message to a named member root, without writing its inbox.
    /// This uses the connection's existing user authority and grants no new
    /// directory, file, control or child access.
    ///
    /// # Errors
    /// Invalid roots, disabled participation, or an authenticated query failure.
    pub async fn prepare_peer_message(
        &self,
        recipient: AgentPath,
        id: &str,
        prompt: &str,
    ) -> Result<PreparedPeerMessage> {
        let sender = self.agent().clone();
        if sender.depth() != 0 || recipient.depth() != 0 || sender == recipient {
            return Err(Error::Invalid(
                "peer messages require distinct session roots".into(),
            ));
        }
        let admission = match self
            .connection
            .execute(&AgentCommand::PreparePeer {
                sender: sender.clone(),
                agent: recipient.clone(),
            })
            .await?
        {
            AgentReply::PeerAdmission {
                admission: Some(admission),
            } => admission,
            AgentReply::PeerAdmission { admission: None } => {
                return Err(Error::Refused(
                    "the sessions do not admit peer messages".into(),
                ));
            }
            _ => return Err(unexpected()),
        };
        Ok(PreparedPeerMessage {
            sender: sender.clone(),
            command: AgentCommand::Prompt {
                agent: recipient,
                id: intent(id)?,
                body: ModelContent::Text(prompt.to_owned()),
                peer: Some((sender, admission)),
            },
        })
    }

    /// Deliver or retry the exact prepared message through the existing inbox
    /// path. Accepted messages wake the receiver and remain attributed as peers.
    ///
    /// # Errors
    /// A different sender connection, preparation failure, or malformed reply.
    pub async fn deliver_peer_message(&self, message: &PreparedPeerMessage) -> Result<Sent> {
        if message.sender != *self.agent() {
            return Err(Error::Invalid(
                "the prepared message belongs to another sender".into(),
            ));
        }
        sent(self.connection.exchange(&message.command).await)
    }

    /// Open a demand-driven diff on the original authenticated client socket.
    /// Dropping it releases the server's immutable traversal and comparison.
    ///
    /// # Errors
    /// The original typed admission, framing or transport failure.
    pub async fn diff_query(
        &self,
        page: AgentPageRequest,
        projection: crate::client::DiffProjection,
    ) -> std::result::Result<WorkspaceDiff, AgentExchangeError> {
        let query = self
            .connection
            .controls
            .diff_query(self.agent().clone(), page, projection)
            .await?;
        Ok(WorkspaceDiff {
            query,
            _connection: self.connection.clone(),
        })
    }

    /// The conversation of `connection`'s root.
    #[must_use]
    pub fn open(connection: Connection) -> Arc<Self> {
        Arc::new(Self { connection })
    }

    /// The same conversation over [`Connection::observation`]: its committed
    /// records stay readable after a failure stopped the agents' runtime.
    ///
    /// # Errors
    /// Fails when the observation cannot be bound.
    pub async fn observation(&self) -> Result<Arc<Self>> {
        Ok(Self::open(self.connection.observation().await?))
    }

    /// The selected agent; navigation never reconstructs identity from UI text.
    pub fn agent(&self) -> &AgentPath {
        self.connection.root()
    }

    /// Read one visible diagnostic page through this connection's existing
    /// authenticated session. The original identity and UTF-8 byte cursor are
    /// preserved; no complete history or diagnostic is loaded.
    pub async fn diagnostic_page(
        &self,
        diagnostic: &crate::agent::Diagnostic,
        from: u64,
        maximum_bytes: u64,
    ) -> Result<crate::agent::DiagnosticRead> {
        if diagnostic.agent != *self.agent() {
            return Err(Error::Unauthorized(
                "diagnostic belongs to another selected agent".into(),
            ));
        }
        diagnostic
            .read_text_with(from, maximum_bytes, |request| {
                self.connection.session.read(request)
            })
            .await
            .map_err(Error::from)
    }

    /// The same authenticated tree connection, selected at an existing node.
    pub async fn select(&self, agent: AgentPath) -> Result<Arc<Self>> {
        Ok(Self::open(self.connection.select(agent).await?))
    }

    /// A lazy sidebar on this session's primitives. No descendant is read
    /// until the shared explorer plans a visible row or lookahead page.
    pub fn tree(
        &self,
    ) -> Result<acyclic_harness_client::explorer::LiveExplorer<acyclic_stream::session::ReadProvider>>
    {
        acyclic_harness_client::explorer::LiveExplorer::children(
            acyclic_harness_client::ReaderPolicy::default(),
            self.agent(),
            Arc::new(self.connection.session.read_provider()),
        )
        .map_err(failed)
    }

    /// The authenticated selection shared by session controls and observation.
    #[must_use]
    pub fn connection(&self) -> &Connection {
        &self.connection
    }

    /// The root agent's id in operation form: the task its failures name.
    #[must_use]
    pub fn task(&self) -> crate::OperationId {
        crate::OperationId::from_bytes(self.agent().id_bytes())
    }

    /// The inbox sequence `command` committed at, which its settled entry
    /// carries.
    async fn committed(&self, command: &AgentCommand) -> Result<u64> {
        match self.connection.execute(command).await? {
            AgentReply::Delivered {
                delivery: Delivered::Committed { inbox },
            } => Ok(inbox),
            AgentReply::Delivered {
                delivery: Delivered::Closed,
            } => Err(Error::Conflict("conversation is archived".into())),
            _ => Err(unexpected()),
        }
    }

    /// Compact the conversation as request `id`: one model request
    /// summarizes the current context, guided by `instructions`, and later
    /// turns start from that summary. Returns its inbox sequence; the
    /// checkpoint settles as the entry at it. The same id replays exactly.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn compact(&self, id: &str, instructions: Option<&str>) -> Result<u64> {
        self.committed(&AgentCommand::Compact {
            agent: self.agent().clone(),
            id: intent(id)?,
            instructions: instructions.map(|text| ModelContent::Text(text.to_owned())),
        })
        .await
    }

    /// Rewind the conversation as request `id` to its latest checkpoint:
    /// every turn settled after it leaves later turns' context. Returns its
    /// inbox sequence, like [`Self::compact`].
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn rewind(&self, id: &str) -> Result<u64> {
        self.committed(&AgentCommand::Rewind {
            agent: self.agent().clone(),
            id: intent(id)?,
        })
        .await
    }

    /// Read the authorized current project heads for a restore confirmation.
    /// A later edit makes these roots stale and refuses the complete restore.
    pub async fn project_heads(&self) -> Result<Vec<crate::agent::HistoryVolume>> {
        match self
            .connection
            .execute(&AgentCommand::ProjectHeads {
                agent: self.agent().clone(),
            })
            .await?
        {
            AgentReply::ProjectHeads { items } => Ok(items),
            _ => Err(unexpected()),
        }
    }

    /// Restore a selected settled turn. Project files and the context cut
    /// share one conditional commit in `Both` mode. The caller supplies every
    /// project head it observed; a concurrent edit refuses the restoration.
    /// Child agents and external effects continue independently.
    pub async fn restore(
        &self,
        id: &str,
        at: u64,
        mode: crate::agent::RestoreMode,
        expected: Vec<crate::agent::HistoryVolume>,
    ) -> Result<u64> {
        self.committed(&AgentCommand::Restore {
            agent: self.agent().clone(),
            id: intent(id)?,
            at,
            mode,
            expected,
        })
        .await
    }

    /// Retire the conversation as request `id`: later turns, and a reopened
    /// transcript, start after it with no history. Returns its inbox
    /// sequence, like [`Self::compact`].
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn clear_all(&self, id: &str) -> Result<u64> {
        self.committed(&AgentCommand::ClearAll {
            agent: self.agent().clone(),
            id: intent(id)?,
        })
        .await
    }

    /// Send `prompt` as message `id`: accepted at its inbox sequence,
    /// refused, or of unknown outcome. The same id and prompt replay exactly.
    ///
    /// # Errors
    /// When the prompt cannot be prepared, or the endpoint answers another
    /// command.
    pub async fn deliver(&self, id: &str, prompt: &str) -> Result<Sent> {
        // An id that names no intent never leaves either.
        let intent = match IntentId::try_from(id) {
            Ok(intent) => intent,
            Err(error) => return Ok(Sent::Refused(error.to_string())),
        };
        let command = AgentCommand::Prompt {
            peer: None,
            agent: self.agent().clone(),
            id: intent,
            body: ModelContent::Text(prompt.to_owned()),
        };
        let sent = sent(self.connection.exchange(&command).await)?;
        tracing::debug!(id, ?sent, "message sent");
        Ok(sent)
    }

    /// [`Self::deliver`] for a caller that treats every outcome but
    /// acceptance as a failure; returns its inbox sequence.
    ///
    /// # Errors
    /// A refusal with its reason, an unknown outcome, or as [`Self::deliver`].
    pub async fn send(&self, id: &str, prompt: &str) -> Result<u64> {
        match self.deliver(id, prompt).await? {
            Sent::Accepted(inbox) => Ok(inbox),
            // A refusal committed nothing: it keeps the host's own class,
            // never a storage failure the caller would treat as a fault.
            Sent::Refused(reason) => Err(Error::Refused(reason)),
            // The session retains the terminal cause that left the outcome
            // undecided; report it with the outcome instead of dropping it.
            Sent::Unknown => Err(self
                .connection
                .stream_failure("the message's commit outcome is unknown; resend it")),
        }
    }

    /// Stop the current input, including accepted work not yet checkpointed. The conversation
    /// stays open; a newer queued prompt is never selected in its place.
    /// Returns `None` when the host has no current input.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn cancel(&self) -> Result<Option<u64>> {
        let reply = self
            .connection
            .execute(&AgentCommand::Settlement {
                agent: self.agent().clone(),
                wait: false,
            })
            .await?;
        match reply {
            AgentReply::Settlement {
                input: Some(input),
                settled,
                ..
            } => {
                self.cancel_at(input, settled).await?;
                Ok(Some(input))
            }
            AgentReply::Settlement { input: None, .. } => Ok(None),
            _ => Err(unexpected()),
        }
    }

    /// Cancel this exact accepted input. A later turn cannot inherit the cancel.
    pub async fn cancel_input(&self, input: u64) -> Result<()> {
        let conversation = self.settlement(false).await?.0;
        self.cancel_at(input, conversation).await
    }

    async fn cancel_at(&self, input: u64, conversation: u64) -> Result<()> {
        let command = AgentCommand::Cancel {
            input,
            conversation,
            agent: self.agent().clone(),
            id: intent(&crate::OperationId::new().to_string())?,
        };
        self.committed(&command).await.map(drop)
    }

    /// Decides the exact immutable action already shown to the caller. The
    /// caller retains this binding for retries; no pending-request listing or
    /// client history cache is needed. The original host validates the binding.
    ///
    /// # Errors
    /// Original authorization, binding, session or publication refusal.
    pub async fn decide_with_binding(
        &self,
        request: &IntentId,
        binding: ApprovalBinding,
        verdict: ApprovalVerdict,
    ) -> Result<()> {
        self.committed(&AgentCommand::Decide {
            agent: self.agent().clone(),
            id: request.clone(),
            binding,
            verdict,
            reason: None,
        })
        .await
        .map(drop)
    }

    /// Original pending binding and waiting-status sequence. The request bytes
    /// stay at that immutable status and are read separately in bounded chunks.
    ///
    /// # Errors
    /// Original session, authorization or reply validation failure.
    pub async fn pending_approval(
        &self,
        request: &IntentId,
    ) -> Result<Option<acyclic_harness_client::PendingApproval>> {
        let AgentReply::Approvals { items, .. } = self
            .connection
            .execute(&AgentCommand::Approvals {
                agent: self.agent().clone(),
                page: AgentPageRequest::default(),
            })
            .await?
        else {
            return Err(unexpected());
        };
        Ok(items
            .into_iter()
            .next()
            .filter(|pending| &pending.id == request))
    }

    /// The explanation in the first request page, or an instruction to review
    /// the full request when its explanation spans pages. `None` means it is no
    /// longer pending. The request subject is never decoded for this preview.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn approval_prompt(&self, request: &IntentId) -> Result<Option<String>> {
        let Some(pending) = self.pending_approval(request).await? else {
            return Ok(None);
        };
        let (bytes, length) = self.approval_request(pending.status, 0).await?;
        match Interaction::approval_prompt_preview(&bytes)? {
            Some(prompt) => Ok(Some(prompt)),
            None if (bytes.len() as u64) < length => Ok(Some(
                "Review the complete approval request before deciding; its explanation continues beyond this preview."
                    .into(),
            )),
            None => Err(Error::Invalid("approval request has no explanation".into())),
        }
    }

    /// The exact action binding approval `request` shows its approver, or
    /// `None` once it is no longer pending. A client answers with it through
    /// [`Self::decide_with_binding`].
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn approval_binding(&self, request: &IntentId) -> Result<Option<ApprovalBinding>> {
        Ok(self
            .pending_approval(request)
            .await?
            .map(|pending| pending.binding))
    }

    /// The agent's current phase, from its latest status: a reconnecting
    /// client's pending approval, independent of any saved event cursor.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn phase(&self) -> Result<Phase> {
        Ok(self.settlement(false).await?.2)
    }

    /// Reads all of `target` on `agent`, one bounded range at a time.
    pub(super) async fn read(
        connection: &Connection,
        agent: &AgentPath,
        target: ReadTarget,
    ) -> Result<Vec<u8>> {
        let mut bytes = Vec::new();
        loop {
            let (range, length) =
                Self::read_at(connection, agent, target.clone(), bytes.len() as u64).await?;
            let done = range.is_empty();
            bytes.extend_from_slice(&range);
            if done || bytes.len() as u64 >= length {
                return Ok(bytes);
            }
        }
    }

    /// One bounded range of `target` on `agent` from `offset`, and the
    /// target's whole length.
    pub(super) async fn read_at(
        connection: &Connection,
        agent: &AgentPath,
        target: ReadTarget,
        offset: u64,
    ) -> Result<(Vec<u8>, u64)> {
        let reply = connection
            .execute(&AgentCommand::Read {
                agent: agent.clone(),
                target,
                offset,
            })
            .await?;
        reply.content().map_err(failed)
    }

    /// The text of a staged prompt or turn output.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn text(&self, file: &FileRef) -> Result<String> {
        self.selected_text(file, None).await
    }

    async fn selected_text(&self, file: &FileRef, sequence: Option<u64>) -> Result<String> {
        let target = match sequence {
            Some(sequence) => ReadTarget::Conversation {
                sequence,
                file: file.clone(),
            },
            None => ReadTarget::File { file: file.clone() },
        };
        let bytes = Self::read(&self.connection, self.agent(), target).await?;
        String::from_utf8(bytes).map_err(failed)
    }

    /// Exports the tree's committed model-call usage as JSONL, one original
    /// call at a time. Pages and child traversal are bounded; only the pending
    /// call and the active traversal path are retained. Call after settlement.
    /// Version-two rows retain the original usage plus an unknown price estimate.
    pub async fn write_usage_jsonl(&self, output: &mut dyn std::io::Write) -> Result<()> {
        self.write_usage_jsonl_with_prices(output, &|_| None).await
    }

    /// The same usage-file projection with explicitly selected immutable price
    /// records. The selector runs once per emitted call, never per redraw or
    /// token. It must retain the selected committed revision for replay; this
    /// method does not discover prices, read accounts or infer effective dates.
    pub async fn write_usage_jsonl_with_prices<'p>(
        &self,
        output: &mut dyn std::io::Write,
        prices: &(
             dyn Fn(
            &crate::executor::ModelUsageRecord,
        ) -> Option<&'p crate::executor::ModelPriceRecord>
                 + 'p
         ),
    ) -> Result<()> {
        usage::write_tree(&self.connection, self.agent(), output, prices).await
    }

    /// Visits this agent's committed model calls, and with
    /// [`UsageScope::Tree`] its descendants' outside the pruned subtrees, in
    /// the same order and with the same bounds as
    /// [`Self::write_usage_jsonl`], without encoding them. A visitor error
    /// stops the traversal and is returned.
    ///
    /// # Errors
    /// Session, host or decoding failures, or the visitor's error.
    pub async fn visit_usage(
        &self,
        scope: UsageScope<'_>,
        visit: &mut dyn FnMut(&crate::executor::ModelUsageRecord) -> Result<()>,
    ) -> Result<()> {
        match scope {
            UsageScope::Tree(prune) => {
                usage::visit_tree(&self.connection, self.agent(), prune, visit).await
            }
            UsageScope::Agent => usage::visit_agent(&self.connection, self.agent(), visit).await,
        }
    }

    /// Fold one explicitly selected agent or subtree's original usage. Uses
    /// the same bounded visitor and shared estimator as the JSONL exporter;
    /// retains only fixed totals and the active traversal path. Missing usage
    /// or price observations remain unknown, including after provider failure.
    pub async fn usage_totals(
        &self,
        scope: UsageScope<'_>,
        prices: &crate::executor::ModelPrices,
    ) -> Result<crate::executor::ModelUsageTotals> {
        let mut totals = prices.totals();
        self.visit_usage(scope, &mut |row| {
            totals
                .push(&row.estimate(prices.price(row)).map_err(failed)?)
                .map_err(failed)
        })
        .await?;
        Ok(totals)
    }

    /// A settled turn as a view shows it: its prompt, its tool calls and its
    /// answer, from the entry's input and its one output record. `sequence`
    /// is the entry's committed conversation sequence, as [`super::Event::Turn`]
    /// and [`Reply::Answered`] report it: that proof is what admits its prompt,
    /// which stays host-private until committed, and inherited turns read
    /// without source-volume authority.
    ///
    /// # Errors
    /// Session, host or decoding failures, or a sequence that does not commit
    /// `entry`.
    pub async fn turn(&self, sequence: u64, entry: &ConversationEntry) -> Result<Turn> {
        let prompt = match &entry.input {
            TurnInput::Message { body, .. } => {
                Some(self.selected_text(body, Some(sequence)).await?)
            }
            _ => None,
        };
        let output = self.output(entry, Some(sequence)).await?;
        Ok(Turn {
            prompt,
            tools: output.tool_cards(),
            answer: output.text,
        })
    }

    /// The retained output text of a settled turn, including partial output
    /// from a cancelled turn. This reads only the output; its outcome remains
    /// in `entry` and its prompt is not fetched.
    ///
    /// # Errors
    /// Session, host or output decoding failures.
    pub async fn output_text(&self, entry: &ConversationEntry) -> Result<String> {
        Ok(self.output(entry, None).await?.text)
    }

    /// Read an inherited output with its exact committed record as proof.
    pub async fn output_text_at(&self, sequence: u64, entry: &ConversationEntry) -> Result<String> {
        Ok(self.output(entry, Some(sequence)).await?.text)
    }

    async fn output(&self, entry: &ConversationEntry, sequence: Option<u64>) -> Result<TurnOutput> {
        let output = self.selected_text(&entry.output, sequence).await?;
        serde_json::from_str(&output).map_err(failed)
    }

    /// Wait for all inputs committed before this request, including queued or
    /// paused work. Inputs arriving later do not extend the captured fence.
    /// Dropping the request cancels observation without changing the agent.
    pub async fn wait_current_input(&self) -> Result<InputSettlement> {
        let (conversation, _, phase) = self.settlement(true).await?;
        Ok(InputSettlement {
            conversation,
            phase,
        })
    }

    async fn settlement(&self, wait: bool) -> Result<(u64, u64, Phase)> {
        match self
            .connection
            .execute(&AgentCommand::Settlement {
                agent: self.agent().clone(),
                wait,
            })
            .await?
        {
            AgentReply::Settlement {
                settled,
                statuses,
                phase,
                ..
            } => Ok((settled, statuses, phase)),
            _ => Err(unexpected()),
        }
    }

    /// The committed reply to message `id`, which [`Self::send`] committed at
    /// inbox sequence `inbox`: its settled entry, pending, or dropped.
    ///
    /// # Errors
    /// Session, host or decoding failures, an entry at `inbox` that settled
    /// another request, or a pending request without a matching committed input.
    pub async fn reply(&self, id: &str, inbox: u64) -> Result<Reply> {
        match self
            .connection
            .execute(&AgentCommand::Reply {
                agent: self.agent().clone(),
                id: intent(id)?,
                inbox,
            })
            .await?
        {
            AgentReply::Reply { reply } => Ok(reply),
            _ => Err(unexpected()),
        }
    }

    /// Waits for message `id`, committed at inbox sequence `inbox`, on
    /// `events`, opened from this connection or a clone before sending it so
    /// no committed reply loses its event. Returns once the reply settles, or
    /// at the first status that needs the caller: the agent ended, or asks for
    /// an approval.
    /// Waiting is on the observer's follows; no read is repeated without an
    /// event.
    ///
    /// # Errors
    /// An observer from another connection or a retired conversation/status
    /// selection is refused before I/O.
    /// Otherwise, session, host or decoding failures.
    pub async fn settle(
        &self,
        id: &str,
        inbox: u64,
        events: &mut super::Observer,
    ) -> Result<Settlement> {
        if !events.belongs_to(&self.connection) {
            return Err(Error::Invalid(
                "settlement requires an observer from the original connection with live conversation and status follows; recovery: observe this conversation again".into(),
            ));
        }
        let mut status: Option<StatusRecord> = None;
        loop {
            match self.reply(id, inbox).await? {
                reply @ (Reply::Answered { .. } | Reply::Dropped) => {
                    return Ok(Settlement::Reply(reply));
                }
                Reply::Pending | Reply::Continuing { .. } => {}
            }
            if let Some(status) = status.take() {
                if let Some(reason) = ended(&status) {
                    return Ok(Settlement::Ended(reason));
                }
                if let Phase::WaitingApproval { id, binding } = status.phase {
                    return Ok(Settlement::Approval { id, binding });
                }
            }
            status = loop {
                match events.next().await? {
                    super::Event::Status(next) => break Some(next),
                    super::Event::Turn { .. } => break None,
                    super::Event::Delta => {}
                }
            };
        }
    }

    /// Why a dropped prompt got no turn: the agent finished, or a cancel
    /// cleared it.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn dropped(&self) -> Result<String> {
        let (_, _, phase) = self.settlement(false).await?;
        Ok(finished(&phase).unwrap_or_else(|| "the prompt was cancelled".into()))
    }

    /// Number of settled turns.
    ///
    /// # Errors
    /// Session, host or decoding failures.
    pub async fn settled(&self) -> Result<u64> {
        Ok(self.settlement(false).await?.0)
    }

    /// Observes new turns and the latest status, returning the current phase
    /// from the same settlement that selected both opening positions. A
    /// reconnecting client can admit input before consuming its first event.
    ///
    /// # Errors
    /// Session, host, decoding or follow admission failures.
    pub async fn observe_current(&self) -> Result<(super::Observer, Phase)> {
        let (settled, statuses, phase) = self.settlement(false).await?;
        Ok((
            super::Observer::open(&self.connection, settled, statuses.saturating_sub(1))?,
            phase,
        ))
    }

    /// Every settled turn from conversation sequence `from`, or from the current
    /// settled tail when `None`, together with the agent's latest phase and its
    /// newest tool call's output. The default turn cursor and status cursor come
    /// from one actor observation. A status records only a phase change; a
    /// settled turn is its conversation entry.
    ///
    /// # Errors
    /// Session or host failures, or refused follows.
    pub async fn observe(&self, from: Option<u64>) -> Result<super::Observer> {
        self.open_observation(from, true)
            .await
            .map(|(observer, _)| observer)
    }

    /// Opens the same observation with its authoritative initial phase. A
    /// reattached client can accept control input before the first follow
    /// delivery without mistaking an unknown phase for an idle agent.
    pub async fn observe_with_phase(&self, from: Option<u64>) -> Result<(super::Observer, Phase)> {
        self.open_observation(from, false).await
    }

    async fn open_observation(
        &self,
        from: Option<u64>,
        replay_status: bool,
    ) -> Result<(super::Observer, Phase)> {
        let (settled, statuses, phase) = self.settlement(false).await?;
        let from = from.unwrap_or(settled);
        let observer = if replay_status {
            super::Observer::open(&self.connection, from, statuses.saturating_sub(1))?
        } else {
            super::Observer::open_at_phase(&self.connection, from, statuses)?
        };
        Ok((observer, phase))
    }

    /// The settled entries at conversation sequences `from..to`, every one
    /// of them below the settled tail, read over one follow of the
    /// conversation that is dropped at `to`. A transcript pages its older
    /// turns with this, so a view holds only the turns it has shown.
    ///
    /// # Errors
    /// Session or host failures, or a refused follow.
    pub async fn entries(&self, from: u64, to: u64) -> Result<Vec<ConversationEntry>> {
        let mut entries = Vec::new();
        if from >= to {
            return Ok(entries);
        }
        // The status follow starts at its tail: only the turns are read.
        let (_, statuses, _) = self.settlement(false).await?;
        let mut turns = self
            .connection
            .controls
            .observe(acyclic_harness_client::AgentSelection::new(
                self.agent().clone(),
                from,
                statuses,
            ))
            .map_err(failed)?;
        loop {
            if let acyclic_harness_client::AgentEvent::Conversation {
                sequence, entry, ..
            } = turns.next().await.map_err(failed)?
            {
                if sequence >= to {
                    return Ok(entries);
                }
                entries.push(entry);
                if sequence + 1 == to {
                    return Ok(entries);
                }
            }
        }
    }

    /// Replay every committed transition after the source cursors in `projection`.
    /// Opening is lazy and uses this conversation's authenticated session.
    ///
    /// # Errors
    /// When the follows or their provider cursors are refused.
    pub fn replay(
        &self,
        projection: &acyclic_harness_client::EventProjection,
    ) -> Result<super::Observer> {
        super::Observer::replay(&self.connection, projection)
    }
}

/// Which calls [`Conversation::visit_usage`] visits.
#[derive(Clone, Copy)]
pub enum UsageScope<'p> {
    /// The agent's own calls.
    Agent,
    /// The agent's and its descendants', skipping each subtree whose root
    /// `prune` names.
    Tree(&'p dyn Fn(&AgentPath) -> bool),
}

/// Where [`Conversation::settle`] stopped waiting.
#[derive(Debug)]
pub enum Settlement {
    /// The message's reply settled: answered or dropped.
    Reply(Reply),
    /// The agent finished before answering, for this reason.
    Ended(String),
    /// The agent waits on this approval before it can answer.
    Approval {
        /// Approval request identity.
        id: IntentId,
        /// The displayed action's binding, when the status carries one.
        binding: Option<ApprovalBinding>,
    },
}

/// The selection a settled `entry` made when it is a checkpoint (a
/// compaction, rewind or clear); `None` for a turn.
#[must_use]
pub const fn checkpoint(entry: &ConversationEntry) -> Option<&ContextAction> {
    entry.checkpoint()
}

/// Whether `status` ends the wait for a turn without its output: the agent
/// finished.
#[must_use]
pub fn ended(status: &StatusRecord) -> Option<String> {
    finished(&status.phase)
}

fn finished(phase: &Phase) -> Option<String> {
    match phase {
        Phase::Done { outcome } => Some(format!("conversation ended: {outcome:?}")),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use acyclic_harness_client::CommandError;
    use acyclic_stream::{CommitEffect, CommitRequest, IdempotencyKey, StreamError};

    #[test]
    fn only_a_host_refusal_settles_a_send() -> Result<()> {
        let refused = AgentExchangeError::Control(AgentControlFailure::Refused {
            code: "agent_turn_error".into(),
            reason: "Unknown command: /gone".into(),
        });
        assert_eq!(
            sent(Ok(Err(refused)))?,
            Sent::Refused("Unknown command: /gone".into())
        );
        // A command that never left, such as one over the command limit.
        assert_eq!(
            sent(Err(CommandError::Capacity))?,
            Sent::Refused(CommandError::Capacity.to_string())
        );
        assert_eq!(
            sent(Ok(Ok(AgentReply::Delivered {
                delivery: Delivered::Committed { inbox: 4 },
            })))?,
            Sent::Accepted(4)
        );
        // Negative controls: an unknown commit outcome, a session that
        // failed after the request left, and a reply that failed to decode
        // may each have committed it.
        let request = CommitRequest {
            deadline_unix_millis: None,
            effects: vec![CommitEffect::hard(Vec::new(), Vec::new())],
            idempotency_key: IdempotencyKey::new(b"message".to_vec()).map_err(failed)?,
        };
        let unknown = AgentControlFailure::Unknown(Box::new(request));
        assert_eq!(
            sent(Ok(Err(AgentExchangeError::Control(unknown))))?,
            Sent::Unknown
        );
        assert_eq!(
            sent(Ok(Err(AgentExchangeError::Session(
                StreamError::Unavailable
            ))))?,
            Sent::Unknown
        );
        assert_eq!(
            sent(Ok(Err(AgentExchangeError::Command(CommandError::Invalid))))?,
            Sent::Unknown
        );
        Ok(())
    }
}
