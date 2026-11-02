//! `call`: the one invocation protocol for tools, model providers, devices,
//! Machines and agents (dispatch-and-yield).
//!
//! A call is one caller-owned journal, `<root>/calls/<id>`, and it is the
//! call's claim ledger. The caller delivers the request to its executor in
//! the caller's own claim commit, so a claimed call was sent exactly once and
//! an unclaimed one never. The caller does not wait: completion is a record
//! in the journal. The journal holds, in order:
//!
//! 0. the caller's request and escrow hold, when the executor reads them here;
//! 1. the executor's acceptance, durable and exclusive (`Absent{accepted}`);
//! 2. the executor's ordered chunks;
//! 3. exactly one terminal: `ok`, `error`, `cancelled` or `indeterminate`.
//!
//! Every terminal is guarded by `Absent{<log>/terminal}` and creates that
//! marker, so whichever hop ends the call writes the only terminal: the
//! executor, a canceller (or a gate deny) or the executor host's recovery.
//! Cancellation is one conditional commit and never waits for the executor;
//! the executor's late chunk or terminal then fails its condition and is
//! dropped. A call cancelled after its acceptance may have run, so it reads
//! as `indeterminate`.
//!
//! An acceptance may name a fence: the end marker of the executor's session.
//! Everything the executor writes under that session is guarded by
//! `Absent{fence}`, so once the fence exists nothing more of it commits, and
//! an accepted call with no terminal settles `indeterminate`. One marker
//! covers every call of the session, so executor loss is O(1) in the number
//! of calls it held. An accepted call is never dispatched again.
//!
//! The request record and the one terminal record are the call's original
//! committed receipt ([`Receipt`]).
//!
//! Records keep their payload as the record value and name their kind in one
//! indexed attribute, so a chunk is never re-encoded or copied.

use acyclic_stream::{
    AppendRecord, Attribute, CommitCondition, CommitId, CommitMutation, CommitRequest,
    IdempotencyKey, OwnedBytes, Record, RecordHeaders, StreamError, StreamPath,
};

/// Subtree segment under which a caller keeps its call journals.
pub const CALLS: &str = "calls";
const SETTLED: &str = "terminal";
const ACCEPTED: &str = "accepted";
const KIND: &str = "acyclic.call.v2";
const FENCE: &str = "acyclic.call.v2.fence";

/// A call journal that cannot be addressed or does not follow the protocol.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A path or record the stream refuses.
    #[error("call stream: {0}")]
    Stream(StreamError),
    /// The journal breaks the protocol; nothing in it is trusted.
    #[error("malformed call journal: {0}")]
    Malformed(&'static str),
}

/// How a call ended.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Outcome {
    /// The executor's result.
    Ok(OwnedBytes),
    /// The executor reported a failure, or refused the call before running it.
    Error(String),
    /// Cancelled before any executor accepted it: it never ran.
    Cancelled(String),
    /// It may have run partly or fully; it is never run again.
    Indeterminate(String),
}

impl Outcome {
    const fn kind(&self) -> &'static [u8] {
        match self {
            Self::Ok(_) => b"ok",
            Self::Error(_) => b"error",
            Self::Cancelled(_) => b"cancelled",
            Self::Indeterminate(_) => b"indeterminate",
        }
    }

    fn body(&self) -> OwnedBytes {
        match self {
            Self::Ok(body) => body.clone(),
            Self::Error(reason) | Self::Cancelled(reason) | Self::Indeterminate(reason) => {
                OwnedBytes::from(reason.as_str())
            }
        }
    }
}

/// The executor's exclusive acceptance.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Accepted {
    /// Executor-defined description of where the call runs.
    pub executor: OwnedBytes,
    /// End marker of the executor's session; `None` when the executor host
    /// recovers its own accepted calls.
    pub fence: Option<StreamPath>,
}

/// One journal record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Entry {
    /// The caller's request.
    Request(OwnedBytes),
    /// Funding transferred by the caller in the request's original commit.
    /// A terminal does not release it; actual spend reconciles separately.
    Hold(crate::escrow::Escrow),
    /// The call was accepted.
    Accepted(Accepted),
    /// One ordered chunk of the executor's output.
    Chunk(OwnedBytes),
    /// The call's one terminal, as the caller reads it.
    Terminal(Outcome),
}

/// The conditions and mutations of one protocol step, published in one commit
/// (alone, or joined with the publisher's own state).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    /// Guards; the step commits only if every one holds.
    pub conditions: Vec<CommitCondition>,
    /// Appends to the journal and its marker.
    pub mutations: Vec<CommitMutation>,
}

impl Commit {
    /// The step as its own commit.
    #[must_use]
    pub fn request(self, idempotency_key: IdempotencyKey) -> CommitRequest {
        CommitRequest {
            deadline_unix_millis: None,
            conditions: self.conditions,
            mutations: self.mutations,
            idempotency_key,
        }
    }
}

/// The paths of one call.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CallPath {
    log: StreamPath,
    settled: StreamPath,
    accepted: StreamPath,
}

impl CallPath {
    /// Call `id` of the caller whose subtree is `root`: `<root>/calls/<id>`.
    ///
    /// # Errors
    /// When `id` is not one path segment.
    pub fn new(root: &StreamPath, id: &str) -> Result<Self, Error> {
        if id.is_empty() || id.contains('/') {
            return Err(Error::Malformed("call id is not one path segment"));
        }
        let log = StreamPath::new(format!("{root}/{CALLS}/{id}")).map_err(Error::Stream)?;
        let settled = StreamPath::new(format!("{log}/{SETTLED}")).map_err(Error::Stream)?;
        let accepted = StreamPath::new(format!("{log}/{ACCEPTED}")).map_err(Error::Stream)?;
        Ok(Self {
            log,
            settled,
            accepted,
        })
    }

    /// The journal.
    #[must_use]
    pub const fn log(&self) -> &StreamPath {
        &self.log
    }

    /// The marker every terminal creates.
    #[must_use]
    pub const fn settled(&self) -> &StreamPath {
        &self.settled
    }

    /// The marker created atomically with the sole executor acceptance.
    #[must_use]
    pub const fn accepted(&self) -> &StreamPath {
        &self.accepted
    }

    /// The caller's request and hold, in the same journal and commit. Join
    /// this step with the original caller escrow debit and request delivery.
    /// The hold conveys no execution authority and survives every terminal.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn request(&self, body: OwnedBytes, hold: crate::escrow::Escrow) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: vec![CommitCondition::Tail {
                path: self.log.clone(),
                expected: 0,
            }],
            mutations: vec![CommitMutation::Append {
                path: self.log.clone(),
                records: vec![
                    record(b"request", body, None)?,
                    record(b"hold", hold.encode().into(), None)?,
                ],
            }],
        })
    }

    /// Exclusive acceptance, under the accepted and terminal absence guards.
    /// `requested` selects a journal request and hold; otherwise the original
    /// request is on the executor's own queue and this journal must be absent.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn accept(&self, accepted: &Accepted, requested: bool) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: [
                if requested {
                    CommitCondition::Present {
                        path: self.log.clone(),
                    }
                } else {
                    CommitCondition::Absent {
                        path: self.log.clone(),
                    }
                },
                CommitCondition::Absent {
                    path: self.accepted.clone(),
                },
            ]
            .into_iter()
            .chain(self.open(accepted.fence.as_ref()))
            .collect(),
            mutations: vec![
                append(
                    &self.log,
                    record(
                        b"accepted",
                        accepted.executor.clone(),
                        accepted.fence.as_ref(),
                    )?,
                ),
                append(&self.accepted, AppendRecord::from(OwnedBytes::default())),
            ],
        })
    }

    /// One chunk from the executor that accepted under `fence`.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn chunk(&self, fence: Option<&StreamPath>, body: OwnedBytes) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: self.open(fence).collect(),
            mutations: vec![append(&self.log, record(b"chunk", body, fence)?)],
        })
    }

    /// The call's one terminal, written by whichever hop ends it. An executor
    /// passes the fence it accepted under; any other hop passes `None`.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn settle(&self, fence: Option<&StreamPath>, outcome: &Outcome) -> Result<Commit, Error> {
        self.settle_with_attributes(fence, outcome, std::iter::empty())
    }

    /// Settle with executor-defined receipt metadata while retaining the result
    /// bytes as the terminal value. Reserved call attributes cannot be replaced.
    ///
    /// # Errors
    /// When the record or its attributes cannot be built.
    pub fn settle_with_attributes(
        &self,
        fence: Option<&StreamPath>,
        outcome: &Outcome,
        attributes: impl IntoIterator<Item = Attribute>,
    ) -> Result<Commit, Error> {
        self.settle_body(fence, outcome.kind(), outcome.body(), attributes)
    }

    fn settle_body(
        &self,
        fence: Option<&StreamPath>,
        kind: &'static [u8],
        body: OwnedBytes,
        attributes: impl IntoIterator<Item = Attribute>,
    ) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: self.open(fence).collect(),
            mutations: vec![
                append(
                    &self.log,
                    record_with_attributes(kind, body, fence, attributes)?,
                ),
                append(&self.settled, AppendRecord::from(OwnedBytes::from(kind))),
            ],
        })
    }

    /// Cancels the call in one commit, never waiting for its executor. It
    /// fails its condition only when the call has already ended.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn cancel(&self, reason: &str) -> Result<Commit, Error> {
        self.settle_body(
            None,
            b"cancelled",
            OwnedBytes::from(reason),
            std::iter::empty(),
        )
    }

    fn open(&self, fence: Option<&StreamPath>) -> impl Iterator<Item = CommitCondition> {
        std::iter::once(CommitCondition::Absent {
            path: self.settled.clone(),
        })
        .chain(fence.map(|path| CommitCondition::Absent { path: path.clone() }))
    }
}

fn append(path: &StreamPath, record: AppendRecord) -> CommitMutation {
    CommitMutation::Append {
        path: path.clone(),
        records: vec![record],
    }
}

fn record(
    kind: &'static [u8],
    value: OwnedBytes,
    fence: Option<&StreamPath>,
) -> Result<AppendRecord, Error> {
    record_with_attributes(kind, value, fence, std::iter::empty())
}

fn record_with_attributes(
    kind: &'static [u8],
    value: OwnedBytes,
    fence: Option<&StreamPath>,
    attributes: impl IntoIterator<Item = Attribute>,
) -> Result<AppendRecord, Error> {
    let mut attributes = std::iter::once(Ok(Attribute {
        name: KIND.into(),
        value: OwnedBytes::from(kind),
    }))
    .chain(fence.map(|fence| {
        Ok(Attribute {
            name: FENCE.into(),
            value: OwnedBytes::from(fence.as_str()),
        })
    }))
    .chain(attributes.into_iter().map(|attribute| {
        if matches!(attribute.name.as_str(), KIND | FENCE) {
            Err(Error::Malformed(
                "executor metadata replaces a call attribute",
            ))
        } else {
            Ok(attribute)
        }
    }))
    .collect::<Result<Vec<_>, _>>()?;
    attributes.sort_unstable_by(|left, right| left.name.cmp(&right.name));
    Ok(AppendRecord {
        value,
        headers: RecordHeaders::new(Vec::new(), attributes).map_err(Error::Stream)?,
    })
}

fn attribute<'a>(record: &'a Record, name: &str) -> Option<&'a OwnedBytes> {
    let attributes = record.headers.attributes();
    let index = attributes
        .binary_search_by(|attribute| attribute.name.cmp(name))
        .ok()?;
    attributes.get(index).map(|attribute| attribute.value)
}

/// The fence a record was written under, if any.
///
/// # Errors
/// When the record names a fence that is not a path.
pub fn fence(record: &Record) -> Result<Option<StreamPath>, Error> {
    attribute(record, FENCE)
        .map(|fence| {
            std::str::from_utf8(fence)
                .ok()
                .and_then(|fence| StreamPath::new(fence).ok())
                .ok_or(Error::Malformed("call fence is not a path"))
        })
        .transpose()
}

/// Acceptance validated the path once; later records compare borrowed bytes.
fn session_matches(accepted: &Accepted, fence: Option<&OwnedBytes>) -> bool {
    fence.map(AsRef::as_ref)
        == accepted
            .fence
            .as_ref()
            .map(|fence| fence.as_str().as_bytes())
}

fn reason(value: &OwnedBytes) -> Result<String, Error> {
    std::str::from_utf8(value)
        .map(str::to_owned)
        .map_err(|_| Error::Malformed("call reason is not UTF-8"))
}

impl Entry {
    /// One journal record, before the journal's order is applied.
    ///
    /// # Errors
    /// When the record is no call record.
    pub fn decode(record: &Record) -> Result<Self, Error> {
        let kind = attribute(record, KIND).ok_or(Error::Malformed("record is no call record"))?;
        let value = &record.value;
        Ok(match kind.as_ref() {
            b"request" => Self::Request(value.clone()),
            b"hold" => Self::Hold(
                crate::escrow::Escrow::decode(value)
                    .map_err(|_| Error::Malformed("invalid call hold"))?,
            ),
            b"accepted" => Self::Accepted(Accepted {
                executor: value.clone(),
                fence: fence(record)?,
            }),
            b"chunk" => Self::Chunk(value.clone()),
            b"ok" => Self::Terminal(Outcome::Ok(value.clone())),
            b"error" => Self::Terminal(Outcome::Error(reason(value)?)),
            b"cancelled" => Self::Terminal(Outcome::Cancelled(reason(value)?)),
            b"indeterminate" => Self::Terminal(Outcome::Indeterminate(reason(value)?)),
            _ => return Err(Error::Malformed("unknown call record kind")),
        })
    }
}

/// One committed journal record: its sequence and the commit that wrote it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Committed {
    /// Sequence in the journal.
    pub sequence: u64,
    /// The commit that introduced it.
    pub commit_id: CommitId,
}

impl Committed {
    const fn of(record: &Record) -> Self {
        Self {
            sequence: record.sequence,
            commit_id: record.commit_id,
        }
    }
}

/// The original committed receipt of one call: its request record, when the
/// caller wrote one, and its one terminal record. Session loss is recorded
/// in that same journal; a fence alone is not a call terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// The call's journal.
    pub log: StreamPath,
    /// The committed request.
    pub request: Option<Committed>,
    /// The committed terminal.
    pub terminal: Committed,
    /// How the call ended, as the caller reads it.
    pub outcome: Outcome,
}

/// Borrowed original receipt from the call reader, without copying result bytes
/// or failure text. The owned [`Receipt`] uses this same assembly boundary.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ReceiptRef<'a> {
    /// The call's journal.
    pub log: &'a StreamPath,
    /// The committed request, when present.
    pub request: Option<Committed>,
    /// The original committed terminal.
    pub terminal: Committed,
    /// The original reader's settled disposition.
    pub outcome: &'a Outcome,
}

impl From<ReceiptRef<'_>> for Receipt {
    fn from(receipt: ReceiptRef<'_>) -> Self {
        Self {
            log: receipt.log.clone(),
            request: receipt.request,
            terminal: receipt.terminal,
            outcome: receipt.outcome.clone(),
        }
    }
}

/// A reader's fold of one original journal, applied record by record in order.
/// Inherited records retain their original append destination; they cannot
/// authorize recovery or label receipts for another call.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Journal {
    origin: Option<StreamPath>,
    request: Option<Committed>,
    hold: Option<crate::escrow::Escrow>,
    accepted: Option<Accepted>,
    acceptance: Option<Committed>,
    terminal: Option<Committed>,
    outcome: Option<Outcome>,
    next: u64,
}

impl Journal {
    /// Applies the journal's next record and returns it as the caller reads
    /// it: a cancel after acceptance reads as `indeterminate`.
    ///
    /// # Errors
    /// When the record is out of order or breaks the protocol, including a
    /// different original journal or acceptance session.
    pub fn apply(&mut self, record: &Record) -> Result<Entry, Error> {
        if record.sequence != self.next {
            return Err(Error::Malformed("call record out of order"));
        }
        if self.outcome.is_some() {
            return Err(Error::Malformed("call record after its terminal"));
        }
        if self
            .origin
            .as_ref()
            .is_some_and(|origin| origin != &record.origin)
        {
            return Err(Error::Malformed("record belongs to another call journal"));
        }
        let first = self.next == u64::from(self.request.is_some()) + u64::from(self.hold.is_some());
        let entry = match Entry::decode(record)? {
            Entry::Request(body) if self.next == 0 => {
                self.request = Some(Committed::of(record));
                Entry::Request(body)
            }
            Entry::Request(_) => return Err(Error::Malformed("call request is not first")),
            Entry::Hold(hold)
                if self.next == 1
                    && self
                        .request
                        .is_some_and(|request| request.commit_id == record.commit_id) =>
            {
                self.hold = Some(hold);
                Entry::Hold(hold)
            }
            Entry::Hold(_) => {
                return Err(Error::Malformed(
                    "call hold is not committed with its request",
                ));
            }
            Entry::Accepted(accepted)
                if first && (self.request.is_none() || self.hold.is_some()) =>
            {
                self.accepted = Some(accepted.clone());
                self.acceptance = Some(Committed::of(record));
                Entry::Accepted(accepted)
            }
            Entry::Accepted(_) => return Err(Error::Malformed("call accepted twice")),
            Entry::Chunk(body) => {
                let accepted = self
                    .accepted
                    .as_ref()
                    .ok_or(Error::Malformed("call chunk before its acceptance"))?;
                if !session_matches(accepted, attribute(record, FENCE)) {
                    return Err(Error::Malformed("call chunk from another session"));
                }
                Entry::Chunk(body)
            }
            Entry::Terminal(outcome) => {
                if self.request.is_some() && self.hold.is_none() {
                    return Err(Error::Malformed("call request has no committed hold"));
                }
                if let Some(fence) = attribute(record, FENCE)
                    && !self
                        .accepted
                        .as_ref()
                        .is_some_and(|accepted| session_matches(accepted, Some(fence)))
                {
                    return Err(Error::Malformed("call terminal from another session"));
                }
                let outcome = match outcome {
                    Outcome::Cancelled(reason) if self.accepted.is_some() => {
                        Outcome::Indeterminate(format!("cancelled after acceptance: {reason}"))
                    }
                    outcome => outcome,
                };
                self.terminal = Some(Committed::of(record));
                self.outcome = Some(outcome.clone());
                Entry::Terminal(outcome)
            }
        };
        if self.origin.is_none() {
            self.origin = Some(record.origin.clone());
        }
        self.next += 1;
        Ok(entry)
    }

    /// Publish the original accepted session's loss as the sole terminal.
    /// The accepted marker must still name the original acceptance commit,
    /// and that acceptance's own fence must exist. A live session refuses;
    /// a racing terminal wins its original absence guard. Retain this exact
    /// publication and identity across an unknown result; never redispatch.
    /// The immutable hold remains until the executor host reconciles spend.
    ///
    /// # Errors
    /// When the acceptance has no session fence or the terminal cannot be built.
    pub fn recover(&self, call: &CallPath, reason: &str) -> Result<Option<Commit>, Error> {
        if self.outcome.is_some() {
            return Ok(None);
        }
        let Some(accepted) = &self.accepted else {
            return Ok(None);
        };
        if self.origin.as_ref() != Some(call.log()) {
            return Err(Error::Malformed("recovery belongs to another call journal"));
        }
        let fence = accepted
            .fence
            .as_ref()
            .ok_or(Error::Malformed("acceptance has no recovery fence"))?;
        let acceptance = self
            .acceptance
            .ok_or(Error::Malformed("acceptance has no committed record"))?;
        let mut commit = call.settle_body(
            None,
            b"indeterminate",
            OwnedBytes::from(reason),
            std::iter::empty(),
        )?;
        commit.conditions.extend([
            CommitCondition::Head {
                path: call.accepted.clone(),
                expected: 1,
                commit_id: acceptance.commit_id,
            },
            CommitCondition::Present {
                path: fence.clone(),
            },
        ]);
        Ok(Some(commit))
    }

    /// The call's committed receipt, once settled, at its original destination.
    #[must_use]
    pub fn receipt(&self, log: &StreamPath) -> Option<Receipt> {
        self.receipt_ref(log).map(Receipt::from)
    }

    /// Borrows the original committed receipt, once settled, with no allocation
    /// or copy. Consumers must use this reader's disposition rather than decode
    /// a cancellation terminal in isolation and lose its acceptance context.
    #[must_use]
    pub fn receipt_ref<'a>(&'a self, log: &'a StreamPath) -> Option<ReceiptRef<'a>> {
        if self.origin.as_ref() != Some(log) {
            return None;
        }
        let outcome = self.outcome.as_ref()?;
        Some(ReceiptRef {
            log,
            request: self.request,
            terminal: self.terminal?,
            outcome,
        })
    }

    /// The acceptance, once applied.
    #[must_use]
    pub const fn accepted(&self) -> Option<&Accepted> {
        self.accepted.as_ref()
    }

    /// The original caller's immutable hold. Settlement never refunds it.
    #[must_use]
    pub const fn hold(&self) -> Option<crate::escrow::Escrow> {
        self.hold
    }

    /// The call's outcome, once settled.
    #[must_use]
    pub const fn outcome(&self) -> Option<&Outcome> {
        self.outcome.as_ref()
    }

    /// Sequence of the next record to apply.
    #[must_use]
    pub const fn next(&self) -> u64 {
        self.next
    }
}

#[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod model;

/// Decoder fuzz entry point shared with the original committed-record controls.
#[cfg(all(any(test, feature = "test-support"), not(target_arch = "wasm32")))]
#[doc(hidden)]
pub mod codec_fuzz;
