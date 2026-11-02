//! `call`: the one invocation protocol for tools, model providers, devices,
//! Machines and agents (dispatch-and-yield).
//!
//! A call is one caller-owned journal, `<root>/calls/<id>`, and it is the
//! call's claim ledger. The caller delivers the request to its executor in
//! the caller's own claim commit, so a claimed call was sent exactly once and
//! an unclaimed one never. The caller does not wait: completion is a record
//! in the journal. The journal holds, in order:
//!
//! 0. optionally the caller's request, when the executor reads it here;
//! 1. the executor's acceptance, durable and exclusive (a `Tail` guard);
//! 2. the executor's ordered chunks;
//! 3. exactly one terminal: `ok`, `error`, `cancelled` or `indeterminate`.
//!
//! Every terminal is guarded by `Absent{<log>/settled}` and creates that
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
const SETTLED: &str = "settled";
const KIND: &str = "acyclic.call.v1";
const FENCE: &str = "acyclic.call.v1.fence";

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
        Ok(Self { log, settled })
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

    /// The caller's request as the journal's first record, for a call whose
    /// executor reads its request here. It commits with the caller's claim.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn request(&self, body: OwnedBytes) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: vec![CommitCondition::Tail {
                path: self.log.clone(),
                expected: 0,
            }],
            mutations: vec![append(&self.log, record(b"request", body, None)?)],
        })
    }

    /// Exclusive acceptance: only directly after the request (`requested`)
    /// or into an empty journal when the request was delivered on the
    /// executor's own queue, so never after another acceptance or a
    /// terminal, and never by an ended session.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn accept(&self, accepted: &Accepted, requested: bool) -> Result<Commit, Error> {
        Ok(Commit {
            conditions: std::iter::once(CommitCondition::Tail {
                path: self.log.clone(),
                expected: u64::from(requested),
            })
            .chain(self.open(accepted.fence.as_ref()))
            .collect(),
            mutations: vec![append(
                &self.log,
                record(
                    b"accepted",
                    accepted.executor.clone(),
                    accepted.fence.as_ref(),
                )?,
            )],
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
        Ok(Commit {
            conditions: self.open(fence).collect(),
            mutations: vec![
                append(
                    &self.log,
                    record_with_attributes(outcome.kind(), outcome.body(), fence, attributes)?,
                ),
                append(
                    &self.settled,
                    AppendRecord::from(OwnedBytes::from(outcome.kind())),
                ),
            ],
        })
    }

    /// Cancels the call in one commit, never waiting for its executor. It
    /// fails its condition only when the call has already ended.
    ///
    /// # Errors
    /// When the record cannot be built.
    pub fn cancel(&self, reason: &str) -> Result<Commit, Error> {
        self.settle(None, &Outcome::Cancelled(reason.to_owned()))
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
    String::from_utf8(value.to_vec()).map_err(|_| Error::Malformed("call reason is not UTF-8"))
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
/// caller wrote one, and its one terminal record. A call settled by its
/// fence has no terminal record; its fence is the receipt's terminal.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Receipt {
    /// The call's journal.
    pub log: StreamPath,
    /// The committed request.
    pub request: Option<Committed>,
    /// The committed terminal; `None` only for a fenced call.
    pub terminal: Option<Committed>,
    /// The fence that settled an accepted call with no terminal record.
    pub fence: Option<StreamPath>,
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
    /// The original terminal, absent for a fenced call.
    pub terminal: Option<Committed>,
    /// The original acceptance's settling fence, when no terminal exists.
    pub fence: Option<&'a StreamPath>,
    /// The original reader's settled disposition.
    pub outcome: &'a Outcome,
}

impl From<ReceiptRef<'_>> for Receipt {
    fn from(receipt: ReceiptRef<'_>) -> Self {
        Self {
            log: receipt.log.clone(),
            request: receipt.request,
            terminal: receipt.terminal,
            fence: receipt.fence.cloned(),
            outcome: receipt.outcome.clone(),
        }
    }
}

/// A reader's fold of one journal, applied record by record in order.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Journal {
    request: Option<Committed>,
    accepted: Option<Accepted>,
    terminal: Option<Committed>,
    outcome: Option<Outcome>,
    next: u64,
}

impl Journal {
    /// Applies the journal's next record and returns it as the caller reads
    /// it: a cancel after acceptance reads as `indeterminate`.
    ///
    /// # Errors
    /// When the record is out of order or breaks the protocol, including an
    /// executor chunk or terminal naming another acceptance session.
    pub fn apply(&mut self, record: &Record) -> Result<Entry, Error> {
        if record.sequence != self.next {
            return Err(Error::Malformed("call record out of order"));
        }
        if self.outcome.is_some() {
            return Err(Error::Malformed("call record after its terminal"));
        }
        let first = self.next == u64::from(self.request.is_some());
        let entry = match Entry::decode(record)? {
            Entry::Request(body) if self.next == 0 => {
                self.request = Some(Committed::of(record));
                Entry::Request(body)
            }
            Entry::Request(_) => return Err(Error::Malformed("call request is not first")),
            Entry::Accepted(accepted) if first => {
                self.accepted = Some(accepted.clone());
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
        self.next += 1;
        Ok(entry)
    }

    /// The acceptance's fence exists and every record committed before it has
    /// been applied: an accepted call with no terminal is `indeterminate`.
    pub fn fenced(&mut self, reason: &str) -> Option<&Outcome> {
        if self.outcome.is_none() && self.accepted.is_some() {
            self.outcome = Some(Outcome::Indeterminate(reason.to_owned()));
        }
        self.outcome.as_ref()
    }

    /// The call's committed receipt, once settled.
    #[must_use]
    pub fn receipt(&self, log: &StreamPath) -> Option<Receipt> {
        self.receipt_ref(log).map(Receipt::from)
    }

    /// Borrows the original committed receipt, once settled, with no allocation
    /// or copy. Consumers must use this reader's disposition rather than decode
    /// a cancellation terminal in isolation and lose its acceptance context.
    #[must_use]
    pub fn receipt_ref<'a>(&'a self, log: &'a StreamPath) -> Option<ReceiptRef<'a>> {
        let outcome = self.outcome.as_ref()?;
        Some(ReceiptRef {
            log,
            request: self.request,
            terminal: self.terminal,
            fence: self
                .terminal
                .is_none()
                .then(|| {
                    self.accepted
                        .as_ref()
                        .and_then(|accepted| accepted.fence.as_ref())
                })
                .flatten(),
            outcome,
        })
    }

    /// The acceptance, once applied.
    #[must_use]
    pub const fn accepted(&self) -> Option<&Accepted> {
        self.accepted.as_ref()
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

#[cfg(all(test, not(target_arch = "wasm32")))]
mod model;
