//! Sender-local routing context; credentials remain in the owning host.
use super::{AgentPath, AgentRecord, Error, PeerAdmission};
use acyclic_actors::{addressing::ActorReference, route};

pub(super) mod notices;
pub use notices::{PeerRejectionRoute, PeerResultRoute};
mod account;
pub use account::PeerAccountPolicy;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

pub(super) const OUTBOX: &str = "acyclic.actors.route.outbox.v1";
pub(super) const SOURCE_INTENT: &str = "acyclic.actors.route.source-intent.v1";
pub(super) const ADMISSION: &str = "acyclic.harness.peer-admission.v1";
const SENDER: &str = "acyclic.actors.route.sender.v1";

impl AgentRecord for PeerAdmission {}

fn qualified_intent(
    kind: &str,
    sender: &ActorReference,
    to: &ActorReference,
    id: &super::IntentId,
) -> Result<acyclic_stream::IdempotencyKey, Error> {
    let mut hash = blake3::Hasher::new_derive_key("acyclic.harness.routed-intent.v1");
    for part in [
        kind.as_bytes(),
        to.account().as_bytes(),
        to.namespace().as_str().as_bytes(),
        to.id().as_bytes(),
        sender.account().as_bytes(),
        sender.namespace().as_str().as_bytes(),
        sender.id().as_bytes(),
        id.as_str().as_bytes(),
    ] {
        hash.update(&(part.len() as u64).to_le_bytes());
        hash.update(part);
    }
    acyclic_stream::IdempotencyKey::new(hash.finalize().to_hex().as_bytes().to_vec())
        .map_err(Error::Stream)
}

/// Routing subtree and source identity admitted by this node's host.
#[derive(Clone, Debug)]
pub struct Routing {
    /// Account routing subtree on the sender's local provider.
    pub paths: route::Paths,
    /// This node's identity, bound by the authenticated account connection.
    pub node: route::NodeId,
}

pub(super) fn error(error: route::Error) -> Error {
    match error {
        route::Error::Stream(error) => Error::Stream(error),
        route::Error::Actor(error) => Error::Actor(Box::new(error)),
        _ => Error::Record("invalid route record"),
    }
}

fn account_error(error: Error) -> route::Error {
    match error {
        Error::Stream(error) => route::Error::Stream(error),
        Error::Actor(error) => route::Error::Actor(*error),
        _ => route::Error::InvalidRecord,
    }
}

fn inbox_append(
    envelope: &route::Envelope,
) -> Result<(&acyclic_stream::StreamPath, &acyclic_stream::AppendRecord), Error> {
    let [effect] = envelope.request.effects.as_slice() else {
        return Err(Error::Record("peer route is not one inbox effect"));
    };
    let [acyclic_stream::CommitMutation::Append { path, records }] = effect.mutations.as_slice()
    else {
        return Err(Error::Record("peer route is not one inbox append"));
    };
    let [record] = records.as_slice() else {
        return Err(Error::Record("peer route is not one input"));
    };
    Ok((path, record))
}

fn is_message(envelope: &route::Envelope) -> Result<bool, Error> {
    Ok(inbox_append(envelope)?
        .1
        .headers
        .attributes()
        .iter()
        .any(|attribute| attribute.name == ADMISSION))
}

impl Routing {
    /// Prepare one durable sender-local publication for an admitted peer send.
    /// Commit this hard contribution with the sender's original intent, alongside
    /// its journal effect when there is one. The outbox and receipt must never
    /// be published separately. Preparation performs no IO or authorization.
    pub fn publication(
        &self,
        sender: ActorReference,
        to: ActorReference,
        input: &super::InboxRecord,
        admission: &PeerAdmission,
    ) -> Result<acyclic_stream::CommitEffect, Error> {
        use acyclic_stream::{
            AppendRecord, Attribute, CommitCondition, CommitEffect, CommitMutation, RecordHeaders,
        };
        let envelope = PeerRoute::prepare(self.node.clone(), sender, to, input, admission)?;
        let (super::InboxRecord::Message(message) | super::InboxRecord::FollowUp(message)) = input
        else {
            unreachable!()
        };
        let super::Sender::Agent { path } = &message.from else {
            unreachable!()
        };
        let marker = path.sent_message(&message.id)?;
        let outbox = self.paths.outbox(&self.node).map_err(Error::Stream)?;
        let record = envelope.outbox_record(&self.paths).map_err(error)?;
        let intent = record
            .headers
            .keys()
            .first()
            .ok_or(Error::Record("routed publication has no original key"))?
            .clone();
        let mut receipt: AppendRecord = super::sent::SentRecord {
            to: super::AgentAddress::qualified(envelope.to)?,
            id: message.id.clone(),
        }
        .encode()?
        .into();
        receipt.headers = RecordHeaders::new(
            Vec::new(),
            vec![
                Attribute {
                    name: OUTBOX.into(),
                    value: outbox.as_str().as_bytes().to_vec().into(),
                },
                Attribute {
                    name: SOURCE_INTENT.into(),
                    value: intent,
                },
            ],
        )
        .map_err(Error::Stream)?;
        Ok(CommitEffect::hard(
            vec![CommitCondition::Absent {
                path: marker.clone(),
            }],
            vec![
                CommitMutation::Append {
                    path: outbox,
                    records: vec![record],
                },
                CommitMutation::Append {
                    path: marker,
                    records: vec![receipt],
                },
            ],
        ))
    }

    /// Drive one accepted link using this node's agent journal and inbox writer.
    /// The connection owner supplies its authenticated account provider and
    /// credential-selecting publication callbacks. Dropping this future drops
    /// the original pumps; their committed cursors survive reconnect.
    pub async fn run_link<P, A, R, F>(
        &self,
        link: route::NodeLink,
        host: &super::AgentHost<P>,
        account: &A,
        publish: R,
        admit: F,
    ) -> Result<std::convert::Infallible, route::Error>
    where
        P: crate::store::JournalProvider + ?Sized + 'static,
        A: acyclic_stream::StreamProvider + ?Sized,
        R: AsyncFnMut(route::Publication) -> Result<(), route::Error>,
        F: AsyncFnMut(route::Envelope) -> Result<route::Routed, route::Error>,
    {
        route::run_link(
            link,
            host.provider().as_ref(),
            account,
            &self.paths,
            &self.node,
            publish,
            admit,
            async |record| {
                host.deliver_routed_peer(self, &record)
                    .await
                    .map(|_| ())
                    .map_err(account_error)
            },
        )
        .await
    }

    /// Lower one sender publication, staging only targets known to be remote.
    /// The host determines locality through its actor store, never directory
    /// presence or a caller-supplied network location. All other effects retain
    /// the ordinary lowering path.
    pub fn lower(
        &self,
        within: &ActorReference,
        remote: &std::collections::BTreeSet<super::AgentAddress>,
        step: &super::Step,
        cursor: u64,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        super::lower::lower_routed(step, cursor, key, Some((self, within, remote)))
    }
}

impl<P: crate::store::JournalProvider + ?Sized + 'static> super::AgentHost<P> {
    /// Consume one record supplied by this node's authenticated account drain.
    /// The connection owner supplies its admitted routing context. This method
    /// grants no authority: it shares the canonical message and terminal
    /// validators, and every accepted input uses the ordinary inbox writer.
    /// A successful return includes any durable refusal needed before ack.
    pub async fn deliver_routed_peer(
        &self,
        routing: &Routing,
        record: &route::RelayRecord,
    ) -> Result<super::Delivered, Error> {
        if is_message(&record.envelope)? {
            return PeerRoute::parse(&record.envelope)?
                .deliver(self, routing, &record.receipt)
                .await;
        }
        notices::deliver_terminal(self, record).await
    }
}

/// An append-message request whose shape and attribution match its qualified
/// route. This is data validation, never a credential or account admission.
/// Account and destination hosts use the same validation before any provider IO.
pub struct PeerRoute<'a> {
    envelope: &'a route::Envelope,
    sender: AgentPath,
    recipient: AgentPath,
    id: super::IntentId,
    admission: PeerAdmission,
}

impl<'a> PeerRoute<'a> {
    /// Prepare the ordinary inbox request with its fully qualified source.
    /// The owning host must authorize both identities and the retained grant;
    /// this value confers no authority and performs no publication or IO.
    pub fn prepare(
        source: route::NodeId,
        sender: ActorReference,
        to: ActorReference,
        input: &super::InboxRecord,
        admission: &PeerAdmission,
    ) -> Result<route::Envelope, Error> {
        Ok(route::Envelope {
            source,
            request: Self::request(&sender, &to, input, admission)?,
            sender,
            to,
        })
    }

    fn request(
        sender: &ActorReference,
        to: &ActorReference,
        input: &super::InboxRecord,
        admission: &PeerAdmission,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        let (super::InboxRecord::Message(message) | super::InboxRecord::FollowUp(message)) = input
        else {
            return Err(Error::Record("peer route cannot carry control input"));
        };
        let recipient = AgentPath::root(&super::AgentId::try_from(to.id().to_owned())?);
        let super::Sender::Agent { path } = &message.from else {
            return Err(Error::Record("peer route requires agent attribution"));
        };
        if path.depth() != 0
            || sender.id() != path.id().as_str()
            || sender == to
            || sender.account() != to.account()
            || !admission.sender.enabled()
            || !admission.recipient.enabled()
        {
            return Err(Error::Record("peer route identity or membership differs"));
        }
        let mut request = super::commit::send(&recipient, &message.from, input)?;
        request.idempotency_key = qualified_intent("message", sender, to, &message.id)?;
        let acyclic_stream::CommitMutation::Append { records, .. } =
            &mut request.effects[0].mutations[0]
        else {
            unreachable!()
        };
        records[0].headers = Self::headers(sender, admission)?;
        Ok(request)
    }

    pub(super) fn headers(
        sender: &ActorReference,
        admission: &PeerAdmission,
    ) -> Result<acyclic_stream::RecordHeaders, Error> {
        acyclic_stream::RecordHeaders::new(
            Vec::new(),
            vec![
                acyclic_stream::Attribute {
                    name: SENDER.into(),
                    value: sender.encode().into(),
                },
                acyclic_stream::Attribute {
                    name: ADMISSION.into(),
                    value: admission.encode()?.into(),
                },
            ],
        )
        .map_err(Error::Stream)
    }

    /// Accept exactly one ordinary, root-to-root Message or FollowUp. Extra
    /// conditions, mutations, control records and substituted intent keys are
    /// refused; a node credential cannot turn an inbox grant into other access.
    pub fn parse(envelope: &'a route::Envelope) -> Result<Self, Error> {
        use super::{InboxRecord, Sender};
        let (path, record) = inbox_append(envelope)?;
        let input = InboxRecord::decode(&record.value)?;
        let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = &input else {
            return Err(Error::Record("peer route cannot carry control input"));
        };
        let Sender::Agent { path: sender } = &message.from else {
            return Err(Error::Record("peer route requires agent attribution"));
        };
        let recipient = AgentPath::root(&super::AgentId::try_from(envelope.to.id().to_owned())?);
        if sender.depth() != 0
            || envelope.sender == envelope.to
            || envelope.sender.account() != envelope.to.account()
            || envelope.sender.id().as_bytes() != sender.id().as_str().as_bytes()
            || path != &recipient.inbox()?
        {
            return Err(Error::Record("peer route attribution differs"));
        }
        let attributes = record.headers.attributes();
        if attributes.len() != 2 {
            return Err(Error::Record("peer route has no unique admission"));
        }
        let attribute = attributes
            .iter()
            .find(|attribute| attribute.name == ADMISSION)
            .ok_or(Error::Record("peer route has no admission"))?;
        if !record.headers.keys().is_empty() {
            return Err(Error::Record("peer route has foreign headers"));
        }
        let admission = PeerAdmission::decode(attribute.value)?;
        if !admission.sender.enabled() || !admission.recipient.enabled() {
            return Err(Error::Record(
                "peer route requires enabled membership intervals",
            ));
        }
        let expected = Self::request(&envelope.sender, &envelope.to, &input, &admission)?;
        if envelope.request != expected {
            return Err(Error::Record(
                "peer route differs from ordinary message delivery",
            ));
        }
        Ok(Self {
            envelope,
            sender: sender.clone(),
            recipient,
            id: message.id.clone(),
            admission,
        })
    }

    /// Root whose append-message authority the admitting host must verify.
    pub fn sender(&self) -> &AgentPath {
        &self.sender
    }

    /// Root whose current membership the account and owning node must check.
    pub fn recipient(&self) -> &AgentPath {
        &self.recipient
    }

    /// Caller-owned message intent, preserved through every routing stage.
    pub fn id(&self) -> &super::IntentId {
        &self.id
    }

    /// Retained membership versions, authenticated with the original request.
    pub fn admission(&self) -> &PeerAdmission {
        &self.admission
    }

    /// Deliver on the recipient's owning node through its ordinary inbox writer.
    /// The caller must have admitted the account relay and its sender grant.
    /// Only the recipient's local membership is read; the source may be offline.
    /// A refusal returns only after its terminal Notice is durable in this
    /// node's outbox, so the caller can then acknowledge the original relay.
    pub async fn deliver<P: crate::store::JournalProvider + ?Sized + 'static>(
        &self,
        host: &super::AgentHost<P>,
        routing: &Routing,
        receipt: &route::RelayReceipt,
    ) -> Result<super::Delivered, Error> {
        if host.actor(&self.recipient)? != self.envelope.to {
            return Err(Error::Record("peer route belongs to another actor host"));
        }
        let inbox = self.recipient.inbox()?;
        let mut known_present = false;
        let delivered = loop {
            let retained = self
                .admission
                .recipient
                .retained(host.provider().as_ref(), &self.recipient)
                .await;
            match retained {
                Ok(true) => match host
                    .deliver(&self.recipient, self.delivery_request(receipt)?)
                    .await
                {
                    Ok(delivered) => break delivered,
                    Err(Error::Stream(cause))
                        if cause.category() == acyclic_stream::StreamError::IdempotencyMismatch => {
                    }
                    Err(cause) => return Err(cause),
                },
                Ok(false) if !known_present => (),
                Ok(false) => return Err(Error::Record("peer membership record is missing")),
                Err(cause @ (Error::Record(_) | Error::UnsupportedRecordVersion(_))) => {
                    // A root created after a retained absence may have a
                    // different initial membership commit. Recover only an
                    // already-spent original intent; invalid fresh witnesses
                    // must not publish a refusal for a present root.
                    if host
                        .provider()
                        .inspect_idempotency(self.envelope.request.idempotency_key.clone())
                        .await
                        .map_err(Error::Stream)?
                        .is_none()
                    {
                        return Err(cause);
                    }
                }
                Err(cause) => return Err(cause),
            }
            match self
                .deliver_absent(host, routing, receipt, self.delivery_request(receipt)?)
                .await
            {
                Err(Error::Actor(cause))
                    if matches!(&*cause,
                        acyclic_actors::local::Error::PreconditionChanged(conflicts)
                        if conflicts.iter().any(|conflict| matches!(conflict,
                            acyclic_stream::CommitConflict::Exists { path } if path == &inbox
                        ))
                    ) =>
                {
                    // The conditional result, not a timer or readiness
                    // probe, proves creation won this race. Revalidate the
                    // now-present root; a still-missing witness is invalid.
                    known_present = true;
                }
                outcome => return outcome,
            }
        };
        if delivered == super::Delivered::Closed {
            // The drain may acknowledge only after the return publication is
            // durable. A crash between these commits replays the retained
            // refusal and this same result intent, never a new delivery.
            let result = self.result(
                routing.node.clone(),
                receipt,
                super::SentReplyStatus::Dropped,
            )?;
            let outcome = super::submit(
                host.provider().as_ref(),
                acyclic_stream::CommitRequest {
                    deadline_unix_millis: None,
                    idempotency_key: result.request.idempotency_key.clone(),
                    effects: vec![result.outbox_effect(&routing.paths).map_err(error)?],
                },
            )
            .await?;
            if !matches!(outcome, super::Submitted::Committed(_)) {
                return Err(Error::Record("peer refusal result was not published"));
            }
        }
        Ok(delivered)
    }

    fn delivery_request(
        &self,
        receipt: &route::RelayReceipt,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        let mut request = self.envelope.request.clone();
        let acyclic_stream::CommitMutation::Append { records, .. } =
            &mut request.effects[0].mutations[0]
        else {
            unreachable!()
        };
        // Preserve the account's original reference in the one inbox write,
        // so a later answer can prove its cause without a separate receipt log.
        let mut attributes = vec![receipt.attribute()];
        attributes.extend(
            records[0]
                .headers
                .attributes()
                .iter()
                .map(|attribute| attribute.to_owned()),
        );
        records[0].headers =
            acyclic_stream::RecordHeaders::new(Vec::new(), attributes).map_err(Error::Stream)?;
        request.effects[0]
            .conditions
            .extend(self.admission.recipient.conditions(&self.recipient)?);
        // A refused membership fence spends this original intent as a durable
        // skipped append. A crash before routing its Notice cannot later turn
        // that refusal into delivery after the user rejoins.
        request.effects[0].soft = true;
        Ok(request)
    }

    async fn deliver_absent<P: crate::store::JournalProvider + ?Sized + 'static>(
        &self,
        host: &super::AgentHost<P>,
        routing: &Routing,
        receipt: &route::RelayReceipt,
        request: acyclic_stream::CommitRequest,
    ) -> Result<super::Delivered, Error> {
        let request = self.absent_request(routing, receipt, request)?;
        match host.deliver(&self.recipient, request).await? {
            super::Delivered::Closed => Ok(super::Delivered::Closed),
            super::Delivered::Committed { .. } => {
                Err(Error::Record("absent peer accepted inbox input"))
            }
        }
    }
    fn absent_request(
        &self,
        routing: &Routing,
        receipt: &route::RelayReceipt,
        mut request: acyclic_stream::CommitRequest,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        let result = self.result(
            routing.node.clone(),
            receipt,
            super::SentReplyStatus::Dropped,
        )?;
        // The ordinary Present{inbox} effect is skipped under this hard absence
        // fence. Its original intent and return Notice commit together, so a
        // later creation cannot revive it or require a second refusal ledger.
        request.effects.push(acyclic_stream::CommitEffect::hard(
            vec![acyclic_stream::CommitCondition::Absent {
                path: self.recipient.inbox()?,
            }],
            Vec::new(),
        ));
        request
            .effects
            .push(result.outbox_effect(&routing.paths).map_err(error)?);
        Ok(request)
    }
}
