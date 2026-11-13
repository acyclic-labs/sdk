//! Sender-local routing context; credentials remain in the owning host.
use super::{AgentPath, AgentRecord, Error, PeerAdmission};
use acyclic_actors::{addressing::ActorReference, route};

pub(super) mod notices;
pub use notices::PeerResultRoute;

pub(super) const OUTBOX: &str = "acyclic.actors.route.outbox.v1";
pub(super) const ADMISSION: &str = "acyclic.harness.peer-admission.v1";

impl AgentRecord for PeerAdmission {}

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

impl Routing {
    /// Lower one sender publication, staging only targets known to be remote.
    /// The host determines locality through its actor store, never directory
    /// presence or a caller-supplied network location. All other effects retain
    /// the ordinary lowering path.
    pub fn lower(
        &self,
        within: &ActorReference,
        remote: &std::collections::BTreeSet<AgentPath>,
        step: &super::Step,
        cursor: u64,
        key: acyclic_stream::IdempotencyKey,
    ) -> Result<acyclic_stream::CommitRequest, Error> {
        super::lower::lower_routed(step, cursor, key, Some((self, within, remote)))
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
    /// Accept exactly one ordinary, root-to-root Message or FollowUp. Extra
    /// conditions, mutations, control records and substituted intent keys are
    /// refused; a node credential cannot turn an inbox grant into other access.
    pub fn parse(envelope: &'a route::Envelope) -> Result<Self, Error> {
        use super::{InboxRecord, Sender};
        use acyclic_stream::CommitMutation;
        let [effect] = envelope.request.effects.as_slice() else {
            return Err(Error::Record("peer route is not one inbox effect"));
        };
        let [CommitMutation::Append { path, records }] = effect.mutations.as_slice() else {
            return Err(Error::Record("peer route is not one inbox append"));
        };
        let [record] = records.as_slice() else {
            return Err(Error::Record("peer route is not one message"));
        };
        let input = InboxRecord::decode(&record.value)?;
        let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = &input else {
            return Err(Error::Record("peer route cannot carry control input"));
        };
        let Sender::Agent { path: sender } = &message.from else {
            return Err(Error::Record("peer route requires agent attribution"));
        };
        let recipient = AgentPath::root(&super::AgentId::try_from(envelope.to.id().to_owned())?);
        if sender.depth() != 0
            || sender == &recipient
            || envelope.sender.account() != envelope.to.account()
            || envelope.sender.namespace() != envelope.to.namespace()
            || envelope.sender.id().as_bytes() != sender.id().as_str().as_bytes()
            || path != &recipient.inbox()?
        {
            return Err(Error::Record("peer route attribution differs"));
        }
        let attributes = record.headers.attributes();
        if attributes.len() != 1 {
            return Err(Error::Record("peer route has no unique admission"));
        }
        let attribute = attributes
            .iter()
            .next()
            .ok_or(Error::Record("peer route has no admission"))?;
        if attribute.name != ADMISSION || !record.headers.keys().is_empty() {
            return Err(Error::Record("peer route has foreign headers"));
        }
        let admission = PeerAdmission::decode(attribute.value)?;
        let mut expected = super::commit::send(&recipient, &message.from, &input)?;
        let CommitMutation::Append { records, .. } = &mut expected.effects[0].mutations[0] else {
            unreachable!()
        };
        records[0].headers = record.headers.clone();
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
    pub async fn deliver<P: crate::store::JournalProvider + ?Sized + 'static>(
        &self,
        host: &super::AgentHost<P>,
        receipt: &route::RelayReceipt,
    ) -> Result<super::Delivered, Error> {
        if host.actor(&self.recipient)? != self.envelope.to {
            return Err(Error::Record("peer route belongs to another actor host"));
        }
        self.admission
            .recipient
            .validate(host.provider().as_ref(), &self.recipient)
            .await?;
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
        request.effects[0].conditions.push(
            self.admission
                .recipient
                .condition(self.recipient.peer_membership()?),
        );
        // A refused membership fence spends this original intent as a durable
        // skipped append. A crash before routing its Notice cannot later turn
        // that refusal into delivery after the user rejoins.
        request.effects[0].soft = true;
        host.deliver(&self.recipient, request).await
    }
}
