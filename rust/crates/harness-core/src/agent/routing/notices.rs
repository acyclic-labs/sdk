//! Terminal peer results use the ordinary inbox and its existing key index.
use super::{PeerRoute, route};
use crate::agent::{
    AgentPath, AgentRecord, Error, InboxRecord, IntentId, NoticePayload, Sender, SentReplyStatus,
    Watch,
};
use acyclic_stream::{
    AppendRecord, CommitMutation, CommitRequest, ReadKeySeek, ReadRequest, StreamProvider,
};
use futures::TryStreamExt as _;
use serde::{Deserialize, Serialize};

const KIND: &str = "harness.peer.result";

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ResultRecord {
    version: u32,
    id: IntentId,
    peer: AgentPath,
    status: SentReplyStatus,
    rejection: Option<route::Rejection>,
}

fn key(sender: &AgentPath, id: &IntentId) -> Result<Vec<u8>, Error> {
    Ok(
        crate::agent::intent_key("peer-result-index", &[sender.as_str(), id.as_str()])?
            .as_bytes()
            .to_vec(),
    )
}

impl ResultRecord {
    fn envelope(
        &self,
        node: route::NodeId,
        sender: acyclic_actors::addressing::ActorReference,
        to: acyclic_actors::addressing::ActorReference,
        target: &AgentPath,
        receipt: Option<&route::RelayReceipt>,
    ) -> Result<route::Envelope, Error> {
        Ok(route::Envelope {
            source: node,
            sender,
            to,
            request: self.request(target, receipt)?,
        })
    }

    fn request(
        &self,
        target: &AgentPath,
        receipt: Option<&route::RelayReceipt>,
    ) -> Result<CommitRequest, Error> {
        if self.version != 1
            || self.peer.depth() != 0
            || target.depth() != 0
            || target == &self.peer
            || matches!(self.status, SentReplyStatus::Pending)
            || (self.rejection.is_some() != receipt.is_none())
            || (self.rejection.is_some() && self.status != SentReplyStatus::Dropped)
        {
            return Err(Error::Record("invalid terminal peer result"));
        }
        let identity = crate::agent::intent_key(
            "peer-result",
            &[target.as_str(), self.peer.as_str(), self.id.as_str()],
        )?;
        let id = IntentId::try_from(
            std::str::from_utf8(identity.as_bytes())
                .map_err(|_| Error::Record("invalid result identity"))?,
        )?;
        let input = InboxRecord::Notice {
            id,
            notice: NoticePayload::Quiet(crate::conversation::wire::Notice {
                source: "harness".into(),
                kind: KIND.into(),
                body: serde_json::to_value(self)
                    .map_err(|_| Error::Record("peer result cannot encode"))?,
                wake: false,
                coalesce_key: None,
            }),
        };
        let mut request = crate::agent::send(
            target,
            &Sender::Host {
                host: "actors.route".into(),
            },
            &input,
        )?;
        let CommitMutation::Append { records, .. } = &mut request.effects[0].mutations[0] else {
            unreachable!()
        };
        let mut record = AppendRecord::latest(
            std::mem::take(&mut records[0].value),
            key(target, &self.id)?,
            false,
        )
        .map_err(Error::Stream)?;
        let mut attributes = receipt
            .map(route::RelayReceipt::attribute)
            .into_iter()
            .collect::<Vec<_>>();
        attributes.extend(
            record
                .headers
                .attributes()
                .iter()
                .map(|attribute| attribute.to_owned()),
        );
        record.headers =
            acyclic_stream::RecordHeaders::new(record.headers.keys().to_vec(), attributes)
                .map_err(Error::Stream)?;
        records[0] = record;
        Ok(request)
    }

    fn parse(
        envelope: &route::Envelope,
    ) -> Result<(Self, AgentPath, Option<route::RelayReceipt>), Error> {
        let (_, record) = super::inbox_append(envelope)?;
        let InboxRecord::Notice {
            notice: NoticePayload::Quiet(notice),
            ..
        } = InboxRecord::decode(&record.value)?
        else {
            return Err(Error::Record("result is not a quiet Notice"));
        };
        if notice.kind != KIND {
            return Err(Error::Record("result kind differs"));
        }
        let result: Self = serde_json::from_value(notice.body)
            .map_err(|_| Error::Record("invalid peer result"))?;
        let target = AgentPath::root(&crate::agent::AgentId::try_from(
            envelope.to.id().to_owned(),
        )?);
        let receipt = route::RelayReceipt::from_headers(&record.headers).map_err(super::error)?;
        if envelope.sender.account() != envelope.to.account()
            || envelope.sender.namespace() != envelope.to.namespace()
            || envelope.sender.id() != result.peer.id().as_str()
            || result.request(&target, receipt.as_ref())? != envelope.request
        {
            return Err(Error::Record("result differs from ordinary delivery"));
        }
        Ok((result, target, receipt))
    }
}

impl PeerRoute<'_> {
    /// Prepare a host result on the same outbox as all other routed input.
    /// The account host must verify node authority and the original receipt.
    pub fn result(
        &self,
        node: route::NodeId,
        receipt: &route::RelayReceipt,
        status: SentReplyStatus,
    ) -> Result<route::Envelope, Error> {
        let result = ResultRecord {
            version: 1,
            id: self.id.clone(),
            peer: self.recipient.clone(),
            status,
            rejection: None,
        };
        result.envelope(
            node,
            self.envelope.to.clone(),
            self.envelope.sender.clone(),
            &self.sender,
            Some(receipt),
        )
    }

    /// Construct an account refusal before a destination relay exists. The
    /// account must publish it with `Envelope::publish_rejection`; its retained
    /// original-request binding, rather than a fabricated destination receipt,
    /// proves the cause when the sender's node drains it.
    pub fn rejection(
        &self,
        node: route::NodeId,
        reason: route::Rejection,
    ) -> Result<route::Envelope, Error> {
        ResultRecord {
            version: 1,
            id: self.id.clone(),
            peer: self.recipient.clone(),
            status: SentReplyStatus::Dropped,
            rejection: Some(reason),
        }
        .envelope(
            node,
            self.envelope.to.clone(),
            self.envelope.sender.clone(),
            &self.sender,
            None,
        )
    }
}

/// The original host receipt of an accepted routed peer input. Parsing does
/// not create a result, so cancellation order can be decided before encoding.
pub(in crate::agent) struct PeerInput<'a> {
    peer: &'a AgentPath,
    sender: &'a AgentPath,
    id: &'a IntentId,
    receipt: route::RelayReceipt,
}

impl<'a> PeerInput<'a> {
    pub(in crate::agent) fn parse(
        agent: &'a AgentPath,
        source: &'a InboxRecord,
        record: &acyclic_stream::Record,
    ) -> Result<Option<Self>, Error> {
        let (InboxRecord::Message(message) | InboxRecord::FollowUp(message)) = source else {
            return Ok(None);
        };
        let Sender::Agent { path: sender } = &message.from else {
            return Ok(None);
        };
        if agent.depth() != 0
            || sender.depth() != 0
            || sender == agent
            || record.origin != agent.inbox()?
        {
            return Ok(None);
        }
        let Some(receipt) =
            route::RelayReceipt::from_headers(&record.headers).map_err(super::error)?
        else {
            return Ok(None);
        };
        Ok(Some(Self {
            peer: agent,
            sender,
            id: &message.id,
            receipt,
        }))
    }

    pub(in crate::agent) fn terminal(
        &self,
        routing: &super::Routing,
        within: &acyclic_actors::addressing::ActorReference,
        status: SentReplyStatus,
    ) -> Result<AppendRecord, Error> {
        let actor = |path: &AgentPath| {
            crate::agent::handler::Agents::<dyn crate::store::JournalProvider>::actor(
                within,
                &path.id(),
            )
        };
        Ok(ResultRecord {
            version: 1,
            id: self.id.clone(),
            peer: self.peer.clone(),
            status,
            rejection: None,
        }
        .envelope(
            routing.node.clone(),
            actor(self.peer)?,
            actor(self.sender)?,
            self.sender,
            Some(&self.receipt),
        )?
        .outbox_record(&routing.paths)
        .map_err(super::error)?)
    }
}

/// Host-result data validation; this does not confer a peer or node grant.
pub struct PeerResultRoute<'a> {
    envelope: &'a route::Envelope,
    target: AgentPath,
    result: ResultRecord,
    receipt: route::RelayReceipt,
}

/// Account refusal data, without a destination receipt. Only the authenticated
/// account relay's original admission binding can authorize its delivery.
pub struct PeerRejectionRoute<'a> {
    envelope: &'a route::Envelope,
    target: AgentPath,
    result: ResultRecord,
    reason: route::Rejection,
}

pub(super) async fn deliver_terminal<P: crate::store::JournalProvider + ?Sized + 'static>(
    host: &crate::agent::AgentHost<P>,
    record: &route::RelayRecord,
) -> Result<crate::agent::Delivered, Error> {
    let envelope = &record.envelope;
    let (result, target, receipt) = ResultRecord::parse(envelope)?;
    if let Some(receipt) = receipt {
        PeerResultRoute {
            envelope,
            target,
            result,
            receipt,
        }
        .deliver(host)
        .await
    } else {
        PeerRejectionRoute {
            envelope,
            target,
            reason: result
                .rejection
                .ok_or(Error::Record("result has no cause"))?,
            result,
        }
        .deliver(host, record)
        .await
    }
}

impl<'a> PeerRejectionRoute<'a> {
    /// Validate the canonical Dropped Notice. This is not an authority check.
    pub fn parse(envelope: &'a route::Envelope) -> Result<Self, Error> {
        let (result, target, _) = ResultRecord::parse(envelope)?;
        let reason = result
            .rejection
            .ok_or(Error::Record("result is not an account rejection"))?;
        Ok(Self {
            envelope,
            target,
            result,
            reason,
        })
    }

    /// Consume only an authenticated account relay, verifying its retained
    /// refusal against the exact sender-owned publication before any inbox write.
    /// A later opt-out cannot revoke the terminal of that original send.
    pub async fn deliver<P: crate::store::JournalProvider + ?Sized + 'static>(
        &self,
        host: &crate::agent::AgentHost<P>,
        record: &route::RelayRecord,
    ) -> Result<crate::agent::Delivered, Error> {
        if record.envelope != *self.envelope || host.actor(&self.target)? != self.envelope.to {
            return Err(Error::Record("rejection belongs to another host or relay"));
        }
        let original = crate::agent::sent::original_route(
            host.provider().as_ref(),
            &self.target,
            &self.result.id,
        )
        .await?;
        if record.rejection(&original).map_err(super::error)? != Some(self.reason)
            || original.sender != self.envelope.to
            || original.to != self.envelope.sender
            || PeerRoute::parse(&original)?.recipient() != &self.result.peer
        {
            return Err(Error::Record("rejection belongs to another send"));
        }
        host.deliver(&self.target, self.envelope.request.clone())
            .await
    }
}

impl<'a> PeerResultRoute<'a> {
    /// Accept one canonical quiet terminal Notice, never user or control input.
    pub fn parse(envelope: &'a route::Envelope) -> Result<Self, Error> {
        let (result, target, receipt) = ResultRecord::parse(envelope)?;
        let receipt = receipt.ok_or(Error::Record("result has no original relay"))?;
        Ok(Self {
            envelope,
            target,
            result,
            receipt,
        })
    }

    /// Verify the cause after authenticating this source node, before relay
    /// admission. The source can settle only a message addressed to its root.
    pub async fn verify_original<P: StreamProvider + ?Sized>(
        &self,
        account: &P,
        paths: &route::Paths,
    ) -> Result<(), Error> {
        let original = self
            .receipt
            .read(account, paths, &self.envelope.source)
            .await
            .map_err(super::error)?;
        let peer = PeerRoute::parse(&original)?;
        if original.sender != self.envelope.to
            || original.to != self.envelope.sender
            || peer.id() != &self.result.id
        {
            return Err(Error::Record("result belongs to another send"));
        }
        Ok(())
    }

    /// Deliver only from an already admitted account relay. Membership changes
    /// cannot revoke the terminal of this sender's original, owned message.
    pub async fn deliver<P: crate::store::JournalProvider + ?Sized + 'static>(
        &self,
        host: &crate::agent::AgentHost<P>,
    ) -> Result<crate::agent::Delivered, Error> {
        if host.actor(&self.target)? != self.envelope.to {
            return Err(Error::Record("result belongs to another host"));
        }
        let sent = crate::agent::sent(host.provider().as_ref(), &self.target, &self.result.id)
            .await?
            .ok_or(Error::Record("result has no owned send"))?;
        if sent.to != self.result.peer {
            return Err(Error::Record("result recipient differs"));
        }
        host.deliver(&self.target, self.envelope.request.clone())
            .await
    }
}

pub(in crate::agent) fn matches(
    record: &acyclic_stream::Record,
    sender: &AgentPath,
    id: &IntentId,
    peer: &AgentPath,
) -> Result<Option<SentReplyStatus>, Error> {
    if record.headers.keys().is_empty() {
        return Ok(None);
    }
    let wanted = key(sender, id)?;
    if !record
        .headers
        .keys()
        .iter()
        .any(|candidate| candidate.as_ref() == wanted.as_slice())
    {
        return Ok(None);
    }
    let InboxRecord::Notice {
        notice: NoticePayload::Quiet(notice),
        ..
    } = InboxRecord::decode(&record.value)?
    else {
        return Ok(None);
    };
    if notice.kind != KIND {
        return Ok(None);
    }
    let result: ResultRecord =
        serde_json::from_value(notice.body).map_err(|_| Error::Record("invalid peer result"))?;
    let receipt = route::RelayReceipt::from_headers(&record.headers).map_err(super::error)?;
    if result.version != 1
        || matches!(result.status, SentReplyStatus::Pending)
        || (result.rejection.is_some() != receipt.is_none())
        || (result.rejection.is_some() && result.status != SentReplyStatus::Dropped)
    {
        return Err(Error::Record("invalid terminal peer result"));
    }
    Ok((result.id == *id && result.peer == *peer).then_some(result.status))
}

pub(in crate::agent) async fn observe<P: StreamProvider + ?Sized>(
    provider: &P,
    sender: &AgentPath,
    id: &IntentId,
    peer: &AgentPath,
) -> Result<(SentReplyStatus, Option<Watch>), Error> {
    let path = sender.inbox()?;
    let mut records = provider
        .read(ReadRequest {
            path,
            from: 0,
            limit: 1,
            key: Some(ReadKeySeek::exact(key(sender, id)?, u64::MAX)),
            at_least: Default::default(),
        })
        .await
        .map_err(Error::Stream)?;
    let tail = records
        .selection
        .end
        .ok_or(Error::Stream(acyclic_stream::StreamError::InvalidResponse))?;
    if let Some(record) = records.try_next().await.map_err(Error::Stream)? {
        let status =
            matches(&record, sender, id, peer)?.ok_or(Error::Record("indexed result differs"))?;
        return Ok((status, None));
    }
    Ok((
        SentReplyStatus::Pending,
        Some(Watch::PeerReply {
            sender: sender.clone(),
            peer: peer.clone(),
            id: id.clone(),
            from: tail,
        }),
    ))
}
