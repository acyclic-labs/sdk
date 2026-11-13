//! Bounded account views with one liveness observation per distinct visible node.
use super::{Error, SessionPublication, SessionRow};
use acyclic_actors::{
    addressing::ActorReference,
    route::{NodeId, NodePresence, Paths},
};
use acyclic_auth::cursor::CursorKey;
use acyclic_stream::{
    ChildTail, ChildrenPageRequest, HeadProjection, HeadsRequest, Revision, RevisionCut,
    StreamError, StreamPath, StreamProvider,
};
use std::collections::BTreeMap;

#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests;

/// Public metadata for one participating session, without file or control access.
#[derive(Clone, Debug)]
pub struct SessionPeer {
    position: StreamPath,
    /// Fully qualified root, retained across node and namespace boundaries.
    pub actor: ActorReference,
    /// Original published display metadata and immutable node identity.
    pub row: SessionRow,
    /// Latest socket presence at this page's liveness cut; absence is offline.
    pub online: bool,
}

/// One bounded scan. Continuations remain private until sealed for this caller.
pub struct SessionPage {
    peers: Vec<SessionPeer>,
    scanned: Option<StreamPath>,
    more: bool,
    context: Vec<u8>,
    /// Authority cut shared by the requester membership and every returned row.
    pub rows_at: Revision,
    /// Presence cut at least as new as the rows; absent for a fully filtered page.
    pub presence_at: Option<Revision>,
}

impl SessionPage {
    /// Immutable candidates for the host's existing encoded-byte credit logic.
    pub fn peers(&self) -> &[SessionPeer] {
        &self.peers
    }

    /// Exact sealed continuation length for a returned prefix, before encoding
    /// it for the transport. No encryption or path copy is needed to price it.
    pub fn cursor_bytes(&self, through: usize) -> Result<Option<usize>, Error> {
        Ok(self
            .boundary(through)?
            .map(|path| CursorKey::sealed_len(1 + path.as_str().len())))
    }

    /// Return the credited prefix and its confidential continuation. The host
    /// supplies its process-owned key, also used to open the next request.
    pub fn finish(
        mut self,
        through: usize,
        key: &CursorKey,
    ) -> Result<(Vec<SessionPeer>, Option<Vec<u8>>), Error> {
        let next = self
            .boundary(through)?
            .map(|path| {
                let mut plain = Vec::with_capacity(1 + path.as_str().len());
                plain.push(1);
                plain.extend_from_slice(path.as_str().as_bytes());
                key.seal(&self.context, &plain)
                    .map_err(|_| Error::Record("peer cursor encryption failed"))
            })
            .transpose()?;
        self.peers.truncate(through);
        Ok((self.peers, next))
    }

    fn boundary(&self, through: usize) -> Result<Option<&StreamPath>, Error> {
        if through > self.peers.len() {
            return Err(Error::Record("peer page prefix exceeds its candidates"));
        }
        if through < self.peers.len() {
            let index = through
                .checked_sub(1)
                .ok_or(Error::Stream(StreamError::Capacity))?;
            return Ok(Some(&self.peers[index].position));
        }
        if self.more {
            return self
                .scanned
                .as_ref()
                .map(Some)
                .ok_or(Error::Record("peer scan lost its continuation"));
        }
        Ok(None)
    }
}

impl SessionPublication {
    /// Query one account-directory scan after authenticating its caller's
    /// peer-directory grant. Requester membership and child row projections
    /// share one native view; only distinct live-row nodes need a second view.
    /// The host rechecks its current credential before publishing the response.
    ///
    /// Pages seek within the current hierarchy rather than freezing unrelated
    /// stream creation. The host restarts discovery when its directory event
    /// reports rows added before the cursor. This method creates no subscription.
    pub async fn peers_page<P: StreamProvider + ?Sized>(
        provider: &P,
        routes: &Paths,
        requester: &ActorReference,
        key: &CursorKey,
        cursor: Option<&[u8]>,
        limit: u32,
    ) -> Result<SessionPage, Error> {
        crate::agent::AgentId::try_from(requester.id().to_owned())?;
        let parent = Self::directory(requester.account())?;
        let mut context = b"acyclic.harness.peers.page.v1\0".to_vec();
        context.extend_from_slice(&requester.encode());
        let after = open_cursor(key, &context, &parent, cursor)?;
        let requester_path = Self::row_path(requester)?;
        let request = HeadsRequest {
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::History,
            paths: [requester_path.clone()].into_iter().collect(),
            children: Some(ChildrenPageRequest {
                parent: Some(parent.clone()),
                after,
                hierarchy_version: None,
                limit,
                tail_stream: Some(ChildTail::Child),
            }),
            limit: limit
                .checked_add(1)
                .ok_or(Error::Stream(StreamError::LimitExceeded))?,
            at_least: Default::default(),
        };
        let view = provider
            .heads(request.clone())
            .await
            .map_err(Error::Stream)?;
        view.validate_for(&request).map_err(Error::Stream)?;
        let mut heads = view.heads.into_iter();
        let head = heads
            .next()
            .flatten()
            .ok_or(Error::Record("peer discovery requires a member root"))?;
        let record = head.last.ok_or(Error::Record("requester row is empty"))?;
        if SessionRow::from_record(&requester_path, head.tail, &record)?
            .profile
            .is_none()
        {
            return Err(Error::Record("peer discovery requires an enabled root"));
        }
        let mut page = collect_rows(view.children, view.snapshot, requester, context)?;
        page.presence_at = observe_nodes(provider, routes, &mut page.peers, &page.rows_at).await?;
        Ok(page)
    }
}

fn collect_rows(
    children: Option<acyclic_stream::ChildrenPage>,
    rows_at: Revision,
    requester: &ActorReference,
    context: Vec<u8>,
) -> Result<SessionPage, Error> {
    let children = children.ok_or(Error::Stream(StreamError::InvalidResponse))?;
    let scanned = children
        .next_after
        .clone()
        .or_else(|| children.children.last().map(|child| child.path.clone()));
    let capacity = children.children.len();
    let mut page = SessionPage {
        peers: Vec::new(),
        scanned,
        more: children.next_after.is_some(),
        context,
        rows_at,
        presence_at: None,
    };
    for child in children.children {
        let record = child
            .tail_record
            .ok_or(Error::Record("session row has no projected head"))?;
        let row = SessionRow::from_record(
            &child.path,
            child
                .tail
                .ok_or(Error::Record("session row has no projected tail"))?,
            &record,
        )?;
        let name = child
            .path
            .as_str()
            .rsplit('/')
            .next()
            .ok_or(Error::Record("session row has no root identity"))?;
        let actor = ActorReference::decode(
            &acyclic_auth::decode_component(name)
                .map_err(|_| Error::Record("invalid session row identity"))?,
        )
        .map_err(|_| Error::Record("invalid session root"))?;
        if actor.account() != requester.account()
            || SessionPublication::row_path(&actor)? != child.path
        {
            return Err(Error::Record("session row belongs to another account"));
        }
        crate::agent::AgentId::try_from(actor.id().to_owned())?;
        if row.profile.is_some() && &actor != requester {
            // Acquire result custody only when a visible row exists.
            if page.peers.is_empty() {
                page.peers.reserve(capacity);
            }
            page.peers.push(SessionPeer {
                position: child.path,
                actor,
                row,
                online: false,
            });
        }
    }
    Ok(page)
}

fn open_cursor(
    key: &CursorKey,
    context: &[u8],
    parent: &StreamPath,
    cursor: Option<&[u8]>,
) -> Result<Option<StreamPath>, Error> {
    cursor
        .map(|sealed| {
            let plain = key
                .open(context, sealed)
                .map_err(|_| Error::Record("invalid peer cursor"))?;
            if plain.first() != Some(&1) {
                return Err(Error::Record("unsupported peer cursor"));
            }
            let path = StreamPath::new(
                std::str::from_utf8(&plain[1..])
                    .map_err(|_| Error::Record("invalid peer cursor path"))?,
            )
            .map_err(Error::Stream)?;
            if path.as_str().rsplit_once('/').map(|(parent, _)| parent) != Some(parent.as_str()) {
                return Err(Error::Record("peer cursor belongs to another directory"));
            }
            Ok(path)
        })
        .transpose()
}

async fn observe_nodes<P: StreamProvider + ?Sized>(
    provider: &P,
    routes: &Paths,
    peers: &mut [SessionPeer],
    rows_at: &Revision,
) -> Result<Option<Revision>, Error> {
    let mut nodes = BTreeMap::<NodeId, bool>::new();
    for peer in peers.iter() {
        if !nodes.contains_key(&peer.row.node) {
            nodes.insert(peer.row.node.clone(), false);
        }
    }
    if nodes.is_empty() {
        return Ok(None);
    }
    let request = HeadsRequest {
        paths: nodes
            .keys()
            .map(|node| routes.presence(node))
            .collect::<Result<_, _>>()
            .map_err(Error::Stream)?,
        retained: Default::default(),
        durable_through: None,
        projection: HeadProjection::History,
        children: None,
        limit: u32::try_from(nodes.len()).map_err(|_| Error::Stream(StreamError::LimitExceeded))?,
        at_least: RevisionCut::of(rows_at.clone()),
    };
    let view = provider
        .heads(request.clone())
        .await
        .map_err(Error::Stream)?;
    view.validate_for(&request).map_err(Error::Stream)?;
    for ((node, online), (path, head)) in nodes
        .iter_mut()
        .zip(request.paths.into_iter().zip(view.heads))
    {
        if let Some(head) = head {
            let record = head.last.ok_or(Error::Record("node presence is empty"))?;
            *online = NodePresence::from_record(&record, &path, node)
                .map_err(crate::agent::routing::error)?
                .online;
        }
    }
    for peer in peers {
        peer.online = *nodes
            .get(&peer.row.node)
            .ok_or(Error::Record("session node was not observed"))?;
    }
    Ok(Some(view.snapshot))
}
