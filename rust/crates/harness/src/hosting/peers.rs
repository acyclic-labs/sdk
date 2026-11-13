//! Account rows share the existing session command and encoded page credit.
use super::*;
use acyclic_harness_client::PeerSession;
use acyclic_stream::StreamProvider;
use base64::{Engine as _, engine::general_purpose::STANDARD};

/// Host-owned account connection, local or remote through the same serving API.
/// A network binding uses an authenticated Streams client; no credential is
/// serialized in directory records or returned to the requesting session.
pub struct PeerDirectory {
    /// Provider already admitted to the account's public directory and presence.
    pub provider: Arc<dyn StreamProvider>,
    /// The account's canonical routing and node-presence subtree.
    pub paths: acyclic_actors::route::Paths,
}

impl<P, O> Server<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    pub(super) async fn account_peers(
        &self,
        directory: &PeerDirectory,
        requester: &AgentPath,
        page: &AgentPageRequest,
    ) -> Result<AgentReply, Error> {
        let requester = self.bound.agents().actor(requester)?;
        let cursor = page
            .cursor()
            .map(|cursor| {
                STANDARD
                    .decode(cursor)
                    .map_err(|_| Error::Record("invalid peer cursor"))
            })
            .transpose()?;
        let selected = crate::agent::SessionPublication::peers_page(
            directory.provider.as_ref(),
            &directory.paths,
            &requester,
            &self.cursor_key,
            cursor.as_deref(),
            page.items(),
        )
        .await?;
        let mut credit = PageCredit::new(page, |next| AgentReply::Children {
            items: Vec::new(),
            next,
        })?;
        let mut items = Vec::new();
        for (index, peer) in selected.peers().iter().enumerate() {
            let item = ChildAgent {
                agent: AgentPath::root(&crate::agent::AgentId::try_from(
                    peer.actor.id().to_owned(),
                )?),
                status: None,
                created_at_micros: None,
                updated_at_micros: None,
                peer: Some(PeerSession {
                    address: acyclic_auth::encode_component(&peer.actor.encode()),
                    node: acyclic_auth::encode_component(peer.row.node.as_bytes()),
                    profile: peer
                        .row
                        .profile
                        .clone()
                        .ok_or(Error::Record("peer row was removed"))?,
                    rev: peer.row.rev,
                    online: peer.online,
                }),
            };
            let next_bytes = selected
                .cursor_bytes(index + 1)?
                .map(|bytes| (bytes.div_ceil(3) * 4) as u64);
            if !credit.accept(&item, next_bytes)? {
                break;
            }
            items.push(item);
        }
        let (_, next) = selected.finish(items.len(), &self.cursor_key)?;
        let empty = items.is_empty();
        let reply = AgentReply::Children {
            items,
            next: next.map(|next| STANDARD.encode(next)),
        };
        // A fully filtered scan still owes its confidential continuation bytes.
        if empty
            && !acyclic_harness_client::encoded_agent_reply_size(page_json_bytes(&reply)?)
                .is_ok_and(|bytes| bytes <= u64::from(page.bytes()))
        {
            return Err(Error::Stream(StreamError::Capacity));
        }
        Ok(reply)
    }
}
