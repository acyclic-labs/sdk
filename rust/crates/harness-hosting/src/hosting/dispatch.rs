//! The bounded children and sessions page: one immutable continuation.
use super::*;

impl<P, O> Server<P, O>
where
    P: AuthorityProvider + ?Sized + 'static,
    O: AsyncObjectStore + 'static,
{
    /// One bounded page of direct children, or of the placement's sessions and
    /// peers, with the unchanged host continuation.
    pub(super) async fn dispatch_page(
        &self,
        command: &AgentCommand,
        page: &AgentPageRequest,
    ) -> std::result::Result<AgentReply, Error> {
        let agents = self.bound.agents();
        let sessions = matches!(command, AgentCommand::Sessions { .. });
        let peers = matches!(
            command,
            AgentCommand::Sessions {
                scope: acyclic_harness_client::SessionsScope::Peers,
                ..
            }
        );
        // The command ceiling is not a Stream request size. Return a
        // protocol-sized chunk with the existing immutable continuation,
        // reserving room for every row projection and the peer witness.
        let row_heads = if peers {
            3
        } else if sessions {
            2
        } else {
            1
        };
        let chunk = page
            .items()
            .min((acyclic_stream::MAX_ITEMS as u32 - u32::from(peers)) / row_heads);
        let requester = match command {
            AgentCommand::Sessions { requester, .. } => requester.as_ref().unwrap_or(&self.root),
            _ => &self.root,
        };
        if peers && requester.parent().is_some() {
            return Err(Error::Record("peer discovery requires a root requester"));
        }
        let parent = match command {
            AgentCommand::Children { agent, .. } => self.within_tree(agent)?.children()?,
            AgentCommand::Sessions { .. } if peers && self.platform.sessions() => {
                self.bound.peer_roots()?
            }
            AgentCommand::Sessions { .. } if self.platform.sessions() => {
                self.bound.session_roots()?
            }
            _ => return Err(Error::Record("this host serves one tree")),
        };
        use base64::{Engine as _, engine::general_purpose::STANDARD};
        let cursor_context = peers.then(|| format!("peer-page-v1\0{}\0{}", requester, parent));
        let mut credit = PageCredit::new(page, |next| AgentReply::Children {
            items: Vec::new(),
            next,
        })?;
        let (hierarchy_version, after) = match page.cursor() {
            Some(cursor) => {
                let decrypted;
                let cursor = if let Some(context) = &cursor_context {
                    let sealed = STANDARD
                        .decode(cursor)
                        .map_err(|_| Error::Record("invalid peer cursor"))?;
                    decrypted = self
                        .cursor_key
                        .open(context.as_bytes(), &sealed)
                        .map_err(|_| Error::Record("invalid peer cursor"))?;
                    std::str::from_utf8(&decrypted)
                        .map_err(|_| Error::Record("invalid peer cursor"))?
                } else {
                    cursor
                };
                let (version, after) = parse_children_cursor(cursor)?;
                (Some(version), Some(after))
            }
            None => (None, None),
        };
        let provider = agents.provider();
        let request = HeadsRequest {
            at: None,
            retained: Default::default(),
            durable_through: None,
            projection: HeadProjection::Watermarks,
            paths: Default::default(),
            children: Some(ChildrenPageRequest {
                offset: 0,
                tail_stream: None,
                parent: Some(parent),
                after,
                hierarchy_version,
                limit: chunk,
            }),
            limit: chunk,
            at_least: Default::default(),
        };
        let view = match provider.heads(request.clone()).await {
            Ok(view) => {
                view.validate_for(&request).map_err(Error::Stream)?;
                view
            }
            Err(error) if error.category() == StreamCategory::NotFound => {
                return Ok(AgentReply::Children {
                    items: Vec::new(),
                    next: None,
                });
            }
            Err(error) => return Err(Error::Stream(error)),
        };
        let listed = view
            .children
            .ok_or(Error::Stream(StreamError::InvalidResponse))?;
        let mut items = Vec::with_capacity(listed.children.len());
        let mut listed_paths = Vec::with_capacity(listed.children.len());
        let mut paths = acyclic_stream::HeadSet::with_capacity(listed.children.len());
        for child in listed.children {
            let agent = if matches!(command, AgentCommand::Sessions { .. }) {
                let id = child
                    .path
                    .as_str()
                    .rsplit('/')
                    .next()
                    .ok_or(Error::Record("session entry has no identity"))?;
                AgentPath::root(&crate::agent::AgentId::try_from(id.to_owned())?)
            } else {
                AgentPath::try_from(child.path.as_str().to_owned())?
            };
            listed_paths.push(child.path.clone());
            paths.push(agent.status()?);
            if sessions {
                paths.push(child.path);
            }
            if peers {
                paths.push(agent.peer_membership()?);
            }
            items.push(ChildAgent {
                agent,
                status: None,
                created_at_micros: None,
                updated_at_micros: None,
            });
        }
        let last_scanned = listed_paths.last().cloned();
        if !items.is_empty() || peers {
            if peers {
                paths.push(requester.peer_membership()?);
            }
            // Status is published outside each child's fork destination.
            // Observe its actual canonical path, at or after the page's
            // creation cut, in one bounded view for the returned rows.
            let limit =
                u32::try_from(paths.len()).map_err(|_| Error::Stream(StreamError::Capacity))?;
            let request = HeadsRequest {
                at: None,
                retained: Default::default(),
                durable_through: None,
                projection: HeadProjection::History,
                paths,
                children: None,
                limit,
                at_least: RevisionCut::of(view.snapshot),
            };
            let statuses = provider
                .heads(request.clone())
                .await
                .map_err(Error::Stream)?;
            statuses.validate_for(&request).map_err(Error::Stream)?;
            let mut heads = statuses.heads.into_iter();
            let mut eligible = Vec::with_capacity(items.len());
            for item in &mut items {
                let record = heads
                    .next()
                    .flatten()
                    .and_then(|head| head.last)
                    .ok_or(Error::Record("admitted child has no published status"))?;
                item.updated_at_micros = Some(record.committed_at_micros);
                item.status = Some(StatusRecord::decode(&record.value)?);
                if sessions {
                    let member = heads
                        .next()
                        .flatten()
                        .and_then(|head| head.last)
                        .ok_or(Error::Record("session has no directory admission"))?;
                    if member.sequence != 0 {
                        return Err(Error::Record(
                            "session directory admission is not immutable",
                        ));
                    }
                    item.created_at_micros = Some(member.committed_at_micros);
                }
                if peers {
                    let member = heads.next().flatten().and_then(|head| head.last);
                    eligible.push(
                        member
                            .map(|record| crate::agent::PeerMembership::decode(&record.value))
                            .transpose()?
                            .is_some_and(|member| member.enabled)
                            && &item.agent != requester,
                    );
                }
            }
            if peers {
                let member = heads.next().flatten().and_then(|head| head.last);
                if !member
                    .map(|record| crate::agent::PeerMembership::decode(&record.value))
                    .transpose()?
                    .is_some_and(|member| member.enabled)
                {
                    return Err(Error::Record("peer discovery requires an enabled root"));
                }
                let mut index = 0;
                items.retain(|_| {
                    let keep = eligible[index];
                    index += 1;
                    keep
                });
                let mut index = 0;
                listed_paths.retain(|_| {
                    let keep = eligible[index];
                    index += 1;
                    keep
                });
            }
        }
        for (index, item) in items.iter().enumerate() {
            let next_bytes = (index + 1 < items.len() || listed.next_after.is_some()).then(|| {
                let bytes =
                    children_cursor_bytes(&listed.hierarchy_version, listed_paths[index].as_str());
                (if peers {
                    acyclic_auth::cursor::CursorKey::sealed_len(bytes).div_ceil(3) * 4
                } else {
                    bytes
                }) as u64
            });
            if !credit.accept(item, next_bytes)? {
                break;
            }
        }
        let all_fit = credit.items() == items.len();
        let continued = !all_fit || listed.next_after.is_some();
        items.truncate(credit.items());
        let next =
            if continued {
                let last =
                    if all_fit {
                        last_scanned.as_ref().ok_or(Error::Record(
                            "children page cannot continue without a source row",
                        ))?
                    } else {
                        listed_paths
                            .get(items.len().checked_sub(1).ok_or(Error::Record(
                                "children page cannot continue without a row",
                            ))?)
                            .ok_or(Error::Record("children page lost its source cursor"))?
                    };
                let cursor = children_cursor(&listed.hierarchy_version, last.as_str());
                Some(if let Some(context) = &cursor_context {
                    STANDARD.encode(
                        self.cursor_key
                            .seal(context.as_bytes(), cursor.as_bytes())
                            .map_err(|_| Error::Record("peer cursor encryption failed"))?,
                    )
                } else {
                    cursor
                })
            } else {
                None
            };
        if items.is_empty()
            && next.is_some()
            && !acyclic_harness_client::encoded_agent_reply_size(page_json_bytes(&AgentReply::<
                acyclic_harness_core::conversation::PrivateDirectoryPage,
            >::Children {
                items: Vec::new(),
                next: next.clone(),
            })?)
            .is_ok_and(|bytes| bytes <= u64::from(page.bytes()))
        {
            return Err(Error::Stream(StreamError::Capacity));
        }
        Ok(AgentReply::Children { items, next })
    }
}
