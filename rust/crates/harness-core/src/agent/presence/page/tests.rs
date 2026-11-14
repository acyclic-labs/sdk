//! Real directory records and node sockets prove bounded paging and disclosure.
use super::*;
use crate::agent::{AgentId, SessionPhase, SessionProfile};
use acyclic_actors::{addressing::AccountId, domain::ActorLimits, local::SystemClock, route};
use acyclic_stream::{
    CommitCondition, CommitEffect, CommitMutation, CommitOutcome, CommitRequest, IdempotencyKey,
    MemoryStream, StreamProviderExt as _,
    session::{CallAuthorization, Client},
};
use std::{collections::BTreeSet, sync::Arc};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

struct Directory {
    provider: Arc<MemoryStream>,
    routes: Paths,
    requester: ActorReference,
    nodes: [NodeId; 3],
}

fn actor(id: u128) -> TestResult<ActorReference> {
    Ok(ActorReference::new(
        b"account",
        b"agents",
        AgentId::from_bytes(id.to_be_bytes()).as_str().as_bytes(),
    )?)
}

fn profile() -> SessionProfile {
    SessionProfile {
        phase: SessionPhase::Idle,
        title: "Session".into(),
        workspaces: Vec::new(),
        goal_preview: "Goal".into(),
    }
}

impl Directory {
    async fn new(count: u128, removed: bool) -> TestResult<Self> {
        let this = Self {
            provider: Arc::new(MemoryStream::default()),
            routes: Paths::new(StreamPath::new("routing")?),
            requester: actor(u128::MAX)?,
            nodes: [
                NodeId::new(b"online")?,
                NodeId::new(b"offline")?,
                NodeId::new(b"absent")?,
            ],
        };
        let mut effects = Vec::new();
        for index in 0..count {
            let row = SessionRow::new(this.nodes[(index % 3) as usize].clone(), profile(), [7; 32]);
            let path = SessionPublication::row_path(&actor(index + 1)?)?;
            let mut effect = row.effect(path.clone())?;
            if removed {
                let removal = row.changed(None)?.ok_or("removal missing")?.effect(path)?;
                let CommitMutation::Append { records, .. } = &mut effect.mutations[0] else {
                    return Err("row append missing".into());
                };
                let CommitMutation::Append {
                    records: removed, ..
                } = &removal.mutations[0]
                else {
                    return Err("removal append missing".into());
                };
                records.extend(removed.iter().cloned());
            }
            effects.push(effect);
            if effects.len() == 128 || index + 1 == count {
                publish(this.provider.as_ref(), std::mem::take(&mut effects)).await?;
            }
        }
        publish(
            this.provider.as_ref(),
            vec![
                SessionRow::new(this.nodes[0].clone(), profile(), [7; 32])
                    .effect(SessionPublication::row_path(&this.requester)?)?,
            ],
        )
        .await?;
        Ok(this)
    }

    async fn page<P: StreamProvider + ?Sized>(
        &self,
        provider: &P,
        key: &CursorKey,
        cursor: Option<&[u8]>,
    ) -> Result<SessionPage, Error> {
        SessionPublication::peers_page(provider, &self.routes, &self.requester, key, cursor, 16)
            .await
    }
}

async fn publish(provider: &MemoryStream, effects: Vec<CommitEffect>) -> TestResult {
    assert!(matches!(
        provider
            .commit(CommitRequest {
                deadline_unix_millis: None,
                idempotency_key: IdempotencyKey::unique(),
                effects
            })
            .await?,
        CommitOutcome::Committed(_)
    ));
    Ok(())
}

struct NoMessages;
impl route::AdmissionPolicy for NoMessages {
    fn authorize(&self, _: &CallAuthorization, _: &route::Envelope) -> Result<(), StreamError> {
        Err(StreamError::AccessDenied)
    }
    fn prepare<'a>(
        &'a self,
        _: &'a CallAuthorization,
        _: &'a route::Envelope,
        _: Option<&'a route::Placement>,
    ) -> acyclic_stream::BoxProviderFuture<'a, Result<route::Admission, route::Error>> {
        Box::pin(async { Err(StreamError::AccessDenied.into()) })
    }
}

async fn with_online<T, F>(directory: &Directory, work: F) -> TestResult<T>
where
    F: AsyncFnOnce() -> TestResult<T>,
{
    let node = Arc::new(
        route::AccountNode::open(
            acyclic_actors::guest::Requests::default(),
            AccountId::new(b"account")?,
            directory.provider.clone(),
            directory.routes.clone(),
            Arc::new(NoMessages),
            ActorLimits::default(),
            Arc::new(SystemClock),
            Arc::new(acyclic_stream::runtime::PlatformExecutor),
        )
        .await?,
    );
    for id in &directory.nodes[..2] {
        node.enroll(id, IdempotencyKey::new(id.as_bytes().to_vec())?)
            .await?;
    }
    let client = Client::in_process_with_products(
        directory.provider.clone(),
        acyclic_stream::session::testing::authority(),
        None,
        node.clone(),
        Some(node.clone()),
        Arc::new(acyclic_stream::runtime::PlatformExecutor),
        None,
    );
    let resource = route::open_request(b"account", &directory.nodes[0])?.resource();
    client.register_credential(
        0,
        acyclic_auth::testing::root(|block| {
            use acyclic_auth::contract::*;
            let key = &mut block.certificates.as_mut().expect("signed fixture").keys[0].key;
            key.grants = GrantSet {
                grants: vec![Grant {
                    kind: GrantKind::Resource(ResourceGrant {
                        resource: ResourcePattern {
                            name: resource.clone(),
                            prefix: false,
                        },
                        actions: vec!["actors/invoke".into()],
                    }),
                }],
            };
        })
        .into(),
    )?;
    let running = node.run(|_, cause| panic!("node dispatcher failed: {cause}"));
    let action = async {
        let _link = route::open_link(&client, 0, b"account", &directory.nodes[0]).await?;
        work().await
    };
    futures::pin_mut!(running, action);
    match futures::future::select(running, action).await {
        futures::future::Either::Right((result, _)) => result,
        futures::future::Either::Left((outcome, _)) => {
            Err(format!("node dispatcher stopped: {outcome:?}").into())
        }
    }
}

#[tokio::test]
async fn account_pages_bound_work_and_batch_repeated_node_presence_at_scale() -> TestResult {
    let mut baseline = None;
    for count in [64, 100_000] {
        let directory = Directory::new(count, false).await?;
        let measured = with_online(&directory, async || {
            let counted =
                acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
            let key = CursorKey::new()?;
            let page = directory.page(&counted, &key, None).await?;
            assert_eq!(page.peers().len(), 16);
            let distinct: BTreeSet<_> = page.peers().iter().map(|peer| &peer.row.node).collect();
            assert!(
                distinct.len() < page.peers().len(),
                "the fixture must repeat visible nodes"
            );
            assert_eq!(
                counted.named_head_selections(),
                distinct.len() as u64 + 1,
                "one requester head and one lookup per distinct visible node"
            );
            assert_eq!(counted.requests().control(), 2);
            assert_eq!(counted.requests().data(), 0);
            assert!(RevisionCut::of(page.rows_at.clone()).proven_by(page.presence_at.as_ref()));
            assert!(
                page.peers()
                    .iter()
                    .all(|peer| peer.online == (peer.row.node == directory.nodes[0]))
            );
            let measured = counted.requests();
            assert!(matches!(
                page.cursor_bytes(0),
                Err(Error::Stream(StreamError::Capacity))
            ));
            let expected = page.peers()[1].actor.clone();
            let bytes = page.cursor_bytes(1)?.ok_or("continuation missing")?;
            let (first, next) = page.finish(1, &key)?;
            assert_eq!(first.len(), 1);
            let next = next.ok_or("continuation missing")?;
            assert_eq!(
                next.len(),
                bytes,
                "byte credit must price the actual confidential cursor"
            );
            publish(
                directory.provider.as_ref(),
                vec![CommitEffect::hard(
                    Vec::new(),
                    vec![CommitMutation::Append {
                        path: StreamPath::new("unrelated/new-path")?,
                        records: vec![b"new".to_vec().into()],
                    }],
                )],
            )
            .await?;
            let resumed = directory.page(&counted, &key, Some(&next)).await?;
            assert_eq!(
                resumed.peers()[0].actor,
                expected,
                "unrelated stream creation must not invalidate or skip the returned prefix"
            );
            Ok(measured)
        })
        .await?;
        if let Some(previous) = baseline {
            assert_eq!(previous, measured);
        }
        baseline = Some(measured);
    }
    Ok(())
}

#[tokio::test]
async fn account_pages_exclude_only_the_fully_qualified_requester() -> TestResult {
    let directory = Directory::new(0, false).await?;
    publish(
        directory.provider.as_ref(),
        vec![CommitEffect::hard(
            Vec::new(),
            vec![CommitMutation::Append {
                path: directory.routes.presence(&directory.nodes[0])?,
                records: vec![vec![0xff].into()],
            }],
        )],
    )
    .await?;
    let key = CursorKey::new()?;
    let counted = acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
    let page = directory.page(&counted, &key, None).await?;
    assert!(page.peers().is_empty());
    assert!(page.presence_at.is_none());
    assert!(page.finish(0, &key)?.1.is_none());
    assert_eq!(counted.named_head_selections(), 1);
    assert_eq!(counted.requests().control(), 1);

    // Equal root IDs in different namespaces are different sessions.
    let other = ActorReference::new(
        directory.requester.account().as_bytes(),
        b"other-agents",
        directory.requester.id().as_bytes(),
    )?;
    publish(
        directory.provider.as_ref(),
        vec![
            SessionRow::new(directory.nodes[1].clone(), profile(), [7; 32])
                .effect(SessionPublication::row_path(&other)?)?,
        ],
    )
    .await?;
    let page = directory.page(&counted, &key, None).await?;
    assert_eq!(page.peers().len(), 1);
    assert_eq!(page.peers()[0].actor, other);
    assert!(!page.peers()[0].online);

    // A different visible root on the requester's node must read its presence.
    publish(
        directory.provider.as_ref(),
        vec![
            SessionRow::new(directory.nodes[0].clone(), profile(), [7; 32])
                .effect(SessionPublication::row_path(&actor(1)?)?)?,
        ],
    )
    .await?;
    assert!(directory.page(&counted, &key, None).await.is_err());
    Ok(())
}

#[tokio::test]
async fn filtered_account_pages_keep_confidential_progress_without_presence_reads() -> TestResult {
    let directory = Directory::new(64, true).await?;
    for node in &directory.nodes {
        publish(
            directory.provider.as_ref(),
            vec![CommitEffect::hard(
                Vec::new(),
                vec![CommitMutation::Append {
                    path: directory.routes.presence(node)?,
                    records: vec![vec![0xff].into()],
                }],
            )],
        )
        .await?;
    }
    let key = CursorKey::new()?;
    let counted = acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
    let page = directory.page(&counted, &key, None).await?;
    assert!(page.peers().is_empty());
    assert!(page.presence_at.is_none());
    assert_eq!(counted.requests().control(), 1);
    assert_eq!(counted.requests().data(), 0);
    assert_eq!(counted.named_head_selections(), 1);
    let context = page.context.clone();
    let scanned = page.scanned.clone().ok_or("source scan missing")?;
    let bytes = page
        .cursor_bytes(0)?
        .ok_or("filtered continuation missing")?;
    let (rows, cursor) = page.finish(0, &key)?;
    assert!(rows.is_empty());
    let cursor = cursor.ok_or("filtered continuation missing")?;
    assert_eq!(cursor.len(), bytes);
    assert_eq!(
        open_cursor(
            &key,
            &context,
            &SessionPublication::directory(directory.requester.account())?,
            Some(&cursor)
        )?,
        Some(scanned.clone())
    );
    reject_cursors(&directory, &key, &cursor, &scanned).await?;
    let next = directory.page(&counted, &key, Some(&cursor)).await?;
    assert!(next.scanned.as_ref().is_some_and(|next| next > &scanned));
    let head = directory.provider.head(scanned.clone()).await?;
    let row = SessionRow::from_record(
        &scanned,
        head.tail,
        head.last.as_ref().ok_or("row missing")?,
    )?;
    publish(
        directory.provider.as_ref(),
        vec![
            row.changed_membership(
                Some(profile()),
                crate::agent::PeerMembership {
                    epoch: Some([8; 32]),
                },
            )?
            .ok_or("rejoin missing")?
            .effect(scanned)?,
        ],
    )
    .await?;
    // The genuine malformed presence was irrelevant while its row was hidden.
    // Making that row visible must now reach the presence decoder and fail.
    assert!(directory.page(&counted, &key, None).await.is_err());
    let path = SessionPublication::row_path(&directory.requester)?;
    publish(
        directory.provider.as_ref(),
        vec![
            SessionRow::new(directory.nodes[0].clone(), profile(), [7; 32])
                .changed(None)?
                .ok_or("requester removal missing")?
                .effect(path)?,
        ],
    )
    .await?;
    let denied = acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
    assert!(directory.page(&denied, &key, Some(&cursor)).await.is_err());
    assert_eq!(denied.named_head_selections(), 1);
    assert_eq!(denied.requests().control(), 1);
    Ok(())
}

async fn reject_cursors(
    directory: &Directory,
    key: &CursorKey,
    cursor: &[u8],
    scanned: &StreamPath,
) -> TestResult {
    let counted = acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
    let mut changed = cursor.to_vec();
    *changed.last_mut().ok_or("cursor missing")? ^= 1;
    assert!(directory.page(&counted, key, Some(&changed)).await.is_err());
    assert!(
        directory
            .page(&counted, &CursorKey::new()?, Some(cursor))
            .await
            .is_err()
    );
    assert!(
        directory
            .page(&counted, key, Some(scanned.as_str().as_bytes()))
            .await
            .is_err(),
        "a raw opted-out root must never be a usable continuation"
    );
    assert!(
        SessionPublication::peers_page(
            &counted,
            &directory.routes,
            &actor(u128::MAX - 1)?,
            key,
            Some(cursor),
            16
        )
        .await
        .is_err()
    );
    assert_eq!(counted.requests().control(), 0);
    assert_eq!(counted.requests().data(), 0);
    Ok(())
}

#[tokio::test]
async fn account_pages_reject_inherited_session_and_presence_records() -> TestResult {
    let directory = Directory::new(2, false).await?;
    with_online(&directory, async || {
        let key = CursorKey::new()?;
        assert!(
            directory
                .page(directory.provider.as_ref(), &key, None)
                .await?
                .peers()
                .iter()
                .any(|peer| peer.online)
        );
        let other = Paths::new(StreamPath::new("other-routing")?);
        publish(
            directory.provider.as_ref(),
            vec![CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: other.presence(&directory.nodes[0])?,
                }],
                vec![CommitMutation::Fork {
                    source: directory.routes.presence(&directory.nodes[0])?,
                    destination: other.presence(&directory.nodes[0])?,
                    at_tail: 1,
                    exclude: Vec::new(),
                    records: Vec::new(),
                }],
            )],
        )
        .await?;
        assert!(
            NodePresence::read(directory.provider.as_ref(), &other, &directory.nodes[0])
                .await
                .is_err()
        );
        assert!(
            SessionPublication::peers_page(
                directory.provider.as_ref(),
                &other,
                &directory.requester,
                &key,
                None,
                16
            )
            .await
            .is_err()
        );
        let row = SessionRow::new(directory.nodes[0].clone(), profile(), [7; 32]);
        let original = StreamPath::new("other-row")?;
        let destination = SessionPublication::row_path(&actor(9)?)?;
        publish(
            directory.provider.as_ref(),
            vec![row.effect(original.clone())?],
        )
        .await?;
        publish(
            directory.provider.as_ref(),
            vec![CommitEffect::hard(
                vec![CommitCondition::Absent {
                    path: destination.clone(),
                }],
                vec![CommitMutation::Fork {
                    source: original,
                    destination: destination.clone(),
                    at_tail: 1,
                    exclude: Vec::new(),
                    records: Vec::new(),
                }],
            )],
        )
        .await?;
        let inherited = directory
            .provider
            .head(destination)
            .await?
            .last
            .ok_or("inherited row missing")?;
        assert_eq!(
            SessionRow::decode(&inherited)?,
            row,
            "the inherited head must otherwise be a valid live row"
        );
        let counted = acyclic_stream::testing::Counted::new(directory.provider.as_ref().clone());
        assert!(directory.page(&counted, &key, None).await.is_err());
        assert_eq!(
            counted.named_head_selections(),
            1,
            "an invalid row must fail before presence lookup"
        );
        Ok(())
    })
    .await
}
