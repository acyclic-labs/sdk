//! Filesystem changes use the ordinary credential-bound record channel.
use acyclic_auth::{contract::*, testing as signed};
use acyclic_fs::{CancellationToken, Fs, WorkBudget, access};
use acyclic_stream::{StreamError, session as engine, wire};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, channel::mpsc};
use prost::Message;
use std::sync::Arc;
use wire::session_frame::Body;
type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
type Space =
    acyclic_fs::Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>;

async fn fixture() -> Result<(
    acyclic_fs::MemoryFs,
    Space,
    Arc<acyclic_stream::MemoryStream>,
)> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace_with_operation::<_, (), acyclic_fs::WorkspaceError>(
            "session-fs",
            acyclic_fs::VolumeInitialization::new(
                acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
                Some(access::INITIAL_POLICY),
            ),
            acyclic_fs::OperationId::new(),
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
            None,
        )
        .await
        .map_err(|failure| failure.error)?
        .value
        .workspace();
    let (streams, _) = fs.authority_records(workspace.id().volume_id())?;
    Ok((fs, workspace, streams))
}
async fn policy(workspace: &Space) -> Result {
    let cancel = CancellationToken::new();
    let mut checkout = workspace
        .checkout(
            acyclic_fs::model::GenerationSelector::Head,
            acyclic_fs::model::CheckoutMode::tracking_transaction(),
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?
        .value;
    let mut policy: access::Policy = serde_json::from_slice(access::INITIAL_POLICY)?;
    policy.version += 1;
    let layer = policy.layers.first_mut().ok_or("missing policy layer")?;
    let mut deny = layer.first().ok_or("missing initial policy rule")?.clone();
    deny.id = "denied".into();
    deny.effect = access::Effect::Deny;
    deny.resource.path = "**".into();
    layer.push(deny);
    checkout
        .write_access_policy(
            &serde_json::to_vec(&policy)?,
            acyclic_fs::NamedAttributeWriteMode::Upsert,
            WorkBudget::UNBOUNDED,
            &cancel,
        )
        .await?;
    checkout
        .commit(
            acyclic_fs::OperationId::new(),
            None,
            None,
            None,
            acyclic_fs::PublicationPermit::Unrestricted,
            WorkBudget::UNBOUNDED,
            &cancel,
            |_| async {
                Ok::<_, acyclic_fs::OperationFailure<acyclic_fs::FsError>>(
                    acyclic_fs::OperationReceipt {
                        value: (),
                        work: acyclic_fs::WorkCounters::UNCHARGED,
                    },
                )
            },
        )
        .await?;
    Ok(())
}
fn credential(name: &str) -> Vec<u8> {
    signed::root(|block| {
        for certificate in block.certificates.iter_mut().flat_map(|chain| chain.keys.iter_mut()).take(1) {
            certificate.key.grants = GrantSet {
            grants: vec![Grant {
                kind: GrantKind::Resource(ResourceGrant {
                    resource: ResourcePattern {
                        name: name.into(),
                        prefix: true,
                    },
                    actions: vec!["fs/list".into()],
                }),
            }],
        };
        }
    })
}
fn frame(id: u64, body: Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id: id,
        generation: u64::from(id != 0),
        body: Some(body),
    }
}
struct Observed {
    inner: Arc<dyn engine::ResourceProvider>,
    seen: Arc<tokio::sync::Notify>,
    broken: bool,
}
impl engine::ResourceProvider for Observed {
    fn follow(
        &self,
        request: engine::ResourceFollowRequest,
    ) -> std::result::Result<engine::ResourceSubscription, StreamError> {
        let mut prepared = self.inner.follow(request)?;
        if self.broken {
            prepared.revalidate = Arc::new(|| Ok(()));
            prepared.terminal = Box::pin(futures::future::pending());
        }
        let open = prepared.open;
        let seen = self.seen.clone();
        prepared.open = Box::pin(async move {
            Ok(Box::pin(open.await?.inspect(move |record| {
                if matches!(record, Ok(acyclic_stream::FollowUpdate::Record(_))) {
                    seen.notify_one();
                }
            })) as acyclic_stream::FollowStream)
        });
        Ok(prepared)
    }
}
struct FollowSpec<'a> {
    path: &'a str,
    granted_path: &'a str,
    filter: Option<wire::SessionFilter>,
    delivery: wire::SessionDelivery,
    credit: bool,
    broken: bool,
}
impl Default for FollowSpec<'_> {
    fn default() -> Self {
        Self {
            path: "",
            granted_path: "",
            filter: None,
            delivery: wire::SessionDelivery::All,
            credit: true,
            broken: false,
        }
    }
}
struct Connection {
    sender: mpsc::UnboundedSender<std::result::Result<wire::SessionFrame, StreamError>>,
    output: engine::transport::Responses,
    seen: Arc<tokio::sync::Notify>,
}
impl Connection {
    async fn open(
        fs: acyclic_fs::MemoryFs,
        streams: Arc<acyclic_stream::MemoryStream>,
        volume: acyclic_fs::VolumeId,
        from: u64,
        anchor: Bytes,
        credit: bool,
        broken: bool,
    ) -> Result<Self> {
        Self::open_filtered(
            fs,
            streams,
            volume,
            from,
            anchor,
            FollowSpec {
                credit,
                broken,
                ..Default::default()
            },
        )
        .await
    }
    async fn open_filtered(
        fs: acyclic_fs::MemoryFs,
        streams: Arc<acyclic_stream::MemoryStream>,
        volume: acyclic_fs::VolumeId,
        from: u64,
        anchor: Bytes,
        spec: FollowSpec<'_>,
    ) -> Result<Self> {
        let mut connection = Self::start_filtered(fs, streams, volume, from, anchor, spec).await?;
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::InterestApplied(_))
        ));
        Ok(connection)
    }
    async fn start_filtered(
        fs: acyclic_fs::MemoryFs,
        streams: Arc<acyclic_stream::MemoryStream>,
        volume: acyclic_fs::VolumeId,
        from: u64,
        anchor: Bytes,
        spec: FollowSpec<'_>,
    ) -> Result<Self> {
        let FollowSpec {
            path,
            granted_path,
            filter,
            delivery,
            credit,
            broken,
        } = spec;
        let resource = acyclic_fs::session::resource(volume, path)?;
        let granted_resource = acyclic_fs::session::resource(volume, granted_path)?;
        let provider = Arc::new(acyclic_fs::session::ChangeProvider::new(
            fs,
            streams.clone(),
        )?);
        let seen = Arc::new(tokio::sync::Notify::new());
        let provider = Arc::new(Observed {
            inner: provider,
            seen: seen.clone(),
            broken,
        });
        let (sender, receiver) = mpsc::unbounded();
        let output = engine::serve_with_resources(
            streams,
            engine::testing::authority(),
            provider,
            "local".into(),
            engine::Limits::default(),
            Box::pin(receiver),
        )?;
        sender.unbounded_send(Ok(frame(
            0,
            Body::Hello(wire::SessionHello {
                version: 1,
                credentials: vec![credential(&granted_resource).into()],
                client_id: Bytes::from_static(b"fs-test"),
                destination_node: "local".into(),
                receive_bytes: if credit { 1_000_000 } else { 0 },
                receive_frames: if credit { 100 } else { 0 },
            }),
        )))?;
        sender.unbounded_send(Ok(frame(
            0,
            Body::Interest(wire::SessionInterest {
                revision: 1,
                remove: vec![],
                upsert: vec![wire::SessionSubscription {
                    channel: Some(wire::SessionChannel {
                        channel_id: 1,
                        generation: 1,
                    }),
                    credential: 0,
                    filter,
                    resume: Some(wire::SessionCursor {
                        next: from,
                        opaque: Bytes::new(),
                    }),
                    kind: Some(wire::session_subscription::Kind::Follow(
                        wire::SessionFollow {
                            resource,
                            delivery: delivery as i32,
                            preceding_commit_id: anchor,
                        },
                    )),
                }],
            }),
        )))?;
        let mut connection = Self {
            sender,
            output,
            seen,
        };
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Welcome(_))
        ));
        Ok(connection)
    }
    async fn next(&mut self) -> Result<wire::SessionFrame> {
        Ok(
            tokio::time::timeout(std::time::Duration::from_secs(1), self.output.next())
                .await?
                .ok_or("session ended")??,
        )
    }
    fn grant(&self, id: u64) -> Result {
        self.sender.unbounded_send(Ok(frame(
            id,
            Body::Window(wire::SessionWindow {
                bytes: 1_000_000,
                frames: 100,
            }),
        )))?;
        Ok(())
    }
    async fn data(&mut self) -> Result<(wire::Record, wire::SessionCursor)> {
        loop {
            let next = self.next().await?;
            if matches!(next.body, Some(Body::Watermark(_))) {
                continue;
            }
            let Some(Body::Data(data)) = next.body else {
                return Err(format!("expected ordinary record: {next:?}").into());
            };
            let Some(wire::session_data::Item::Record(record)) = data.item else {
                return Err("not a record".into());
            };
            return Ok((record, data.after.ok_or("missing cursor")?));
        }
    }
    async fn queued(&mut self) -> Result {
        let notified = self.seen.notified();
        let pending = self.output.next();
        match tokio::time::timeout(
            std::time::Duration::from_secs(1),
            futures::future::select(Box::pin(notified), pending),
        )
        .await?
        {
            futures::future::Either::Left(_) => Ok(()),
            futures::future::Either::Right((frame, _)) => {
                Err(format!("queued data escaped credit: {frame:?}").into())
            }
        }
    }
}
fn closed(frame: wire::SessionFrame) -> bool {
    matches!(frame.body, Some(Body::Closed(close)) if close.code == StreamError::AccessDenied.code())
}
fn changes(record: &wire::Record) -> Result<wire::ChangeRecord> {
    Ok(wire::ChangeRecord::decode(record.value.as_ref())?)
}
#[tokio::test]
async fn resume_exactly_once_through_ordinary_session() -> Result {
    let (fs, workspace, streams) = fixture().await?;
    let volume = workspace.id().volume_id();
    let mut first = Connection::open(
        fs.clone(),
        streams.clone(),
        volume,
        0,
        Bytes::new(),
        true,
        false,
    )
    .await?;
    assert!(matches!(first.next().await?.body, Some(Body::Opened(_))));
    first.grant(1)?;
    let (created, cursor) = first.data().await?;
    assert_eq!(created.sequence, 0);
    assert_eq!(changes(&created)?.entries.first().ok_or("missing change entry")?.operation, "create");
    drop(first);
    workspace.write_text("/entry", "one").await?;
    workspace.write_text("/entry", "two").await?;
    let mut resumed = Connection::open(
        fs,
        streams,
        volume,
        cursor.next,
        created.commit_id,
        true,
        false,
    )
    .await?;
    assert!(matches!(resumed.next().await?.body, Some(Body::Opened(_))));
    resumed.grant(1)?;
    let (one, _) = resumed.data().await?;
    let (two, _) = resumed.data().await?;
    assert_eq!((one.sequence, two.sequence), (1, 2));
    assert_eq!(changes(&one)?.entries.first().ok_or("missing change entry")?.key.as_ref(), b"entry");
    assert!(resumed.output.next().now_or_never().is_none());
    Ok(())
}
#[tokio::test]
async fn wrong_anchor_cannot_be_downgraded_to_an_unchecked_follow() -> Result {
    let (fs, workspace, streams) = fixture().await?;
    workspace.write_text("/entry", "one").await?;
    let mut connection = Connection::open(
        fs,
        streams,
        workspace.id().volume_id(),
        1,
        Bytes::from(vec![99; 32]),
        true,
        false,
    )
    .await?;
    let Some(Body::Closed(close)) = connection.next().await?.body else {
        return Err("forged cut opened".into());
    };
    assert_eq!(close.code, StreamError::PrefixNotRetained.code());
    Ok(())
}
#[tokio::test]
async fn revocation_and_deletion_discard_queued_data_without_connection_or_channel_credit() -> Result
{
    for deleted in [false, true] {
        for global_credit in [false, true] {
            for broken in [false, true] {
                let (fs, workspace, streams) = fixture().await?;
                let mut connection = Connection::open(
                    fs,
                    streams,
                    workspace.id().volume_id(),
                    0,
                    Bytes::new(),
                    global_credit,
                    broken,
                )
                .await?;
                assert!(matches!(
                    connection.next().await?.body,
                    Some(Body::Opened(_))
                ));
                connection.queued().await?;
                if deleted {
                    workspace.delete(acyclic_fs::IdempotencyKey::new()).await?;
                } else {
                    policy(&workspace).await?;
                }
                if broken {
                    // Negative control: removing both independent owner fences
                    // makes the exact same queued record observable after revoke.
                    if !global_credit {
                        connection.grant(0)?;
                    }
                    connection.grant(1)?;
                    assert!(matches!(connection.next().await?.body, Some(Body::Data(_))));
                } else {
                    assert!(closed(connection.next().await?));
                    assert!(connection.output.next().now_or_never().is_none());
                }
            }
        }
    }
    Ok(())
}
#[tokio::test]
async fn policy_change_between_prepare_and_open_fails_closed() -> Result {
    use engine::ResourceProvider;
    let (fs, workspace, streams) = fixture().await?;
    let resource = acyclic_fs::session::resource(workspace.id().volume_id(), "")?;
    let context = engine::testing::resource_authorization(&credential(&resource), &resource)?;
    let prepared = acyclic_fs::session::ChangeProvider::new(fs, streams)?.follow(
        engine::ResourceFollowRequest {
            resource,
            from: 0,
            preceding: None,
            filter: Default::default(),
            authorization: context,
        },
    )?;
    policy(&workspace).await?;
    assert!(matches!(
        prepared.open.await,
        Err(StreamError::AccessDenied)
    ));
    Ok(())
}
#[test]
fn retired_kind_twelve_never_decodes_as_an_authorized_subscription() -> Result {
    // Field 12, length-delimited. Prost preserves no retired oneof alternative.
    let subscription = wire::SessionSubscription::decode(&[0x62, 0][..])?;
    assert!(subscription.kind.is_none());
    let data = wire::SessionData::decode(&[0x62, 0][..])?;
    assert!(data.item.is_none());
    Ok(())
}

#[tokio::test]
async fn filtered_resume_uses_the_consumed_cut_and_never_reveals_skipped_paths() -> Result {
    let (fs, workspace, streams) = fixture().await?;
    let volume = workspace.id().volume_id();
    workspace.write_text("/private", "hidden").await?;
    let filter = || {
        Some(wire::SessionFilter {
            keys: vec![wire::KeyRange {
                start: Bytes::from_static(b"public"),
                end: Bytes::from_static(b"publid"),
            }],
            attributes: vec![],
            all_of: vec![],
        })
    };
    let mut first = Connection::open_filtered(
        fs.clone(),
        streams.clone(),
        volume,
        0,
        Bytes::new(),
        FollowSpec {
            filter: filter(),
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(first.next().await?.body, Some(Body::Opened(_))));
    first.grant(1)?;
    // Hidden history advances the logical cut without needing a visible record.
    let (cut, anchor) = loop {
        let next = first.next().await?;
        match next.body {
            Some(Body::Watermark(mark)) if mark.after.as_ref().is_some_and(|cut| cut.next == 2) => {
                break (mark.after.unwrap(), mark.preceding_commit_id);
            }
            Some(Body::Watermark(_)) => {}
            other => return Err(format!("filtered history disclosed data: {other:?}").into()),
        }
    };
    drop(first);
    workspace.write_text("/private", "still hidden").await?;
    workspace.write_text("/public", "visible").await?;
    let mut resumed = Connection::open_filtered(
        fs,
        streams,
        volume,
        cut.next,
        anchor,
        FollowSpec {
            filter: filter(),
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(resumed.next().await?.body, Some(Body::Opened(_))));
    resumed.grant(1)?;
    let (record, after) = resumed.data().await?;
    assert_eq!(record.sequence, 3);
    assert_eq!(after.next, 4);
    assert_eq!(changes(&record)?.entries.first().ok_or("missing change entry")?.key.as_ref(), b"public");
    assert!(resumed.output.next().now_or_never().is_none());
    Ok(())
}
#[tokio::test]
async fn semantic_path_widening_and_latest_delivery_cannot_open_filesystem_records() -> Result {
    for (path, granted, delivery) in [
        ("public", "private", wire::SessionDelivery::All),
        ("", "", wire::SessionDelivery::Latest),
    ] {
        let (fs, workspace, streams) = fixture().await?;
        let mut connection = Connection::start_filtered(
            fs,
            streams,
            workspace.id().volume_id(),
            0,
            Bytes::new(),
            FollowSpec {
                path,
                granted_path: granted,
                delivery,
                ..Default::default()
            },
        )
        .await?;
        let refusal = connection.output.next().await.ok_or("missing refusal")?;
        match refusal {
            Ok(frame) => assert!(matches!(frame.body, Some(Body::Closed(_)))),
            Err(error) => assert!(matches!(error, StreamError::InvalidArgument)),
        }
        // Neither a channel opening nor a data frame may precede the refusal.
    }
    // Positive counterpart proves the narrow credential works at its exact path.
    let (fs, workspace, streams) = fixture().await?;
    workspace
        .write_text("/private", "visible only to its authorized reader")
        .await?;
    let mut exact = Connection::open_filtered(
        fs,
        streams,
        workspace.id().volume_id(),
        0,
        Bytes::new(),
        FollowSpec {
            path: "private",
            granted_path: "private",
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(exact.next().await?.body, Some(Body::Opened(_))));
    exact.grant(1)?;
    let (record, _) = exact.data().await?;
    assert_eq!(changes(&record)?.entries.first().ok_or("missing change entry")?.key.as_ref(), b"private");
    Ok(())
}

#[tokio::test]
async fn wide_discovery_under_a_narrow_capability_projects_every_full_path() -> Result {
    let (fs, workspace, streams) = fixture().await?;
    let volume = workspace.id().volume_id();
    workspace
        .write_text("/public", "outside this capability")
        .await?;
    workspace
        .write_text("/private", "inside this capability")
        .await?;
    let mut narrow = Connection::open_filtered(
        fs.clone(),
        streams.clone(),
        volume,
        0,
        Bytes::new(),
        FollowSpec {
            granted_path: "private",
            ..Default::default()
        },
    )
    .await?;
    assert!(matches!(narrow.next().await?.body, Some(Body::Opened(_))));
    narrow.grant(1)?;
    let (record, after) = narrow.data().await?;
    assert_eq!(record.sequence, 2);
    assert_eq!(after.next, 3);
    assert_eq!(changes(&record)?.entries.first().ok_or("missing change entry")?.key.as_ref(), b"private");
    assert!(narrow.output.next().now_or_never().is_none());
    // Removing only the capability restriction makes the public path visible.
    let mut broad = Connection::open(fs, streams, volume, 1, Bytes::new(), true, false).await?;
    assert!(matches!(broad.next().await?.body, Some(Body::Opened(_))));
    broad.grant(1)?;
    let (record, _) = broad.data().await?;
    assert_eq!(changes(&record)?.entries.first().ok_or("missing change entry")?.key.as_ref(), b"public");
    Ok(())
}
