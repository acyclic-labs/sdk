//! Session filesystem conformance.
use acyclic_fs::path::PortablePath;
use acyclic_fs::session::{Cursor, Provider, Publication, Subscription};
use acyclic_fs::wire::filesystem::v2 as wire;
use acyclic_fs::{CancellationToken, Fs, WorkBudget};
use acyclic_stream::session::{self as engine, PublicationProvider};
use acyclic_stream::{StreamError, wire as session};
use bytes::Bytes;
use futures::{StreamExt, channel::mpsc};
use proptest::prelude::*;
use prost::Message;
use session::session_frame::Body;
use std::sync::Arc;

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;

fn subscription(volume_id: acyclic_fs::VolumeId, latest: bool) -> Subscription {
    Subscription {
        volume_id,
        workspace_name: acyclic_fs::WorkspaceName::new(
            "session-fs",
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .unwrap()
        .value,
        path_prefix: PortablePath::parse("/", acyclic_fs::VolumeLimits::default()).unwrap(),
        delivery: if latest {
            wire::SessionFsDelivery::Latest
        } else {
            wire::SessionFsDelivery::All
        },
        start_next: 0,
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]
    // Every valid typed subscription/publication/cursor survives encode-decode.
    #[test]
    fn round_trip(volume in any::<[u8;16]>(), root in any::<[u8;32]>(), commit in any::<[u8;32]>(),
        sequence in 1u64..u64::MAX, start in any::<u64>(), latest in any::<bool>(), parts in prop::collection::vec("[a-z]{1,12}",0..5)) {
        let mut sub = subscription(acyclic_fs::VolumeId::from_bytes(volume), latest);
        sub.start_next = start % sequence;
        sub.path_prefix = PortablePath::parse(&format!("/{}", parts.join("/")), acyclic_fs::VolumeLimits::default()).unwrap();
        sub.workspace_name = acyclic_fs::WorkspaceName::new(format!("ws-{}", parts.join("-")), &WorkBudget::UNBOUNDED, &CancellationToken::new()).unwrap().value;
        prop_assert_eq!(Subscription::decode(&sub.encode().unwrap()).unwrap(), sub.clone());
        let expected = allocations::wire_subscription(&sub).encode_to_vec();
        let encoded = sub.encode().unwrap();
        prop_assert_eq!(encoded.as_ref(), expected.as_slice());
        let publication = Publication {
            workspace_name: sub.workspace_name.clone(),
            generation: acyclic_fs::kernel::PublishedGeneration {
                volume_id: sub.volume_id,
                generation_root: acyclic_fs::ObjectId {
                    kind: acyclic_fs::ObjectKind::GenerationRoot,
                    digest: acyclic_fs::Digest::from_bytes(root),
                },
            },
            sequence: acyclic_fs::Sequence::new(sequence),
            commit_id: acyclic_stream::CommitId::from_bytes(commit),
        };
        let expected = wire::SessionFsPublication {
            version: 1, volume_id: volume.to_vec(), generation: Some(publication.generation_ref()),
            sequence, commit_id: commit.to_vec(),
        }.encode_to_vec();
        let encoded = publication.encode().unwrap();
        prop_assert_eq!(encoded.as_ref(), expected.as_slice());
        prop_assert_eq!(Publication::decode(&publication.encode().unwrap()).unwrap(), publication);
        let cursor = Cursor { subscription: sub, next: sequence+1, preceding_commit_id: acyclic_stream::CommitId::from_bytes(commit) };
        let expected = wire::SessionFsCursor {
            version: 1, subscription: Some(allocations::wire_subscription(&cursor.subscription)),
            next: cursor.next, preceding_commit_id: commit.to_vec(),
        }.encode_to_vec();
        let encoded = cursor.encode().unwrap();
        prop_assert_eq!(encoded.as_ref(), expected.as_slice());
        prop_assert_eq!(Cursor::decode(&cursor.encode().unwrap()).unwrap(), cursor);
    }
    // Every accepted arbitrary protobuf normalizes to a stable typed round-trip.
    #[test]
    fn arbitrary_input(bytes in prop::collection::vec(any::<u8>(), 0..512)) {
        if let Ok(value) = Subscription::decode(&bytes) { prop_assert_eq!(Subscription::decode(&value.encode().unwrap()).unwrap(), value); }
        if let Ok(value) = Publication::decode(&bytes) { prop_assert_eq!(Publication::decode(&value.encode().unwrap()).unwrap(), value); }
        if let Ok(value) = Cursor::decode(&bytes) { prop_assert_eq!(Cursor::decode(&value.encode().unwrap()).unwrap(), value); }
    }
}

#[test]
fn unknown_version_negative_control() {
    for version in [0, 2, u32::MAX] {
        assert!(matches!(
            Subscription::decode(
                &wire::SessionFsSubscription {
                    version,
                    ..Default::default()
                }
                .encode_to_vec()
            ),
            Err(StreamError::Unsupported)
        ));
        assert!(matches!(
            Publication::decode(
                &wire::SessionFsPublication {
                    version,
                    ..Default::default()
                }
                .encode_to_vec()
            ),
            Err(StreamError::Unsupported)
        ));
        assert!(matches!(
            Cursor::decode(
                &wire::SessionFsCursor {
                    version,
                    ..Default::default()
                }
                .encode_to_vec()
            ),
            Err(StreamError::Unsupported)
        ));
    }
}

async fn fixture() -> Result<(
    acyclic_fs::MemoryFs,
    acyclic_fs::Workspace<acyclic_fs::MemoryAuthorityBackend, acyclic_fs::MemoryObjectBackend>,
    Arc<acyclic_stream::MemoryStream>,
)> {
    let fs = Fs::memory();
    let workspace = fs
        .create_workspace(
            "session-fs",
            WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .await
        .map_err(|failure| failure.error)?
        .value;
    let (stream, _) = fs.authority_records(workspace.id().volume_id())?;
    Ok((fs, workspace, stream))
}

fn frame(channel: u64, body: Body) -> session::SessionFrame {
    session::SessionFrame {
        channel_id: channel,
        generation: u64::from(channel != 0),
        body: Some(body),
    }
}

struct Connection {
    sender: mpsc::UnboundedSender<std::result::Result<session::SessionFrame, StreamError>>,
    output: engine::transport::Responses,
}
impl Connection {
    async fn open(
        stream: Arc<acyclic_stream::MemoryStream>,
        sub: &Subscription,
        resume: session::SessionCursor,
    ) -> Result<Self> {
        let provider = Arc::new(Provider::new(stream.clone(), |_: &Subscription| Ok(())));
        let mut connection = Self::start(stream, provider, sub, resume).await?;
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Opened(_))
        ));
        Ok(connection)
    }
    async fn start<P: acyclic_stream::StreamProvider>(
        stream: Arc<P>,
        provider: Arc<dyn PublicationProvider>,
        sub: &Subscription,
        resume: session::SessionCursor,
    ) -> Result<Self> {
        Self::start_with_credit(stream, provider, sub, resume, true).await
    }
    async fn start_with_credit<P: acyclic_stream::StreamProvider>(
        stream: Arc<P>,
        provider: Arc<dyn PublicationProvider>,
        sub: &Subscription,
        resume: session::SessionCursor,
        credit: bool,
    ) -> Result<Self> {
        let (sender, receiver) = mpsc::unbounded();
        let output = engine::serve_with_publications(
            stream,
            Some(provider),
            "local".into(),
            engine::Limits::default(),
            Box::pin(receiver),
        )?;
        sender.unbounded_send(Ok(frame(
            0,
            Body::Hello(session::SessionHello {
                version: 1,
                client_id: Bytes::from_static(b"fs-client"),
                destination_node: "local".into(),
                receive_bytes: if credit { 1_000_000 } else { 0 },
                receive_frames: if credit { 100 } else { 0 },
            }),
        )))?;
        sender.unbounded_send(Ok(frame(
            0,
            Body::Interest(session::SessionInterest {
                revision: 1,
                remove: vec![],
                upsert: vec![session::SessionSubscription {
                    channel: Some(session::SessionChannel {
                        channel_id: 1,
                        generation: 1,
                    }),
                    resume: Some(resume),
                    kind: Some(session::session_subscription::Kind::FsPublications(
                        sub.encode()?,
                    )),
                }],
            }),
        )))?;
        let mut connection = Self { sender, output };
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Welcome(_))
        ));
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::InterestApplied(_))
        ));
        Ok(connection)
    }
    fn grant(&self) -> Result {
        self.sender.unbounded_send(Ok(frame(
            1,
            Body::Window(session::SessionWindow {
                bytes: 1_000_000,
                frames: 100,
            }),
        )))?;
        Ok(())
    }
    async fn next(&mut self) -> Result<session::SessionFrame> {
        Ok(
            tokio::time::timeout(std::time::Duration::from_secs(1), self.output.next())
                .await?
                .ok_or("session ended")??,
        )
    }
    async fn data(&mut self) -> Result<(Publication, session::SessionCursor)> {
        let Some(Body::Data(data)) = self.next().await?.body else {
            return Err("expected data".into());
        };
        let Some(session::session_data::Item::FsPublication(encoded)) = data.item else {
            return Err("expected publication".into());
        };
        Ok((
            Publication::decode(&encoded)?,
            data.after.ok_or("missing cursor")?,
        ))
    }
}

#[tokio::test]
async fn resume_exactly_once_through_session() -> Result {
    let (_fs, workspace, stream) = fixture().await?;
    let sub = subscription(workspace.id().volume_id(), false);
    let mut first =
        Connection::open(stream.clone(), &sub, session::SessionCursor::default()).await?;
    first.grant()?;
    let (created, cursor) = first.data().await?;
    assert_eq!(created.sequence.get(), 1);
    first.sender.unbounded_send(Ok(frame(
        1,
        Body::Ack(session::SessionAck {
            applied: Some(cursor.clone()),
        }),
    )))?;
    assert!(first.output.next().now_or_never().is_none());
    drop(first);
    workspace.write_text("/entry", "first").await?;
    workspace.write_text("/entry", "second").await?;
    let mut resumed = Connection::open(stream.clone(), &sub, cursor.clone()).await?;
    resumed.grant()?;
    let (one, _) = resumed.data().await?;
    let (two, cursor) = resumed.data().await?;
    assert_eq!((one.sequence.get(), two.sequence.get()), (2, 3));
    assert!(resumed.output.next().now_or_never().is_none());
    drop(resumed);
    let mut resumed = Connection::open(stream, &sub, cursor).await?;
    resumed.grant()?;
    assert!(resumed.output.next().now_or_never().is_none());
    workspace.write_text("/entry", "third").await?;
    let (three, _) = resumed.data().await?;
    assert_eq!(three.sequence.get(), 4);
    let head = workspace
        .head(WorkBudget::UNBOUNDED, &CancellationToken::new())
        .await
        .map_err(|failure| failure.error)?
        .value;
    assert_eq!(three.generation.generation_root.digest, head.id().digest());
    Ok(())
}

#[tokio::test]
async fn latest_conflates_full_manifests_all_preserves_history() -> Result {
    let (_fs, workspace, stream) = fixture().await?;
    for latest in [false, true] {
        let sub = subscription(workspace.id().volume_id(), latest);
        let mut connection =
            Connection::open(stream.clone(), &sub, session::SessionCursor::default()).await?;
        // No channel credit: LATEST may fold pending roots; ALL must retain them.
        assert!(connection.output.next().now_or_never().is_none());
        workspace
            .write_text("/entry", if latest { "latest-1" } else { "all-1" })
            .await?;
        assert!(connection.output.next().now_or_never().is_none());
        workspace
            .write_text("/entry", if latest { "latest-2" } else { "all-2" })
            .await?;
        assert!(connection.output.next().now_or_never().is_none());
        connection.grant()?;
        let (publication, _) = connection.data().await?;
        if latest {
            assert_eq!(publication.sequence.get(), 5);
        } else {
            assert_eq!(publication.sequence.get(), 1);
            assert_eq!(connection.data().await?.0.sequence.get(), 2);
            assert_eq!(connection.data().await?.0.sequence.get(), 3);
        }
        assert!(connection.output.next().now_or_never().is_none());
    }
    Ok(())
}

use futures::FutureExt;

#[tokio::test]
async fn cursors_reject_scope_offset_and_anchor_changes() -> Result {
    let (_fs, workspace, stream) = fixture().await?;
    let sub = subscription(workspace.id().volume_id(), false);
    let provider = Provider::new(stream, |_: &Subscription| Ok(()));
    let source = provider.subscribe(sub.encode()?, session::SessionCursor::default())?;
    let mut records = source.open.await?;
    let data = records.next().await.ok_or("missing publication")??;
    let cursor = data.after.ok_or("missing cursor")?;
    let mut other = sub.clone();
    other.path_prefix = PortablePath::parse("/other", acyclic_fs::VolumeLimits::default())?;
    assert!(matches!(
        provider.subscribe(other.encode()?, cursor.clone()),
        Err(StreamError::InvalidArgument)
    ));
    let mut wrong = cursor.clone();
    wrong.next += 1;
    assert!(matches!(
        provider.subscribe(sub.encode()?, wrong),
        Err(StreamError::InvalidArgument)
    ));
    let mut decoded = Cursor::decode(&cursor.opaque)?;
    decoded.preceding_commit_id = acyclic_stream::CommitId::from_bytes([99; 32]);
    let wrong = session::SessionCursor {
        next: cursor.next,
        opaque: decoded.encode()?,
    };
    assert!(matches!(
        provider.subscribe(sub.encode()?, wrong)?.open.await,
        Err(StreamError::PrefixNotRetained)
    ));

    Ok(())
}

#[test]
fn seed_corpus_round_trips() -> Result {
    let sub = include_bytes!("../../fuzz/corpus/session_fs/subscription");
    assert_eq!(Subscription::decode(sub)?.encode()?.as_ref(), sub);
    let publication = include_bytes!("../../fuzz/corpus/session_fs/publication");
    assert_eq!(
        Publication::decode(publication)?.encode()?.as_ref(),
        publication
    );
    let cursor = include_bytes!("../../fuzz/corpus/session_fs/cursor");
    assert_eq!(Cursor::decode(cursor)?.encode()?.as_ref(), cursor);
    assert!(matches!(
        Subscription::decode(include_bytes!(
            "../../fuzz/corpus/session_fs/unknown-version"
        )),
        Err(StreamError::Unsupported)
    ));
    Ok(())
}

#[path = "../support/session_fs.rs"]
mod support;

fn closed(frame: &session::SessionFrame, error: StreamError) -> bool {
    matches!(&frame.body, Some(Body::Closed(close)) if close.code == error.code()
        && StreamError::from_session_close(&close.code) == error)
}

#[tokio::test]
async fn published_reference_works_in_the_filesystem_server() -> Result {
    use wire::filesystem_service_server::FilesystemService;
    let (fs, workspace, stream) = fixture().await?;
    workspace.write_text("/entry", "published").await?;
    let sub = subscription(workspace.id().volume_id(), false);
    let mut connection = Connection::open(stream, &sub, session::SessionCursor::default()).await?;
    connection.grant()?;
    connection.data().await?;
    let (publication, _) = connection.data().await?;
    let service = acyclic_fs::FilesystemWireService::new(fs, Default::default())?;
    let reference = publication.generation_ref();
    let read = service
        .read(tonic::Request::new(wire::ReadRequest {
            generation: Some(reference.clone()),
            path: "/entry".into(),
            maximum_bytes: 64,
            range: None,
        }))
        .await?
        .into_inner();
    assert_eq!(read.contents.as_slice(), b"published");
    service
        .list_directory(tonic::Request::new(wire::ListDirectoryRequest {
            generation: Some(reference.clone()),
            path: "/".into(),
            page: Some(wire::PageOptions {
                maximum_items: 10,
                after: None,
            }),
        }))
        .await?;
    let mut broken = reference;
    broken.workspace = None;
    let error = service
        .read(tonic::Request::new(wire::ReadRequest {
            generation: Some(broken),
            path: "/entry".into(),
            maximum_bytes: 64,
            range: None,
        }))
        .await
        .err()
        .ok_or("missing workspace unexpectedly accepted")?;
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    Ok(())
}

#[tokio::test]
async fn publication_during_checked_resume_has_no_gap_or_duplicate() -> Result {
    for after_registration in [false, true] {
        for skip in [0, 1] {
            let (_fs, workspace, stream) = fixture().await?;
            let sub = subscription(workspace.id().volume_id(), false);
            let mut first =
                Connection::open(stream.clone(), &sub, session::SessionCursor::default()).await?;
            first.grant()?;
            let (_, cursor) = first.data().await?;
            drop(first);
            let entered = Arc::new(tokio::sync::Barrier::new(2));
            let release = Arc::new(tokio::sync::Barrier::new(2));
            let control = Arc::new(Provider::new(stream.clone(), |_: &Subscription| Ok(())));
            let mut history = support::History::new(stream);
            history.gate = Some((entered.clone(), release.clone(), after_registration));
            history.skip = skip;
            let history = Arc::new(history);
            let provider = Arc::new(support::SeparateTerminal {
                data: Arc::new(Provider::new(history.clone(), |_: &Subscription| Ok(()))),
                control,
            });
            let mut connection = Connection::start(history, provider, &sub, cursor).await?;
            let open = connection.next();
            let publish = async {
                entered.wait().await;
                workspace.write_text("/entry", "one").await?;
                workspace.write_text("/entry", "two").await?;
                release.wait().await;
                Result::Ok(())
            };
            let (opened, published) =
                tokio::time::timeout(std::time::Duration::from_secs(1), async {
                    futures::join!(open, publish)
                })
                .await?;
            published?;
            assert!(matches!(opened?.body, Some(Body::Opened(_))));
            connection.grant()?;
            let (one, _) = connection.data().await?;
            if skip == 0 {
                assert_eq!(one.sequence.get(), 2);
                assert_eq!(connection.data().await?.0.sequence.get(), 3);
            } else {
                // Negative control: skipping one real record fails the exact oracle.
                assert_ne!(one.sequence.get(), 2);
            }
            assert!(connection.output.next().now_or_never().is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn replaced_or_lost_history_is_terminal_for_all_and_latest() -> Result {
    for latest in [false, true] {
        let (_fs, workspace, stream) = fixture().await?;
        workspace.write_text("/entry", "original").await?;
        let sub = subscription(workspace.id().volume_id(), latest);
        let provider = Provider::new(stream.clone(), |_: &Subscription| Ok(()));
        let mut records = provider
            .subscribe(sub.encode()?, session::SessionCursor::default())?
            .open
            .await?;
        records.next().await.ok_or("missing creation")??;
        let cursor = records
            .next()
            .await
            .ok_or("missing publication")??
            .after
            .ok_or("missing cursor")?;
        for loss in 0..3 {
            for unchecked in [false, true] {
                let (_replacement_fs, replacement, replacement_stream) = fixture().await?;
                // Same volume, independently initialized authority. Loss 0: short
                // replacement; loss 1: different anchor; loss 2: retained floor.
                if loss != 0 {
                    replacement.write_text("/entry", "replacement").await?;
                }
                if loss == 1 {
                    replacement.write_text("/later", "next").await?;
                }
                let mut history = support::History::new(if loss == 2 {
                    stream.clone()
                } else {
                    replacement_stream
                });
                history.unchecked = unchecked;
                if loss == 2 {
                    history.floor = cursor.next;
                }
                let history = Arc::new(history);
                let provider = Arc::new(Provider::new(history.clone(), |_: &Subscription| Ok(())));
                let mut resumed =
                    Connection::start(history, provider, &sub, cursor.clone()).await?;
                let outcome = resumed.next().await?;
                if unchecked {
                    assert!(
                        !closed(&outcome, StreamError::PrefixNotRetained),
                        "negative control accepted by oracle"
                    );
                } else {
                    assert!(closed(&outcome, StreamError::PrefixNotRetained));
                }
            }
        }
    }
    Ok(())
}

#[tokio::test]
async fn workspace_deletion_closes_instead_of_becoming_idle() -> Result {
    for skip_deletion in [false, true] {
        let (_fs, workspace, stream) = fixture().await?;
        let sub = subscription(workspace.id().volume_id(), false);
        let provider = Arc::new(Provider::new(stream.clone(), |_: &Subscription| Ok(())));
        let provider = Arc::new(support::Broken {
            inner: provider,
            skip_deletion,
            skip_authorization: false,
        });
        let mut connection = Connection::start(
            stream.clone(),
            provider.clone(),
            &sub,
            session::SessionCursor::default(),
        )
        .await?;
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Opened(_))
        ));
        connection.grant()?;
        let (_, cursor) = connection.data().await?;
        drop(connection);
        let mut connection = Connection::start(stream.clone(), provider, &sub, cursor).await?;
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Opened(_))
        ));
        connection.grant()?;
        workspace.delete(acyclic_fs::IdempotencyKey::new()).await?;
        if !skip_deletion {
            let mut tail_sub = sub.clone();
            tail_sub.start_next = acyclic_stream::StreamProvider::tail(
                &*stream,
                acyclic_stream::StreamPath::new(format!(
                    "fs/authorities/{}/records",
                    hex::encode(sub.volume_id.into_bytes())
                ))?,
            )
            .await?;
            let provider = Arc::new(Provider::new(stream.clone(), |_: &Subscription| Ok(())));
            let mut from_tail = Connection::start(
                stream.clone(),
                provider,
                &tail_sub,
                session::SessionCursor::default(),
            )
            .await?;
            assert!(closed(&from_tail.next().await?, StreamError::NotFound));
        }
        if skip_deletion {
            assert!(connection.output.next().now_or_never().is_none());
        } else {
            assert!(closed(&connection.next().await?, StreamError::NotFound));
            assert!(connection.output.next().now_or_never().is_none());
        }
    }
    Ok(())
}

#[tokio::test]
async fn revocation_discards_buffered_frames_at_delivery() -> Result {
    use std::sync::atomic::{AtomicBool, Ordering};
    for skip_authorization in [false, true] {
        let (_fs, workspace, stream) = fixture().await?;
        let sub = subscription(workspace.id().volume_id(), true);
        let allowed = Arc::new(AtomicBool::new(true));
        let policy = allowed.clone();
        let provider = Arc::new(Provider::new(stream.clone(), move |_: &Subscription| {
            if policy.load(Ordering::Acquire) {
                Ok(())
            } else {
                Err(StreamError::AccessDenied)
            }
        }));
        let provider = Arc::new(support::Broken {
            inner: provider,
            skip_deletion: false,
            skip_authorization,
        });
        let mut connection =
            Connection::start(stream, provider, &sub, session::SessionCursor::default()).await?;
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Opened(_))
        ));
        assert!(connection.output.next().now_or_never().is_none()); // queues creation
        allowed.store(false, Ordering::Release);
        connection.grant()?;
        let outcome = connection.next().await?;
        assert_eq!(
            closed(&outcome, StreamError::AccessDenied),
            !skip_authorization
        );
        if skip_authorization {
            assert!(matches!(outcome.body, Some(Body::Data(_))));
        } else {
            assert!(connection.output.next().now_or_never().is_none());
        }
    }
    assert_eq!(
        StreamError::from_session_close("revoked"),
        StreamError::AccessDenied
    );
    Ok(())
}

#[tokio::test]
async fn latest_resumes_after_a_conflated_cursor() -> Result {
    let (_fs, workspace, stream) = fixture().await?;
    let sub = subscription(workspace.id().volume_id(), true);
    let mut first =
        Connection::open(stream.clone(), &sub, session::SessionCursor::default()).await?;
    assert!(first.output.next().now_or_never().is_none());
    for text in ["one", "two"] {
        workspace.write_text("/entry", text).await?;
        assert!(first.output.next().now_or_never().is_none());
    }
    first.grant()?;
    let (last, cursor) = first.data().await?;
    assert_eq!(last.sequence.get(), 3);
    drop(first);
    // Negative control: discarding the conflated cursor replays an applied root.
    let mut reset =
        Connection::open(stream.clone(), &sub, session::SessionCursor::default()).await?;
    reset.grant()?;
    assert!(reset.data().await?.0.sequence.get() <= 3);
    drop(reset);
    workspace.write_text("/entry", "three").await?;
    let mut resumed = Connection::open(stream.clone(), &sub, cursor).await?;
    resumed.grant()?;
    assert_eq!(resumed.data().await?.0.sequence.get(), 4);
    assert!(resumed.output.next().now_or_never().is_none());
    Ok(())
}

#[tokio::test]
async fn authorization_is_current_when_the_provider_open_is_polled() -> Result {
    use std::sync::atomic::{AtomicBool, Ordering};
    let (_fs, workspace, stream) = fixture().await?;
    let sub = subscription(workspace.id().volume_id(), false);
    let allowed = Arc::new(AtomicBool::new(true));
    let policy = allowed.clone();
    let provider = Provider::new(stream, move |_: &Subscription| {
        if policy.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(StreamError::AccessDenied)
        }
    });
    let prepared = provider.subscribe(sub.encode()?, session::SessionCursor::default())?;
    allowed.store(false, Ordering::Release);
    assert!(matches!(
        prepared.open.await,
        Err(StreamError::AccessDenied)
    ));
    Ok(())
}

#[path = "../session_fs/allocations.rs"]
mod allocations;

#[tokio::test]
async fn deletion_closes_all_without_channel_or_connection_credit() -> Result {
    for global_credit in [false, true] {
        for buffered in [false, true] {
            for skip_deletion in [false, true] {
                let (_fs, workspace, stream) = fixture().await?;
                let sub = subscription(workspace.id().volume_id(), false);
                let provider = Arc::new(support::Broken {
                    inner: Arc::new(Provider::new(stream.clone(), |_: &Subscription| Ok(()))),
                    skip_deletion,
                    skip_authorization: false,
                });
                let mut connection = Connection::start_with_credit(
                    stream,
                    provider,
                    &sub,
                    session::SessionCursor::default(),
                    global_credit,
                )
                .await?;
                assert!(matches!(
                    connection.next().await?.body,
                    Some(Body::Opened(_))
                ));
                if buffered {
                    // Channel has credit, global credit can still be zero. ALL
                    // keeps at most one queued publication while control follows.
                    connection.sender.unbounded_send(Ok(frame(
                        1,
                        Body::Window(session::SessionWindow {
                            bytes: 1_000_000,
                            frames: 1,
                        }),
                    )))?;
                    if global_credit {
                        connection.data().await?; // exhaust channel credit
                    }
                    assert!(connection.output.next().now_or_never().is_none());
                    for text in ["one", "two", "three"] {
                        workspace.write_text("/entry", text).await?;
                        assert!(connection.output.next().now_or_never().is_none());
                    }
                }
                workspace.delete(acyclic_fs::IdempotencyKey::new()).await?;
                if skip_deletion {
                    assert!(
                        connection.output.next().now_or_never().is_none(),
                        "negative control unexpectedly observes deletion"
                    );
                } else {
                    assert!(closed(&connection.next().await?, StreamError::NotFound));
                    assert!(connection.output.next().now_or_never().is_none());
                }
            }
        }
    }
    Ok(())
}
