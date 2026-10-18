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
        sequence in 1u64..u64::MAX, latest in any::<bool>(), parts in prop::collection::vec("[a-z]{1,12}",0..5)) {
        let mut sub = subscription(acyclic_fs::VolumeId::from_bytes(volume), latest);
        sub.path_prefix = PortablePath::parse(&format!("/{}", parts.join("/")), acyclic_fs::VolumeLimits::default()).unwrap();
        prop_assert_eq!(Subscription::decode(&sub.encode().unwrap()).unwrap(), sub.clone());
        let publication = Publication {
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
        prop_assert_eq!(Publication::decode(&publication.encode().unwrap()).unwrap(), publication);
        let cursor = Cursor { subscription: sub, next: sequence+1, preceding_commit_id: acyclic_stream::CommitId::from_bytes(commit) };
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
        let (sender, receiver) = mpsc::unbounded();
        let output = engine::serve_with_publications(
            stream.clone(),
            Some(Arc::new(Provider(stream))),
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
                receive_bytes: 1_000_000,
                receive_frames: 100,
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
        assert!(matches!(
            connection.next().await?.body,
            Some(Body::Opened(_))
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
    let provider = Provider(stream);
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
    assert!(
        provider
            .subscribe(sub.encode()?, wrong)?
            .open
            .await
            .is_err()
    );
    Ok(())
}

#[test]
fn seed_corpus_round_trips() -> Result {
    let sub = include_bytes!("../fuzz/corpus/session_fs/subscription");
    assert_eq!(Subscription::decode(sub)?.encode()?.as_ref(), sub);
    let publication = include_bytes!("../fuzz/corpus/session_fs/publication");
    assert_eq!(
        Publication::decode(publication)?.encode()?.as_ref(),
        publication
    );
    let cursor = include_bytes!("../fuzz/corpus/session_fs/cursor");
    assert_eq!(Cursor::decode(cursor)?.encode()?.as_ref(), cursor);
    assert!(matches!(
        Subscription::decode(include_bytes!("../fuzz/corpus/session_fs/unknown-version")),
        Err(StreamError::Unsupported)
    ));
    Ok(())
}
