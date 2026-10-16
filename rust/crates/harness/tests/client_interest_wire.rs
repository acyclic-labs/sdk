//! Real platform-session admission of the client's emitted subscriptions.
use super::path;
use acyclic_harness::{
    Error,
    client::{SwarmClient, SwarmClientLimits, SwarmUpdate},
    task_volume::{Channel, FileKey, FileValue, Query, TaskVolume, Write},
};
use acyclic_stream::{AppendRequest, CommitId, MemoryStream, StreamProvider as _, wire};
use futures::{FutureExt as _, StreamExt as _, channel::mpsc, future::Either};
use prost::Message as _;
use std::sync::Arc;
use wire::{session_frame::Body, session_subscription::Kind};

#[derive(Clone, Copy)]
enum Fault {
    None,
    MissingDiscoveryCursor,
    OpaqueContentAnchor,
    EvenChannel,
}

fn frame(id: u64, body: Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id: id,
        generation: u64::from(id != 0),
        body: Some(body),
    }
}

async fn chunk(volume: &TaskVolume<MemoryStream>, sequence: u64, bytes: &[u8]) {
    let cut = volume.snapshot(&Query::Root, 4).await.unwrap();
    let mut write = Write::new(
        FileKey::parse(&format!(".tasks/t/turns/1/chunks/{sequence}")).unwrap(),
        bytes.to_vec(),
    );
    write.channel = Some(Channel::Text);
    volume.append(cut.generation, &write).await.unwrap();
}

async fn exercise(before: &[u8], after: &[u8], fault: Fault) {
    let provider = Arc::new(MemoryStream::default());
    let config = TaskVolume::open(provider.clone(), path("config/files"))
        .await
        .unwrap();
    let cut = config.snapshot(&Query::Root, 4).await.unwrap();
    config
        .append(
            cut.generation,
            &Write::new(
                FileKey::parse("workspace/layout.json").unwrap(),
                br#"{"in_view":["swarm/a"]}"#.to_vec(),
            ),
        )
        .await
        .unwrap();
    let files = TaskVolume::open(provider.clone(), path("swarm/a/files"))
        .await
        .unwrap();
    chunk(&files, 1, before).await;
    let status = wire::SessionLatestValue {
        key: b"status".to_vec().into(),
        value: b"running".to_vec().into(),
        attention: false,
    };
    provider
        .append(AppendRequest {
            path: path("swarm/a/status"),
            records: vec![status.encode_to_vec().into()],
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await
        .unwrap();
    let (sender, receiver) = mpsc::unbounded();
    sender
        .unbounded_send(Ok(frame(
            0,
            Body::Hello(wire::SessionHello {
                version: 1,
                client_id: b"swarm-test".to_vec().into(),
                destination_node: "node".into(),
                receive_bytes: 1_000_000,
                receive_frames: 100,
            }),
        )))
        .unwrap();
    let incoming = acyclic_stream::session::serve(
        provider.clone(),
        "node".into(),
        Default::default(),
        receiver.boxed(),
    )
    .unwrap()
    .map(|item| item.map_err(|error| Error::Storage(error.to_string())));
    let sink = Box::pin(futures::sink::unfold(
        sender,
        move |sender, mut update: wire::SessionInterest| async move {
            let channels: Vec<_> = update
                .upsert
                .iter()
                .map(|sub| sub.channel.unwrap())
                .collect();
            for subscription in &mut update.upsert {
                match (&mut subscription.kind, fault) {
                    (Some(Kind::Children(_)), Fault::MissingDiscoveryCursor) => {
                        subscription.resume = None
                    }
                    (Some(Kind::Follow(follow)), Fault::OpaqueContentAnchor)
                        if follow.delivery == wire::SessionDelivery::All as i32 =>
                    {
                        subscription.resume.as_mut().unwrap().opaque =
                            std::mem::take(&mut follow.preceding_commit_id);
                    }
                    (Some(Kind::Follow(_)), Fault::EvenChannel) => {
                        subscription.channel.as_mut().unwrap().channel_id -= 1;
                    }
                    _ => {}
                }
            }
            // Only canonical frame wrapping and fixture credit; never repair client fields.
            sender
                .unbounded_send(Ok(frame(0, Body::Interest(update))))
                .unwrap();
            for channel in channels {
                sender
                    .unbounded_send(Ok(frame(
                        channel.channel_id,
                        Body::Window(wire::SessionWindow {
                            bytes: 1_000_000,
                            frames: 100,
                        }),
                    )))
                    .unwrap();
            }
            Ok::<_, Error>(sender)
        },
    ));
    let mut snapshots = Vec::new();
    let mut delivered = Vec::new();
    let client = SwarmClient::new(
        path("swarm"),
        SwarmClientLimits {
            agents: 1,
            selected: 1,
            layout_bytes: 1024,
            config_files: 4,
            snapshot_files: 4,
        },
    )
    .unwrap();
    let consume = client.follow(
        provider.as_ref(),
        &config,
        sink,
        incoming,
        |_, maximum| files.heads(&Query::Root, None, maximum),
        |update| {
            match update {
                SwarmUpdate::Snapshot { snapshot, .. } => {
                    snapshots.push(snapshot);
                    // Actual append after C but before the production follow is sent.
                    return Either::Left(async {
                        chunk(&files, 2, after).await;
                        Ok(())
                    });
                }
                SwarmUpdate::Data {
                    overview: false,
                    data,
                    ..
                } => {
                    let Some(wire::session_data::Item::Record(record)) = data.item else {
                        panic!("content record")
                    };
                    let record = acyclic_stream::Record {
                        sequence: record.sequence,
                        value: record.value.into(),
                        commit_id: CommitId::from_bytes(
                            record.commit_id.as_ref().try_into().unwrap(),
                        ),
                        committed_at_micros: record.committed_at_micros,
                    };
                    let file = FileValue::from_record(&record).unwrap().unwrap();
                    assert_eq!(file.key.path(), ".tasks/t/turns/1/chunks/2");
                    assert_eq!(file.channel, Some(Channel::Text));
                    delivered.push(file.bytes);
                    return Either::Right(futures::future::ready(Err(Error::Unsupported(
                        "complete".into(),
                    ))));
                }
                _ => {}
            }
            Either::Right(futures::future::ready(Ok(())))
        },
    );
    // The memory provider already owns every required record. Pending is a
    // delivery failure here, with no unbounded wait or wall-clock assumption.
    let outcome = tokio::task::unconstrained(consume)
        .now_or_never()
        .expect("accepted in-memory records were not delivered");
    match fault {
        Fault::None => {
            assert!(
                matches!(outcome, Err(Error::Unsupported(ref reason)) if reason == "complete"),
                "{outcome:?}"
            );
            assert_eq!(snapshots.len(), 1);
            assert_eq!(snapshots[0].files.len(), 1);
            let bytes: Vec<_> = snapshots[0].files[0]
                .segments
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect();
            assert_eq!(bytes, before);
            assert_eq!(delivered.len(), 1);
            assert_eq!(delivered[0].as_ref(), after);
            let current = files.heads(&Query::Root, None, 4).await.unwrap();
            let actual: Vec<_> = current.files[0]
                .segments
                .iter()
                .flat_map(|part| part.iter().copied())
                .collect();
            assert_eq!(actual, [before, after].concat());
        }
        _ => {
            assert!(matches!(outcome, Err(Error::Storage(_))), "{outcome:?}");
            assert!(delivered.is_empty(), "malformed subscription admitted");
        }
    }
}

#[test]
fn actual_session_rejects_malformed_subscriptions() {
    futures::executor::block_on(async {
        for fault in [
            Fault::None,
            Fault::MissingDiscoveryCursor,
            Fault::OpaqueContentAnchor,
            Fault::EvenChannel,
        ] {
            exercise(b"before", b"after", fault).await;
        }
    });
}

proptest::proptest! {
    #![proptest_config(proptest::test_runner::Config::with_cases(64))]
    // The real session accepts emitted subscriptions and delivers exactly the
    // tail after C; materialized bytes equal the complete append history.
    #[test]
    fn platform_snapshot_plus_follow_equals_the_complete_history(
        before in proptest::collection::vec(proptest::num::u8::ANY, 0..32),
        after in proptest::collection::vec(proptest::num::u8::ANY, 0..32),
    ) {
        futures::executor::block_on(exercise(&before, &after, Fault::None));
    }
}
