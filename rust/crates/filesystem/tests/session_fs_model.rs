//! Exhaustive schedules through the filesystem publication adapter and Session.
#![cfg(acyclic_loom)]
use acyclic_fs::session::{Cursor, Provider, Publication, Subscription};
use acyclic_fs::{CancellationToken, WorkBudget};
use acyclic_stream::session::{self as engine, PublicationProvider};
use acyclic_stream::{verification, *};
use bytes::Bytes;
use futures::{FutureExt, StreamExt, channel::mpsc};
use loom::sync::atomic::{AtomicBool, Ordering};
use std::mem::ManuallyDrop;
use std::sync::Arc;
use wire::session_frame::Body;

#[path = "support/session_fs.rs"]
mod support;

fn run<F: std::future::Future>(future: F) -> F::Output {
    verification::block_on(future)
}
fn control(run: impl FnOnce(), message: &str) {
    let failure = std::panic::catch_unwind(std::panic::AssertUnwindSafe(run))
        .expect_err("broken path escaped model");
    let text = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .unwrap_or("");
    assert!(text.contains(message), "unexpected model failure: {text}");
}

// Seed only canonical records: the full workspace mutation fixture has large
// futures unrelated to these adapter races. Actors still use the real Stream
// append/recovery and Provider checked-follow paths with their real Loom locks.
fn input(texts: &[&str]) -> (Subscription, Vec<Bytes>) {
    use acyclic_fs::kernel::{PublishedGeneration, VolumeCreated};
    use acyclic_fs::{Digest, DurableCommit, Epoch, OperationId, Sequence, VolumeId};
    let sub = Subscription {
        volume_id: VolumeId::from_bytes([7; 16]),
        workspace_name: acyclic_fs::WorkspaceName::new(
            "session-fs",
            &WorkBudget::UNBOUNDED,
            &CancellationToken::new(),
        )
        .unwrap()
        .value,
        path_prefix: acyclic_fs::path::PortablePath::parse("/", Default::default()).unwrap(),
        delivery: acyclic_fs::wire::filesystem::v2::SessionFsDelivery::All,
        start_next: 0,
    };
    let root = |text: &[u8]| acyclic_fs::ObjectId {
        kind: acyclic_fs::ObjectKind::GenerationRoot,
        digest: Digest::from_bytes(*blake3::hash(text).as_bytes()),
    };
    let created = acyclic_fs::kernel::encode_volume_created::<acyclic_fs::FsError>(
        VolumeCreated {
            volume_id: sub.volume_id,
            config: acyclic_fs::VolumeConfig::portable(acyclic_fs::Lifecycle::Ephemeral),
            initial_generation_root: root(b"initial"),
        },
        |_| Ok(()),
        |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed.into(),
    )
    .unwrap();
    let mut payloads = vec![Bytes::new(), Bytes::from(created)];
    for text in texts {
        let publication = PublishedGeneration {
            volume_id: sub.volume_id,
            generation_root: root(text.as_bytes()),
        };
        let mut payload = b"acyclic-fs-publish-generation-v1\0".to_vec();
        payload.extend_from_slice(&1_u16.to_le_bytes());
        payload.extend_from_slice(&publication.volume_id.into_bytes());
        payload.push(publication.generation_root.kind.canonical_tag());
        payload.extend_from_slice(publication.generation_root.digest.as_bytes());
        assert_eq!(
            acyclic_fs::kernel::decode_published_generation::<acyclic_fs::FsError>(
                &payload,
                MAX_RECORD_BYTES as u64,
                |_| Ok(()),
            )
            .unwrap(),
            publication
        );
        payloads.push(Bytes::from(payload));
    }
    let mut previous_digest = Digest::ZERO;
    let records = payloads
        .into_iter()
        .enumerate()
        .map(|(index, payload)| {
            let sequence = Sequence::new(index as u64);
            let operation_id = OperationId::from_bytes([u8::try_from(index).unwrap(); 16]);
            let fingerprint = Digest::from_bytes(*blake3::hash(&payload).as_bytes());
            let digest = acyclic_fs::authority_commit_digest(
                acyclic_fs::kernel::volume_authority_id(sub.volume_id),
                Epoch::GENESIS,
                sequence,
                operation_id,
                fingerprint,
                previous_digest,
                &payload,
            );
            let commit = DurableCommit {
                epoch: Epoch::GENESIS,
                sequence,
                operation_id,
                fingerprint,
                previous_digest,
                digest,
                payload,
                settled_at_micros: None,
            };
            previous_digest = digest;
            acyclic_fs::StreamsDurableRecord::encode(&commit, MAX_RECORD_BYTES as u64).unwrap()
        })
        .collect();
    (sub, records)
}
fn path(sub: &Subscription) -> StreamPath {
    StreamPath::new(format!(
        "fs/authorities/{}/records",
        hex::encode(sub.volume_id.into_bytes())
    ))
    .unwrap()
}
fn append(stream: &MemoryStream, sub: &Subscription, records: &[Bytes], from: u64) -> CommitId {
    let outcome = run(stream.append(AppendRequest {
        path: path(sub),
        records: records.iter().map(|bytes| bytes.to_vec().into()).collect(),
        if_tail: Some(from),
        idempotency_key: None,
    }))
    .unwrap();
    let AppendOutcome::Committed(receipt) = outcome else {
        panic!("append conflicted")
    };
    receipt.commit_id
}
fn cursor(sub: &Subscription, next: u64, anchor: CommitId) -> wire::SessionCursor {
    wire::SessionCursor {
        next,
        opaque: Cursor {
            subscription: sub.clone(),
            next,
            preceding_commit_id: anchor,
        }
        .encode()
        .unwrap(),
    }
}

fn resume(omit_wake: bool) {
    verification::check(move || {
        let (sub, records) = input(&["next"]);
        let stream = Arc::new(MemoryStream::default());
        let anchor = append(&stream, &sub, &records[..2], 0);
        let keeper = ManuallyDrop::new(stream.clone());
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            let sub = sub.clone();
            move || {
                if omit_wake {
                    verification::omit_append_wake();
                }
                append(&stream, &sub, &records[2..], 2);
            }
        });
        run(async {
            let mut source = Provider::new(stream.clone(), |_: &Subscription| Ok(()))
                .subscribe(sub.encode().unwrap(), cursor(&sub, 2, anchor))
                .unwrap()
                .open
                .await
                .unwrap();
            let data = source.next().await.unwrap().unwrap();
            assert_eq!(data.after.as_ref().unwrap().next, 3);
            let Some(wire::session_data::Item::FsPublication(bytes)) = data.item else {
                panic!("missing publication")
            };
            assert_eq!(Publication::decode(&bytes).unwrap().sequence.get(), 2);
            writer.join().unwrap();
            assert!(
                source.next().now_or_never().is_none(),
                "duplicate publication"
            );
        });
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}
#[test]
fn checked_resume_exhausts_append_registration_and_wakeup() {
    resume(false);
    control(|| resume(true), "deadlock");
}

fn replacement(unchecked: bool) {
    verification::check(move || {
        let (sub, records) = input(&["original"]);
        let (other, replacement) = input(&["replacement", "later"]);
        assert_eq!(sub.volume_id, other.volume_id);
        let stream = Arc::new(MemoryStream::default());
        let anchor = append(&stream, &sub, &records, 0);
        let keeper = ManuallyDrop::new(stream.clone());
        let replaced = MemoryStream::default();
        append(&replaced, &other, &replacement, 0);
        let writer = loom::thread::spawn({
            let stream = stream.clone();
            move || run(verification::recover(&stream, &replaced)).unwrap()
        });
        let mut history = support::History::new(stream);
        history.unchecked = unchecked;
        let provider = Provider::new(Arc::new(history), |_: &Subscription| Ok(()));
        run(async {
            let error = match provider
                .subscribe(sub.encode().unwrap(), cursor(&sub, 3, anchor))
                .unwrap()
                .open
                .await
            {
                Err(error) => error,
                Ok(mut source) => match source.next().await {
                    Some(Err(error)) => error,
                    _ => panic!("history was not rejected"),
                },
            };
            assert_eq!(
                error,
                StreamError::PrefixNotRetained,
                "history was not rejected"
            );
        });
        writer.join().unwrap();
        drop(ManuallyDrop::into_inner(keeper));
        Ok(())
    });
}
#[test]
fn checked_resume_exhausts_independent_history_replacement() {
    replacement(false);
    control(|| replacement(true), "history was not rejected");
}

fn frame(id: u64, body: Body) -> wire::SessionFrame {
    wire::SessionFrame {
        channel_id: id,
        generation: u64::from(id != 0),
        body: Some(body),
    }
}
fn revocation(skip_authorization: bool) {
    verification::check(move || {
        let (sub, records) = input(&["buffered"]);
        let stream = Arc::new(MemoryStream::default());
        let anchor = append(&stream, &sub, &records, 0);
        // Host authority is a Loom atomic. Observation fields below are touched
        // only by the Session owner; they add no scheduling/synchronization edge.
        let allowed = Arc::new(AtomicBool::new(true));
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let last = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let provider = Arc::new(Provider::new(stream.clone(), {
            let allowed = allowed.clone();
            let calls = calls.clone();
            let last = last.clone();
            move |_: &Subscription| {
                let current = allowed.load(Ordering::SeqCst);
                calls.fetch_add(1, Ordering::Relaxed);
                last.store(current, Ordering::Relaxed);
                if current {
                    Ok(())
                } else {
                    Err(StreamError::AccessDenied)
                }
            }
        }));
        let provider = Arc::new(support::Broken {
            inner: provider,
            skip_deletion: false,
            skip_authorization,
        });
        let (sender, incoming) = mpsc::unbounded();
        let mut output = engine::serve_with_publications(
            stream,
            Some(provider),
            "node".into(),
            engine::Limits::default(),
            Box::pin(incoming),
        )
        .unwrap();
        sender
            .unbounded_send(Ok(frame(
                0,
                Body::Hello(wire::SessionHello {
                    version: 1,
                    client_id: Bytes::from_static(b"model"),
                    destination_node: "node".into(),
                    receive_bytes: 1_000_000,
                    receive_frames: 100,
                }),
            )))
            .unwrap();
        sender
            .unbounded_send(Ok(frame(
                0,
                Body::Interest(wire::SessionInterest {
                    revision: 1,
                    remove: vec![],
                    upsert: vec![wire::SessionSubscription {
                        channel: Some(wire::SessionChannel {
                            channel_id: 1,
                            generation: 1,
                        }),
                        resume: Some(cursor(&sub, 2, anchor)),
                        kind: Some(wire::session_subscription::Kind::FsPublications(
                            sub.encode().unwrap(),
                        )),
                    }],
                }),
            )))
            .unwrap();
        run(async {
            assert!(matches!(
                output.next().await.unwrap().unwrap().body,
                Some(Body::Welcome(_))
            ));
            assert!(matches!(
                output.next().await.unwrap().unwrap().body,
                Some(Body::InterestApplied(_))
            ));
            assert!(matches!(
                output.next().await.unwrap().unwrap().body,
                Some(Body::Opened(_))
            ));
            assert!(output.next().now_or_never().is_none()); // buffer before granting credit
        });
        let writer = loom::thread::spawn(move || allowed.store(false, Ordering::SeqCst));
        sender
            .unbounded_send(Ok(frame(
                1,
                Body::Window(wire::SessionWindow {
                    bytes: 1_000_000,
                    frames: 1,
                }),
            )))
            .unwrap();
        let outcome = run(output.next()).unwrap().unwrap();
        assert_eq!(
            calls.load(Ordering::Relaxed),
            2,
            "delivery skipped authorization"
        );
        if last.load(Ordering::Relaxed) {
            assert!(matches!(outcome.body, Some(Body::Data(_))));
        } else {
            let Some(Body::Closed(close)) = outcome.body else {
                panic!("denied frame delivered")
            };
            assert_eq!(close.code, StreamError::AccessDenied.code());
            assert_eq!(
                StreamError::from_session_close(&close.code),
                StreamError::AccessDenied
            );
        }
        writer.join().unwrap();
        Ok(())
    });
}
#[test]
fn resumed_buffered_delivery_exhausts_revocation() {
    revocation(false);
    control(|| revocation(true), "delivery skipped authorization");
}
