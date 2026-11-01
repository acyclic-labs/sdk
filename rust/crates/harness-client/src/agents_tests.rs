use super::*;
use crate::{AgentId, Phase};
use acyclic_stream::StreamProviderExt as _;
use acyclic_stream::{
    AppendRecord, AppendRequest, BoxProviderFuture, MemoryStream, StreamError, StreamProvider,
    runtime::PlatformExecutor,
    session::{self, transport},
};
use futures::{StreamExt as _, future::poll_fn};
use std::sync::Arc;

struct Endpoint(Arc<MemoryStream>);
impl transport::Transport for Endpoint {
    fn session(
        &self,
        source: transport::Requests,
    ) -> BoxProviderFuture<'_, std::result::Result<transport::Responses, StreamError>> {
        Box::pin(async move {
            session::serve(
                self.0.clone(),
                session::testing::authority(),
                "node".into(),
                session::Limits::default(),
                source.map(Ok).boxed(),
            )
        })
    }
}

async fn append(
    memory: &MemoryStream,
    path: StreamPath,
    records: Vec<AppendRecord>,
) -> Result<(), StreamError> {
    memory
        .append(AppendRequest {
            path,
            records,
            if_tail: None,
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    Ok(())
}

async fn status(client: &mut AgentClient) -> Result<StatusRecord, AgentClientError> {
    loop {
        if let AgentEvent::Status { status, .. } = poll_fn(|cx| client.poll_next(cx)).await? {
            return Ok(status);
        }
    }
}

#[tokio::test]
async fn empty_conversation_waits_for_creation_without_selection_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let memory = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&AgentId::from_bytes([9; 16]));
    let path = agent.conversation()?;
    let pool = Client::with_transport(
        Arc::new(Endpoint(memory.clone())),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
        Arc::new(PlatformExecutor),
    );
    pool.register_credential(0, session::testing::credential())?;
    // Admission completes while the stream is absent. Creation must wake the
    // same subscription; no viewport replacement or second open is involved.
    let follow = pool.subscribe(subscription(&path, 0, 0, false)).await?;
    let payload = acyclic_stream::OwnedBytes::from(b"created".to_vec());
    let key = acyclic_stream::OwnedBytes::from(b"key".to_vec());
    let attribute = acyclic_stream::OwnedBytes::from(b"attribute".to_vec());
    let key_owner = acyclic_stream::OwnedBytes::from(vec![b'z'; 64 * 1024]);
    let tiny_key = key_owner.slice(..3);
    let attribute_owner = acyclic_stream::OwnedBytes::from(vec![b'x'; 64 * 1024]);
    let tiny_attribute = attribute_owner.slice(..7);
    append(
        &memory,
        path.clone(),
        vec![AppendRecord {
            value: payload.clone(),
            headers: acyclic_stream::RecordHeaders::new(
                vec![key.clone(), tiny_key.clone()],
                vec![
                    acyclic_stream::Attribute {
                        name: "a".into(),
                        value: attribute.clone(),
                    },
                    acyclic_stream::Attribute {
                        name: "z".into(),
                        value: tiny_attribute.clone(),
                    },
                ],
            )?,
        }],
    )
    .await?;
    let forked = StreamPath::new("reader/inherited-conversation")?;
    memory
        .fork(acyclic_stream::ForkRequest {
            source: path.clone(),
            destination: forked.clone(),
            at_tail: Some(1),
            exclude: Vec::new(),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let inherited = pool.subscribe(subscription(&forked, 0, 0, false)).await?;
    let mut own_charge = 0;
    let backings = crate::reader::backing::BackingPool::default();
    let mut retained = Vec::new();
    for (followed, mut source) in [(path.clone(), follow), (forked.clone(), inherited)] {
        let frame = loop {
            let frame = source.next().await.ok_or("creation interest ended")??;
            match frame.wire().body.as_ref() {
                Some(wire::session_frame::Body::Data(wire::SessionData {
                    item: Some(wire::session_data::Item::Record(record)),
                    ..
                })) => {
                    assert_eq!(record.sequence, 0);
                    assert_eq!(record.value.as_ref(), b"created");
                    assert_eq!(
                        record.origin,
                        if followed == path { "" } else { path.as_str() }
                    );
                    break frame;
                }
                Some(wire::session_frame::Body::Closed(_)) => {
                    return Err("creation interest closed before its record".into());
                }
                _ => {}
            }
        };
        let mut view = StreamView::new(
            crate::StreamViewPolicy::default(),
            0,
            0,
            false,
            Some(followed.clone()),
            backings.clone(),
        );
        let Some(wire::session_frame::Body::Data(wire::SessionData {
            item: Some(wire::session_data::Item::Record(original)),
            ..
        })) = frame.wire().body.as_ref()
        else {
            return Err("missing original wire record".into());
        };
        let original_commit = original.commit_id.clone();
        view.apply(&frame, 1)?;
        assert_eq!(
            view.record(0).ok_or("missing row")?.value.as_ptr(),
            payload.as_slice().as_ptr(),
            "qualified view copied the original payload"
        );
        let row = view.record(0).ok_or("missing header row")?;
        assert_eq!(
            row.commit_id.as_ptr(),
            original_commit.as_ptr(),
            "native commit metadata was copied"
        );
        let headers = [
            (&row.keys[0], &key),
            (&row.attributes[0].value, &attribute),
            (&row.keys[1], &tiny_key),
            (&row.attributes[1].value, &tiny_attribute),
        ];
        let mut copied = 0;
        for (retained, original) in headers {
            assert_eq!(retained.as_ref(), original.as_slice());
            if original.retained_capacity() == Some(original.len()) {
                assert_eq!(
                    retained.as_ptr(),
                    original.as_slice().as_ptr(),
                    "full header backing was copied"
                );
            } else {
                assert_ne!(
                    retained.as_ptr(),
                    original.as_slice().as_ptr(),
                    "tiny header retained uncharged excess backing"
                );
                copied += retained.len();
            }
        }
        assert_eq!(copied, tiny_key.len() + tiny_attribute.len());
        println!(
            "header byte copies: {} -> {copied}",
            original_commit.len()
                + key.len()
                + attribute.len()
                + tiny_key.len()
                + tiny_attribute.len()
        );
        let mut wire_only = StreamView::new(
            crate::StreamViewPolicy::default(),
            0,
            0,
            false,
            Some(followed.clone()),
            Default::default(),
        );
        let raw = frame.into_wire().into();
        wire_only.apply(&raw, 1)?;
        assert_ne!(
            wire_only
                .record(0)
                .ok_or("missing wire row")?
                .value
                .as_ptr(),
            payload.as_slice().as_ptr(),
            "wire-only view retained uncharged backing"
        );
        let row = wire_only.record(0).ok_or("missing wire header row")?;
        assert_ne!(
            row.commit_id.as_ptr(),
            original_commit.as_ptr(),
            "wire-only commit retained unknown backing"
        );
        assert_ne!(
            row.keys[0].as_ptr(),
            key.as_slice().as_ptr(),
            "wire-only key retained unknown backing"
        );
        assert_ne!(
            row.attributes[0].value.as_ptr(),
            attribute.as_slice().as_ptr(),
            "wire-only attribute retained unknown backing"
        );
        if followed == path {
            own_charge = view.retained_bytes();
        } else {
            assert_eq!(
                view.retained_bytes(),
                own_charge + path.as_str().len(),
                "inherited origin was not charged"
            );
            let mut bounded = StreamView::new(
                crate::StreamViewPolicy::new(1, own_charge)?,
                0,
                0,
                false,
                Some(followed),
                Default::default(),
            );
            assert!(matches!(
                bounded.apply(&raw, 1),
                Err(StreamError::LimitExceeded)
            ));
            assert_eq!(bounded.next_sequence(), 0);
            assert_eq!(bounded.revision(), 0);
            assert_eq!(bounded.retained_bytes(), 0);
            assert_eq!(bounded.records().len(), 0);
        }
        retained.push(view);
    }
    assert_eq!(
        backings.bytes(),
        payload.retained_capacity().ok_or("missing capacity")?,
        "shared rows charged the same allocation twice"
    );
    let old = retained[0]
        .record(0)
        .ok_or("missing old row")?
        .value
        .clone();
    drop(retained);
    assert_eq!(
        backings.bytes(),
        payload.len(),
        "old row escaped residency accounting"
    );
    drop(old);
    assert_eq!(backings.bytes(), 0);

    let mut observed = WireSession::new(pool.clone(), DemandPolicy::default());
    observed.replace_streams([path.clone(), forked.clone()].into_iter().map(|path| {
        let request = subscription(&path, 0, 0, false);
        (path, request)
    }))?;
    for path in [&path, &forked] {
        loop {
            let frame = poll_fn(|cx| observed.poll_stream(path, cx))
                .await?
                .ok_or("view interest ended")?;
            if matches!(frame.body, Some(wire::session_frame::Body::Data(_))) {
                break;
            }
        }
        assert_eq!(
            observed
                .view(path)
                .ok_or("missing observed view")?
                .record(0)
                .ok_or("missing observed row")?
                .value
                .as_ptr(),
            payload.as_slice().as_ptr()
        );
    }
    let own = observed.view(&path).ok_or("missing own view")?;
    let inherited = observed.view(&forked).ok_or("missing fork view")?;
    assert_eq!(
        observed.retained_bytes(),
        own.retained_bytes() + inherited.retained_bytes() - payload.len(),
        "session double-counted shared backing"
    );
    let clone = own.record(0).ok_or("missing clone row")?.value.clone();
    observed.replace_streams(std::iter::empty())?;
    assert_eq!(
        observed.retained_bytes(),
        payload.len(),
        "deselection forgot cloned payload custody"
    );
    drop(clone);
    assert_eq!(observed.retained_bytes(), 0);

    // An owner equal to the whole display budget still cannot fit beside row
    // metadata. Both delivery modes detach it, just as for a larger owner.
    for capacity in [1024, 64 * 1024] {
        for latest in [false, true] {
            let owner = acyclic_stream::OwnedBytes::from(vec![b'x'; capacity]);
            let tiny = owner.slice(0..7);
            let path = StreamPath::new(format!("reader/tiny-value-{capacity}-{latest}"))?;
            append(
                &memory,
                path.clone(),
                vec![AppendRecord::latest(tiny.clone(), "status", true)?],
            )
            .await?;
            let mut source = pool.subscribe(subscription(&path, 0, 0, latest)).await?;
            let frame = loop {
                let frame = source.next().await.ok_or("tiny interest ended")??;
                match frame.wire().body.as_ref() {
                    Some(wire::session_frame::Body::Data(_)) => break frame,
                    Some(wire::session_frame::Body::Closed(_)) => {
                        return Err("tiny interest closed before its value".into());
                    }
                    _ => {}
                }
            };
            assert_eq!(
                frame
                    .original_record()
                    .ok_or("missing original tiny record")?
                    .value
                    .as_slice()
                    .as_ptr(),
                tiny.as_slice().as_ptr(),
            );
            let mut tiny_view = StreamView::new(
                crate::StreamViewPolicy::new(1, 1024)?,
                0,
                0,
                latest,
                Some(path),
                backings.clone(),
            );
            tiny_view
                .apply(&frame, 1)
                .expect("tiny slice failed display admission");
            let retained = if latest {
                &tiny_view
                    .latest_value(b"status")
                    .ok_or("missing tiny value")?
                    .value
            } else {
                &tiny_view.record(0).ok_or("missing tiny row")?.value
            };
            assert_eq!(retained.as_ref(), tiny.as_slice());
            assert_ne!(
                retained.as_ptr(),
                tiny.as_slice().as_ptr(),
                "tiny view pinned a giant allocation"
            );
            assert_eq!(tiny_view.next_sequence(), 1);
            assert!(tiny_view.retained_bytes() <= 1024);
            assert_eq!(backings.bytes(), 7, "tiny view charged a giant allocation");
            drop(tiny_view);
            assert_eq!(backings.bytes(), 0);
        }
    }
    Ok(())
}

#[tokio::test]
async fn real_json_status_bootstraps_head_filters_keys_and_rejects_invalid_payload()
-> Result<(), Box<dyn std::error::Error>> {
    let memory = Arc::new(MemoryStream::default());
    let agent = AgentPath::root(&AgentId::from_bytes([8; 16]));
    let path = agent.status()?;
    append(&memory, agent.conversation()?, vec!["past".into()]).await?;
    let history = (0..64)
        .map(|cursor| {
            StatusRecord {
                phase: Phase::Running,
                cursor,
                conversation: 1,
            }
            .append_record()
        })
        .collect::<Result<Vec<_>, _>>()?;
    let original = history[0].value.clone();
    append(&memory, path.clone(), history).await?;
    let pool = Client::with_transport(
        Arc::new(Endpoint(memory.clone())),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
        Arc::new(PlatformExecutor),
    );
    pool.register_credential(0, session::testing::credential())?;
    let mut observer = AgentClient::new(pool.clone(), agent, 1, 0, 0)?;
    assert_eq!(status(&mut observer).await?.cursor, 63);
    assert_eq!(observer.current_status()?.unwrap().cursor, 63);
    // ALL retains the exact JSON bytes; LATEST never requires a nested
    // protobuf value in the stored record.
    let mut all = pool
        .follow_records(acyclic_stream::FollowRequest::new(path.clone(), 0))
        .await?;
    assert_eq!(all.next().await.ok_or("ALL ended")??.value, original);
    append(
        &memory,
        path.clone(),
        vec![AppendRecord::latest(
            "not status JSON",
            "another-key",
            true,
        )?],
    )
    .await?;
    let attention = StatusRecord {
        phase: Phase::WaitingHost {
            host: "codex".into(),
        },
        cursor: 64,
        conversation: 1,
    };
    append(&memory, path.clone(), vec![attention.append_record()?]).await?;
    assert_eq!(status(&mut observer).await?, attention);
    append(
        &memory,
        path,
        vec![AppendRecord::latest("invalid JSON", "status", true)?],
    )
    .await?;
    assert!(matches!(
        status(&mut observer).await,
        Err(AgentClientError::Record(_))
    ));

    // Exercise the same retained-key fold with actual session deliveries,
    // including its eviction order and opaque producer JSON. Attention
    // preserves each original record rather than conflating this history.
    let indexed = StreamPath::new("reader/indexed-status")?;
    let json = attention.append_record()?.value;
    append(
        &memory,
        indexed.clone(),
        (0..128)
            .map(|key| AppendRecord::latest(json.clone(), format!("key{key:03}"), true))
            .collect::<Result<Vec<_>, _>>()?,
    )
    .await?;
    let mut request = subscription(&indexed, 0, 0, true);
    request.filter = None;
    if let Some(wire::session_subscription::Kind::Follow(follow)) = request.kind.as_mut() {
        follow.bootstrap_head = false;
    }
    let mut source = pool.subscribe(request).await?;
    let mut view = StreamView::new(
        crate::StreamViewPolicy::default(),
        0,
        0,
        true,
        Some(indexed.clone()),
        Default::default(),
    );
    let mut one_charge = 0;
    for delivered in 1..=130 {
        let frame = loop {
            let frame = source.next().await.ok_or("indexed status ended")??;
            match frame.wire().body.as_ref() {
                Some(wire::session_frame::Body::Data(_)) => break frame,
                Some(wire::session_frame::Body::Closed(_)) => {
                    return Err("indexed status closed".into());
                }
                _ => {}
            }
        };
        assert_eq!(view.take_latest_probes(), 0);
        view.apply(&frame, delivered)?;
        assert_eq!(view.take_latest_probes(), 1);
        assert_eq!(view.next_sequence(), delivered);
        if delivered == 1 {
            one_charge = view.retained_bytes();
        }
        if delivered == 128 {
            assert_eq!(
                StatusRecord::decode(&view.latest_value(b"key127").unwrap().value)?,
                attention
            );
            assert_eq!(
                view.take_latest_probes(),
                1,
                "retained-key lookup visited unrelated values"
            );
            append(
                &memory,
                indexed.clone(),
                vec![
                    AppendRecord::latest(json.clone(), "key000", true)?,
                    AppendRecord::latest(json.clone(), "key128", true)?,
                ],
            )
            .await?;
        }
        if delivered == 130 {
            assert!(view.latest_value(b"key000").is_some());
            assert!(view.latest_value(b"key001").is_none());
            assert_eq!(
                view.latest_value(b"key128").unwrap().value.as_ref(),
                json.as_ref()
            );
            assert_eq!(view.take_latest_probes(), 3);
            assert_eq!(view.retained_bytes(), 128 * one_charge);
            // A replayed cursor must not remove/reorder a retained value
            // or change the aggregate residency before it is rejected.
            let revision = view.revision();
            let retained = view.retained_bytes();
            let pointer = view.latest_value(b"key128").unwrap().value.as_ptr();
            view.take_latest_probes();
            assert!(matches!(
                view.apply(&frame, delivered + 1),
                Err(StreamError::InvalidResponse)
            ));
            assert_eq!(view.take_latest_probes(), 0);
            assert_eq!(view.revision(), revision);
            assert_eq!(view.retained_bytes(), retained);
            assert_eq!(
                view.latest_value(b"key128").unwrap().value.as_ptr(),
                pointer
            );
        }
    }
    Ok(())
}

/// Capture frames from the real in-process Streams service, rather than a
/// handwritten provider fixture. Resume must exclude an accepted line while
/// retaining every byte of binary application data and its original cursor.
#[tokio::test]
async fn lossless_projection_resumes_at_the_flushed_provider_cursor()
-> Result<(), Box<dyn std::error::Error>> {
    use base64::{Engine as _, engine::general_purpose::STANDARD};
    use prost::Message as _;
    let subscription = |path: &StreamPath, from: u64, credential: u32, latest: bool| {
        let mut request = subscription(path, from, credential, latest);
        if let Some(wire::session_subscription::Kind::Follow(follow)) = &mut request.kind {
            follow.opening_cut = true;
        }
        request
    };
    let memory = Arc::new(MemoryStream::default());
    let path = StreamPath::new("projection/binary")?;
    append(
        &memory,
        path.clone(),
        vec![
            vec![0, 255, 128].into(),
            b"{\"after\":1}".as_slice().into(),
            b"{\"after\":2}".as_slice().into(),
            b"{\"after\":3}".as_slice().into(),
        ],
    )
    .await?;
    let pool = Client::with_transport(
        Arc::new(Endpoint(Arc::clone(&memory))),
        Arc::new(|_| Box::pin(async { Err(StreamError::Unavailable) })),
        Arc::new(PlatformExecutor),
    );
    pool.register_credential(0, session::testing::credential())?;
    let resource = wire_codec::session::stream_resource(path.as_str());
    let mut projection = crate::EventProjection::new("projection-operation".into());
    let mut drain = crate::EventDrain::new([(resource.clone(), 0), ("not-yet-followed".into(), 0)]);
    let mut selected = crate::EventDrain::new([(resource.clone(), 0)]);
    let mut source = pool.subscribe(subscription(&path, 0, 0, false)).await?;
    loop {
        let frame = source
            .next()
            .await
            .ok_or("provider ended before first record")??
            .into_wire();
        let delivery = AgentDelivery {
            resource: resource.clone(),
            frame,
        };
        let event = projection.event(&delivery);
        selected.accepted(&delivery);
        let decoded = wire::SessionFrame::decode(STANDARD.decode(&event.frame)?.as_slice())?;
        assert_eq!(decoded, delivery.frame);
        if event.sequence == Some(0) {
            assert!(event.value.is_none());
            assert_eq!(
                projection.cursor(&resource).next,
                0,
                "unflushed output advanced resume"
            );
            let cursor = event.after.clone();
            projection.accepted(resource.clone(), cursor);
            drain.accepted(&delivery);
            assert!(!drain.complete(), "an unobserved source was skipped");
            assert!(
                !selected.complete(),
                "one record was mistaken for the opening tail"
            );
            break;
        }
    }
    drop(source);
    let selected_agent = AgentPath::root(&crate::AgentId::from_bytes([13; 16]));
    let position = crate::ExecutionPosition { input: 7, after: 3 };
    projection.accepted_execution(&selected_agent, position)?;
    let checkpoint = serde_json::to_vec(&projection)?;
    let mut restored: crate::EventProjection = serde_json::from_slice(&checkpoint)?;
    assert_eq!(restored.execution_position(&selected_agent)?, position);
    let another_agent = AgentPath::root(&crate::AgentId::from_bytes([14; 16]));
    assert!(restored.execution_position(&another_agent).is_err());
    assert!(
        restored
            .accepted_execution(
                &selected_agent,
                crate::ExecutionPosition { input: 7, after: 2 }
            )
            .is_err()
    );
    assert_eq!(
        serde_json::to_vec(&restored)?,
        checkpoint,
        "refusal mutated resume"
    );
    restored.accepted_execution(
        &selected_agent,
        crate::ExecutionPosition {
            input: u64::MAX,
            after: u64::MAX,
        },
    )?;
    assert!(
        serde_json::to_vec(&restored)?.len() <= checkpoint.len() + 40,
        "retired inputs accumulated cursor state instead of replacing it"
    );
    restored.validate("projection-operation")?;
    assert!(restored.validate("another-operation").is_err());
    assert!(
        restored
            .validate_sources(&["another-source".into()])
            .is_err()
    );
    let mut request = subscription(&path, 0, 0, false);
    request.resume = Some(restored.cursor(&resource).wire()?);
    let mut resumed = pool.subscribe(request).await?;
    let mut drain = crate::EventDrain::new([(resource.clone(), 1)]);
    let mut records = 0;
    let mut appended = false;
    while !drain.complete() {
        let delivery = AgentDelivery {
            resource: resource.clone(),
            frame: resumed
                .next()
                .await
                .ok_or("provider ended before resumed cut")??
                .into_wire(),
        };
        let event = restored.event(&delivery);
        if let Some(sequence) = event.sequence {
            records += 1;
            assert_eq!(sequence, records, "resume replayed or skipped a record");
            assert_eq!(event.value, Some(serde_json::json!({"after":records})));
        }
        let after = event.after.clone();
        restored.accepted(resource.clone(), after);
        drain.accepted(&delivery);
        if matches!(&delivery.frame.body, Some(wire::session_frame::Body::Watermark(mark)) if mark.opening_tail == Some(4))
            && !appended
        {
            append(&memory, path.clone(), vec![b"later".as_slice().into()]).await?;
            appended = true;
        }
    }
    assert_eq!(records, 3);
    assert!(
        appended,
        "the provider did not expose its fixed opening cut"
    );
    // Consume the later record, then reopen at the exact tail. Completion must
    // come from the registration event, without waiting for another append.
    let mut request = subscription(&path, 0, 0, false);
    request.resume = Some(restored.cursor(&resource).wire()?);
    let mut source = pool.subscribe(request).await?;
    let mut drain = crate::EventDrain::new([(resource.clone(), 4)]);
    while !drain.complete() {
        let delivery = AgentDelivery {
            resource: resource.clone(),
            frame: source.next().await.ok_or("missing later cut")??.into_wire(),
        };
        let after = restored.event(&delivery).after;
        restored.accepted(resource.clone(), after);
        drain.accepted(&delivery);
    }
    assert_eq!(restored.cursor(&resource).next, 5);
    drop(source);
    let mut request = subscription(&path, 0, 0, false);
    request.resume = Some(restored.cursor(&resource).wire()?);
    let mut source = pool.subscribe(request).await?;
    let mut drain = crate::EventDrain::new([(resource.clone(), 5)]);
    while !drain.complete() {
        let delivery = AgentDelivery {
            resource: resource.clone(),
            frame: source
                .next()
                .await
                .ok_or("exact-tail replay ended")??
                .into_wire(),
        };
        assert!(restored.event(&delivery).sequence.is_none());
        drain.accepted(&delivery);
    }
    let empty = StreamPath::new("projection/empty")?;
    memory
        .fork(acyclic_stream::ForkRequest {
            exclude: Vec::new(),
            source: path,
            destination: empty.clone(),
            at_tail: Some(0),
            idempotency_key: acyclic_stream::IdempotencyKey::unique(),
        })
        .await?;
    let resource = wire_codec::session::stream_resource(empty.as_str());
    let mut source = pool.subscribe(subscription(&empty, 0, 0, false)).await?;
    let mut drain = crate::EventDrain::new([(resource.clone(), 0)]);
    while !drain.complete() {
        let delivery = AgentDelivery {
            resource: resource.clone(),
            frame: source
                .next()
                .await
                .ok_or("empty replay ended")??
                .into_wire(),
        };
        assert!(matches!(
            delivery.frame.body,
            Some(wire::session_frame::Body::Watermark(_))
        ));
        drain.accepted(&delivery);
    }
    Ok(())
}
