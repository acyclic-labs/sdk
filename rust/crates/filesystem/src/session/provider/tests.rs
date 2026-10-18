use super::*;
use acyclic_stream::{AppendOutcome, AppendRequest, MemoryStream, RecordStream};
use acyclic_test_support::{differential, measure};
use futures::FutureExt;

// Independent old encoder/owned decoder, kept solely as the comparison oracle.
fn reference(sub: &Subscription, record: acyclic_stream::Record) -> session::SessionData {
    acyclic_test_support::differential::record(("publication", record.sequence));
    let durable = crate::streams_record::StreamsDurableRecord::decode(
        &record.value,
        acyclic_stream::MAX_RECORD_BYTES as u64,
    )
    .unwrap();
    let generation = crate::kernel::decode_published_generation::<crate::FsError>(
        &durable.0.payload,
        acyclic_stream::MAX_RECORD_BYTES as u64,
        |_| Ok(()),
    )
    .unwrap();
    let workspace = || wire::WorkspaceRef {
        workspace_id: generation.volume_id.into_bytes().to_vec(),
        name: sub.workspace_name.as_str().into(),
    };
    let publication = wire::SessionFsPublication {
        version: 1,
        volume_id: generation.volume_id.into_bytes().to_vec(),
        generation: Some(wire::GenerationRef {
            workspace: Some(workspace()),
            generation_id: generation.generation_root.digest.as_bytes().to_vec(),
        }),
        sequence: record.sequence,
        commit_id: record.commit_id.as_bytes().to_vec(),
    };
    let next = record.sequence + 1;
    let cursor = wire::SessionFsCursor {
        version: 1,
        subscription: Some(wire::SessionFsSubscription {
            version: 1,
            volume_id: sub.volume_id.into_bytes().to_vec(),
            path_prefix: sub.path_prefix.as_str().into(),
            delivery: sub.delivery as i32,
            start_next: sub.start_next,
            workspace: Some(workspace()),
        }),
        next,
        preceding_commit_id: record.commit_id.as_bytes().to_vec(),
    };
    session::SessionData {
        after: Some(session::SessionCursor {
            next,
            opaque: Bytes::from(cursor.encode_to_vec()),
        }),
        accepted: next,
        durable: 0,
        item: Some(session::session_data::Item::FsPublication(Bytes::from(
            publication.encode_to_vec(),
        ))),
    }
}

async fn initialize(sub: &Subscription, input: &[Bytes]) -> (Arc<MemoryStream>, CommitId) {
    let stream = Arc::new(MemoryStream::default());
    let AppendOutcome::Committed(receipt) = stream
        .append(AppendRequest {
            path: crate::distributed::authority_records_path(sub.volume_id).unwrap(),
            records: input.iter().map(|bytes| bytes.to_vec().into()).collect(),
            if_tail: Some(0),
            idempotency_key: None,
        })
        .await
        .unwrap()
    else {
        panic!("append conflicted")
    };
    (stream, receipt.commit_id)
}

async fn candidate(sub: &Subscription, input: &[Bytes]) -> PublicationStream {
    let (stream, anchor) = initialize(sub, input).await;
    let resume = session::SessionCursor {
        next: 2,
        opaque: Cursor::encode_parts(sub, 2, anchor).unwrap(),
    };
    Provider::new(stream, |_: &Subscription| Ok(()))
        .subscribe(sub.encode().unwrap(), resume)
        .unwrap()
        .open
        .await
        .unwrap()
}

async fn baseline(sub: &Subscription, input: &[Bytes]) -> RecordStream {
    let (stream, anchor) = initialize(sub, input).await;
    acyclic_test_support::differential::record(("checked_follow", 2u64));
    stream
        .follow_checked(
            crate::distributed::authority_records_path(sub.volume_id).unwrap(),
            2,
            anchor,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn provider_resume_allocates_only_two_outputs_and_detects_subscription_clone() {
    let fs = crate::Fs::memory();
    let workspace = fs
        .create_workspace(
            "allocation",
            crate::WorkBudget::UNBOUNDED,
            &crate::CancellationToken::new(),
        )
        .await
        .unwrap()
        .value;
    workspace.write_text("/entry", "published").await.unwrap();
    let sub = Subscription {
        volume_id: workspace.id().volume_id(),
        workspace_name: workspace.name().clone(),
        path_prefix: PortablePath::parse("/", crate::VolumeLimits::default()).unwrap(),
        delivery: wire::SessionFsDelivery::All,
        start_next: 0,
    };
    let (original, _) = fs.authority_records(sub.volume_id).unwrap();
    let input: Vec<Bytes> = original
        .read(acyclic_stream::ReadRequest {
            path: crate::distributed::authority_records_path(sub.volume_id).unwrap(),
            from: 0,
            limit: 3,
        })
        .await
        .unwrap()
        .map(|record| Bytes::copy_from_slice(&record.unwrap().value))
        .collect()
        .await;
    assert_eq!(input.len(), 3);
    let mut old = baseline(&sub, &input).await;
    let mut new = candidate(&sub, &input).await;
    let (output, counts) = differential(
        || reference(&sub, old.next().now_or_never().unwrap().unwrap().unwrap()),
        || new.next().now_or_never().unwrap().unwrap().unwrap(),
    );
    assert_eq!(
        counts[1].count_total, 2,
        "provider added temporary allocations"
    );
    let publication_bytes = match output.item.as_ref().unwrap() {
        session::session_data::Item::FsPublication(bytes) => bytes.len(),
        _ => panic!("expected publication"),
    };
    let output_bytes = publication_bytes + output.after.as_ref().unwrap().opaque.len();
    assert_eq!(counts[1].bytes_total, output_bytes as u64);
    assert!(counts[0].count_total > counts[1].count_total);
    // Mutate the actual provider projection, rather than an encoder-only call.
    let mut broken = candidate(&sub, &input).await;
    COPY_SUBSCRIPTION.set(true);
    let mut copied = None;
    let extra = measure(|| copied = Some(broken.next().now_or_never().unwrap().unwrap().unwrap()));
    COPY_SUBSCRIPTION.set(false);
    assert_eq!(copied.as_ref(), Some(&output));
    assert_eq!(extra.count_total, counts[1].count_total + 1);
    assert_eq!(
        extra.bytes_total,
        counts[1].bytes_total + sub.path_prefix.as_str().len() as u64
    );
    let trace = acyclic_test_support::differential::pair(
        || (),
        |(), old| {
            futures::executor::block_on(async {
                if old {
                    let mut stream = baseline(&sub, &input).await;
                    reference(&sub, stream.next().await.unwrap().unwrap()).encode_to_vec()
                } else {
                    candidate(&sub, &input)
                        .await
                        .next()
                        .await
                        .unwrap()
                        .unwrap()
                        .encode_to_vec()
                }
            })
        },
    );
    let [(before, operations), after]: [(Vec<u8>, Vec<(&str, u64)>); 2] = trace;
    assert_eq!(operations, vec![("checked_follow", 2), ("publication", 2)]);
    assert_eq!((before, operations), after);
}
