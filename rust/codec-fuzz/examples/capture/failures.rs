use acyclic_actors::{
    guest::{GuestError, Subscription, Trigger},
    local::{FailureDisposition, FailureEntry, FailureResolution, PreparedPublish, Store},
};
use acyclic_auth::addressing::ActorReference;
use acyclic_stream::{
    AppendRequest, ChildrenPageRequest, IdempotencyKey, LocalStream, ReadRequest, Record,
    RevisionCut, StreamPath, StreamProvider, wire_codec,
};
use bytes::Bytes;
use futures::TryStreamExt;
use prost::Message;
use std::{collections::BTreeMap, error::Error, sync::Arc, sync::atomic::AtomicU64};

async fn read(
    provider: &LocalStream,
    path: &StreamPath,
    sequence: u64,
) -> Result<Record, Box<dyn Error>> {
    Ok(provider
        .read(ReadRequest {
            key: None,
            path: path.clone(),
            from: sequence,
            limit: 1,
            at_least: RevisionCut::default(),
        })
        .await?
        .try_next()
        .await?
        .ok_or("committed failure capture is missing")?)
}

pub(super) fn capture() -> Result<Vec<(String, Vec<u8>)>, Box<dyn Error>> {
    tokio::runtime::Builder::new_current_thread()
        .build()?
        .block_on(async {
            let directory = tempfile::tempdir()?;
            let clock = Arc::new(AtomicU64::new(42));
            let provider = Arc::new(
                LocalStream::open_with_clock(directory.path(), Default::default(), clock.clone())
                    .await?,
            );
            let mut records = Vec::new();
            for resolution in [FailureResolution::Replay, FailureResolution::Purge] {
                let name = match resolution {
                    FailureResolution::Replay => "replay",
                    FailureResolution::Purge => "purge",
                };
                let key = |operation: &str| {
                    IdempotencyKey::new(format!("capture/failure/{name}/{operation}").into_bytes())
                };
                let actor = ActorReference::new(b"codec", b"failures", name.as_bytes())?;
                let source = StreamPath::new(format!("codec/inputs/{name}"))?;
                let store = Store::new(provider.clone(), StreamPath::new("codec/actors")?);
                let mut create = PreparedPublish::foreign(key("create")?);
                for effect in store.create(
                    &actor,
                    BTreeMap::from([(
                        "inbox".into(),
                        Subscription {
                            path: source.clone(),
                            cursor: 0,
                        },
                    )]),
                    None,
                )? {
                    create.join(effect)?;
                }
                store.submit(create).await?;
                store
                    .claim(&store.load(&actor).await?, 1, key("claim")?)
                    .await?;
                provider
                    .append(AppendRequest {
                        path: source.clone(),
                        records: vec![Bytes::from_static(b"original failed input").into()],
                        if_tail: None,
                        idempotency_key: key("input")?,
                    })
                    .await?;
                let input = read(&provider, &source, 0).await?;
                let previous = store.load(&actor).await?;
                let cause = GuestError::Decode("selected application failure".into());
                store
                    .submit(store.prepare_failed_input(
                        &previous,
                        1,
                        &Trigger::Record {
                            subscription: "inbox".into(),
                            sequence: 0,
                        },
                        &input,
                        &cause,
                        key("failed")?,
                    )?)
                    .await?;
                let page = provider
                    .children_page(ChildrenPageRequest {
                        parent: Some(store.failures_path(&actor)?),
                        after: None,
                        hierarchy_version: None,
                        tail_stream: Some(StreamPath::new("status")?),
                        limit: 1,
                    })
                    .await?;
                let child = page
                    .children
                    .first()
                    .ok_or("failure child was not published")?;
                let entry = child
                    .tail_record
                    .as_ref()
                    .ok_or("failure status is missing")?;
                let FailureEntry::Failed(failed) = FailureEntry::decode(&child.path, entry)? else {
                    return Err("new failure was already resolved".into());
                };
                if failed.cause() != &cause || failed.source() != &source {
                    return Err("failure capture lost original cause or source".into());
                }
                let body = read(&provider, &failed.input_path()?, 0).await?;
                if failed.input(&body)? != input {
                    return Err("failure capture lost original input".into());
                }
                records.push((format!("failure-{name}"), entry.clone()));
                let disposition = match resolution {
                    FailureResolution::Replay => FailureDisposition::Replay(&body),
                    FailureResolution::Purge => FailureDisposition::Purge,
                };
                store
                    .submit(store.prepare_disposition(
                        &actor,
                        &failed,
                        disposition,
                        key("resolve")?,
                    )?)
                    .await?;
                let terminal = read(&provider, &entry.origin, 1).await?;
                if FailureEntry::decode(&child.path, &terminal)?
                    != FailureEntry::Resolved(resolution)
                {
                    return Err("failure capture lost its committed resolution".into());
                }
                records.push((format!("failure-{name}-resolved"), terminal));
            }
            drop(provider);
            let reopened =
                LocalStream::open_with_clock(directory.path(), Default::default(), clock).await?;
            let mut captures = Vec::new();
            for (name, original) in records {
                let recovered = read(&reopened, &original.origin, original.sequence).await?;
                if recovered != original {
                    return Err("failure capture changed across durable reopen".into());
                }
                let bytes = wire_codec::record_wire(recovered, None).encode_to_vec();
                let admitted = wire_codec::record(
                    acyclic_stream::wire::Record::decode(bytes.as_slice())?,
                    None,
                )?;
                if admitted != original {
                    return Err("failure wire capture changed its recovered record".into());
                }
                captures.push((name, bytes));
            }
            Ok(captures)
        })
}
