use super::*;

// Keep the call expression inside the admitted future: constructing a provider
// future can allocate or start work even before its first poll.
macro_rules! run_operation {
    ($operation:expr, $cancellation:expr $(, $project:expr)?) => {
        run_with_cancellation(
            async { ($operation).await$(.map($project))?.map_err(|error| stream_error(&error)) },
            cancellation_state($cancellation),
        )
    };
}

pub(super) async fn inspect_idempotency(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::InspectIdempotencyRequest>(&request, "inspect_idempotency")?;
    let key = acyclic_stream::IdempotencyKey::new(request.idempotency_key)
        .map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.inspect_idempotency(key), cancellation).await?;
    encode(
        &wire::InspectIdempotencyResponse {
            observation: value.map(wire_codec::observation_wire),
        },
        "inspect_idempotency",
    )
}

pub(super) async fn heads(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::heads_from_wire(decode::<wire::HeadsRequest>(&request, "heads")?)
        .map_err(|error| stream_error(&error))?;
    let view = run_operation!(client.heads(request), cancellation).await?;
    let view = wire_codec::heads_to_wire(view).map_err(|error| stream_error(&error))?;
    encode(&view, "heads")
}

pub(super) async fn head(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::HeadRequest>(&request, "head")?;
    let value = run_operation!(
        {
            let path = StreamPath::new(request.path).map_err(|error| stream_error(&error))?;
            client.head(path)
        },
        cancellation
    )
    .await?;
    encode(&wire_codec::head_to_wire(value), "head")
}

pub(super) async fn tail(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::TailRequest>(&request, "tail")?;
    let value = run_operation!(wire_codec::tail_response(&**client, request), cancellation).await?;
    encode(&value, "tail")
}

pub(super) async fn append(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::append_from_wire(decode::<wire::AppendRequest>(&request, "append")?)
        .map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.append(request), cancellation).await?;
    encode(&wire_codec::append_outcome_to_wire(value), "append")
}

pub(super) async fn fork(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::fork_from_wire(decode::<wire::ForkRequest>(&request, "fork")?)
        .map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.fork(request), cancellation).await?;
    encode(&wire_codec::fork_receipt_to_wire(&value), "fork")
}

pub(super) async fn read(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request = wire_codec::read_from_wire(decode::<wire::ReadRequest>(&request, "read")?)
        .map_err(|error| stream_error(&error))?;
    let path = request.path.clone();
    let records = run_operation!(client.read(request), cancellation).await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records, path)))),
            token: cancellation_state(cancellation).unwrap_or_default(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    })
}

pub(super) async fn open_follow(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request = wire_codec::follow_from_wire(decode::<wire::FollowRequest>(&request, "follow")?)
        .map_err(|error| stream_error(&error))?;
    if request.progress {
        return Err(stream_error(&StreamError::InvalidArgument));
    }
    let path = request.path.clone();
    let records = run_operation!(
        client.follow(request),
        cancellation,
        acyclic_stream::records_only
    )
    .await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records, path)))),
            token: cancellation_state(cancellation).unwrap_or_default(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    })
}

/// Encodes the records of `path`, spelling only an origin other than it.
pub(super) fn record_frames(
    records: impl futures::Stream<
        Item = std::result::Result<acyclic_stream::Record, acyclic_stream::StreamError>,
    > + Send
    + 'static,
    path: acyclic_stream::StreamPath,
) -> FollowFrames {
    records
        .map(move |record| {
            record.map(|record| {
                wire_codec::read_response_wire(vec![wire_codec::record_wire(record, Some(&path))])
                    .encode_to_vec()
            })
        })
        .boxed()
}

pub(super) async fn children_page(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::children_page_from_wire(decode::<wire::ChildrenPageRequest>(
        &request,
        "children_page",
    )?)
    .map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.children_page(request), cancellation).await?;
    encode(&wire_codec::children_page_to_wire(value), "children_page")
}

pub(super) async fn commit(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::CommitRequest>(&request, "commit")?;
    let request = wire_codec::commit_from_wire(request).map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.commit(request), cancellation).await?;
    encode(&wire_codec::commit_outcome_to_wire(value), "commit")
}

pub(super) async fn read_commit(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::ReadCommitRequest>(&request, "read_commit")?;
    let commit_id =
        wire_codec::commit_id(&request.commit_id).map_err(|error| stream_error(&error))?;
    let value = run_operation!(client.read_commit(commit_id), cancellation).await?;
    encode(&wire_codec::envelope_to_wire(value), "read_commit")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn operation_admission_defers_construction_and_releases_cancelled_work() {
        let cancellation = NativeStreamCancellation::new();
        cancellation.cancel();
        let constructed = std::cell::Cell::new(0);
        let operation = || {
            constructed.set(constructed.get() + 1);
            std::future::ready(Ok::<_, StreamError>(()))
        };
        let error = run_operation!(operation(), Some(&cancellation))
            .await
            .expect_err("cancelled before construction");
        assert_eq!(error.code, "cancelled");
        assert_eq!(constructed.get(), 0);
        // Negative control: an eagerly constructed future has already done work.
        let eager = operation();
        assert!(run_operation!(eager, Some(&cancellation)).await.is_err());
        assert_eq!(constructed.get(), 1);

        let (producer, event) = tokio::sync::oneshot::channel::<()>();
        let pending = run_operation!(
            async { event.await.map_err(|_| StreamError::Unavailable) },
            None
        );
        futures::pin_mut!(pending);
        // Retaining the producer is the negative control for terminal progress.
        assert!(futures::poll!(&mut pending).is_pending());
        drop(producer);
        let error = pending.await.expect_err("dropped event producer");
        assert_eq!(error.code, StreamError::Unavailable.code());
        assert_eq!(error.message, StreamError::Unavailable.to_string());

        let cancellation = NativeStreamCancellation::new();
        let (producer, event) = tokio::sync::oneshot::channel::<()>();
        let pending = run_operation!(
            async { event.await.map_err(|_| StreamError::Unavailable) },
            Some(&cancellation)
        );
        futures::pin_mut!(pending);
        assert!(futures::poll!(&mut pending).is_pending());
        cancellation.cancel();
        assert_eq!(
            pending.await.expect_err("cancelled event wait").code,
            "cancelled"
        );
        assert!(
            producer.send(()).is_err(),
            "cancelled work released its event"
        );
    }
}
use acyclic_stream::StreamProviderExt as _;
