use super::*;

pub(super) async fn inspect_idempotency(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::InspectIdempotencyRequest>(&request, "inspect_idempotency")?;
    let key = acyclic_stream::IdempotencyKey::new(request.idempotency_key)
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            client
                .inspect_idempotency(key)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
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
    let view = run_with_cancellation(
        async {
            client
                .heads(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::heads_to_wire(view), "heads")
}

pub(super) async fn head(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::HeadRequest>(&request, "head")?;
    let value = run_with_cancellation(
        async {
            let path = StreamPath::new(request.path).map_err(|error| stream_error(&error))?;
            client
                .head(path)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
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
    let value = run_with_cancellation(
        async {
            wire_codec::tail_response(&**client, request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&value, "tail")
}

pub(super) async fn append(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::append_from_wire(decode::<wire::AppendRequest>(&request, "append")?)
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            client
                .append(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::append_outcome_to_wire(value), "append")
}

pub(super) async fn fork(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::fork_from_wire(decode::<wire::ForkRequest>(&request, "fork")?)
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            client
                .fork(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::fork_receipt_to_wire(&value), "fork")
}

pub(super) async fn read(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request = wire_codec::read_from_wire(decode::<wire::ReadRequest>(&request, "read")?)
        .map_err(|error| stream_error(&error))?;
    let records = run_with_cancellation(
        async {
            client
                .read(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records)))),
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
    let records = run_with_cancellation(
        async move {
            client
                .follow(request)
                .await
                .map(acyclic_stream::records_only)
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(record_frames(records)))),
            token: cancellation_state(cancellation).unwrap_or_default(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    })
}

pub(super) fn record_frames(
    records: impl futures::Stream<
        Item = std::result::Result<acyclic_stream::Record, acyclic_stream::StreamError>,
    > + Send
    + 'static,
) -> FollowFrames {
    records
        .map(|record| {
            record.map(|record| {
                wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
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
    let value = run_with_cancellation(
        async {
            client
                .children_page(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::children_page_to_wire(value), "children_page")
}

pub(super) async fn commit(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::CommitRequest>(&request, "commit")?;
    let request = wire_codec::commit_from_wire(request).map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            client
                .commit(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
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
    let value = run_with_cancellation(
        async {
            client
                .read_commit(commit_id)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(&wire_codec::envelope_to_wire(value), "read_commit")
}
use acyclic_stream::StreamProviderExt as _;
