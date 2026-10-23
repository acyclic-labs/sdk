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

pub(super) async fn tail(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::TailRequest>(&request, "tail")?;
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(client), request.path)
        .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async { stream.tail().await.map_err(|error| stream_error(&error)) },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire::TailResponse {
            tail: value,
            ..Default::default()
        },
        "tail",
    )
}

pub(super) async fn bounds(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = decode::<wire::TailRequest>(&request, "bounds")?;
    let value = run_with_cancellation(
        async {
            let path = StreamPath::new(&request.path).map_err(|error| stream_error(&error))?;
            client
                .bounds(path)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    encode(
        &wire::TailResponse {
            tail: value.tail,
            ..Default::default()
        },
        "bounds",
    )
}

pub(super) async fn append(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Buffer, NativeStreamErrorMetadata> {
    let request = wire_codec::append_from_wire(decode::<wire::AppendRequest>(&request, "append")?)
        .map_err(|error| stream_error(&error))?;
    let stream =
        acyclic_stream::Stream::new(std::sync::Arc::clone(client), request.path.to_string())
            .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            stream
                .append_batch(request.records, request.if_tail, request.idempotency_key)
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
    let stream =
        acyclic_stream::Stream::new(std::sync::Arc::clone(client), request.source.to_string())
            .map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async move {
            stream
                .fork(
                    request.destination.to_string(),
                    request.at_tail,
                    request.idempotency_key,
                )
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
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request = wire_codec::read_from_wire(decode::<wire::ReadRequest>(&request, "read")?)
        .map_err(|error| stream_error(&error))?;
    let stream =
        acyclic_stream::Stream::new(std::sync::Arc::clone(client), request.path.to_string())
            .map_err(|error| stream_error(&error))?;
    let token = cancellation_state(cancellation);
    let records = stream.read(request.from, request.limit);
    let mut records = run_with_cancellation(
        async { records.await.map_err(|error| stream_error(&error)) },
        token.clone(),
    )
    .await?;
    let mut output = Vec::new();
    while let Some(value) = run_with_cancellation(
        async {
            records
                .next()
                .await
                .transpose()
                .map_err(|error| stream_error(&error))
        },
        token.clone(),
    )
    .await?
    {
        output.push(encode(
            &wire_codec::read_response_wire(vec![wire_codec::record_wire(value)]),
            "read",
        )?);
    }
    Ok(output)
}

pub(super) async fn open_follow(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request = decode::<wire::FollowRequest>(&request, "follow")?;
    let filter = acyclic_stream::FollowFilter::new(request.filter.unwrap_or_default())
        .map_err(|error| stream_error(&error))?;
    let stream = acyclic_stream::Stream::new(std::sync::Arc::clone(client), request.path)
        .map_err(|error| stream_error(&error))?;
    let records = run_with_cancellation(
        async move {
            stream
                .follow(request.from, filter)
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

pub(super) fn record_frames(records: acyclic_stream::RecordStream) -> FollowFrames {
    records
        .map(|record| {
            record.map(|record| {
                wire_codec::read_response_wire(vec![wire_codec::record_wire(record)])
                    .encode_to_vec()
            })
        })
        .boxed()
}

pub(super) async fn open_children(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<NativeStreamFollow, NativeStreamErrorMetadata> {
    let request =
        wire_codec::children_from_wire(decode::<wire::ChildrenRequest>(&request, "children")?)
            .map_err(|error| stream_error(&error))?;
    let children = run_with_cancellation(
        async {
            client
                .children(request)
                .await
                .map_err(|error| stream_error(&error))
        },
        cancellation_state(cancellation),
    )
    .await?;
    let frames = children
        .map(|child| {
            child.map(|child| {
                wire::ChildrenResponse {
                    child: Some(wire::Child {
                        path: child.path.to_string(),
                    }),
                }
                .encode_to_vec()
            })
        })
        .boxed();
    Ok(NativeStreamFollow {
        state: FollowState {
            records: Arc::new(Mutex::new(Some(frames))),
            token: cancellation_state(cancellation).unwrap_or_default(),
            closed: Arc::new(AtomicBool::new(false)),
        },
    })
}

pub(super) async fn children(
    client: &Arc<grpc::Client>,
    request: Buffer,
    cancellation: Option<&NativeStreamCancellation>,
) -> std::result::Result<Vec<Buffer>, NativeStreamErrorMetadata> {
    let request =
        wire_codec::children_from_wire(decode::<wire::ChildrenRequest>(&request, "children")?)
            .map_err(|error| stream_error(&error))?;
    let token = cancellation_state(cancellation);
    let values = run_with_cancellation(
        async {
            client
                .children_page(acyclic_stream::ChildrenPageRequest {
                    parent: request.parent,
                    limit: request.limit,
                    after: None,
                    hierarchy_version: None,
                })
                .await
                .map_err(|error| stream_error(&error))
        },
        token.clone(),
    )
    .await?;
    let mut output = Vec::new();
    for value in values.children {
        output.push(encode(
            &wire::ChildrenResponse {
                child: Some(wire::Child {
                    path: value.path.to_string(),
                }),
            },
            "children",
        )?);
    }
    Ok(output)
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
    let deadline = request.deadline_unix_millis;
    let request = wire_codec::commit_from_wire(request).map_err(|error| stream_error(&error))?;
    let value = run_with_cancellation(
        async {
            match deadline {
                Some(deadline) => client.commit_before(request, deadline).await,
                None => client.commit(request).await,
            }
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
