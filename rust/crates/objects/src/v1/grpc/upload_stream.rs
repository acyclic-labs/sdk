use super::*;

impl GrpcObjects {
    /// Publishes a caller-owned stream; a source failure cancels the RPC before valid EOF.
    pub fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::ObjectInfo, Error>> + Send + 'static {
        let owner = self.clone();
        async move {
            Ok(Self::put_stream_controlled(
                owner,
                response::RequestInput::Ordinary(query),
                body,
                None,
                None,
            )
            .await?
            .0)
        }
    }

    /// Streams one staged multipart part without publishing the final object.
    pub fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> impl Future<Output = Result<wire::UploadedPart, Error>> + Send + 'static {
        let owner = self.clone();
        async move {
            obs::traced(
                obs::span!(
                    INFO,
                    "acyclic.objects.grpc.upload_part",
                    rpc.code = obs::Empty
                ),
                async {
                    request::upload_part(&query)?;
                    let part_number = query.part_number;
                    let header = wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Header(query)),
                    };
                    let (chunks, state) = upload::chunks(body, None)?;
                    let frames = chunks.map(
                        part_frame as fn(Result<OwnedBytes, Error>) -> wire::UploadPartRequest,
                    );
                    let mut client = owner.multipart();
                    let complete = wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Complete(true)),
                    };
                    let input = owner.authenticated(
                        stream::iter([header])
                            .chain(frames)
                            .chain(stream::iter([complete]))
                            .boxed(),
                        owner.transfer_timeout,
                    );
                    let result = upload::run(
                        client
                            .upload_part(input)
                            .map(|reply| {
                                reply
                                    .map(ok)
                                    .map_err(|status| error_from_status(&status, None))
                            })
                            .boxed(),
                        &state,
                    )
                    .await?;
                    response::part(&result, part_number, state.finish()?)?;
                    Ok(result)
                },
            )
            .await
        }
    }

    pub(super) async fn put_stream_controlled(
        owner: Self,
        request_input: response::RequestInput<wire::PutObjectHeader>,
        body: UploadBody,
        controls: Option<Arc<super::NativeTransportErrors>>,
        wire: Option<PreparedWireBody>,
    ) -> Result<
        (
            wire::ObjectInfo,
            Option<acyclic_native_runtime::ReadAllocation>,
        ),
        Error,
    > {
        obs::traced(
            obs::span!(INFO, "acyclic.objects.grpc.put", rpc.code = obs::Empty),
            async {
                let query = request_input.value();
                let response_allocation = controls
                    .as_ref()
                    .map(|controls| {
                        acyclic_native_runtime::ReadAllocation::adopt(
                            &controls.meter,
                            MESSAGE_BYTES * 4,
                        )
                        .map_err(|error| controls.fail(error))
                    })
                    .transpose()?;
                request::put(query)?;
                let header = request_input
                    .map(|query| wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Header(query)),
                    })
                    .with_controls(controls.clone())?;
                let (chunks, state) = upload::chunks(body, controls.clone())?;
                let frames = put_frames(chunks, controls.clone(), header);
                let mut client = tonic::client::Grpc::new(ObjectsChannel {
                    channel: owner.channel.clone(),
                    wire,
                })
                .max_decoding_message_size(MESSAGE_BYTES)
                .max_encoding_message_size(MESSAGE_BYTES);
                client.ready().await.map_err(|_| {
                    obs::record!("rpc.code" = tonic::Code::Unknown as u64);
                    response::invalid()
                })?;
                let complete = wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Complete(true)),
                };
                let complete = if let Some(controls) = &controls {
                    let packet = super::ObjectsNativeRequest::new(complete, (), &controls.meter)
                        .map_err(|error| controls.fail(error))?;
                    response::RequestInput::native(packet, &controls.meter)
                        .map_err(|error| controls.fail(error))?
                        .with_controls(Some(controls.clone()))?
                } else {
                    response::RequestInput::Ordinary(complete)
                };
                let mut input = owner.authenticated(
                    frames.chain(stream::iter([complete])),
                    owner.transfer_timeout,
                );
                input.extensions_mut().insert(tonic::GrpcMethod::new(
                    "acyclic.objects.v1.ObjectsService",
                    "PutObject",
                ));
                let invoke = || {
                    client.client_streaming(
                    input,
                    tonic::codegen::http::uri::PathAndQuery::from_static(
                        "/acyclic.objects.v1.ObjectsService/PutObject",
                    ),
                    response::CustodyProstCodec::<wire::PutObjectRequest, wire::ObjectInfo>::new(),
                )
                };
                let (rpc, dispatch_failed) = if let Some(controls) = &controls {
                    controls.dispatch(acyclic_native_runtime::NativeOperationKind::Write, invoke)?
                } else {
                    (invoke(), false)
                };
                let result = upload::run(
                    rpc.map(|reply| {
                        reply
                            .map(ok)
                            .map_err(|status| error_from_status(&status, controls.as_ref()))
                    }),
                    &state,
                )
                .await?;
                let length = if dispatch_failed {
                    Err(protocol())
                } else {
                    state.finish()
                };
                response::validate_put_controlled(
                    result,
                    response_allocation,
                    length,
                    controls.as_ref(),
                )
            },
        )
        .await
    }
}
