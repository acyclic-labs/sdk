use super::*;

impl GrpcObjects {
    /// Starts a validated download without buffering the selected representation.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        self.get_stream_controlled(
            response::RequestInput::Ordinary(query),
            maximum_bytes,
            None,
            &mut None,
            None,
        )
        .await
    }

    pub(super) async fn get_stream_controlled(
        &self,
        request_input: response::RequestInput<wire::GetObjectRequest>,
        maximum_bytes: u64,
        controls: Option<Arc<super::NativeTransportErrors>>,
        header_allocation: &mut Option<acyclic_native_runtime::ReadAllocation>,
        wire: Option<PreparedWireBody>,
    ) -> Result<Download, Error> {
        obs::download(
            obs::span!(INFO, "acyclic.objects.grpc.get", rpc.code = obs::Empty),
            async {
                let query = request_input.value();
                request::get(query)?;
                let range = query.range;
                let deadline = tokio::time::Instant::now() + self.transfer_timeout;
                let stream_controls = controls.clone();
                let body_factory = |frames: tonic::Streaming<wire::GetObjectResponse>,
                                    remaining: u64| {
                    stream::try_unfold(
                        (frames, remaining, stream_controls),
                        move |(mut frames, remaining, controls)| async move {
                            let allocation = if let Some(controls) = &controls {
                                controls.check()?;
                                Some(
                                    acyclic_native_runtime::ReadAllocation::adopt(
                                        &controls.meter,
                                        MESSAGE_BYTES,
                                    )
                                    .map_err(|error| controls.fail(error))?,
                                )
                            } else {
                                None
                            };
                            let Some(frame) = tokio::time::timeout_at(deadline, frames.message())
                                .await
                                .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                                .map_err(|status| error_from_status(&status, controls.as_ref()))?
                            else {
                                return if remaining == 0 {
                                    Ok(None)
                                } else {
                                    Err(protocol())
                                };
                            };
                            match frame.frame {
                                Some(wire::get_object_response::Frame::Body(frame)) => {
                                    let bytes = if let Some(controls) = &controls {
                                        controls.body(
                                            frame,
                                            remaining,
                                            allocation.expect("native frame preclaim"),
                                        )?
                                    } else {
                                        bytes::Bytes::from(response::body(frame, remaining)?)
                                    };
                                    let remaining = remaining - bytes.len() as u64;
                                    Ok(Some((bytes, (frames, remaining, controls))))
                                }
                                frame => Err(response::reject_frame(
                                    frame,
                                    allocation,
                                    controls.as_ref(),
                                )),
                            }
                        },
                    )
                };
                let prepared_body =
                    response::prepare_body_stream(&body_factory, controls.as_ref())?;
                let mut client = tonic::client::Grpc::new(ObjectsChannel {
                    channel: self.channel.clone(),
                    wire,
                })
                .max_decoding_message_size(MESSAGE_BYTES)
                .max_encoding_message_size(MESSAGE_BYTES);
                client.ready().await.map_err(|_| {
                    obs::record!("rpc.code" = tonic::Code::Unknown as u64);
                    response::invalid()
                })?;
                let mut request = self.authenticated(
                    request_input.with_controls(controls.clone())?,
                    self.transfer_timeout,
                );
                request.extensions_mut().insert(tonic::GrpcMethod::new(
                    "acyclic.objects.v1.ObjectsService",
                    "GetObject",
                ));
                let invoke = || {
                    client.server_streaming(
                        request,
                        tonic::codegen::http::uri::PathAndQuery::from_static(
                            "/acyclic.objects.v1.ObjectsService/GetObject",
                        ),
                        response::CustodyProstCodec::<
                            wire::GetObjectRequest,
                            wire::GetObjectResponse,
                        >::new(),
                    )
                };
                let (rpc, dispatch_failed) = if let Some(controls) = &controls {
                    controls.dispatch(acyclic_native_runtime::NativeOperationKind::Read, invoke)?
                } else {
                    (invoke(), false)
                };
                let mut frames = rpc
                    .await
                    .map(ok)
                    .map_err(|status| error_from_status(&status, controls.as_ref()))?;
                let first = tokio::time::timeout_at(deadline, frames.message())
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::Unavailable))?
                    .map_err(|status| error_from_status(&status, controls.as_ref()))?
                    .ok_or_else(protocol)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => {
                        return Err(response::reject_frame(
                            frame,
                            header_allocation.take(),
                            controls.as_ref(),
                        ));
                    }
                };
                if let Some(mut allocation) = header_allocation.take() {
                    if let Err(error) =
                        allocation.reconcile_capacity(response::header_capacity(&header))
                    {
                        return Err(controls.as_ref().expect("native header custody").fail(
                            NativeObjectsError::from(error).with_header(header, allocation),
                        ));
                    }
                    *header_allocation = Some(allocation);
                }
                if dispatch_failed {
                    let controls = controls.as_ref().expect("native RPC attempt");
                    let original = controls.take().expect("recorded native RPC error");
                    return Err(controls.fail(original.with_header(
                        header,
                        header_allocation.take().expect("native header custody"),
                    )));
                }
                let remaining = match response::get_header(&header, &range, maximum_bytes) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        if let Some(allocation) = header_allocation.take() {
                            return Err(controls.as_ref().expect("native header custody").fail(
                                NativeObjectsError::from(error).with_header(header, allocation),
                            ));
                        }
                        return Err(error);
                    }
                };
                let body =
                    response::finish_body_stream(body_factory(frames, remaining), prepared_body);
                Ok(Download { header, body })
            },
        )
        .await
    }
}
