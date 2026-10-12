use super::*;

impl HttpObjects {
    /// Publishes a header-first streamed value. Source failure aborts the HTTP request.
    pub async fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        Ok(self
            .put_stream_controlled(response::RequestInput::Ordinary(query), body, None)
            .await?
            .0)
    }

    pub(super) async fn put_stream_controlled(
        &self,
        request_input: response::RequestInput<wire::PutObjectHeader>,
        body: UploadBody,
        controls: Option<Arc<super::NativeTransportErrors>>,
    ) -> Result<
        (
            wire::ObjectInfo,
            Option<acyclic_native_runtime::ReadAllocation>,
        ),
        Error,
    > {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/put",
                rpc.code = obs::Empty
            ),
            async {
                let query = request_input.value();
                let mut response_allocation = controls
                    .as_ref()
                    .map(|controls| {
                        acyclic_native_runtime::ReadAllocation::adopt(
                            &controls.meter,
                            self.maximum.saturating_mul(4),
                        )
                        .map_err(|error| controls.fail(error))
                    })
                    .transpose()?;
                request::put(query)?;
                let header_frame = request_input.map(|query| wire::PutObjectRequest {
                    frame: Some(wire::put_object_request::Frame::Header(query)),
                });
                let header = line("PutObjectRequest", header_frame.value(), controls.as_ref())?;
                drop(header_frame);
                let (chunks, state) = upload::chunks(body, controls.clone())?;
                let frame_controls = controls.clone();
                let frames = chunks.map(move |chunk| {
                    let chunk = chunk?;
                    let (bytes, allocation) = if let Some(controls) = &frame_controls {
                        let (bytes, allocation) = controls.copy(chunk.as_ref())?;
                        (bytes, Some(allocation))
                    } else {
                        (chunk.to_vec(), None)
                    };
                    let frame = wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Body(bytes)),
                    };
                    let encoded = line("PutObjectRequest", &frame, frame_controls.as_ref());
                    drop(frame);
                    drop(allocation);
                    encoded
                });
                let complete = line(
                    "PutObjectRequest",
                    &wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Complete(true)),
                    },
                    controls.as_ref(),
                )?;
                let terminal_controls = controls.clone();
                let header_controls = controls.clone();
                let (result, retained_response): (wire::ObjectInfo, _) = upload::run(
                    async {
                        let reply = self
                            .post(
                                "objects/put",
                                "application/x-ndjson",
                                response::http_body(
                                    stream::once(async move {
                                        if let Some(controls) = header_controls {
                                            controls.check()?;
                                        }
                                        Ok::<_, Error>(header)
                                    })
                                    .chain(frames)
                                    .chain(stream::once(
                                        async move {
                                            if let Some(controls) = terminal_controls {
                                                controls.check()?;
                                            }
                                            Ok::<_, Error>(complete)
                                        },
                                    )),
                                    controls.as_ref(),
                                )?,
                                controls.as_ref(),
                            )
                            .await?;
                        self.decode_put(reply, controls.as_ref(), response_allocation.take())
                            .await
                    },
                    &state,
                )
                .await?;
                response::validate_put_controlled(
                    result,
                    retained_response,
                    state.finish(),
                    controls.as_ref(),
                )
            },
        )
        .await
    }

    /// Streams one independently staged multipart part.
    pub async fn upload_part_stream(
        &self,
        query: wire::UploadPartHeader,
        body: UploadBody,
    ) -> Result<wire::UploadedPart, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "multipart/upload-part",
                rpc.code = obs::Empty
            ),
            async {
                request::upload_part(&query)?;
                let part_number = query.part_number;
                let header = line(
                    "UploadPartRequest",
                    &wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Header(query)),
                    },
                    None,
                )?;
                let (chunks, state) = upload::chunks(body, None)?;
                let frames = chunks.map(|chunk| {
                    line(
                        "UploadPartRequest",
                        &wire::UploadPartRequest {
                            frame: Some(wire::upload_part_request::Frame::Body(chunk?.to_vec())),
                        },
                        None,
                    )
                });
                let complete = line(
                    "UploadPartRequest",
                    &wire::UploadPartRequest {
                        frame: Some(wire::upload_part_request::Frame::Complete(true)),
                    },
                    None,
                )?;
                let result: wire::UploadedPart = upload::run(
                    async {
                        let reply = self
                            .post(
                                "multipart/upload-part",
                                "application/x-ndjson",
                                Body::wrap_stream(
                                    stream::once(async { Ok::<_, Error>(header) })
                                        .chain(frames)
                                        .chain(stream::once(async { Ok::<_, Error>(complete) })),
                                ),
                                None,
                            )
                            .await?;
                        self.decode("UploadedPart", reply).await
                    },
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
