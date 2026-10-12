use super::*;

impl HttpObjects {
    /// Publishes a header-first streamed value. Source failure aborts the HTTP request.
    pub async fn put_stream(
        &self,
        query: wire::PutObjectHeader,
        body: UploadBody,
    ) -> Result<wire::ObjectInfo, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/put",
                rpc.code = obs::Empty
            ),
            async {
                request::put(&query)?;
                let header = line(
                    "PutObjectRequest",
                    &wire::PutObjectRequest {
                        frame: Some(wire::put_object_request::Frame::Header(query)),
                    },
                    None,
                )?;
                Ok(self.send_upload(header, body, None).await?.0)
            },
        )
        .await
    }

    pub(super) async fn send_upload(
        &self,
        header: Bytes,
        body: UploadBody,
        controls: Option<Arc<super::NativeTransportErrors>>,
    ) -> Result<
        (
            wire::ObjectInfo,
            Option<acyclic_native_runtime::ReadAllocation>,
        ),
        Error,
    > {
        let response_allocation = controls
            .as_ref()
            .map(|controls| {
                acyclic_native_runtime::ReadAllocation::adopt(
                    &controls.meter,
                    self.maximum.saturating_mul(4),
                )
                .map_err(|error| controls.fail(error))
            })
            .transpose()?;
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
        let result: wire::ObjectInfo = upload::run(
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
                            .chain(stream::once(async move {
                                if let Some(controls) = terminal_controls {
                                    controls.check()?;
                                }
                                Ok::<_, Error>(complete)
                            })),
                            controls.as_ref(),
                        )?,
                        controls.as_ref(),
                    )
                    .await?;
                self.decode("ObjectInfo", reply, controls.as_ref()).await
            },
            &state,
        )
        .await?;
        response::validate_put_controlled(
            result,
            response_allocation,
            state.finish(),
            controls.as_ref(),
        )
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
                        self.decode("UploadedPart", reply, None).await
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
