use super::*;

impl HttpObjects {
    /// Starts a bounded header-first download without collecting its body.
    pub async fn get_stream(
        &self,
        query: wire::GetObjectRequest,
        maximum_bytes: u64,
    ) -> Result<Download, Error> {
        self.get_stream_controlled(&query, maximum_bytes, None, &mut None)
            .await
    }

    pub(super) async fn get_stream_controlled(
        &self,
        query: &wire::GetObjectRequest,
        maximum_bytes: u64,
        controls: Option<Arc<super::NativeTransportErrors>>,
        header_allocation: &mut Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Result<Download, Error> {
        obs::download(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route = "objects/get",
                rpc.code = obs::Empty
            ),
            async {
                request::get(query)?;
                let body_factory = |reader: Frames, remaining: u64| {
                    stream::try_unfold((reader, remaining), |(mut reader, remaining)| async move {
                        let Some((frame, allocation)) = reader.next().await? else {
                            return if remaining == 0 {
                                Ok(None)
                            } else {
                                Err(response::invalid())
                            };
                        };
                        match frame.frame {
                            Some(wire::get_object_response::Frame::Body(frame)) => {
                                let bytes = if let Some(controls) = &reader.controls {
                                    controls.body(
                                        frame,
                                        remaining,
                                        allocation.expect("native frame preclaim"),
                                    )?
                                } else {
                                    Bytes::from(response::body(frame, remaining)?)
                                };
                                let remaining = remaining - bytes.len() as u64;
                                Ok(Some((bytes, (reader, remaining))))
                            }
                            frame => Err(response::reject_frame(
                                frame,
                                allocation,
                                reader.controls.as_ref(),
                            )),
                        }
                    })
                };
                let prepared_body =
                    response::prepare_body_stream(&body_factory, controls.as_ref())?;
                let bytes = if let Some(controls) = &controls {
                    let (bytes, allocation) =
                        json::encode_native("GetObjectRequest", query, controls)?;
                    controls.wire_bytes(bytes, allocation)?
                } else {
                    Bytes::from(json::encode("GetObjectRequest", query)?)
                };
                if bytes.len() > REQUEST_BYTES {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let reply = self
                    .post(
                        "objects/get",
                        "application/json",
                        bytes.into(),
                        controls.as_ref(),
                    )
                    .await?;
                media_type(&reply, "application/x-ndjson")?;
                if reply
                    .content_length()
                    .is_some_and(|size| size > self.maximum as u64)
                {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let mut reader = Frames {
                    reply,
                    chunk: Bytes::new(),
                    pending: Vec::new(),
                    total: 0,
                    maximum: self.maximum,
                    pending_allocation: controls.as_ref().map(|controls| {
                        acyclic_native_runtime::ReadAllocation::empty(&controls.meter)
                    }),
                    controls: controls.clone(),
                };
                let (first, first_allocation) =
                    reader.next().await?.ok_or_else(response::invalid)?;
                let header = match first.frame {
                    Some(wire::get_object_response::Frame::Header(header)) => header,
                    frame => {
                        return Err(response::reject_frame(
                            frame,
                            first_allocation,
                            controls.as_ref(),
                        ));
                    }
                };
                let remaining = match response::get_header(&header, &query.range, maximum_bytes) {
                    Ok(remaining) => remaining,
                    Err(error) => {
                        if let Some(allocation) = first_allocation {
                            return Err(controls.as_ref().expect("native header custody").fail(
                                NativeObjectsError::from(error).with_header(header, allocation),
                            ));
                        }
                        return Err(error);
                    }
                };
                let body =
                    response::finish_body_stream(body_factory(reader, remaining), prepared_body);
                *header_allocation = first_allocation;
                Ok(Download { header, body })
            },
        )
        .await
    }
}
