use super::*;

impl NativeTransportErrors {
    pub(super) fn append(
        &self,
        bytes: &mut Vec<u8>,
        allocation: &mut acyclic_native_runtime::ReadAllocation,
        chunk: &[u8],
    ) -> Result<(), Error> {
        self.check()?;
        let needed = bytes
            .len()
            .checked_add(chunk.len())
            .ok_or_else(response::invalid)?;
        if needed > bytes.capacity() {
            allocation
                .grow(needed - bytes.capacity())
                .map_err(|error| self.fail(error))?;
            bytes
                .try_reserve_exact(needed - bytes.len())
                .map_err(|error| self.fail(self.meter.allocation_failed(error)))?;
            allocation
                .reconcile_capacity(bytes.capacity())
                .map_err(|error| self.fail(error))?;
        }
        self.meter
            .admit_codec(0, chunk.len() as u64, 0)
            .map_err(|error| self.fail(error))?;
        bytes.extend_from_slice(chunk);
        self.meter
            .record_codec(0, chunk.len() as u64, 0)
            .map_err(|error| self.fail(error))
    }
}

impl HttpObjects {
    pub(super) async fn post(
        &self,
        route: &'static str,
        content_type: &str,
        body: Body,
        controls: Option<&Arc<super::NativeTransportErrors>>,
    ) -> Result<Response, Error> {
        if let Some(controls) = controls {
            controls.check()?;
            let maximum = self
                .endpoint
                .as_str()
                .len()
                .checked_add(route.len())
                .ok_or_else(response::invalid)?;
            let allocation =
                acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, maximum)
                    .map_err(|error| controls.fail(error))?;
            controls
                .retain_wire(allocation)
                .map_err(|error| controls.fail(error))?;
        }
        let url = self
            .endpoint
            .join(route)
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        let builder = self
            .transport
            .post(url)
            .header(AUTHORIZATION, self.authorization.clone())
            .header("content-type", content_type)
            .body(body);
        let (rpc, dispatch_failed) = if let Some(controls) = controls {
            let kind = if route == "objects/get" {
                acyclic_native_runtime::NativeOperationKind::Read
            } else {
                acyclic_native_runtime::NativeOperationKind::Write
            };
            controls.dispatch(kind, || builder.send())?
        } else {
            (builder.send(), false)
        };
        let response = rpc.await.map_err(|_| response::invalid())?;
        if dispatch_failed {
            return Err(response::invalid());
        }
        obs::record!("rpc.code" = response.status().as_u16());
        if response.status().as_u16() != 200 {
            return Err(self.failure(response, controls).await);
        }
        Ok(response)
    }

    pub(super) async fn bytes(
        &self,
        mut response: Response,
        controls: Option<&Arc<super::NativeTransportErrors>>,
    ) -> Result<(Vec<u8>, Option<acyclic_native_runtime::ReadAllocation>), Error> {
        if response
            .content_length()
            .is_some_and(|size| size > self.maximum as u64)
        {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        let mut bytes = Vec::new();
        let mut allocation =
            controls.map(|controls| acyclic_native_runtime::ReadAllocation::empty(&controls.meter));
        let result = async {
            while let Some(chunk) = response.chunk().await.map_err(|_| response::invalid())? {
                if chunk.len() > self.maximum.saturating_sub(bytes.len()) {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                if let Some(controls) = controls {
                    controls.append(
                        &mut bytes,
                        allocation.as_mut().expect("native response custody"),
                        &chunk,
                    )?;
                } else {
                    bytes.extend_from_slice(&chunk);
                }
            }
            Ok::<(), Error>(())
        }
        .await;
        if let Err(error) = result {
            if let Some(controls) = controls {
                let original = controls.take().unwrap_or_else(|| error.into());
                let mut allocation = allocation.expect("native response wire custody");
                let _ = allocation.reconcile_capacity(bytes.capacity());
                return Err(controls.fail(original.with_wire(bytes, allocation)));
            }
            return Err(error);
        }
        Ok((bytes, allocation))
    }

    pub(super) async fn failure(
        &self,
        response: Response,
        controls: Option<&Arc<super::NativeTransportErrors>>,
    ) -> Error {
        let status = response.status().as_u16();
        if status == 304 {
            return wire::ErrorCode::NotModified.into();
        }
        let received = match self.bytes(response, controls).await {
            Ok(value) => value,
            Err(error) => return error,
        };
        let decoded_allocation = if let Some(controls) = controls {
            match acyclic_native_runtime::ReadAllocation::adopt(
                &controls.meter,
                received.0.len().saturating_mul(4),
            ) {
                Ok(allocation) => Some(allocation),
                Err(error) => {
                    let (bytes, allocation) = received;
                    return controls.fail(
                        NativeObjectsError::from(error)
                            .with_wire(bytes, allocation.expect("native failure wire custody")),
                    );
                }
            }
        } else {
            None
        };
        let code = if let Ok(detail) =
            json::decode::<wire::ErrorDetail>("ErrorDetail", &received.0, self.maximum)
            && let Ok(code) = wire::ErrorCode::try_from(detail.code)
            && code != wire::ErrorCode::Unspecified
        {
            code
        } else {
            match status {
                400 => wire::ErrorCode::InvalidArgument,
                401 | 403 => wire::ErrorCode::AccessDenied,
                404 => wire::ErrorCode::NotFound,
                409 => wire::ErrorCode::AlreadyExists,
                412 => wire::ErrorCode::PreconditionFailed,
                413 | 429 => wire::ErrorCode::QuotaExceeded,
                416 => wire::ErrorCode::RangeNotSatisfiable,
                501 => wire::ErrorCode::Unsupported,
                _ => wire::ErrorCode::Unavailable,
            }
        };
        drop(decoded_allocation);
        if let Some(controls) = controls {
            let (bytes, allocation) = received;
            controls.fail(
                NativeObjectsError::from(Error::from(code))
                    .with_wire(bytes, allocation.expect("native failure wire custody")),
            )
        } else {
            code.into()
        }
    }

    pub(super) async fn decode<O: Message + Default>(
        &self,
        name: &str,
        response: Response,
        controls: Option<&Arc<super::NativeTransportErrors>>,
    ) -> Result<O, Error> {
        media_type(&response, "application/json")?;
        let (bytes, allocation) = self.bytes(response, controls).await?;
        let result = (|| {
            let _codec_allocation = controls
                .map(|controls| {
                    acyclic_native_runtime::ReadAllocation::adopt(
                        &controls.meter,
                        bytes.len().saturating_mul(4),
                    )
                    .map_err(|error| controls.fail(error))
                })
                .transpose()?;
            json::decode(name, &bytes, self.maximum).map_err(|_| response::invalid())
        })();
        result.map_err(|error| {
            if let Some(controls) = controls {
                let original = controls.take().unwrap_or_else(|| error.into());
                controls.fail(
                    original.with_wire(bytes, allocation.expect("native response wire custody")),
                )
            } else {
                error
            }
        })
    }

    pub(super) async fn call<I: Message, O: Message + Default>(
        &self,
        route: &'static str,
        value: I,
        validate: impl FnOnce(I, O) -> Result<O, Error>,
    ) -> Result<O, Error> {
        obs::traced(
            obs::span!(
                INFO,
                "acyclic.objects.http.call",
                route,
                rpc.code = obs::Empty
            ),
            async {
                let (_, input, output) = HTTP_ROUTES
                    .iter()
                    .find(|(path, _, _)| *path == route)
                    .ok_or(Error::from(wire::ErrorCode::InvalidArgument))?;
                let bytes = json::encode(input, &value)?;
                if bytes.len() > REQUEST_BYTES {
                    return Err(wire::ErrorCode::QuotaExceeded.into());
                }
                let response = self
                    .post(route, "application/json", bytes.into(), None)
                    .await?;
                validate(value, self.decode(output, response, None).await?)
            },
        )
        .await
    }
}
