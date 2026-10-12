use super::*;

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
        }
        let prefix = "v1/objects/";
        let mut path = (String::new(), None);
        let maximum = prefix
            .len()
            .checked_add(route.len())
            .ok_or_else(response::invalid)?;
        if let Some(controls) = controls {
            path.1 = Some(
                acyclic_native_runtime::ReadAllocation::new(&controls.meter, maximum)
                    .map_err(|error| controls.fail(error))?,
            );
        }
        path.0.try_reserve_exact(maximum).map_err(|error| {
            controls.map_or_else(response::invalid, |controls| {
                controls.fail(controls.meter.allocation_failed(error))
            })
        })?;
        if let Some(controls) = controls {
            path.1
                .as_mut()
                .expect("native route string custody")
                .reconcile_capacity(path.0.capacity())
                .map_err(|error| controls.fail(error))?;
            let url_maximum = self
                .endpoint
                .as_str()
                .len()
                .checked_add(maximum)
                .ok_or_else(response::invalid)?;
            let url_allocation =
                acyclic_native_runtime::ReadAllocation::adopt(&controls.meter, url_maximum)
                    .map_err(|error| controls.fail(error))?;
            controls
                .retain_wire(url_allocation)
                .map_err(|error| controls.fail(error))?;
            controls
                .meter
                .admit_codec(0, maximum as u64, 0)
                .map_err(|error| controls.fail(error))?;
        }
        path.0.push_str(prefix);
        path.0.push_str(route);
        if let Some(controls) = controls {
            controls
                .meter
                .record_codec(0, maximum as u64, 0)
                .map_err(|error| controls.fail(error))?;
        }
        let url = self
            .endpoint
            .join(&path.0)
            .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
        drop(path);
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
                    controls.check()?;
                    let needed = bytes.len() + chunk.len();
                    if needed > bytes.capacity() {
                        let allocation = allocation.as_mut().expect("native response custody");
                        allocation
                            .grow(needed - bytes.capacity())
                            .map_err(|error| controls.fail(error))?;
                        bytes
                            .try_reserve_exact(needed - bytes.len())
                            .map_err(|error| {
                                controls.fail(controls.meter.allocation_failed(error))
                            })?;
                        allocation
                            .reconcile_capacity(bytes.capacity())
                            .map_err(|error| controls.fail(error))?;
                    }
                    controls
                        .meter
                        .admit_codec(0, chunk.len() as u64, 0)
                        .map_err(|error| controls.fail(error))?;
                }
                bytes.extend_from_slice(&chunk);
                if let Some(controls) = controls {
                    controls
                        .meter
                        .record_codec(0, chunk.len() as u64, 0)
                        .map_err(|error| controls.fail(error))?;
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
    ) -> Result<O, Error> {
        media_type(&response, "application/json")?;
        json::decode(name, &self.bytes(response, None).await?.0, self.maximum)
            .map_err(|_| response::invalid())
    }

    pub(super) async fn decode_put(
        &self,
        response: Response,
        controls: Option<&Arc<super::NativeTransportErrors>>,
        allocation: Option<acyclic_native_runtime::ReadAllocation>,
    ) -> Result<
        (
            wire::ObjectInfo,
            Option<acyclic_native_runtime::ReadAllocation>,
        ),
        Error,
    > {
        media_type(&response, "application/json")?;
        let received = self.bytes(response, controls).await?;
        let _codec_allocation = if let Some(controls) = controls {
            match acyclic_native_runtime::ReadAllocation::adopt(
                &controls.meter,
                received.0.len().saturating_mul(4),
            ) {
                Ok(allocation) => Some(allocation),
                Err(error) => {
                    let (bytes, allocation) = received;
                    return Err(controls.fail(
                        NativeObjectsError::from(error)
                            .with_wire(bytes, allocation.expect("native response wire custody")),
                    ));
                }
            }
        } else {
            None
        };
        let value = match json::decode("ObjectInfo", &received.0, self.maximum) {
            Ok(value) => value,
            Err(_) => {
                if let Some(controls) = controls {
                    let (bytes, allocation) = received;
                    return Err(controls.fail(
                        NativeObjectsError::from(response::invalid())
                            .with_wire(bytes, allocation.expect("native response wire custody")),
                    ));
                }
                return Err(response::invalid());
            }
        };
        Ok((value, allocation))
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
                validate(value, self.decode(output, response).await?)
            },
        )
        .await
    }
}
