use super::*;

pub(super) fn media_type(response: &Response, expected: &str) -> Result<(), Error> {
    if response
        .headers()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_none_or(|value| !value.trim().eq_ignore_ascii_case(expected))
    {
        return Err(response::invalid());
    }
    Ok(())
}

pub(super) fn line(
    name: &str,
    frame: &impl Message,
    controls: Option<&Arc<super::NativeTransportErrors>>,
) -> Result<Bytes, Error> {
    let Some(controls) = controls else {
        let mut bytes = json::encode(name, frame)?;
        bytes.push(b'\n');
        if bytes.len() > JSON_FRAME_BYTES {
            return Err(wire::ErrorCode::QuotaExceeded.into());
        }
        return Ok(bytes.into());
    };
    let (mut bytes, mut allocation) = json::encode_native(name, frame, controls)?;
    controls.append(&mut bytes, &mut allocation, b"\n")?;
    if bytes.len() > JSON_FRAME_BYTES {
        return Err(controls.fail(
            NativeObjectsError::from(Error::from(wire::ErrorCode::QuotaExceeded))
                .with_wire(bytes, allocation),
        ));
    }
    controls.wire_bytes(bytes, allocation)
}

pub(super) struct Frames {
    pub(super) reply: Response,
    pub(super) chunk: Bytes,
    pub(super) pending: Vec<u8>,
    pub(super) total: usize,
    pub(super) maximum: usize,
    pub(super) controls: Option<Arc<super::NativeTransportErrors>>,
    pub(super) pending_allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Frames {
    pub(super) async fn next(
        &mut self,
    ) -> Result<
        Option<(
            wire::GetObjectResponse,
            Option<acyclic_native_runtime::ReadAllocation>,
        )>,
        Error,
    > {
        let result = async {
            let mut frame_allocation = if let Some(controls) = &self.controls {
                controls.check()?;
                Some(
                    acyclic_native_runtime::ReadAllocation::adopt(
                        &controls.meter,
                        JSON_FRAME_BYTES * 4,
                    )
                    .map_err(|error| controls.fail(error))?,
                )
            } else {
                None
            };
            loop {
                if self.chunk.is_empty() {
                    if let Some(controls) = &self.controls {
                        controls.check()?;
                    }
                    let Some(chunk) = self.reply.chunk().await.map_err(|_| response::invalid())?
                    else {
                        return if self.pending.is_empty() {
                            Ok(None)
                        } else {
                            Err(response::invalid())
                        };
                    };
                    if chunk.len() > self.maximum.saturating_sub(self.total) {
                        return Err(wire::ErrorCode::QuotaExceeded.into());
                    }
                    self.total += chunk.len();
                    self.chunk = chunk;
                }
                let newline = self.chunk.iter().position(|byte| *byte == b'\n');
                let size = newline.map_or(self.chunk.len(), |index| index + 1);
                if size > JSON_FRAME_BYTES.saturating_sub(self.pending.len()) {
                    return Err(response::invalid());
                }
                let chunk = self.chunk.split_to(size);
                if let Some(controls) = &self.controls {
                    controls.append(
                        &mut self.pending,
                        self.pending_allocation
                            .as_mut()
                            .expect("native pending custody"),
                        &chunk,
                    )?;
                } else {
                    self.pending.extend_from_slice(&chunk);
                }
                if newline.is_some() {
                    self.pending.pop();
                    let bytes = self.pending.strip_suffix(b"\r").unwrap_or(&self.pending);
                    let (frame, allocation) = if let Some(controls) = &self.controls {
                        let (frame, allocation) = json::decode_get_native(
                            bytes,
                            JSON_FRAME_BYTES,
                            controls,
                            frame_allocation.take().expect("native frame preclaim"),
                        )
                        .map_err(|_| response::invalid())?;
                        (frame, Some(allocation))
                    } else {
                        (
                            json::decode("GetObjectResponse", bytes, JSON_FRAME_BYTES)
                                .map_err(|_| response::invalid())?,
                            None,
                        )
                    };
                    self.pending.clear();
                    return Ok(Some((frame, allocation)));
                }
            }
        }
        .await;
        match result {
            Err(error) if self.controls.is_some() => {
                let controls = self.controls.as_ref().expect("native frame controls");
                let original = controls.take().unwrap_or_else(|| error.into());
                let bytes = std::mem::take(&mut self.pending);
                let allocation = self.pending_allocation.take().unwrap_or_else(|| {
                    acyclic_native_runtime::ReadAllocation::empty(&controls.meter)
                });
                Err(controls.fail(original.with_wire(bytes, allocation)))
            }
            result => result,
        }
    }
}
