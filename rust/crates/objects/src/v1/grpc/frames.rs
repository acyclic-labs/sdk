use super::*;

pub(super) fn native_put_wire_bytes(
    header: &wire::PutObjectHeader,
    length: usize,
) -> Result<usize, Error> {
    let header_length = header.encoded_len();
    let full = length / 65_536;
    let remainder = length % 65_536;
    let mut bytes = header_length
        .checked_add(prost::encoding::encoded_len_varint(header_length as u64))
        .and_then(|value| value.checked_add(6))
        .and_then(|value| {
            full.checked_mul(65_545)
                .and_then(|body| value.checked_add(body))
        })
        .ok_or_else(response::invalid)?;
    if remainder != 0 {
        bytes = bytes
            .checked_add(remainder)
            .and_then(|value| {
                value.checked_add(prost::encoding::encoded_len_varint(remainder as u64))
            })
            .and_then(|value| value.checked_add(6))
            .ok_or_else(response::invalid)?;
    }
    bytes.checked_add(7).ok_or_else(response::invalid)
}

pub(super) fn put_frame(chunk: Result<OwnedBytes, Error>) -> wire::PutObjectRequest {
    wire::PutObjectRequest {
        frame: chunk
            .ok()
            .map(|chunk| wire::put_object_request::Frame::Body(chunk.to_vec())),
    }
}

pub(super) fn part_frame(chunk: Result<OwnedBytes, Error>) -> wire::UploadPartRequest {
    wire::UploadPartRequest {
        frame: chunk
            .ok()
            .map(|chunk| wire::upload_part_request::Frame::Body(chunk.to_vec())),
    }
}

pub(super) fn put_frames(
    chunks: UploadBody,
    controls: Option<Arc<super::NativeTransportErrors>>,
    header: response::RequestInput<wire::PutObjectRequest>,
) -> impl futures::Stream<Item = response::RequestInput<wire::PutObjectRequest>> + Send {
    stream::unfold(
        (Some(header), Some(chunks), controls),
        |(mut first, mut chunks, controls)| async move {
            if let Some(frame) = first.take() {
                return Some((frame, (None, chunks, controls)));
            }
            let chunk = chunks.as_mut()?.next().await?;
            let frame = if let Some(controls) = &controls {
                let frame = (|| {
                    let chunk = chunk?;
                    let (bytes, allocation) = controls.copy(chunk.as_ref())?;
                    let packet = super::ObjectsNativeRequest::new(
                        wire::PutObjectRequest {
                            frame: Some(wire::put_object_request::Frame::Body(bytes)),
                        },
                        allocation,
                        &controls.meter,
                    )
                    .map_err(|error| controls.fail(error))?;
                    Ok::<_, Error>(
                        response::RequestInput::native(packet, &controls.meter)
                            .map_err(|error| controls.fail(error))?
                            .with_controls(Some(controls.clone()))?,
                    )
                })();
                match frame {
                    Ok(frame) => frame,
                    Err(_) => {
                        chunks = None;
                        response::RequestInput::CodecFailure(
                            controls
                                .codec_failure()
                                .expect("one prepared native source failure"),
                        )
                    }
                }
            } else {
                response::RequestInput::Ordinary(put_frame(chunk))
            };
            Some((frame, (None, chunks, controls)))
        },
    )
}
