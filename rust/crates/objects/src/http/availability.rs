use super::*;
use crate::availability::client::{Credential, observe};
use acyclic_stream::wire_codec::session::FrameDecoder;
use futures::stream;

struct Source {
    response: Response,
    decoder: FrameDecoder<wire::AvailabilityCursor>,
    chunk: bytes::Bytes,
    consumed: usize,
}

impl HttpObjects {
    pub(super) async fn observe_availability(
        &self,
        digest: Digest,
    ) -> Result<crate::Availability, Error> {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            let credential = Credential::resolve(self.credential.as_ref()).await?;
            let authorization = match &credential.lease {
                Some(lease) => credential_header(lease)
                    .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?,
                None => self
                    .authorization
                    .clone()
                    .ok_or(Error::from(wire::ErrorCode::AccessDenied))?,
            };
            let request = wire::AvailabilityRequest {
                digest: Some(digest.into()),
            };
            let url = self
                .endpoint
                .join("blobs/availability")
                .map_err(|_| Error::from(wire::ErrorCode::InvalidArgument))?;
            let response = self
                .transport
                .post(url)
                .header(AUTHORIZATION, authorization)
                .header("content-type", "application/json")
                .header("accept", "application/x-protobuf")
                .body(json::encode("AvailabilityRequest", &request)?)
                .send()
                .await
                .map_err(|_| unavailable())?;
            if response.status().as_u16() == 401
                && let Some(lease) = &credential.lease
            {
                lease
                    .reject()
                    .await
                    .map_err(|_| Error::from(wire::ErrorCode::AccessDenied))?;
            }
            if response.status().as_u16() != 200 {
                return Err(self.failure(response).await);
            }
            if response
                .headers()
                .get("content-type")
                .and_then(|value| value.to_str().ok())
                != Some("application/x-protobuf")
            {
                return Err(unavailable());
            }
            let source = Source {
                response,
                decoder: FrameDecoder::with_decoder(self.maximum, |bytes| {
                    wire::AvailabilityCursor::decode(bytes)
                        .map_err(|_| acyclic_stream::StreamError::InvalidResponse)
                }),
                chunk: bytes::Bytes::new(),
                consumed: 0,
            };
            let frames = stream::try_unfold(source, |mut source| async move {
                loop {
                    let mut input = &source.chunk[source.consumed..];
                    let before = input.len();
                    let frame = source.decoder.read(&mut input).map_err(|_| unavailable())?;
                    source.consumed += before - input.len();
                    if let Some(frame) = frame {
                        return Ok(Some((frame, source)));
                    }
                    match source.response.chunk().await.map_err(|_| unavailable())? {
                        Some(chunk) => {
                            source.chunk = chunk;
                            source.consumed = 0;
                        }
                        None => {
                            source.decoder.finish().map_err(|_| unavailable())?;
                            return Err(unavailable());
                        }
                    }
                }
            });
            observe(frames, credential).await
        })
        .await
        .map_err(|_| Error::from(wire::ErrorCode::DeadlineElapsed))?
    }
}
