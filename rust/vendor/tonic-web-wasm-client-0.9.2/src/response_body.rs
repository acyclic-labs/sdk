use std::{
    ops::{Deref, DerefMut},
    pin::Pin,
    task::{Context, Poll, ready},
};

use base64::{Engine, prelude::BASE64_STANDARD};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use http::{HeaderMap, HeaderValue, header::HeaderName};
use http_body::Body;
use httparse::{EMPTY_HEADER, Status};
use pin_project::pin_project;
use wasm_bindgen::JsCast;
use web_sys::ReadableStream;

use crate::{
    Error, abort_guard::AbortGuard, body_stream::BodyStream, content_type::Encoding,
    limits::UnaryResponseLimits,
};

/// If 8th MSB of a frame is `0` for data and `1` for trailer
const TRAILER_BIT: u8 = 0b10000000;

pub struct EncodedBytes {
    encoding: Encoding,
    raw_buf: BytesMut,
    buf: BytesMut,
}

impl EncodedBytes {
    pub fn new(content_type: &str) -> Result<Self, Error> {
        Ok(Self {
            encoding: Encoding::from_content_type(content_type)?,
            raw_buf: BytesMut::new(),
            buf: BytesMut::new(),
        })
    }

    fn response_limit(&self, limits: UnaryResponseLimits) -> usize {
        match self.encoding {
            Encoding::None => limits.binary_body_bytes(),
            Encoding::Base64 => limits.text_body_bytes(),
        }
    }

    // Return the largest prefix that can be decoded in one call. Keep incomplete
    // quartets buffered and stop after the first padded quartet, since gRPC-web
    // can concatenate independently padded base64 segments.
    #[inline]
    fn max_decodable(&self) -> usize {
        let complete_quartets = (self.raw_buf.len() / 4) * 4;
        self.raw_buf[..complete_quartets]
            .iter()
            .position(|&byte| byte == b'=')
            .map_or(complete_quartets, |position| (position / 4 + 1) * 4)
    }

    fn decode_base64_chunk(&mut self) -> Result<(), Error> {
        while self.raw_buf.len() >= 4 {
            let index = self.max_decodable();

            let decoded = BASE64_STANDARD
                .decode(self.raw_buf.split_to(index))
                .map(Bytes::from)?;
            self.buf.put(decoded);
        }

        Ok(())
    }

    fn append(&mut self, bytes: Bytes) -> Result<(), Error> {
        match self.encoding {
            Encoding::None => self.buf.put(bytes),
            Encoding::Base64 => {
                self.raw_buf.put(bytes);
                self.decode_base64_chunk()?;
            }
        }

        Ok(())
    }

    fn take(&mut self, length: usize) -> BytesMut {
        let new_buf = self.buf.split_off(length);
        std::mem::replace(&mut self.buf, new_buf)
    }
}

impl Deref for EncodedBytes {
    type Target = BytesMut;

    fn deref(&self) -> &Self::Target {
        &self.buf
    }
}

impl DerefMut for EncodedBytes {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.buf
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadState {
    CompressionFlag,
    DataLength,
    Data(u32),
    TrailerLength,
    Trailer(u32),
    Done,
}

impl ReadState {
    /// Whether the whole body, including the trailers frame, has been read.
    ///
    /// `TrailerLength` and `Trailer(_)` must not count: in those states the
    /// trailers frame has started but more bytes are still needed from the
    /// stream, which happens whenever a chunk boundary lands inside it.
    fn is_done(&self) -> bool {
        matches!(self, ReadState::Done)
    }
}

/// Type to handle HTTP response
#[pin_project]
pub struct ResponseBody {
    #[pin]
    body_stream: BodyStream,
    buf: EncodedBytes,
    incomplete_data: BytesMut,
    data: Option<BytesMut>,
    trailer: Option<HeaderMap>,
    state: ReadState,
    finished_stream: bool,
    maximum: Option<usize>,
    maximum_message: Option<usize>,
    maximum_trailer: Option<usize>,
    maximum_headers: Option<usize>,
}

impl ResponseBody {
    pub(crate) fn new(
        body_stream: ReadableStream,
        content_type: &str,
        abort: AbortGuard,
        limits: Option<UnaryResponseLimits>,
    ) -> Result<Self, Error> {
        let body_stream =
            wasm_streams::ReadableStream::from_raw(body_stream.unchecked_into()).into_stream();

        let buf = EncodedBytes::new(content_type)?;
        let maximum = limits.map(|limits| buf.response_limit(limits));
        Ok(Self {
            body_stream: BodyStream::new(body_stream, abort, maximum),
            buf,
            incomplete_data: BytesMut::new(),
            data: None,
            trailer: None,
            state: ReadState::CompressionFlag,
            finished_stream: false,
            maximum,
            maximum_message: limits.map(|limits| limits.message_bytes()),
            maximum_trailer: limits.map(|limits| limits.trailer_bytes()),
            maximum_headers: limits.map(|limits| limits.header_list_bytes() as usize),
        })
    }

    #[cfg(test)]
    pub(crate) fn from_body_stream(
        body_stream: BodyStream,
        content_type: &str,
    ) -> Result<Self, Error> {
        Ok(Self {
            body_stream,
            buf: EncodedBytes::new(content_type)?,
            incomplete_data: BytesMut::new(),
            data: None,
            trailer: None,
            state: ReadState::CompressionFlag,
            finished_stream: false,
            maximum: None,
            maximum_message: None,
            maximum_trailer: None,
            maximum_headers: None,
        })
    }

    fn read_stream(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.finished_stream {
            return Poll::Ready(Ok(()));
        }

        let this = self.project();

        match ready!(this.body_stream.poll_frame(cx)) {
            Some(Ok(frame)) => {
                if let Some(data) = frame.data_ref()
                    && let Err(e) = this.buf.append(data.clone())
                {
                    return Poll::Ready(Err(e));
                };

                Poll::Ready(Ok(()))
            }
            Some(Err(e)) => Poll::Ready(Err(e)),
            None => {
                *this.finished_stream = true;
                Poll::Ready(Ok(()))
            }
        }
    }

    fn step(self: Pin<&mut Self>) -> Result<(), Error> {
        let this = self.project();

        loop {
            match this.state {
                ReadState::CompressionFlag => {
                    if this.buf.is_empty() {
                        // Can't read compression flag right now
                        return Ok(());
                    } else {
                        let compression_flag = this.buf.take(1);

                        if compression_flag[0] & TRAILER_BIT == 0 {
                            this.incomplete_data.unsplit(compression_flag);
                            *this.state = ReadState::DataLength;
                        } else {
                            *this.state = ReadState::TrailerLength;
                        }
                    }
                }
                ReadState::DataLength => {
                    if this.buf.len() < 4 {
                        // Can't read data length right now
                        return Ok(());
                    } else {
                        let data_length_bytes = this.buf.take(4);
                        // According to [`Buf::get_u32`] docs returns u32 from big-endian bytes
                        let data_length = (&data_length_bytes[..]).get_u32();
                        if this
                            .maximum_message
                            .or(*this.maximum)
                            .is_some_and(|maximum| data_length as usize > maximum)
                        {
                            return Err(Error::ResponseTooLarge);
                        }

                        this.incomplete_data.unsplit(data_length_bytes);
                        *this.state = ReadState::Data(data_length);
                    }
                }
                ReadState::Data(data_length) => {
                    let data_length = *data_length as usize;

                    if this.buf.len() < data_length {
                        // Can't read data right now
                        return Ok(());
                    } else {
                        this.incomplete_data.unsplit(this.buf.take(data_length));

                        let new_data = this.incomplete_data.split();

                        if let Some(data) = this.data {
                            data.unsplit(new_data);
                        } else {
                            *this.data = Some(new_data);
                        }

                        *this.state = ReadState::CompressionFlag;
                    }
                }
                ReadState::TrailerLength => {
                    if this.buf.len() < 4 {
                        // Can't read data length right now
                        return Ok(());
                    } else {
                        let trailer_length_bytes = this.buf.take(4);
                        // According to [`Buf::get_u32`] docs returns u32 from big-endian bytes
                        let trailer_length = (&trailer_length_bytes[..]).get_u32();
                        if this
                            .maximum_trailer
                            .or(*this.maximum)
                            .is_some_and(|maximum| trailer_length as usize > maximum)
                        {
                            return Err(Error::ResponseTooLarge);
                        }
                        *this.state = ReadState::Trailer(trailer_length);
                    }
                }
                ReadState::Trailer(trailer_length) => {
                    let trailer_length = *trailer_length as usize;

                    if this.buf.len() < trailer_length {
                        // Can't read trailer right now
                        return Ok(());
                    } else {
                        // The trailers frame is consumed and is the last one, so the
                        // body is over even if parsing it fails below.
                        *this.state = ReadState::Done;

                        let mut trailer_bytes = this.buf.take(trailer_length);
                        trailer_bytes.put_u8(b'\n');

                        let trailers = parse_trailers(&trailer_bytes, *this.maximum_headers)?;

                        *this.trailer = Some(trailers);
                    }
                }
                ReadState::Done => return Ok(()),
            }
        }
    }
}

// Header slots are bounded by the validated frame bytes, not by distinct-key
// capacity: repeated values must remain representable by HeaderMap.
fn parse_trailers(bytes: &[u8], maximum: Option<usize>) -> Result<HeaderMap, Error> {
    let mut slots = 64.min(bytes.len());
    loop {
        let mut storage = Vec::new();
        storage
            .try_reserve_exact(slots)
            .map_err(|_| Error::HeaderParsingError)?;
        storage.resize(slots, EMPTY_HEADER);
        match httparse::parse_headers(bytes, &mut storage) {
            Ok(Status::Complete((consumed, headers))) => {
                // The caller appends one LF to finish a trailer header block.
                // Any other bytes after its terminator are hidden trailer content.
                let suffix = &bytes[consumed..];
                if !suffix.is_empty() && suffix != b"\n" {
                    return Err(Error::HeaderParsingError);
                }
                let mut trailers = HeaderMap::new();
                let mut weight = 0;
                for header in headers {
                    weight = checked_header_list_size(
                        weight,
                        header.name.len(),
                        header.value.len(),
                        maximum,
                    )?;
                    let name = HeaderName::from_bytes(header.name.as_bytes())?;
                    let value = HeaderValue::from_bytes(header.value)?;
                    trailers
                        .try_append(name, value)
                        .map_err(|_| Error::HeaderParsingError)?;
                }
                return Ok(trailers);
            }
            Err(httparse::Error::TooManyHeaders) => {
                let next = slots.saturating_mul(2).min(bytes.len());
                if next <= slots {
                    return Err(Error::HeaderParsingError);
                }
                slots = next;
            }
            _ => return Err(Error::HeaderParsingError),
        }
    }
}

pub(crate) fn checked_header_list_size(
    received: usize,
    name: usize,
    value: usize,
    maximum: Option<usize>,
) -> Result<usize, Error> {
    received
        .checked_add(name)
        .and_then(|n| n.checked_add(value))
        .and_then(|n| n.checked_add(32))
        .filter(|n| maximum.is_none_or(|maximum| *n <= maximum))
        .ok_or(Error::ResponseTooLarge)
}

impl Body for ResponseBody {
    type Data = Bytes;

    type Error = Error;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        // Check if there's already some data in buffer and return that
        if self.data.is_some() {
            let data = self.data.take().unwrap();

            return Poll::Ready(Some(Ok(http_body::Frame::data(data.freeze()))));
        }

        // If the whole body is read, return trailers (if available) before ending
        if self.state.is_done() {
            if let Some(trailers) = self.trailer.take() {
                return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
            }
            return Poll::Ready(None);
        }

        loop {
            // Read bytes from stream
            if let Err(e) = ready!(self.as_mut().read_stream(cx)) {
                return Poll::Ready(Some(Err(e)));
            }

            // Step the state machine
            if let Err(e) = self.as_mut().step() {
                return Poll::Ready(Some(Err(e)));
            }

            if self.data.is_some() {
                // If data is available in buffer, return that
                let data = self.data.take().unwrap();
                return Poll::Ready(Some(Ok(http_body::Frame::data(data.freeze()))));
            } else if self.state.is_done() {
                // If we finished reading the body, return trailers before ending
                if let Some(trailers) = self.trailer.take() {
                    return Poll::Ready(Some(Ok(http_body::Frame::trailers(trailers))));
                }
                return Poll::Ready(None);
            } else if self.finished_stream {
                // If stream is finished but data is not finished return error
                return Poll::Ready(Some(Err(Error::MalformedResponse)));
            }
        }
    }
}

impl Default for ResponseBody {
    fn default() -> Self {
        Self {
            body_stream: BodyStream::empty(),
            buf: EncodedBytes {
                encoding: Encoding::None,
                raw_buf: BytesMut::new(),
                buf: BytesMut::new(),
            },
            incomplete_data: BytesMut::new(),
            data: None,
            trailer: None,
            state: ReadState::Done,
            finished_stream: true,
            maximum: None,
            maximum_message: None,
            maximum_trailer: None,
            maximum_headers: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decodes_base64_response() {
        let mut buf = EncodedBytes::new("application/grpc-web-text+proto").unwrap();

        buf.append(Bytes::from_static(b"AAAAAAA=")).unwrap();

        assert_eq!(&buf[..], &[0, 0, 0, 0, 0]);
        assert!(buf.raw_buf.is_empty());
    }

    #[test]
    fn buffers_incomplete_base64_quartets() {
        let mut buf = EncodedBytes::new("application/grpc-web-text+proto").unwrap();

        buf.append(Bytes::from_static(b"YWJjZA=")).unwrap();
        assert_eq!(&buf[..], b"abc");
        assert_eq!(&buf.raw_buf[..], b"ZA=");

        // Consuming decoded data must not affect the pending encoded bytes.
        assert_eq!(&buf.take(3)[..], b"abc");
        buf.append(Bytes::from_static(b"=")).unwrap();
        assert_eq!(&buf[..], b"d");
        assert!(buf.raw_buf.is_empty());
    }

    #[test]
    fn decodes_concatenated_base64_segments_at_every_chunk_size() {
        // Include two-byte padding, one-byte padding, and an unpadded segment.
        let encoded = b"YQ==YmM=ZGVmZ2hpag==";

        for chunk_size in 1..=encoded.len() {
            let mut buf = EncodedBytes::new("application/grpc-web-text+proto").unwrap();
            for chunk in encoded.chunks(chunk_size) {
                buf.append(Bytes::copy_from_slice(chunk)).unwrap();
            }

            assert_eq!(&buf[..], b"abcdefghij", "chunk size {chunk_size}");
            assert!(buf.raw_buf.is_empty());
        }
    }

    #[test]
    fn rejects_invalid_base64() {
        for encoded in [b"!!!!", b"AA=A", b"A==="] {
            let mut buf = EncodedBytes::new("application/grpc-web-text+proto").unwrap();
            assert!(matches!(
                buf.append(Bytes::copy_from_slice(encoded)),
                Err(Error::Base64DecodeError(_))
            ));
        }
    }

    #[test]
    fn binary_response_is_not_base64_decoded() {
        let mut buf = EncodedBytes::new("application/grpc-web+proto").unwrap();
        let bytes = Bytes::from_static(b"\x00\x80!!!!");

        buf.append(bytes.clone()).unwrap();

        assert_eq!(&buf[..], &bytes[..]);
        assert!(buf.raw_buf.is_empty());
    }
}

#[cfg(test)]
mod budget_regressions {
    use super::*;
    #[test]
    fn encoding_selects_the_checked_binary_or_text_budget() {
        let limits = UnaryResponseLimits::new(8).unwrap();
        let binary = EncodedBytes::new("application/grpc-web+proto").unwrap();
        let text = EncodedBytes::new("application/grpc-web-text+proto").unwrap();
        assert_eq!(binary.response_limit(limits), limits.binary_body_bytes());
        assert_eq!(text.response_limit(limits), limits.text_body_bytes());
        assert_eq!(
            text.response_limit(limits),
            binary.response_limit(limits) * 4
        );
    }

    #[test]
    fn declared_data_and_trailer_bounds_fail_before_body_accumulation() {
        for state in [ReadState::DataLength, ReadState::TrailerLength] {
            let mut body = ResponseBody::default();
            body.state = state;
            body.maximum = Some(8);
            body.buf.append(Bytes::from_static(&[0, 0, 0, 9])).unwrap();
            assert!(matches!(
                Pin::new(&mut body).step(),
                Err(Error::ResponseTooLarge)
            ));
        }
    }

    #[test]
    fn explicit_raw_trailer_bound_is_independent_of_encoded_body_bound() {
        let mut body = ResponseBody::default();
        body.state = ReadState::TrailerLength;
        body.maximum = Some(128);
        body.maximum_trailer = Some(8);
        body.buf.append(Bytes::from_static(&[0, 0, 0, 9])).unwrap();
        assert!(matches!(
            Pin::new(&mut body).step(),
            Err(Error::ResponseTooLarge)
        ));
        assert!(body.trailer.is_none());
        assert!(body.incomplete_data.is_empty());
    }

    #[test]
    fn explicit_data_frame_bound_is_independent_of_encoded_body_bound() {
        let mut body = ResponseBody::default();
        body.state = ReadState::DataLength;
        body.maximum = Some(128);
        body.maximum_message = Some(8);
        body.buf.append(Bytes::from_static(&[0, 0, 0, 9])).unwrap();
        assert!(matches!(
            Pin::new(&mut body).step(),
            Err(Error::ResponseTooLarge)
        ));
        assert!(body.data.is_none());
        assert!(body.incomplete_data.is_empty());
    }

    #[test]
    fn binary_and_independently_padded_text_accept_the_payload_boundary() {
        let payload = [1_u8; 8];
        let trailer = b"grpc-status:0\r\n";
        let mut bytes = vec![0];
        bytes.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&payload);
        bytes.push(TRAILER_BIT);
        bytes.extend_from_slice(&(trailer.len() as u32).to_be_bytes());
        bytes.extend_from_slice(trailer);
        let encoded_limit = (payload.len() + 32 + 10) * 4;
        assert!(bytes.len() > payload.len());
        for text in [false, true] {
            let mut body = ResponseBody::default();
            body.state = ReadState::CompressionFlag;
            body.maximum = Some(encoded_limit);
            body.maximum_message = Some(payload.len());
            body.maximum_trailer = Some(32);
            body.maximum_headers = Some(64);
            body.buf = EncodedBytes::new(if text {
                "application/grpc-web-text+proto"
            } else {
                "application/grpc-web+proto"
            })
            .unwrap();
            let input = if text {
                bytes
                    .iter()
                    .map(|byte| BASE64_STANDARD.encode([*byte]))
                    .collect::<String>()
                    .into_bytes()
            } else {
                bytes.clone()
            };
            assert!(input.len() <= encoded_limit);
            body.buf.append(input.into()).unwrap();
            Pin::new(&mut body).step().unwrap();
            assert_eq!(body.data.as_ref().unwrap().len(), payload.len() + 5);
            assert_eq!(
                body.trailer.as_ref().unwrap().get("grpc-status").unwrap(),
                "0"
            );
        }
    }

    #[test]
    fn weighted_header_boundary_duplicates_and_overflow_are_checked() {
        assert_eq!(checked_header_list_size(0, 1, 1, Some(34)).unwrap(), 34);
        assert!(matches!(
            checked_header_list_size(0, 1, 2, Some(34)),
            Err(Error::ResponseTooLarge)
        ));
        assert!(matches!(
            checked_header_list_size(usize::MAX, 1, 1, None),
            Err(Error::ResponseTooLarge)
        ));
        assert!(matches!(
            checked_header_list_size(0, usize::MAX, 1, None),
            Err(Error::ResponseTooLarge)
        ));
        let headers = parse_trailers(b"x:v\r\nx:v\r\n\n", Some(68)).unwrap();
        assert_eq!(headers.get_all("x").iter().count(), 2);
        assert!(matches!(
            parse_trailers(b"x:v\r\nx:v\r\nx:v\r\n\n", Some(68)),
            Err(Error::ResponseTooLarge)
        ));
    }

    #[test]
    fn trailer_raw_bytes_and_weight_are_distinct_limits() {
        let bytes = format!("x:{}v\r\n\n", " ".repeat(100));
        let headers = parse_trailers(bytes.as_bytes(), Some(34)).unwrap();
        assert_eq!(headers.get("x").unwrap(), "v");
        assert!(bytes.len() > 34);
        let bytes = "x:v\r\n".repeat(512) + "\n";
        assert!(bytes.len() < 16 * 1024);
        assert!(matches!(
            parse_trailers(bytes.as_bytes(), Some(16 * 1024)),
            Err(Error::ResponseTooLarge)
        ));
    }
}

#[cfg(test)]
mod dynamic_trailer_regressions {
    use super::*;

    #[test]
    fn canonical_trailer_endings_are_consumed() {
        for payload in [b"x-value:value\r\n".as_slice(), b"x-value:value\r\n\r\n"] {
            let mut bytes = payload.to_vec();
            bytes.push(b'\n');
            let headers = parse_trailers(&bytes, None).unwrap();
            assert_eq!(headers.len(), 1);
            assert_eq!(headers.get("x-value").unwrap(), "value");
        }
    }

    #[test]
    fn content_after_a_header_terminator_is_rejected() {
        for payload in [
            b"x-value:value\r\n\r\njunk".as_slice(),
            b"x-value:value\r\n\r\nx-hidden:secret\r\n",
        ] {
            let mut bytes = payload.to_vec();
            bytes.push(b'\n');
            assert!(matches!(
                parse_trailers(&bytes, None),
                Err(Error::HeaderParsingError)
            ));
        }
    }

    #[test]
    fn sixty_five_distinct_headers_are_preserved() {
        let mut bytes = String::new();
        for index in 0..65 {
            bytes.push_str(&format!("x-{index}:value-{index}\r\n"));
        }
        bytes.push('\n');
        let headers = parse_trailers(bytes.as_bytes(), None).unwrap();
        assert_eq!(headers.len(), 65);
        assert_eq!(headers.get("x-64").unwrap(), "value-64");
    }

    #[test]
    fn many_duplicate_values_preserve_order_and_spelling() {
        let mut bytes = String::new();
        for index in 0..4096 {
            bytes.push_str(&format!("x-value:item-{index},literal\r\n"));
        }
        bytes.push('\n');
        let headers = parse_trailers(bytes.as_bytes(), None).unwrap();
        let values: Vec<_> = headers.get_all("x-value").iter().collect();
        assert_eq!(values.len(), 4096);
        assert_eq!(values[0], "item-0,literal");
        assert_eq!(values[4095], "item-4095,literal");
    }

    #[test]
    fn malformed_or_partial_headers_are_rejected() {
        for bytes in [
            b"not a header\r\n\n".as_slice(),
            b"x-value:value\r",
            b"x:value",
        ] {
            assert!(matches!(
                parse_trailers(bytes, None),
                Err(Error::HeaderParsingError)
            ));
        }
    }

    #[test]
    fn real_header_map_capacity_failure_is_controlled() {
        let mut bytes = String::new();
        for index in 0..65536 {
            bytes.push_str(&format!("x-{index}:v\r\n"));
        }
        bytes.push('\n');
        assert!(matches!(
            parse_trailers(bytes.as_bytes(), None),
            Err(Error::HeaderParsingError)
        ));
    }
}
