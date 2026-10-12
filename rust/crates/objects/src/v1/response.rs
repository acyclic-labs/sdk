//! Core response and persisted-metadata validation, independent of transport features.
use super::{Error, request, wire};

/// Validates a generated response against its original request, shared with WASM clients.
pub fn validate_binary(
    route: &str,
    query: &[u8],
    bytes: &[u8],
    body_length: u64,
) -> Result<(), Error> {
    use prost::Message;
    macro_rules! decode {
        ($name:ident, $bytes:expr) => {
            wire::$name::decode($bytes).map_err(|_| invalid())?
        };
    }
    match route {
        "buckets/create" => {
            let query = decode!(CreateBucketRequest, query);
            bucket(
                &decode!(Bucket, bytes),
                &wire::BucketRef { name: query.name },
            )?;
        }
        "buckets/head" => {
            let query = decode!(HeadBucketRequest, query);
            bucket(
                &decode!(Bucket, bytes),
                query.bucket.as_ref().ok_or_else(invalid)?,
            )?;
        }
        "objects/put" => {
            let info = decode!(ObjectInfo, bytes);
            object_info(&info)?;
            if info.size != body_length {
                return Err(invalid());
            }
        }
        "objects/head" => {
            let info = decode!(HeadObjectResponse, bytes);
            object_info(info.object.as_ref().ok_or_else(invalid)?)?;
        }
        "objects/list" => listing(
            &decode!(ListObjectsRequest, query),
            &decode!(ListObjectsResponse, bytes),
        )?,
        "multipart/create" => {
            let upload = decode!(MultipartUpload, bytes);
            if request::upload_id(&upload.upload_id).is_err() {
                return Err(invalid());
            }
        }
        "multipart/upload-part" => {
            let query = decode!(UploadPartHeader, query);
            let part = decode!(UploadedPart, bytes);
            if part.part_number != query.part_number
                || part.size != body_length
                || part.etag.is_empty()
                || part.etag.len() > 8192
                || part.etag.contains(['\r', '\n', '\0'])
            {
                return Err(invalid());
            }
        }
        "multipart/list-parts" => parts(
            &decode!(ListPartsRequest, query),
            &decode!(ListPartsResponse, bytes),
        )?,
        "multipart/complete" => object_info(&decode!(ObjectInfo, bytes))?,
        "buckets/delete" => {
            decode!(DeleteBucketResponse, bytes);
        }
        "objects/delete" => {
            decode!(DeleteObjectResponse, bytes);
        }
        "multipart/abort" => {
            decode!(AbortMultipartResponse, bytes);
        }
        _ => return Err(invalid()),
    }
    Ok(())
}

/// Validates the first download frame and returns its exact selected decoded size.
pub fn validate_get_header(query: &[u8], bytes: &[u8], maximum: u64) -> Result<u64, Error> {
    use prost::Message;
    let query = wire::GetObjectRequest::decode(query).map_err(|_| invalid())?;
    let header = wire::GetObjectHeader::decode(bytes).map_err(|_| invalid())?;
    get_header(&header, &query.range, maximum)
}

pub(crate) fn invalid() -> Error {
    wire::ErrorCode::Unavailable.into()
}
/// Validates the canonical protobuf timestamp range and nanosecond normalization.
pub fn timestamp(value: &prost_types::Timestamp) -> Result<(), Error> {
    if !(-62_135_596_800..=253_402_300_799).contains(&value.seconds)
        || !(0..1_000_000_000).contains(&value.nanos)
    {
        return Err(invalid());
    }
    Ok(())
}
/// Validates current-object metadata, opaque `ETag` and timestamp.
pub fn object_info(value: &wire::ObjectInfo) -> Result<(), Error> {
    let timestamp = value.last_modified.as_ref().ok_or_else(invalid)?;
    self::timestamp(timestamp)?;
    if value.etag.is_empty() || value.etag.len() > 8192 || value.etag.contains(['\r', '\n', '\0']) {
        return Err(invalid());
    }
    request::metadata(&value.metadata).map_err(|_| invalid())
}
/// Validates a complete or ranged download selection within its decoded allocation bound.
pub fn get_header(
    header: &wire::GetObjectHeader,
    selected: &Option<wire::ByteRange>,
    maximum: u64,
) -> Result<u64, Error> {
    let object = header.object.as_ref().ok_or_else(invalid)?;
    object_info(object)?;
    let expected = request::range(selected, object.size).map_err(|_| invalid())?;
    if expected != header.content_range {
        return Err(invalid());
    }
    let size = expected.map_or(object.size, |range| range.end - range.start + 1);
    if size > maximum || usize::try_from(size).is_err() {
        return Err(wire::ErrorCode::QuotaExceeded.into());
    }
    Ok(size)
}
/// Validates persisted bucket metadata against its canonical logical identity.
pub fn bucket(value: &wire::Bucket, expected: &wire::BucketRef) -> Result<(), Error> {
    request::bucket_name(&expected.name).map_err(|_| invalid())?;
    if value.bucket.as_ref() != Some(expected) || value.created_at.is_none() {
        return Err(invalid());
    }
    timestamp(value.created_at.as_ref().ok_or_else(invalid)?)?;
    Ok(())
}
/// Validates ordered bounded multipart results against the original page request.
pub fn parts(
    query: &wire::ListPartsRequest,
    response: &wire::ListPartsResponse,
) -> Result<(), Error> {
    let limit = request::page_size(query.page_size)?;
    let mut previous = query.after_part_number;
    for part in &response.parts {
        if part.part_number <= previous
            || part.part_number > 10_000
            || part.etag.is_empty()
            || part.etag.len() > 8192
            || part.etag.contains(['\r', '\n', '\0'])
            || part.size > 5 * 1024 * 1024 * 1024
        {
            return Err(invalid());
        }
        previous = part.part_number;
    }
    if response.parts.len() > limit
        || response.is_truncated
            && (response.parts.is_empty() || response.next_part_number != previous)
        || !response.is_truncated && response.next_part_number != 0
    {
        return Err(invalid());
    }
    Ok(())
}
/// Validates an eventual listing's keys, prefixes, page bounds and continuation shape.
pub fn listing(
    query: &wire::ListObjectsRequest,
    response: &wire::ListObjectsResponse,
) -> Result<(), Error> {
    let limit = request::page_size(query.page_size)?;
    if response.entries.len() + response.common_prefixes.len() > limit
        || response.is_truncated == response.continuation_token.is_empty()
        || response.continuation_token.len() > 8192
    {
        return Err(invalid());
    }
    let mut previous: Option<&str> = None;
    for entry in &response.entries {
        request::key(&entry.object_key).map_err(|_| invalid())?;
        let suffix = entry
            .object_key
            .strip_prefix(&query.prefix)
            .ok_or_else(invalid)?;
        if previous.is_some_and(|previous| previous >= entry.object_key.as_str())
            || !query.delimiter.is_empty() && suffix.contains(&query.delimiter)
        {
            return Err(invalid());
        }
        previous = Some(&entry.object_key);
        object_info(entry.object.as_ref().ok_or_else(invalid)?)?;
    }
    previous = None;
    for prefix in &response.common_prefixes {
        let suffix = prefix.strip_prefix(&query.prefix).ok_or_else(invalid)?;
        if query.delimiter.is_empty()
            || !suffix.ends_with(&query.delimiter)
            || suffix
                .strip_suffix(&query.delimiter)
                .is_none_or(|value| value.contains(&query.delimiter))
            || previous.is_some_and(|previous| previous >= prefix.as_str())
        {
            return Err(invalid());
        }
        previous = Some(prefix);
    }
    Ok(())
}

/// Decodes one download body frame of at most `remaining` selected bytes.
pub fn body(frame: wire::Body, remaining: u64) -> Result<Vec<u8>, Error> {
    let (length, compressed) = body_spec(&frame, remaining)?;
    if compressed {
        zstd(&frame.data, length, |maximum| zstd_output(maximum).ok(), |_, _| true).ok_or_else(invalid)
    } else {
        Ok(frame.data)
    }
}

fn body_spec(frame: &wire::Body, remaining: u64) -> Result<(usize, bool), Error> {
    if frame.decoded_length > remaining { return Err(invalid()); }
    let length = usize::try_from(frame.decoded_length).map_err(|_| invalid())?;
    match wire::Codec::try_from(frame.codec) {
        Ok(wire::Codec::None)
            if frame.decoded_length <= wire::ObjectsLimit::MaxBodyFrameBytes as u64 && frame.data.len() == length =>
        { Ok((length, false)) }
        Ok(wire::Codec::Zstd)
            if frame.decoded_length <= wire::ObjectsLimit::MaxBodyDecodedBytes as u64 =>
        { Ok((length, true)) }
        _ => Err(invalid()),
    }
}
/// Decompresses one or more complete Zstandard frames to exactly `length` bytes.
fn zstd(
    mut input: &[u8], length: usize,
    allocate: impl FnOnce(usize) -> Option<Vec<u8>>,
    record: impl FnOnce(u64, u64) -> bool,
) -> Option<Vec<u8>> {
    use std::io::Read;
    let encoded = input.len() as u64;
    let mut completed = 0;
    let mut output = allocate(length.checked_add(1)?)?;
    let valid = (|| {
        if input.is_empty() { return None; }
        while !input.is_empty() {
            let mut decoder = ruzstd::decoding::StreamingDecoder::new(&mut input).ok()?;
            loop {
                let count = decoder.read(&mut output[completed..]).ok()?;
                completed += count;
                if completed > length { return None; }
                if count == 0 { break; }
            }
        }
        (completed == length).then_some(())
    })();
    if !record(encoded - input.len() as u64, completed as u64) { return None; }
    valid?;
    output.truncate(completed);
    Some(output)
}

fn zstd_output(maximum: usize) -> Result<Vec<u8>, std::collections::TryReserveError> {
    let mut output = Vec::new();
    output.try_reserve_exact(maximum)?;
    output.resize(maximum, 0);
    Ok(output)
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
#[derive(Clone)]
pub(crate) struct NativeGetControls {
    pub meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    pub errors: std::sync::Arc<super::NativeTransportErrors>,
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
impl NativeGetControls {
    pub(crate) fn fail(&self, error: impl Into<super::NativeObjectsError>) -> Error {
        let error = error.into();
        let service = match &error.cause {
            super::NativeObjectsCause::Service(service) if !error.has_payload() => *service,
            _ => invalid(),
        };
        self.errors.record(error);
        service
    }

    pub(crate) fn check(&self) -> Result<(), Error> {
        self.meter.check().map_err(|error| self.fail(error))
    }

    pub(crate) fn dispatch<T>(
        &self,
        kind: acyclic_native_runtime::NativeOperationKind,
        operation: impl FnOnce() -> T,
    ) -> Result<(T, bool), Error> {
        let mut plan = acyclic_native_runtime::NativeOperationReservation::new(
            self.meter.as_ref(), kind, 1,
        ).map_err(|error| self.fail(error))?;
        let (operation, recorded) = plan.attempt(operation);
        let failed = recorded.is_err();
        if let Err(error) = recorded {
            self.errors.record(super::NativeObjectsError::from(super::NativeObjectsCause::Admission(error)));
        }
        Ok((operation, failed))
    }

    pub(crate) fn request<M: prost::Message>(&self, query: &M) -> Result<acyclic_native_runtime::ReadAllocation, Error> {
        let maximum = query.encoded_len().checked_mul(4)
            .ok_or_else(invalid)?;
        acyclic_native_runtime::ReadAllocation::adopt(&self.meter, maximum)
            .map_err(|error| self.fail(error))
    }

    pub(crate) fn binary(&self, value: &impl prost::Message) -> Result<(Vec<u8>, acyclic_native_runtime::ReadAllocation), Error> {
        let maximum = value.encoded_len();
        self.meter.admit_codec(maximum as u64, 0, 0).map_err(|error| self.fail(error))?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&self.meter, maximum)
            .map_err(|error| self.fail(error))?;
        let mut bytes = Vec::new();
        if let Err(error) = bytes.try_reserve_exact(maximum) {
            allocation.reconcile_capacity(bytes.capacity()).map_err(|error| self.fail(error))?;
            return Err(self.fail(super::NativeObjectsError::from(self.meter.allocation_failed(error)).with_wire(bytes, allocation)));
        }
        if let Err(error) = allocation.reconcile_capacity(bytes.capacity()) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(bytes, allocation)));
        }
        value.encode(&mut bytes).map_err(|_| invalid())?;
        if let Err(error) = self.meter.record_codec(bytes.len() as u64, 0, 0) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(bytes, allocation)));
        }
        Ok((bytes, allocation))
    }

    pub(crate) fn copy(&self, bytes: &[u8]) -> Result<(Vec<u8>, acyclic_native_runtime::ReadAllocation), Error> {
        self.meter.admit_codec(0, bytes.len() as u64, 0).map_err(|error| self.fail(error))?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&self.meter, bytes.len())
            .map_err(|error| self.fail(error))?;
        let mut data = Vec::new();
        if let Err(error) = data.try_reserve_exact(bytes.len()) {
            allocation.reconcile_capacity(data.capacity()).map_err(|error| self.fail(error))?;
            return Err(self.fail(super::NativeObjectsError::from(self.meter.allocation_failed(error)).with_wire(data, allocation)));
        }
        if let Err(error) = allocation.reconcile_capacity(data.capacity()) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(data, allocation)));
        }
        data.extend_from_slice(bytes);
        if let Err(error) = self.meter.record_codec(0, bytes.len() as u64, 0) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(data, allocation)));
        }
        Ok((data, allocation))
    }

    pub(crate) fn wire_bytes(&self, data: Vec<u8>, mut allocation: acyclic_native_runtime::ReadAllocation) -> Result<bytes::Bytes, Error> {
        let mut control = match acyclic_native_runtime::ReadAllocation::new(&self.meter, std::mem::size_of::<NativeBodyChunk>()) {
            Ok(control) => control,
            Err(error) => return Err(self.fail(super::NativeObjectsError::from(error).with_wire(data, allocation))),
        };
        if let Err(error) = allocation.absorb(&mut control) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(data, allocation)));
        }
        Ok(bytes::Bytes::from_owner(NativeBodyChunk { data, _allocation: allocation }))
    }

    pub(crate) fn body(&self, frame: wire::Body, remaining: u64, mut allocation: acyclic_native_runtime::ReadAllocation) -> Result<bytes::Bytes, Error> {
        use acyclic_native_runtime::ReadAllocation;
        if let Err(error) = allocation.reconcile_capacity(frame.data.capacity()) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(frame.data, allocation)));
        }
        let (decoded, compressed) = match body_spec(&frame, remaining) {
            Ok(spec) => spec,
            Err(error) => return Err(self.fail(super::NativeObjectsError::from(error).with_wire(frame.data, allocation))),
        };
        if !compressed { return self.wire_bytes(frame.data, allocation); }
        let encoded = frame.data.len() as u64;
        if let Err(error) = self.meter.admit_codec(encoded, decoded as u64 + 1, 0) {
            return Err(self.fail(super::NativeObjectsError::from(error).with_wire(frame.data, allocation)));
        }
        let mut output_claim = match ReadAllocation::new(&self.meter, decoded + 1) {
            Ok(claim) => claim,
            Err(error) => return Err(self.fail(super::NativeObjectsError::from(error).with_wire(frame.data, allocation))),
        };
        let data = zstd(&frame.data, decoded, |maximum| {
            match zstd_output(maximum) {
                Ok(output) => {
                    if let Err(error) = output_claim.reconcile_capacity(output.capacity()) {
                        self.fail(error);
                        None
                    } else { Some(output) }
                }
                Err(error) => {
                    self.fail(self.meter.allocation_failed(error));
                    None
                }
            }
        }, |encoded, copied| {
            self.meter.record_codec(encoded, copied, 0)
                .map_err(|error| self.fail(error)).is_ok()
        });
        let Some(data) = data else {
            let error = self.errors.take().unwrap_or_else(|| invalid().into());
            return Err(self.fail(error.with_wire(frame.data, allocation)));
        };
        drop(frame.data);
        allocation.resize_retained(0).map_err(|error| self.fail(error))?;
        allocation.absorb(&mut output_claim).map_err(|error| self.fail(error))?;
        self.wire_bytes(data, allocation)
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
struct NativeBodyChunk {
    data: Vec<u8>,
    _allocation: acyclic_native_runtime::ReadAllocation,
}
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
impl AsRef<[u8]> for NativeBodyChunk {
    fn as_ref(&self) -> &[u8] { &self.data }
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn header_capacity(header: &wire::GetObjectHeader) -> usize {
    let Some(object) = &header.object else { return 0; };
    info_capacity(object)
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn info_capacity(object: &wire::ObjectInfo) -> usize {
    object.etag.capacity() + object.metadata.as_ref().map_or(0, metadata_capacity)
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
fn metadata_capacity(metadata: &wire::ObjectMetadata) -> usize {
    metadata.content_type.capacity()
        + metadata.content_encoding.capacity()
        + metadata.cache_control.capacity()
        + metadata.content_disposition.capacity()
        + metadata.content_language.capacity()
        + metadata.user.iter().map(|(key, value)| key.capacity() + value.capacity()).sum::<usize>()
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn validate_put_controlled(
    result: wire::ObjectInfo,
    mut allocation: Option<acyclic_native_runtime::ReadAllocation>,
    length: Result<u64, Error>,
    controls: Option<&NativeGetControls>,
) -> Result<(wire::ObjectInfo, Option<acyclic_native_runtime::ReadAllocation>), Error> {
    let validation = (|| {
        if let Some(allocation) = &mut allocation {
            allocation.reconcile_capacity(info_capacity(&result))
                .map_err(|error| controls.expect("native receipt custody").fail(error))?;
        }
        object_info(&result)?;
        if result.size != length? { return Err(invalid()); }
        Ok(())
    })();
    if let Err(error) = validation {
        if let (Some(controls), Some(allocation)) = (controls, allocation.take()) {
            let original = controls.errors.take().unwrap_or_else(|| error.into());
            let mut failure = original;
            failure.receipt = Some(super::ObjectsPutReceipt::new(result, Some(allocation)));
            return Err(controls.fail(failure));
        }
        return Err(error);
    }
    Ok((result, allocation))
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) enum RequestInput<M> {
    Ordinary(M),
    Native {
        request: super::ObjectsNativeRequest<M>,
        controls: Option<NativeGetControls>,
    },
    #[cfg(feature = "grpc")]
    CodecFailure(tonic::Status),
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
impl<M> RequestInput<M> {
    pub(crate) fn native(
        request: super::ObjectsNativeRequest<M>,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Self, super::NativeObjectsError> {
        super::native_input_meter(meter, &request)?;
        Ok(Self::Native { request, controls: None })
    }
    pub(crate) fn value(&self) -> &M {
        match self {
            Self::Ordinary(value) => value,
            Self::Native { request, .. } => request.data(),
            #[cfg(feature = "grpc")]
            Self::CodecFailure(_) => unreachable!("failed source frames are handled before encoding"),
        }
    }
    pub(crate) fn map<N>(self, transform: impl FnOnce(M) -> N) -> RequestInput<N> {
        match self {
            Self::Ordinary(value) => RequestInput::Ordinary(transform(value)),
            Self::Native { request, controls } => RequestInput::Native { request: request.map(transform), controls },
            #[cfg(feature = "grpc")]
            Self::CodecFailure(error) => RequestInput::CodecFailure(error),
        }
    }
    #[cfg(feature = "grpc")]
    pub(crate) fn with_controls(mut self, current: Option<NativeGetControls>) -> Result<Self, Error>
    where M: prost::Message {
        if let (Self::Native { request, controls }, Some(current)) = (&mut self, current) {
            let encoded = request.data().encoded_len();
            current.meter.admit_codec(encoded as u64, 0, 0)
                .map_err(|error| current.fail(error))?;
            *controls = Some(current);
        }
        Ok(self)
    }
    #[cfg(feature = "grpc")]
    fn controls(&self) -> Option<&NativeGetControls> {
        match self {
            Self::Ordinary(_) | Self::CodecFailure(_) => None,
            Self::Native { controls, .. } => Some(controls.as_ref().expect("native source controls are prepared before IO")),
        }
    }
}

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) struct CustodyProstCodec<M, U>(std::marker::PhantomData<(M, U)>);
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl<M, U> CustodyProstCodec<M, U> {
    pub(crate) fn new() -> Self { Self(std::marker::PhantomData) }
}
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl<M: prost::Message + 'static, U: prost::Message + Default + 'static> tonic::codec::Codec for CustodyProstCodec<M, U> {
    type Encode = RequestInput<M>;
    type Decode = U;
    type Encoder = CustodyProstEncoder<M>;
    type Decoder = tonic_prost::ProstDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder { CustodyProstEncoder(std::marker::PhantomData) }
    fn decoder(&mut self) -> Self::Decoder {
        tonic_prost::ProstDecoder::new(tonic::codec::BufferSettings::default())
    }
}
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) struct CustodyProstEncoder<M>(std::marker::PhantomData<M>);
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl<M: prost::Message> tonic::codec::Encoder for CustodyProstEncoder<M> {
    type Item = RequestInput<M>;
    type Error = tonic::Status;
    fn encode(&mut self, item: Self::Item, buffer: &mut tonic::codec::EncodeBuf<'_>) -> Result<(), Self::Error> {
        let item = match item {
            RequestInput::CodecFailure(error) => return Err(error),
            item => item,
        };
        // The packet retains its guard through encoding.
        let encoded = item.value().encoded_len() as u64;
        if let Some(controls) = item.controls()
            && let Err(error) = controls.meter.admit_codec(encoded, 0, 0) {
            controls.fail(error);
            return Err(controls.errors.codec_failure().expect("one prepared native codec failure"));
        }
        item.value().encode(buffer).expect("Message only errors if not enough space");
        if let Some(controls) = item.controls()
            && let Err(error) = controls.meter.record_codec(encoded, 0, 0) {
            controls.fail(error);
            return Err(controls.errors.codec_failure().expect("one prepared native codec failure"));
        }
        Ok(())
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pin_project_lite::pin_project! {
    pub(crate) struct BodySlot<S> {
        #[pin]
        source: Option<S>,
    }
}
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
impl<S: futures::Stream> futures::Stream for BodySlot<S> {
    type Item = S::Item;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
        -> std::task::Poll<Option<Self::Item>> {
        self.project().source.as_pin_mut()
            .expect("only initialized transport slots are published").poll_next(cx)
    }
}
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn prepare_body_stream<F, A, B, S>(
    _factory: &F, controls: Option<&NativeGetControls>,
) -> Result<Option<(std::pin::Pin<Box<BodySlot<S>>>, acyclic_native_runtime::ReadAllocation)>, Error>
where F: FnOnce(A, B) -> S,
      S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static {
    controls.map(|controls| {
        let allocation = acyclic_native_runtime::ReadAllocation::new(
            &controls.meter, std::mem::size_of::<BodySlot<S>>(),
        ).map_err(|error| controls.fail(error))?;
        Ok((Box::pin(BodySlot { source: None }), allocation))
    }).transpose()
}
#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn finish_body_stream<S>(
    source: S, prepared: Option<(std::pin::Pin<Box<BodySlot<S>>>, acyclic_native_runtime::ReadAllocation)>,
) -> super::BodyStream
where S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static {
    use futures::StreamExt;
    if let Some((mut prepared, allocation)) = prepared {
        prepared.as_mut().project().source.set(Some(source));
        super::BodyStream::from_native_boxed(prepared, allocation)
    } else {
        source.boxed().into()
    }
}
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pin_project_lite::pin_project! {
    struct NativeHttpStream<S> {
        #[pin]
        source: S,
        failure: Option<Box<Error>>,
        _controls: NativeGetControls,
    }
}
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
impl<S> futures::Stream for NativeHttpStream<S>
where S: futures::Stream<Item = Result<bytes::Bytes, Error>> {
    type Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>;
    fn poll_next(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
        -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if this.failure.is_none() { return std::task::Poll::Ready(None); }
        match this.source.poll_next(cx) {
            std::task::Poll::Ready(Some(Err(error))) => {
                let mut failure = this.failure.take().expect("one prepared HTTP source failure");
                *failure = error;
                std::task::Poll::Ready(Some(Err(failure as Box<dyn std::error::Error + Send + Sync>)))
            }
            std::task::Poll::Ready(Some(Ok(bytes))) => std::task::Poll::Ready(Some(Ok(bytes))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub(crate) fn http_body<S>(
    source: S, controls: Option<&NativeGetControls>,
) -> Result<reqwest::Body, Error>
where S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static {
    if let Some(controls) = controls {
        let failure_allocation = acyclic_native_runtime::ReadAllocation::new(
            &controls.meter, std::mem::size_of::<Error>(),
        ).map_err(|error| controls.fail(error))?;
        let failure = Box::new(invalid());
        controls.errors.retain_wire(failure_allocation).map_err(|error| controls.fail(error))?;
        let source_allocation = acyclic_native_runtime::ReadAllocation::new(
            &controls.meter, std::mem::size_of::<NativeHttpStream<S>>(),
        ).map_err(|error| controls.fail(error))?;
        controls.errors.retain_wire(source_allocation).map_err(|error| controls.fail(error))?;
        Ok(reqwest::Body::wrap_stream(NativeHttpStream {
            source, failure: Some(failure), _controls: controls.clone(),
        }))
    } else {
        Ok(reqwest::Body::wrap_stream(source))
    }
}

#[cfg(all(not(target_arch = "wasm32"), any(feature = "grpc", feature = "http")))]
pub(crate) fn reject_frame(
    frame: Option<wire::get_object_response::Frame>,
    allocation: Option<acyclic_native_runtime::ReadAllocation>,
    controls: Option<&NativeGetControls>,
) -> Error {
    use wire::get_object_response::Frame;
    let error = match &frame {
        Some(Frame::Error(detail)) => wire::ErrorCode::try_from(detail.code)
            .ok().filter(|code| *code != wire::ErrorCode::Unspecified)
            .map_or_else(invalid, Error::from),
        _ => invalid(),
    };
    if let (Some(controls), Some(mut allocation)) = (controls, allocation) {
        let capacity = match &frame {
            Some(Frame::Header(header)) => header_capacity(header),
            Some(Frame::Body(body)) => body.data.capacity(),
            Some(Frame::Error(detail)) => detail.request_id.capacity(),
            None => 0,
        };
        let cause = allocation.reconcile_capacity(capacity)
            .err().map_or_else(|| super::NativeObjectsError::from(error), super::NativeObjectsError::from);
        let cause = match frame {
            Some(Frame::Header(header)) => cause.with_header(header, allocation),
            Some(Frame::Body(body)) => cause.with_wire(body.data, allocation),
            Some(Frame::Error(detail)) => cause.with_wire(detail.request_id.into_bytes(), allocation),
            None => cause,
        };
        return controls.fail(cause);
    }
    error
}

/// Builds an uncompressed body frame for a provider that does not compress.
#[must_use]
pub fn plain_body(data: Vec<u8>) -> wire::Body {
    wire::Body {
        codec: wire::Codec::None as i32,
        decoded_length: data.len() as u64,
        data,
    }
}

#[cfg(test)]
pub(crate) fn zstd_body(data: &[u8]) -> wire::Body {
    wire::Body {
        codec: wire::Codec::Zstd as i32,
        decoded_length: data.len() as u64,
        data: ruzstd::encoding::compress_to_vec(data, ruzstd::encoding::CompressionLevel::Fastest),
    }
}

#[cfg(test)]
mod body_tests {
    use super::{body, plain_body, wire, zstd_body};

    const MAXIMUM: u64 = wire::ObjectsLimit::MaxBodyFrameBytes as u64;
    const BLOCK: u64 = wire::ObjectsLimit::MaxBodyDecodedBytes as u64;

    fn pattern(length: usize) -> Vec<u8> {
        (0..=250u8).cycle().take(length).collect()
    }

    #[test]
    fn a_whole_stored_block_decodes_as_one_compressed_frame() {
        let data = pattern(usize::try_from(BLOCK).unwrap_or(0));
        assert_eq!(body(zstd_body(&data), BLOCK).ok(), Some(data.clone()));
        let mut larger = data;
        larger.push(0);
        assert!(body(zstd_body(&larger), u64::MAX).is_err());
    }

    #[test]
    fn empty_zstd_data_is_refused_and_an_empty_plain_frame_is_accepted() {
        let empty = wire::Body {
            codec: wire::Codec::Zstd as i32,
            data: Vec::new(),
            decoded_length: 0,
        };
        assert!(body(empty, MAXIMUM).is_err());
        assert_eq!(body(plain_body(Vec::new()), MAXIMUM).ok(), Some(Vec::new()));
    }

    #[test]
    fn concatenated_zstd_frames_decode_in_order() {
        let (first, second) = (pattern(3000), vec![7; 5000]);
        let mut frame = zstd_body(&first);
        frame.data.extend(zstd_body(&second).data);
        frame.decoded_length = 8000;
        assert_eq!(body(frame, MAXIMUM).ok(), Some([first, second].concat()));
    }

    #[test]
    fn mixed_frames_reassemble_in_order() {
        let data = pattern(150_000);
        let frames = data
            .chunks(65_536)
            .enumerate()
            .map(|(index, chunk)| {
                if index % 2 == 0 {
                    zstd_body(chunk)
                } else {
                    plain_body(chunk.to_vec())
                }
            })
            .collect::<Vec<_>>();
        let mut remaining = data.len() as u64;
        let mut result = Vec::new();
        for frame in frames {
            let bytes = body(frame, remaining).expect("valid frame");
            remaining -= bytes.len() as u64;
            result.extend(bytes);
        }
        assert_eq!(remaining, 0);
        assert_eq!(result, data);
    }

    #[test]
    fn oversized_declared_length_is_refused_before_decompressing() {
        // Garbage data proves the length bound fails before any decompression.
        for (codec, length, remaining) in [
            (wire::Codec::None, MAXIMUM + 1, u64::MAX),
            (wire::Codec::Zstd, BLOCK + 1, u64::MAX),
            (wire::Codec::None, u64::MAX, u64::MAX),
            (wire::Codec::Zstd, u64::MAX, u64::MAX),
            (wire::Codec::None, 10, 9),
            (wire::Codec::Zstd, 10, 9),
        ] {
            let frame = wire::Body {
                codec: codec as i32,
                data: vec![0xff; 16],
                decoded_length: length,
            };
            assert!(body(frame, remaining).is_err());
        }
        // A plain frame above 64 KiB is refused even when its data matches.
        assert!(body(plain_body(pattern(65_537)), u64::MAX).is_err());
        // A frame that would expand beyond its declaration is cut off and refused.
        let mut frame = zstd_body(&vec![0; 65_536]);
        frame.decoded_length = 1024;
        assert!(body(frame, MAXIMUM).is_err());
    }

    #[test]
    fn mismatched_length_unknown_codec_and_corrupt_data_are_refused() {
        let mut short = zstd_body(&pattern(1000));
        short.decoded_length = 1001;
        assert!(body(short, MAXIMUM).is_err());
        let mut plain = plain_body(pattern(10));
        plain.decoded_length = 11;
        assert!(body(plain, MAXIMUM).is_err());
        for codec in [0, 3] {
            let mut unknown = plain_body(pattern(10));
            unknown.codec = codec;
            assert!(body(unknown, MAXIMUM).is_err());
        }
        let mut corrupt = zstd_body(&pattern(1000));
        corrupt.data.truncate(corrupt.data.len() / 2);
        assert!(body(corrupt, MAXIMUM).is_err());
    }
}
