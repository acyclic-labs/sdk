use super::super::super::NativeObjectsError;
use super::*;
use crate::sync::Mutex;
use acyclic_native_runtime::{NativeReadMeter, ReadAllocation, ReadAllocationReservation};
use bytes::Buf;
use prost::encoding::{DecodeContext, WireType, decode_key, decode_varint, skip_field};
use std::io;

#[cfg(all(test, not(objects_loom)))]
mod tests;

pub(super) struct Exchange {
    // Reply settlement is independent. Decoding acquires metadata (1), then
    // resources (2); both guards are released before settlement or an await.
    pub(super) reply: Mutex<Option<Reply>>,
    pub(super) sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
    metadata: Mutex<Guarded<Vec<Option<wire::ObjectMetadata>>>>,
    resources: Mutex<Resources, 2>,
}

const REPLY_BYTES: u64 = 16 * 1024 * 1024;
const TEXT_BYTES: usize = 8192;
// An object has an etag and five metadata strings; an error has one string.
const TEXT_FIELDS: usize = 6;
type TextCredits = [Option<ReadAllocationReservation>; TEXT_FIELDS];

struct CodecCredit {
    meter: std::sync::Arc<dyn NativeReadMeter>,
    encoded: u64,
    copied: u64,
    failure: Option<io::Error>,
}
impl CodecCredit {
    fn record(&mut self, encoded: u64, copied: u64) -> Result<(), NativeObjectsError> {
        if encoded > self.encoded || copied > self.copied {
            return Err(invalid());
        }
        self.encoded -= encoded;
        self.copied -= copied;
        if let Err(error) = self.meter.record_reserved_codec(encoded, copied) {
            self.failure.get_or_insert(error);
        }
        Ok(())
    }
}
impl Drop for CodecCredit {
    fn drop(&mut self) {
        self.meter.cancel_reserved_codec(self.encoded, self.copied);
    }
}
struct Resources {
    texts: Guarded<Vec<TextCredits>>,
    codec: Option<CodecCredit>,
}

impl Exchange {
    pub(super) fn keep_metadata(
        &self,
        header: &mut wire::PutObjectsHeader,
        meter: Option<&std::sync::Arc<dyn NativeReadMeter>>,
    ) -> Result<(), NativeObjectsError> {
        let mut metadata = self
            .metadata
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !metadata.value.is_empty() || header.objects.len() != self.sizes.value.len() {
            return Err(invalid());
        }
        for object in &mut header.objects {
            let info = wire::ObjectInfo {
                metadata: object.metadata.take(),
                ..Default::default()
            };
            if let Some(meter) = meter {
                let mut credit = ReadAllocation::adopt(meter, info_capacity(&info)?)?;
                metadata
                    ._allocation
                    .as_mut()
                    .ok_or_else(invalid)?
                    .absorb(&mut credit)?;
            }
            metadata.value.push(info.metadata);
        }
        Ok(())
    }
}

pub(super) type Received = Arc<Guarded<Exchange>>;

pub(super) fn received(
    meter: Option<&std::sync::Arc<dyn NativeReadMeter>>,
    sizes: Guarded<Vec<u64>>,
    completed: Arc<Completion>,
) -> Result<Received, Error> {
    let native_count = if meter.is_some() {
        sizes.value.len()
    } else {
        0
    };
    let metadata = batch_vector(native_count, meter)?;
    let mut texts = batch_vector(native_count, meter)?;
    let codec = if let Some(meter) = meter {
        meter
            .reserve_codec(REPLY_BYTES, REPLY_BYTES)
            .map_err(|_| protocol())?;
        Some(CodecCredit {
            meter: meter.clone(),
            encoded: REPLY_BYTES,
            copied: REPLY_BYTES,
            failure: None,
        })
    } else {
        None
    };
    for _ in 0..native_count {
        let mut credits: TextCredits = std::array::from_fn(|_| None);
        if let Some(meter) = meter {
            for credit in &mut credits {
                *credit = Some(
                    ReadAllocationReservation::new(meter, TEXT_BYTES as u64)
                        .map_err(|_| protocol())?,
                );
            }
        }
        texts.value.push(credits);
    }
    let allocation = meter
        .map(|meter| ReadAllocation::new(meter, std::mem::size_of::<Guarded<Exchange>>()))
        .transpose()
        .map_err(|_| protocol())?;
    Ok(Arc::new(Guarded {
        value: Exchange {
            reply: Mutex::new(None),
            sizes,
            completed,
            metadata: Mutex::new(metadata),
            resources: Mutex::new(Resources { texts, codec }),
        },
        _allocation: allocation,
    }))
}

pub(super) struct Reply {
    pub(super) response: Guarded<wire::PutObjectsResponse>,
    pub(super) failure: Option<NativeObjectsError>,
}

impl Reply {
    pub(super) fn record_write(&mut self, meter: &dyn NativeReadMeter) {
        let completed = self
            .response
            .value
            .results
            .iter()
            .filter_map(|result| match &result.result {
                Some(wire::put_object_result::Result::Object(object)) => {
                    Some(usize::try_from(object.size).unwrap_or(usize::MAX))
                }
                _ => None,
            })
            .fold(0_usize, usize::saturating_add);
        let recorded = meter.record_write(completed);
        if self.failure.is_none() {
            self.failure = recorded.err().map(Into::into);
        }
    }

    pub(super) fn validate(&mut self, sizes: &[u64], completion: Result<(), Error>) {
        let mut failure = completion.err();
        if self.response.value.results.len() != sizes.len() {
            failure.get_or_insert_with(protocol);
        }
        for (result, size) in self.response.value.results.iter_mut().zip(sizes) {
            let valid = match &result.result {
                Some(wire::put_object_result::Result::Object(object)) => {
                    info(object).is_ok() && object.size == *size
                }
                Some(wire::put_object_result::Result::Error(error)) => item_error(error).is_ok(),
                None => false,
            };
            if !valid {
                let retained = match &result.result {
                    Some(wire::put_object_result::Result::Object(object)) => {
                        info_capacity(object).unwrap_or(0)
                    }
                    Some(wire::put_object_result::Result::Error(error)) => {
                        error.request_id.capacity()
                    }
                    None => 0,
                };
                result.result = None;
                if let Some(allocation) = self.response._allocation.as_mut() {
                    drop(allocation.split(retained));
                }
                failure.get_or_insert_with(protocol);
            }
        }
        if self.failure.is_none() {
            self.failure = failure.map(Into::into);
        }
    }
}

pub(super) struct Decoder {
    pub(super) prepared: Option<Reply>,
    pub(super) expected: usize,
    pub(super) meter: Option<std::sync::Arc<dyn NativeReadMeter>>,
    pub(super) received: Received,
}

fn invalid() -> NativeObjectsError {
    io::Error::from(io::ErrorKind::InvalidData).into()
}

fn message<'a>(kind: WireType, input: &mut &'a [u8]) -> Result<&'a [u8], NativeObjectsError> {
    if kind != WireType::LengthDelimited {
        return Err(invalid());
    }
    let length =
        usize::try_from(decode_varint(input).map_err(|_| invalid())?).map_err(|_| invalid())?;
    if length > input.len() {
        return Err(invalid());
    }
    let (value, rest) = input.split_at(length);
    *input = rest;
    Ok(value)
}

fn fields<'a>(
    mut input: &'a [u8],
    mut visit: impl FnMut(u32, WireType, &mut &'a [u8]) -> Result<(), NativeObjectsError>,
) -> Result<(), NativeObjectsError> {
    while !input.is_empty() {
        let (tag, kind) = decode_key(&mut input).map_err(|_| invalid())?;
        visit(tag, kind, &mut input)?;
    }
    Ok(())
}

fn unknown(tag: u32, kind: WireType, input: &mut &[u8]) -> Result<(), NativeObjectsError> {
    skip_field(kind, tag, input, DecodeContext::default()).map_err(|_| invalid())
}

fn text<'a>(kind: WireType, input: &mut &'a [u8]) -> Result<&'a str, NativeObjectsError> {
    std::str::from_utf8(message(kind, input)?).map_err(|_| invalid())
}

fn verify_user_map(
    expected: Option<&wire::ObjectMetadata>,
    bytes: &[u8],
    first: usize,
) -> Result<bool, NativeObjectsError> {
    const MAX: usize = wire::ObjectsLimit::MaxUserMetadataBytes as usize;
    let mut keys = [""; MAX];
    let count = expected.map_or(0, |metadata| metadata.user.len());
    if count > MAX {
        return Err(invalid());
    }
    if let Some(metadata) = expected {
        for (slot, key) in keys.iter_mut().zip(metadata.user.keys()) {
            *slot = key;
        }
    }
    let mut matched = [false; MAX];
    let mut present = false;
    let mut index = 0;
    fields(bytes, |tag, kind, input| {
        if !matches!(tag, 1 | 2) {
            return unknown(tag, kind, input);
        }
        let object = message(kind, input)?;
        index += 1;
        if index < first {
            return Ok(());
        }
        fields(object, |tag, kind, input| {
            if tag != 3 {
                return unknown(tag, kind, input);
            }
            present = true;
            fields(message(kind, input)?, |tag, kind, input| {
                if tag != 2 {
                    return unknown(tag, kind, input);
                }
                let (mut key, mut value) = ("", "");
                fields(message(kind, input)?, |tag, kind, input| match tag {
                    1 => {
                        key = text(kind, input)?;
                        Ok(())
                    }
                    2 => {
                        value = text(kind, input)?;
                        Ok(())
                    }
                    _ => unknown(tag, kind, input),
                })?;
                let index = keys
                    .get(..count)
                    .ok_or_else(invalid)?
                    .binary_search(&key)
                    .map_err(|_| invalid())?;
                *matched.get_mut(index).ok_or_else(invalid)? = expected
                    .and_then(|metadata| metadata.user.get(key))
                    .is_some_and(|expected| expected == value);
                Ok(())
            })
        })
    })?;
    if present && matched.iter().take(count).any(|matched| !matched) {
        return Err(invalid());
    }
    Ok(present)
}

struct Decode<'a> {
    meter: Option<&'a std::sync::Arc<dyn NativeReadMeter>>,
    allocation: Option<&'a mut ReadAllocation>,
    credits: std::array::IntoIter<Option<ReadAllocationReservation>, TEXT_FIELDS>,
    codec: Option<&'a mut CodecCredit>,
}

impl Decode<'_> {
    fn text(&mut self, target: &mut String, text: &str) -> Result<(), NativeObjectsError> {
        if let Some(codec) = self.codec.as_mut()
            && (text.len() > TEXT_BYTES || text.len() as u64 > codec.copied)
        {
            return Err(invalid());
        }
        if target.capacity() < text.len() {
            let previous = target.capacity();
            let capacity = text.len();
            let mut credit = if self.meter.is_some() {
                let mut credit = self
                    .credits
                    .next()
                    .flatten()
                    .ok_or_else(invalid)?
                    .allocate()?;
                credit.resize_retained(capacity - previous)?;
                Some(credit)
            } else {
                None
            };
            target
                .try_reserve_exact(capacity.saturating_sub(target.len()))
                .map_err(|error| {
                    self.meter.map_or_else(
                        || io::Error::from(io::ErrorKind::OutOfMemory),
                        |meter| meter.allocation_failed(error),
                    )
                })?;
            if let Some(credit) = credit.as_mut() {
                credit.reconcile_capacity(target.capacity() - previous)?;
            }
            if let (Some(owner), Some(credit)) = (self.allocation.as_mut(), credit.as_mut()) {
                owner.absorb(credit)?;
            }
        }
        target.clear();
        target.push_str(text);
        if let Some(codec) = self.codec.as_mut() {
            codec.record(0, text.len() as u64)?;
        }
        Ok(())
    }

    fn metadata<'a>(
        &mut self,
        target: &mut wire::ObjectMetadata,
        bytes: &'a [u8],
        texts: &mut [Option<&'a str>; 5],
    ) -> Result<(), NativeObjectsError> {
        fields(bytes, |tag, kind, input| {
            let slot = match tag {
                1 => texts.get_mut(0),
                3..=6 => texts.get_mut(tag as usize - 2),
                _ => None,
            };
            if let Some(slot) = slot {
                *slot = Some(text(kind, input)?);
                return Ok(());
            }
            if tag == 2 {
                let mut key = "";
                let mut value = "";
                fields(message(kind, input)?, |tag, kind, input| match tag {
                    1 => {
                        key = text(kind, input)?;
                        Ok(())
                    }
                    2 => {
                        value = text(kind, input)?;
                        Ok(())
                    }
                    _ => unknown(tag, kind, input),
                })?;
                if self.meter.is_none() {
                    target.user.insert(key.into(), value.into());
                }
                Ok(())
            } else {
                target
                    .merge_field(tag, kind, input, DecodeContext::default())
                    .map_err(|_| invalid())
            }
        })
    }

    fn object<'a>(
        &mut self,
        object: &mut wire::ObjectInfo,
        bytes: &'a [u8],
        etag: &mut &'a str,
        texts: &mut [Option<&'a str>; 5],
    ) -> Result<(), NativeObjectsError> {
        // Protobuf scalar fields use the last value. Borrow through the whole
        // message (including merged metadata) before making one exact copy.
        fields(bytes, |tag, kind, input| match tag {
            1 => {
                *etag = text(kind, input)?;
                Ok(())
            }
            3 => self.metadata(
                object.metadata.get_or_insert_default(),
                message(kind, input)?,
                texts,
            ),
            _ => object
                .merge_field(tag, kind, input, DecodeContext::default())
                .map_err(|_| invalid()),
        })
    }

    fn result(
        &mut self,
        bytes: &[u8],
        metadata: Option<wire::ObjectMetadata>,
    ) -> Result<wire::PutObjectResult, NativeObjectsError> {
        let mut result = wire::PutObjectResult::default();
        let (mut etag, mut request_id) = ("", "");
        let mut texts = [None; 5];
        let (mut index, mut first) = (0, 0);
        fields(bytes, |tag, kind, input| {
            use wire::put_object_result::Result;
            if !matches!(tag, 1 | 2) {
                return unknown(tag, kind, input);
            }
            index += 1;
            let same = matches!(
                (tag, &result.result),
                (1, Some(Result::Object(_))) | (2, Some(Result::Error(_)))
            );
            if !same {
                first = index;
                etag = "";
                request_id = "";
                texts = [None; 5];
                result.result = Some(if tag == 1 {
                    Result::Object(Default::default())
                } else {
                    Result::Error(Default::default())
                });
            }
            match result.result.as_mut().ok_or_else(invalid)? {
                Result::Object(object) => {
                    self.object(object, message(kind, input)?, &mut etag, &mut texts)?;
                }
                Result::Error(error) => {
                    fields(message(kind, input)?, |tag, kind, input| match tag {
                        2 => {
                            request_id = text(kind, input)?;
                            Ok(())
                        }
                        _ => error
                            .merge_field(tag, kind, input, DecodeContext::default())
                            .map_err(|_| invalid()),
                    })?;
                }
            }
            Ok(())
        })?;
        match result.result.as_mut() {
            Some(wire::put_object_result::Result::Object(object)) => {
                if self.meter.is_some() {
                    verify_user_map(metadata.as_ref(), bytes, first)?;
                    if let Some(target) = object.metadata.as_mut() {
                        target.user = metadata.map(|metadata| metadata.user).unwrap_or_default();
                    }
                }
                self.text(&mut object.etag, etag)?;
                if let Some(metadata) = object.metadata.as_mut() {
                    for (target, value) in [
                        &mut metadata.content_type,
                        &mut metadata.content_encoding,
                        &mut metadata.cache_control,
                        &mut metadata.content_disposition,
                        &mut metadata.content_language,
                    ]
                    .into_iter()
                    .zip(texts)
                    {
                        if let Some(value) = value {
                            self.text(target, value)?;
                        }
                    }
                }
            }
            Some(wire::put_object_result::Result::Error(error)) => {
                self.text(&mut error.request_id, request_id)?;
            }
            None => {}
        }
        Ok(result)
    }
}

impl Decoder {
    fn accept(&self, mut reply: Reply) {
        reply.validate(
            &self.received.value.sizes.value,
            self.received.value.completed.validate(),
        );
        // Account before Tonic can wait for trailers or the caller can drop its future.
        if let Some(meter) = self.meter.as_ref() {
            reply.record_write(meter.as_ref());
        }
        *self
            .received
            .value
            .reply
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(reply);
    }

    #[allow(
        clippy::too_many_lines,
        reason = "One pass keeps reply parsing, metering and validation in a single audited order."
    )]
    pub(super) fn bytes(&mut self, bytes: &[u8]) -> Result<Reply, tonic::Status> {
        let mut reply = self
            .prepared
            .take()
            .ok_or_else(|| tonic::Status::internal("duplicate batch reply"))?;
        let mut metadata = self
            .received
            .value
            .metadata
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let mut resources = self
            .received
            .value
            .resources
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let decoded = (|| {
            if let Some(codec) = resources.codec.as_mut() {
                codec.record(bytes.len() as u64, 0)?;
            }
            fields(bytes, |tag, kind, input| {
                if tag == 1 {
                    if reply.response.value.results.len() == self.expected {
                        return Err(invalid());
                    }
                    let index = reply.response.value.results.len();
                    let mut item_credit = self.meter.as_ref().map(ReadAllocation::empty);
                    let metadata = if self.meter.is_some() {
                        let index = reply.response.value.results.len();
                        let value = metadata.value.get_mut(index).ok_or_else(invalid)?.take();
                        let info = wire::ObjectInfo {
                            metadata: value,
                            ..Default::default()
                        };
                        let mut credit = metadata
                            ._allocation
                            .as_mut()
                            .ok_or_else(invalid)?
                            .split(info_capacity(&info)?)?;
                        item_credit
                            .as_mut()
                            .ok_or_else(invalid)?
                            .absorb(&mut credit)?;
                        info.metadata
                    } else {
                        None
                    };
                    let item = message(kind, input)?;
                    let credits = resources.texts.value.get_mut(index).map_or_else(
                        || std::array::from_fn(|_| None),
                        |credits| std::mem::replace(credits, std::array::from_fn(|_| None)),
                    );
                    let mut decode = Decode {
                        meter: self.meter.as_ref(),
                        allocation: item_credit.as_mut(),
                        credits: credits.into_iter(),
                        codec: resources.codec.as_mut(),
                    };
                    let decoded = decode.result(item, metadata).and_then(|result| {
                        match &result.result {
                            Some(wire::put_object_result::Result::Object(object)) => {
                                info(object).map_err(NativeObjectsError::from)?;
                            }
                            Some(wire::put_object_result::Result::Error(error)) => {
                                item_error(error).map_err(NativeObjectsError::from)?;
                            }
                            None => return Err(invalid()),
                        }
                        Ok(result)
                    });
                    drop(decode);
                    match decoded {
                        Ok(result) => {
                            if let Some(credit) = item_credit.as_mut() {
                                let retained = match &result.result {
                                    Some(wire::put_object_result::Result::Object(object)) => {
                                        info_capacity(object)?
                                    }
                                    Some(wire::put_object_result::Result::Error(error)) => {
                                        error.request_id.capacity()
                                    }
                                    None => 0,
                                };
                                credit.resize_retained(retained)?;
                            }
                            if let (Some(owner), Some(credit)) =
                                (reply.response._allocation.as_mut(), item_credit.as_mut())
                            {
                                owner.absorb(credit)?;
                            }
                            reply.response.value.results.push(result);
                        }
                        Err(error) => {
                            reply.failure.get_or_insert(error);
                            reply
                                .response
                                .value
                                .results
                                .push(wire::PutObjectResult::default());
                        }
                    }
                    Ok(())
                } else {
                    unknown(tag, kind, input)
                }
            })
        })();
        reply.failure = reply
            .failure
            .take()
            .or_else(|| decoded.err())
            .or_else(|| (reply.response.value.results.len() != self.expected).then(invalid));
        if let Some(codec) = resources.codec.as_mut() {
            let recorded = codec.failure.take();
            if reply.failure.is_none() {
                reply.failure = recorded.map(Into::into);
            }
        }
        Ok(reply)
    }
}

impl tonic::codec::Decoder for Decoder {
    type Item = ();
    type Error = tonic::Status;
    fn decode(
        &mut self,
        buffer: &mut tonic::codec::DecodeBuf<'_>,
    ) -> Result<Option<()>, tonic::Status> {
        let length = buffer.remaining();
        if buffer.chunk().len() != length {
            return Err(tonic::Status::internal("fragmented batch envelope"));
        }
        let reply = self.bytes(buffer.chunk())?;
        self.accept(reply);
        buffer.advance(length);
        Ok(Some(()))
    }
}

#[cfg(all(test, objects_loom))]
mod model;

#[cfg(feature = "test-support")]
mod fuzz;
#[cfg(feature = "test-support")]
pub use fuzz::fuzz_reply;
