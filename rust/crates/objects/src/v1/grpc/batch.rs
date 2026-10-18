use super::super::Guarded;
use super::*;
use crate::sync::{Arc, AtomicBool, Ordering};

mod reply;
struct PutCodec {
    prepared: Option<reply::Reply>,
    expected: usize,
    meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    received: reply::Received,
}
struct PutEncoder {
    received: reply::Received,
    meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
}

impl tonic::codec::Encoder for PutEncoder {
    type Item = Guarded<wire::PutObjectsRequest>;
    type Error = tonic::Status;

    fn encode(
        &mut self,
        mut item: Self::Item,
        buffer: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        item.value
            .encode(buffer)
            .map_err(|_| tonic::Status::internal("batch encoding"))?;
        if let (Some(meter), Some(wire::put_objects_request::Frame::Header(header))) =
            (&self.meter, &mut item.value.frame)
        {
            self.received
                .value
                .keep_metadata(header, Some(meter))
                .map_err(|_| tonic::Status::resource_exhausted("batch metadata custody"))?;
        }
        Ok(())
    }
}

impl tonic::codec::Codec for PutCodec {
    type Encode = Guarded<wire::PutObjectsRequest>;
    type Decode = ();
    type Encoder = PutEncoder;
    type Decoder = reply::Decoder;

    fn encoder(&mut self) -> Self::Encoder {
        PutEncoder {
            received: self.received.clone(),
            meter: self.meter.clone(),
        }
    }
    fn decoder(&mut self) -> Self::Decoder {
        reply::Decoder {
            prepared: self.prepared.take(),
            expected: self.expected,
            meter: self.meter.clone(),
            received: self.received.clone(),
        }
    }
}

#[derive(Default)]
struct Completion {
    sent: AtomicBool,
    failed: AtomicBool,
    _allocation: Option<acyclic_native_runtime::ReadAllocation>,
}

impl Completion {
    fn new(
        meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    ) -> Result<Arc<Self>, Error> {
        let allocation = meter
            .map(|meter| {
                acyclic_native_runtime::ReadAllocation::new(meter, std::mem::size_of::<Self>())
            })
            .transpose()
            .map_err(|_| protocol())?;
        Ok(Arc::new(Self {
            _allocation: allocation,
            ..Self::default()
        }))
    }

    fn sent(&self) {
        self.sent.store(true, Ordering::Release);
    }

    fn validate(&self) -> Result<(), Error> {
        if self.sent.load(Ordering::Acquire) && !self.failed.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(protocol())
        }
    }
}

fn accepted_upload<S, C>(
    frames: S,
    completion: Arc<Completion>,
    mut check: C,
) -> impl futures::Stream<Item = S::Item>
where
    S: futures::Stream<Item = Guarded<wire::PutObjectsRequest>>,
    C: FnMut() -> bool,
{
    frames.take_while(move |frame| {
        let accepted = check();
        if !accepted {
            completion.failed.store(true, Ordering::Release);
        } else if matches!(
            frame.value.frame,
            Some(wire::put_objects_request::Frame::Complete(true))
        ) {
            completion.sent();
        }
        futures::future::ready(accepted)
    })
}

fn item_error(detail: &wire::ErrorDetail) -> Result<Error, Error> {
    wire::ErrorCode::try_from(detail.code)
        .ok()
        .filter(|code| *code != wire::ErrorCode::Unspecified)
        .map(Error::from)
        .ok_or_else(protocol)
}

fn put_frames(
    bodies: Vec<OwnedBytes>,
    header: wire::PutObjectsHeader,
    completion: Arc<Completion>,
    meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> impl futures::Stream<Item = Guarded<wire::PutObjectsRequest>> + Send + 'static {
    let source = completion.clone();
    let admission = meter.clone();
    let items = stream::iter(bodies.into_iter().zip(0_u32..)).flat_map(move |(body, index)| {
        let meter = meter.clone();
        let source = source.clone();
        stream::unfold((Some(body), false), move |(pending, complete)| {
            let meter = meter.clone();
            let source = source.clone();
            async move {
                use wire::put_objects_chunk::Frame;
                let frame = if let Some(body) = pending.filter(|body| !body.is_empty()) {
                    let size = body.len().min(65_536);
                    let (chunk, remaining) = if size == body.len() {
                        (body, None)
                    } else {
                        (body.slice(..size), Some(body.slice(size..)))
                    };
                    let body = if let Some(meter) = meter.as_ref() {
                        let Ok(body) = upload::native_body_bytes(chunk, meter) else {
                            source.failed.store(true, Ordering::Release);
                            return None;
                        };
                        body
                    } else {
                        Guarded {
                            value: upload::body_bytes(chunk),
                            _allocation: None,
                        }
                    };
                    Some((
                        Frame::Body(body.value),
                        (remaining, false),
                        body._allocation,
                    ))
                } else if !complete {
                    Some((Frame::Complete(true), (None, true), None))
                } else {
                    None
                };
                frame.map(|(frame, next, allocation)| {
                    (
                        Guarded {
                            value: wire::PutObjectsRequest {
                                frame: Some(wire::put_objects_request::Frame::Chunk(
                                    wire::PutObjectsChunk {
                                        index,
                                        frame: Some(frame),
                                    },
                                )),
                            },
                            _allocation: allocation,
                        },
                        next,
                    )
                })
            }
        })
    });
    let frames = stream::once(async move {
        Guarded {
            value: wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Header(header)),
            },
            _allocation: None,
        }
    })
    .chain(items)
    .chain(stream::once(async move {
        Guarded {
            value: wire::PutObjectsRequest {
                frame: Some(wire::put_objects_request::Frame::Complete(true)),
            },
            _allocation: None,
        }
    }));
    accepted_upload(frames, completion, move || {
        admission.as_ref().is_none_or(|meter| meter.check().is_ok())
    })
}

impl GrpcObjects {
    pub(super) async fn put_objects(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
    ) -> Vec<Result<wire::ObjectInfo, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        match self.put_objects_with_custody(requests, (), None).await {
            Ok(mut reply) => {
                let mut results: Vec<_> = reply
                    .response
                    .value
                    .results
                    .drain(..)
                    .map(|result| match result.result {
                        Some(wire::put_object_result::Result::Object(object)) => Ok(object),
                        Some(wire::put_object_result::Result::Error(error)) => {
                            item_error(&error).and_then(Err)
                        }
                        None => Err(protocol()),
                    })
                    .collect();
                results.resize_with(count, || Err(protocol()));
                results
            }
            Err(error) => vec![Err(error); count],
        }
    }

    async fn put_objects_with_custody<G: Send + Sync + 'static>(
        &self,
        requests: Vec<(wire::PutObjectHeader, OwnedBytes)>,
        custody: G,
        meter: Option<std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
    ) -> Result<reply::Reply, Error> {
        let count = requests.len();
        let mut sizes = batch_vector(count, meter.as_ref())?;
        sizes
            .value
            .extend(requests.iter().map(|(_, body)| body.len() as u64));
        let completed = Completion::new(meter.as_ref())?;
        let received = reply::received(meter.as_ref(), sizes, completed.clone())?;
        let result = async {
            let mut objects = batch_vector(count, meter.as_ref())?;
            let mut bodies = batch_vector(count, meter.as_ref())?;
            let mut requests = requests;
            for (header, body) in requests.drain(..) {
                objects.value.push(header);
                bodies.value.push(body);
            }
            let header = wire::PutObjectsHeader {
                objects: std::mem::take(&mut objects.value),
            };
            request::put_batch(&header)?;
            for size in &received.value.sizes.value {
                request::upload_length(*size)?;
            }
            let response = batch_vector(count, meter.as_ref())?;
            let codec = PutCodec {
                expected: count,
                meter: meter.clone(),
                received: received.clone(),
                prepared: Some(reply::Reply {
                    response: Guarded {
                        value: wire::PutObjectsResponse {
                            results: response.value,
                        },
                        _allocation: response._allocation,
                    },
                    failure: None,
                }),
            };
            let input_storage = meter.as_ref().map(|_| requests);
            let body_frames = put_frames(
                std::mem::take(&mut bodies.value),
                header,
                completed.clone(),
                meter.clone(),
            );
            let storage = (input_storage, objects, bodies, custody);
            let frames = body_frames.inspect(move |_| {
                let _ = &storage;
            });
            let input = self.authenticated(frames, self.transfer_timeout).await?;
            let (mut client, path) = self
                .streaming("/acyclic.objects.v1.ObjectsService/PutObjects")
                .await?;
            client
                .client_streaming(input, path, codec)
                .await
                .map(|_| ())
                .map_err(|status| error_from_status(&status))
        };
        let outcome = tokio::time::timeout(self.transfer_timeout, result)
            .await
            .unwrap_or_else(|_| Err(wire::ErrorCode::Unavailable.into()));
        if let Some(mut reply) = received
            .value
            .reply
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            if reply.failure.is_none() {
                reply.failure = outcome.err().map(Into::into);
            }
            Ok(reply)
        } else {
            Err(outcome.err().unwrap_or_else(protocol))
        }
    }
    #[allow(
        clippy::too_many_lines,
        reason = "Native admission, custody and per-item result draining stay in one ordered path."
    )]
    pub(super) fn put_objects_native(
        &self,
        mut input: super::super::ObjectsNativeRequest<Vec<(wire::PutObjectHeader, OwnedBytes)>>,
        meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<
        super::super::ObjectsNativeFuture<
            '_,
            super::super::ObjectsBatch<super::super::ObjectsPutReceipt>,
        >,
        super::super::NativeObjectsError,
    > {
        use super::super::{NativeObjectsError, ObjectsBatch, ObjectsPutReceipt};
        use acyclic_native_runtime::ReadAllocation;
        super::super::native_input_meter(&meter, &input)?;
        super::super::native_future(&meter.clone(), async move {
            let count = input.data().len();
            let bytes = count
                .checked_mul(std::mem::size_of::<
                    Result<ObjectsPutReceipt, NativeObjectsError>,
                >())
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::OutOfMemory))?;
            let mut allocation = ReadAllocation::new(&meter, bytes)?;
            let mut results = Vec::new();
            results
                .try_reserve_exact(count)
                .map_err(|error| meter.allocation_failed(error))?;
            allocation.reconcile_capacity(
                results.capacity()
                    * std::mem::size_of::<Result<ObjectsPutReceipt, NativeObjectsError>>(),
            )?;
            if count == 0 {
                return ObjectsBatch::new(results, allocation);
            }
            let maximum = input.data().iter().try_fold(0usize, |total, (_, body)| {
                total
                    .checked_add(body.len())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidInput))
            })?;
            let mut slots = batch_vector(count, Some(&meter)).map_err(NativeObjectsError::from)?;
            for _ in 0..count {
                let control =
                    ReadAllocation::new(&meter, std::mem::size_of::<ObjectsPutReceipt>())?;
                slots.value.push(Box::new(ObjectsPutReceipt::new(
                    wire::ObjectInfo::default(),
                    Some(control),
                )));
            }
            let mut failure = super::super::NativeObjectsSharedFailure::prepare(&meter)?;
            let failure_slot = std::sync::Arc::get_mut(&mut failure)
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
            meter.admit_write(maximum)?;
            let requests = std::mem::take(&mut input.data);
            match self
                .put_objects_with_custody(requests, input, Some(meter.clone()))
                .await
            {
                Err(error) => {
                    results.extend((0..count).map(|_| Err(NativeObjectsError::from(error))));
                }
                Ok(mut reply) => {
                    failure_slot.value = reply.failure.take();
                    let mut values = reply.response.value.results.drain(..);
                    for mut slot in slots.value.drain(..) {
                        let value = values.next();
                        results.push(match value.and_then(|result| result.result) {
                            Some(wire::put_object_result::Result::Object(info)) => {
                                let retained = info_capacity(&info);
                                slot.value = info;
                                let claim = retained
                                    .and_then(|bytes| {
                                        reply
                                            .response
                                            ._allocation
                                            .as_mut()
                                            .ok_or_else(|| {
                                                std::io::Error::from(
                                                    std::io::ErrorKind::InvalidData,
                                                )
                                            })?
                                            .split(bytes)
                                    })
                                    .and_then(|mut claim| {
                                        slot._allocation
                                            .as_mut()
                                            .ok_or_else(|| {
                                                std::io::Error::from(
                                                    std::io::ErrorKind::InvalidData,
                                                )
                                            })?
                                            .absorb(&mut claim)
                                    });
                                if let Err(error) = claim {
                                    let mut error = NativeObjectsError::from(error);
                                    error.receipt = Some(slot);
                                    Err(error)
                                } else if failure.value.is_some() {
                                    let mut error = NativeObjectsError::from(
                                        super::super::NativeObjectsCause::SharedFailure(
                                            failure.clone(),
                                        ),
                                    );
                                    error.receipt = Some(slot);
                                    Err(error)
                                } else {
                                    match slot
                                        ._allocation
                                        .as_mut()
                                        .ok_or_else(|| {
                                            std::io::Error::from(std::io::ErrorKind::InvalidData)
                                        })
                                        .and_then(|allocation| {
                                            allocation
                                                .split(std::mem::size_of::<ObjectsPutReceipt>())
                                        }) {
                                        Ok(control) => {
                                            let receipt = *slot;
                                            drop(control);
                                            Ok(receipt)
                                        }
                                        Err(error) => {
                                            let mut error = NativeObjectsError::from(error);
                                            error.receipt = Some(slot);
                                            Err(error)
                                        }
                                    }
                                }
                            }
                            Some(wire::put_object_result::Result::Error(error)) => {
                                Err(item_error(&error).unwrap_or_else(|error| error).into())
                            }
                            None => Err(if failure.value.is_some() {
                                super::super::NativeObjectsCause::SharedFailure(failure.clone())
                                    .into()
                            } else {
                                NativeObjectsError::from(protocol())
                            }),
                        });
                    }
                }
            }
            ObjectsBatch::new(results, allocation)
        })
    }

    pub(super) async fn get_objects(
        &self,
        requests: Vec<(wire::GetObjectRequest, u64)>,
    ) -> Vec<Result<Object, Error>> {
        let count = requests.len();
        if count == 0 {
            return Vec::new();
        }
        let result = async {
            let query = wire::GetObjectsRequest {
                objects: requests
                    .iter()
                    .map(|(request, maximum)| wire::GetObjectSelection {
                        request: Some(request.clone()),
                        maximum_bytes: *maximum,
                    })
                    .collect(),
            };
            request::get_batch(&query)?;
            let input = self.authenticated(query, self.transfer_timeout).await?;
            let (mut client, path) = self
                .streaming("/acyclic.objects.v1.ObjectsService/GetObjects")
                .await?;
            let mut frames: tonic::Streaming<wire::GetObjectsResponse> = client
                .server_streaming(input, path, tonic_prost::ProstCodec::default())
                .await
                .map(ok)
                .map_err(|status| error_from_status(&status))?;
            let mut results = Vec::with_capacity(count);
            for (index, (query, maximum)) in requests.into_iter().enumerate() {
                let mut header = None;
                let mut remaining = 0;
                let mut body = bytes::BytesMut::new();
                loop {
                    use wire::get_objects_response::Frame;
                    let frame = frames
                        .message()
                        .await
                        .map_err(|status| error_from_status(&status))?
                        .ok_or_else(protocol)?;
                    if usize::try_from(frame.index).ok() != Some(index) {
                        return Err(protocol());
                    }
                    match frame.frame {
                        Some(Frame::Header(value)) if header.is_none() => {
                            remaining = response::get_header(&value, &query.range, maximum)?;
                            header = Some(value);
                        }
                        Some(Frame::Body(value)) if header.is_some() => {
                            let chunk = response::body(value, remaining)?;
                            remaining -= chunk.len() as u64;
                            body.extend_from_slice(&chunk);
                        }
                        Some(Frame::Complete(true)) if header.is_some() && remaining == 0 => {
                            results.push(Ok(Object::new(
                                header.take().ok_or_else(protocol)?,
                                body.freeze().into(),
                            )));
                            break;
                        }
                        Some(Frame::Error(error)) => {
                            results.push(Err(item_error(&error)?));
                            break;
                        }
                        _ => return Err(protocol()),
                    }
                }
            }
            if frames
                .message()
                .await
                .map_err(|status| error_from_status(&status))?
                .is_some()
            {
                return Err(protocol());
            }
            Ok(results)
        };
        let error = match tokio::time::timeout(self.transfer_timeout, result).await {
            Ok(Ok(results)) => return results,
            Ok(Err(error)) => error,
            Err(_) => wire::ErrorCode::Unavailable.into(),
        };
        (0..count).map(|_| Err(error)).collect()
    }
}

fn batch_vector<T>(
    count: usize,
    meter: Option<&std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>>,
) -> Result<super::super::Guarded<Vec<T>>, Error> {
    let bytes = count
        .checked_mul(std::mem::size_of::<T>())
        .ok_or_else(protocol)?;
    let mut allocation = meter
        .map(|meter| acyclic_native_runtime::ReadAllocation::new(meter, bytes))
        .transpose()
        .map_err(|_| protocol())?;
    let mut value = Vec::new();
    value.try_reserve_exact(count).map_err(|error| {
        if let Some(meter) = meter {
            let _ = meter.allocation_failed(error);
        }
        protocol()
    })?;
    if let Some(allocation) = allocation.as_mut() {
        allocation
            .reconcile_capacity(value.capacity() * std::mem::size_of::<T>())
            .map_err(|_| protocol())?;
    }
    Ok(super::super::Guarded {
        value,
        _allocation: allocation,
    })
}

fn info_capacity(info: &wire::ObjectInfo) -> Result<usize, std::io::Error> {
    let mut bytes = info.etag.capacity();
    if let Some(metadata) = &info.metadata {
        for value in [
            &metadata.content_type,
            &metadata.content_encoding,
            &metadata.cache_control,
            &metadata.content_disposition,
            &metadata.content_language,
        ] {
            bytes = bytes
                .checked_add(value.capacity())
                .ok_or(std::io::ErrorKind::OutOfMemory)?;
        }
        for (key, value) in &metadata.user {
            bytes = bytes
                .checked_add(std::mem::size_of::<(String, String)>())
                .and_then(|bytes| bytes.checked_add(key.capacity()))
                .and_then(|bytes| bytes.checked_add(value.capacity()))
                .ok_or(std::io::ErrorKind::OutOfMemory)?;
        }
    }
    Ok(bytes)
}

#[cfg(all(test, not(objects_loom)))]
#[allow(
    clippy::indexing_slicing,
    reason = "Tests index fixtures whose shape they construct."
)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    fn headers(count: usize) -> wire::PutObjectsHeader {
        wire::PutObjectsHeader {
            objects: (0..count)
                .map(|index| wire::PutObjectHeader {
                    bucket: Some(wire::BucketRef {
                        name: "batch".into(),
                    }),
                    object_key: format!("key-{index}"),
                    ..Default::default()
                })
                .collect(),
        }
    }

    #[cfg(feature = "local")]
    #[test]
    fn batch_completion_custody_retires_with_the_last_alias() {
        use acyclic_native_runtime::{NativeReadMeter, ReadAllocation};
        let counts = std::sync::Arc::new(crate::physical::read_tests::CountingMeter::default());
        let meter: std::sync::Arc<dyn NativeReadMeter> = counts.clone();
        let bytes = std::mem::size_of::<Completion>();
        let completion = Completion::new(Some(&meter)).unwrap();
        let transport = completion.clone();
        drop(completion);
        assert_eq!(counts.retained(), bytes as u64);
        transport.sent();
        transport.validate().unwrap();
        drop(transport);
        assert_eq!(counts.retained(), 0);

        let external_claim = ReadAllocation::new(&meter, bytes).unwrap();
        let control = Completion::new(None).unwrap();
        let transport = control.clone();
        drop(control);
        drop(external_claim);
        assert_ne!(counts.retained(), bytes as u64);
        transport.sent();
        transport.validate().unwrap();
    }

    #[tokio::test]
    async fn batch_upload_moves_unique_headers_and_chunks() {
        for length in [1, 65_536] {
            let body = vec![7; length];
            let body_pointer = body.as_ptr();
            let header = headers(1);
            let key_pointer = header.objects[0].object_key.as_ptr();
            let completion = Arc::new(Completion::default());
            let frames = put_frames(vec![body.into()], header, completion.clone(), None);
            futures::pin_mut!(frames);
            let first = frames.next().await.unwrap();
            let Some(wire::put_objects_request::Frame::Header(header)) = first.value.frame else {
                panic!("header")
            };
            assert_eq!(header.objects[0].object_key.as_ptr(), key_pointer);
            let frame = frames.next().await.unwrap();
            let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
                frame: Some(wire::put_objects_chunk::Frame::Body(body)),
                ..
            })) = frame.value.frame
            else {
                panic!("body")
            };
            assert_eq!(body.as_ptr(), body_pointer);
            assert!(completion.validate().is_err());
            while frames.next().await.is_some() {}
            completion.validate().unwrap();
        }
    }

    #[tokio::test]
    async fn dropping_upload_before_final_marker_rejects_early_success() {
        let completion = Arc::new(Completion::default());
        let mut frames = Box::pin(put_frames(
            vec![vec![1; 65_537].into()],
            headers(1),
            completion.clone(),
            None,
        ));
        frames.next().await.unwrap();
        frames.next().await.unwrap();
        assert!(completion.validate().is_err());
        drop(frames);
        assert!(completion.validate().is_err());
    }

    proptest! {
        #[test]
        fn batch_upload_round_trips_and_rejects_every_truncated_prefix(
            bodies in prop::collection::vec(prop::collection::vec(any::<u8>(), 0..1024), 1..8),
            split in any::<bool>(),
        ) {
            let header = headers(bodies.len());
            let owned: Vec<_> = bodies.iter().map(|body| {
                let body = OwnedBytes::from(body.clone());
                if split { body.slice(..) } else { body }
            }).collect();
            let frames = futures::executor::block_on(put_frames(owned, header.clone(), Arc::new(Completion::default()), None).collect::<Vec<_>>());
            let mut reconstructed = vec![Vec::new(); bodies.len()];
            for prefix in 0..frames.len() {
                let mut parser = request::BatchUploadFraming::new(&header).unwrap();
                for frame in frames.iter().skip(1).take(prefix.saturating_sub(1)) {
                    let decoded = wire::PutObjectsRequest::decode(frame.value.encode_to_vec().as_slice()).unwrap();
                    parser.accept(&decoded).unwrap();
                }
                prop_assert!(parser.finish().is_err());
            }
            let mut parser = request::BatchUploadFraming::new(&header).unwrap();
            for frame in frames.iter().skip(1) {
                let decoded = wire::PutObjectsRequest::decode(frame.value.encode_to_vec().as_slice()).unwrap();
                parser.accept(&decoded).unwrap();
                if let Some(wire::put_objects_request::Frame::Chunk(wire::PutObjectsChunk {
                    index, frame: Some(wire::put_objects_chunk::Frame::Body(body)),
                })) = decoded.frame { reconstructed[index as usize].extend(body); }
            }
            parser.finish().unwrap();
            prop_assert_eq!(reconstructed, bodies);
        }
    }
}

#[cfg(all(test, objects_loom))]
mod model {
    use super::*;

    fn history(early_completion: bool) {
        let completion = Arc::new(Completion::default());
        let accepted = Arc::new(AtomicBool::new(false));
        let cancelled = Arc::new(AtomicBool::new(false));
        let writer = {
            let (completion, accepted, cancelled) =
                (completion.clone(), accepted.clone(), cancelled.clone());
            loom::thread::spawn(move || {
                let source = stream::once(async move {
                    Guarded {
                        value: wire::PutObjectsRequest {
                            frame: Some(wire::put_objects_request::Frame::Complete(true)),
                        },
                        _allocation: None,
                    }
                });
                let control = completion.clone();
                let source = source.inspect(move |_| {
                    if early_completion {
                        control.sent();
                    }
                });
                let frames = accepted_upload(source, completion, move || {
                    let allowed = !cancelled.load(Ordering::Acquire);
                    accepted.store(allowed, Ordering::Release);
                    allowed
                });
                futures::pin_mut!(frames);
                futures::executor::block_on(frames.next());
            })
        };
        let observer = loom::thread::spawn(move || {
            cancelled.store(true, Ordering::Release);
            if completion.validate().is_ok() {
                assert!(
                    accepted.load(Ordering::Acquire),
                    "success after the final marker was filtered out"
                );
            }
        });
        writer.join().unwrap();
        observer.join().unwrap();
    }
    #[test]
    fn batch_completion_models_cancellation_and_early_replies() {
        loom::model(|| history(false));
    }

    #[test]
    fn batch_completion_rejects_the_early_success_control() {
        assert!(std::panic::catch_unwind(|| loom::model(|| history(true))).is_err());
    }
}

#[cfg(feature = "test-support")]
pub use reply::fuzz_reply;
