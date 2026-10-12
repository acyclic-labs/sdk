use super::*;

impl NativeTransportErrors {
    pub(crate) fn fail(&self, error: impl Into<crate::v1::NativeObjectsError>) -> Error {
        let error = error.into();
        let service = match &error.cause {
            crate::v1::NativeObjectsCause::Service(service) if !error.has_payload() => *service,
            _ => invalid(),
        };
        self.record(error);
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
        let mut plan =
            acyclic_native_runtime::NativeOperationReservation::new(self.meter.as_ref(), kind, 1)
                .map_err(|error| self.fail(error))?;
        let (operation, recorded) = plan.attempt(operation);
        let failed = recorded.is_err();
        if let Err(error) = recorded {
            self.record(crate::v1::NativeObjectsError::from(
                crate::v1::NativeObjectsCause::Admission(error),
            ));
        }
        Ok((operation, failed))
    }

    pub(crate) fn binary(
        &self,
        value: &impl prost::Message,
    ) -> Result<(Vec<u8>, acyclic_native_runtime::ReadAllocation), Error> {
        let maximum = value.encoded_len();
        self.meter
            .admit_codec(maximum as u64, 0, 0)
            .map_err(|error| self.fail(error))?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&self.meter, maximum)
            .map_err(|error| self.fail(error))?;
        let mut bytes = Vec::new();
        if let Err(error) = bytes.try_reserve_exact(maximum) {
            allocation
                .reconcile_capacity(bytes.capacity())
                .map_err(|error| self.fail(error))?;
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(self.meter.allocation_failed(error))
                    .with_wire(bytes, allocation),
            ));
        }
        if let Err(error) = allocation.reconcile_capacity(bytes.capacity()) {
            return Err(
                self.fail(crate::v1::NativeObjectsError::from(error).with_wire(bytes, allocation))
            );
        }
        value.encode(&mut bytes).map_err(|_| invalid())?;
        if let Err(error) = self.meter.record_codec(bytes.len() as u64, 0, 0) {
            return Err(
                self.fail(crate::v1::NativeObjectsError::from(error).with_wire(bytes, allocation))
            );
        }
        Ok((bytes, allocation))
    }

    pub(crate) fn copy(
        &self,
        bytes: &[u8],
    ) -> Result<(Vec<u8>, acyclic_native_runtime::ReadAllocation), Error> {
        self.meter
            .admit_codec(0, bytes.len() as u64, 0)
            .map_err(|error| self.fail(error))?;
        let mut allocation = acyclic_native_runtime::ReadAllocation::new(&self.meter, bytes.len())
            .map_err(|error| self.fail(error))?;
        let mut data = Vec::new();
        if let Err(error) = data.try_reserve_exact(bytes.len()) {
            allocation
                .reconcile_capacity(data.capacity())
                .map_err(|error| self.fail(error))?;
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(self.meter.allocation_failed(error))
                    .with_wire(data, allocation),
            ));
        }
        if let Err(error) = allocation.reconcile_capacity(data.capacity()) {
            return Err(
                self.fail(crate::v1::NativeObjectsError::from(error).with_wire(data, allocation))
            );
        }
        data.extend_from_slice(bytes);
        if let Err(error) = self.meter.record_codec(0, bytes.len() as u64, 0) {
            return Err(
                self.fail(crate::v1::NativeObjectsError::from(error).with_wire(data, allocation))
            );
        }
        Ok((data, allocation))
    }

    pub(crate) fn wire_bytes(
        &self,
        data: Vec<u8>,
        mut allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<bytes::Bytes, Error> {
        let mut control = match acyclic_native_runtime::ReadAllocation::new(
            &self.meter,
            std::mem::size_of::<crate::v1::Guarded<Vec<u8>>>(),
        ) {
            Ok(control) => control,
            Err(error) => {
                return Err(self
                    .fail(crate::v1::NativeObjectsError::from(error).with_wire(data, allocation)));
            }
        };
        if let Err(error) = allocation.absorb(&mut control) {
            return Err(
                self.fail(crate::v1::NativeObjectsError::from(error).with_wire(data, allocation))
            );
        }
        Ok(bytes::Bytes::from_owner(crate::v1::Guarded {
            value: data,
            _allocation: Some(allocation),
        }))
    }

    pub(crate) fn body(
        &self,
        frame: wire::Body,
        remaining: u64,
        mut allocation: acyclic_native_runtime::ReadAllocation,
    ) -> Result<bytes::Bytes, Error> {
        use acyclic_native_runtime::ReadAllocation;
        if let Err(error) = allocation.reconcile_capacity(frame.data.capacity()) {
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
            ));
        }
        let (decoded, compressed) = match body_spec(&frame, remaining) {
            Ok(spec) => spec,
            Err(error) => {
                return Err(self.fail(
                    crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
                ));
            }
        };
        if !compressed {
            return self.wire_bytes(frame.data, allocation);
        }
        let encoded = frame.data.len() as u64;
        if let Err(error) = self.meter.admit_codec(encoded, decoded as u64 + 1, 0) {
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
            ));
        }
        let mut output_claim = match ReadAllocation::new(&self.meter, decoded + 1) {
            Ok(claim) => claim,
            Err(error) => {
                return Err(self.fail(
                    crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
                ));
            }
        };
        let mut data = match zstd_output(decoded + 1) {
            Ok(output) => output,
            Err(error) => {
                return Err(self.fail(
                    crate::v1::NativeObjectsError::from(self.meter.allocation_failed(error))
                        .with_wire(frame.data, allocation),
                ));
            }
        };
        if let Err(error) = output_claim.reconcile_capacity(data.capacity()) {
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
            ));
        }
        let (valid, encoded, copied) = zstd(&frame.data, &mut data, decoded);
        if let Err(error) = self.meter.record_codec(encoded, copied as u64, 0) {
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(error).with_wire(frame.data, allocation),
            ));
        }
        if !valid {
            return Err(self.fail(
                crate::v1::NativeObjectsError::from(invalid()).with_wire(frame.data, allocation),
            ));
        }
        data.truncate(copied);
        drop(frame.data);
        allocation
            .resize_retained(0)
            .map_err(|error| self.fail(error))?;
        allocation
            .absorb(&mut output_claim)
            .map_err(|error| self.fail(error))?;
        self.wire_bytes(data, allocation)
    }
}

pub(crate) fn header_capacity(header: &wire::GetObjectHeader) -> usize {
    let Some(object) = &header.object else {
        return 0;
    };
    info_capacity(object)
}

pub(crate) fn info_capacity(object: &wire::ObjectInfo) -> usize {
    object.etag.capacity() + object.metadata.as_ref().map_or(0, metadata_capacity)
}

fn metadata_capacity(metadata: &wire::ObjectMetadata) -> usize {
    metadata.content_type.capacity()
        + metadata.content_encoding.capacity()
        + metadata.cache_control.capacity()
        + metadata.content_disposition.capacity()
        + metadata.content_language.capacity()
        + metadata
            .user
            .iter()
            .map(|(key, value)| key.capacity() + value.capacity())
            .sum::<usize>()
}

pub(crate) fn validate_put_controlled(
    result: wire::ObjectInfo,
    mut allocation: Option<acyclic_native_runtime::ReadAllocation>,
    length: Result<u64, Error>,
    controls: Option<&Arc<NativeTransportErrors>>,
) -> Result<
    (
        wire::ObjectInfo,
        Option<acyclic_native_runtime::ReadAllocation>,
    ),
    Error,
> {
    let validation = (|| {
        if let Some(allocation) = &mut allocation {
            allocation
                .reconcile_capacity(info_capacity(&result))
                .map_err(|error| controls.expect("native receipt custody").fail(error))?;
        }
        object_info(&result)?;
        if result.size != length? {
            return Err(invalid());
        }
        Ok(())
    })();
    if let Err(error) = validation {
        if let (Some(controls), Some(allocation)) = (controls, allocation.take()) {
            let original = controls.take().unwrap_or_else(|| error.into());
            let mut failure = original;
            failure.receipt = Some(crate::v1::ObjectsPutReceipt::new(result, Some(allocation)));
            return Err(controls.fail(failure));
        }
        return Err(error);
    }
    Ok((result, allocation))
}

pub(crate) enum RequestInput<M> {
    Ordinary(M),
    Native {
        request: crate::v1::ObjectsNativeRequest<M>,
        controls: Option<Arc<NativeTransportErrors>>,
    },
    #[cfg(feature = "grpc")]
    CodecFailure(tonic::Status),
}

impl<M> RequestInput<M> {
    pub(crate) fn native(
        request: crate::v1::ObjectsNativeRequest<M>,
        meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    ) -> Result<Self, crate::v1::NativeObjectsError> {
        crate::v1::native_input_meter(meter, &request)?;
        Ok(Self::Native {
            request,
            controls: None,
        })
    }
    pub(crate) fn value(&self) -> &M {
        match self {
            Self::Ordinary(value) => value,
            Self::Native { request, .. } => request.data(),
            #[cfg(feature = "grpc")]
            Self::CodecFailure(_) => {
                unreachable!("failed source frames are handled before encoding")
            }
        }
    }
    pub(crate) fn map<N>(self, transform: impl FnOnce(M) -> N) -> RequestInput<N> {
        match self {
            Self::Ordinary(value) => RequestInput::Ordinary(transform(value)),
            Self::Native { request, controls } => RequestInput::Native {
                request: request.map(transform),
                controls,
            },
            #[cfg(feature = "grpc")]
            Self::CodecFailure(error) => RequestInput::CodecFailure(error),
        }
    }
    #[cfg(feature = "grpc")]
    pub(crate) fn with_controls(
        mut self,
        current: Option<Arc<NativeTransportErrors>>,
    ) -> Result<Self, Error>
    where
        M: prost::Message,
    {
        if let (Self::Native { request, controls }, Some(current)) = (&mut self, current) {
            let encoded = request.data().encoded_len();
            current
                .meter
                .admit_codec(encoded as u64, 0, 0)
                .map_err(|error| current.fail(error))?;
            *controls = Some(current);
        }
        Ok(self)
    }
    #[cfg(feature = "grpc")]
    fn controls(&self) -> Option<&Arc<NativeTransportErrors>> {
        match self {
            Self::Ordinary(_) | Self::CodecFailure(_) => None,
            Self::Native { controls, .. } => Some(
                controls
                    .as_ref()
                    .expect("native source controls are prepared before IO"),
            ),
        }
    }
}

#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
pub(crate) struct CustodyProstCodec<M, U>(std::marker::PhantomData<(M, U)>);
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl<M, U> CustodyProstCodec<M, U> {
    pub(crate) fn new() -> Self {
        Self(std::marker::PhantomData)
    }
}
#[cfg(all(feature = "grpc", not(target_arch = "wasm32")))]
impl<M: prost::Message + 'static, U: prost::Message + Default + 'static> tonic::codec::Codec
    for CustodyProstCodec<M, U>
{
    type Encode = RequestInput<M>;
    type Decode = U;
    type Encoder = CustodyProstEncoder<M>;
    type Decoder = tonic_prost::ProstDecoder<U>;
    fn encoder(&mut self) -> Self::Encoder {
        CustodyProstEncoder(std::marker::PhantomData)
    }
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
    fn encode(
        &mut self,
        item: Self::Item,
        buffer: &mut tonic::codec::EncodeBuf<'_>,
    ) -> Result<(), Self::Error> {
        let item = match item {
            RequestInput::CodecFailure(error) => return Err(error),
            item => item,
        };
        // The packet retains its guard through encoding.
        let encoded = item.value().encoded_len() as u64;
        if let Some(controls) = item.controls()
            && let Err(error) = controls.meter.admit_codec(encoded, 0, 0)
        {
            controls.fail(error);
            return Err(controls
                .codec_failure()
                .expect("one prepared native codec failure"));
        }
        item.value()
            .encode(buffer)
            .expect("Message only errors if not enough space");
        if let Some(controls) = item.controls()
            && let Err(error) = controls.meter.record_codec(encoded, 0, 0)
        {
            controls.fail(error);
            return Err(controls
                .codec_failure()
                .expect("one prepared native codec failure"));
        }
        Ok(())
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct BodySlot<S> {
        #[pin]
        source: Option<S>,
    }
}
impl<S: futures::Stream> futures::Stream for BodySlot<S> {
    type Item = S::Item;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.project()
            .source
            .as_pin_mut()
            .expect("only initialized transport slots are published")
            .poll_next(cx)
    }
}
pub(crate) fn prepare_body_stream<F, A, B, S>(
    _factory: &F,
    controls: Option<&Arc<NativeTransportErrors>>,
) -> Result<
    Option<(
        std::pin::Pin<Box<BodySlot<S>>>,
        acyclic_native_runtime::ReadAllocation,
    )>,
    Error,
>
where
    F: FnOnce(A, B) -> S,
    S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static,
{
    controls
        .map(|controls| {
            let allocation = acyclic_native_runtime::ReadAllocation::new(
                &controls.meter,
                std::mem::size_of::<BodySlot<S>>(),
            )
            .map_err(|error| controls.fail(error))?;
            Ok((Box::pin(BodySlot { source: None }), allocation))
        })
        .transpose()
}
pub(crate) fn finish_body_stream<S>(
    source: S,
    prepared: Option<(
        std::pin::Pin<Box<BodySlot<S>>>,
        acyclic_native_runtime::ReadAllocation,
    )>,
) -> crate::v1::BodyStream
where
    S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static,
{
    use futures::StreamExt;
    if let Some((mut prepared, allocation)) = prepared {
        prepared.as_mut().project().source.set(Some(source));
        crate::v1::BodyStream::from_native_boxed(prepared, allocation)
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
        _controls: Arc<NativeTransportErrors>,
    }
}
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
impl<S> futures::Stream for NativeHttpStream<S>
where
    S: futures::Stream<Item = Result<bytes::Bytes, Error>>,
{
    type Item = Result<bytes::Bytes, Box<dyn std::error::Error + Send + Sync>>;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        if this.failure.is_none() {
            return std::task::Poll::Ready(None);
        }
        match this.source.poll_next(cx) {
            std::task::Poll::Ready(Some(Err(error))) => {
                let mut failure = this
                    .failure
                    .take()
                    .expect("one prepared HTTP source failure");
                *failure = error;
                std::task::Poll::Ready(Some(Err(
                    failure as Box<dyn std::error::Error + Send + Sync>
                )))
            }
            std::task::Poll::Ready(Some(Ok(bytes))) => std::task::Poll::Ready(Some(Ok(bytes))),
            std::task::Poll::Ready(None) => std::task::Poll::Ready(None),
            std::task::Poll::Pending => std::task::Poll::Pending,
        }
    }
}
#[cfg(all(feature = "http", not(target_arch = "wasm32")))]
pub(crate) fn http_body<S>(
    source: S,
    controls: Option<&Arc<NativeTransportErrors>>,
) -> Result<reqwest::Body, Error>
where
    S: futures::Stream<Item = Result<bytes::Bytes, Error>> + Send + 'static,
{
    if let Some(controls) = controls {
        let failure_allocation = acyclic_native_runtime::ReadAllocation::new(
            &controls.meter,
            std::mem::size_of::<Error>(),
        )
        .map_err(|error| controls.fail(error))?;
        let failure = Box::new(invalid());
        controls
            .retain_wire(failure_allocation)
            .map_err(|error| controls.fail(error))?;
        let source_allocation = acyclic_native_runtime::ReadAllocation::new(
            &controls.meter,
            std::mem::size_of::<NativeHttpStream<S>>(),
        )
        .map_err(|error| controls.fail(error))?;
        controls
            .retain_wire(source_allocation)
            .map_err(|error| controls.fail(error))?;
        Ok(reqwest::Body::wrap_stream(NativeHttpStream {
            source,
            failure: Some(failure),
            _controls: controls.clone(),
        }))
    } else {
        Ok(reqwest::Body::wrap_stream(source))
    }
}

pub(crate) fn reject_frame(
    frame: Option<wire::get_object_response::Frame>,
    allocation: Option<acyclic_native_runtime::ReadAllocation>,
    controls: Option<&Arc<NativeTransportErrors>>,
) -> Error {
    use wire::get_object_response::Frame;
    let error = match &frame {
        Some(Frame::Error(detail)) => wire::ErrorCode::try_from(detail.code)
            .ok()
            .filter(|code| *code != wire::ErrorCode::Unspecified)
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
        let cause = allocation.reconcile_capacity(capacity).err().map_or_else(
            || crate::v1::NativeObjectsError::from(error),
            crate::v1::NativeObjectsError::from,
        );
        let cause = match frame {
            Some(Frame::Header(header)) => cause.with_header(header, allocation),
            Some(Frame::Body(body)) => cause.with_wire(body.data, allocation),
            Some(Frame::Error(detail)) => {
                cause.with_wire(detail.request_id.into_bytes(), allocation)
            }
            None => cause,
        };
        return controls.fail(cause);
    }
    error
}
