use super::*;

pin_project_lite::pin_project! {
    struct WireBodySlot {
        #[pin]
        source: Option<tonic::body::Body>,
        controls: Arc<super::NativeTransportErrors>,
        owners: (Vec<acyclic_native_runtime::ReadAllocationReservation>, acyclic_native_runtime::ReadAllocation),
    }
}
impl http_body::Body for WireBodySlot {
    type Data = bytes::Bytes;
    type Error = tonic::Status;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        use std::task::Poll;
        let mut this = self.project();
        if this.source.is_none() {
            return Poll::Ready(None);
        }
        if this.controls.check().is_err() {
            this.source.set(None);
            return Poll::Ready(Some(Err(this
                .controls
                .codec_failure()
                .expect("native codec failure is prepared before RPC"))));
        }
        let frame = match this
            .source
            .as_mut()
            .as_pin_mut()
            .expect("only an initialized transport slot is published")
            .poll_frame(cx)
        {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(None) => return Poll::Ready(None),
            Poll::Ready(Some(Err(error))) => {
                this.source.set(None);
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(Some(Ok(frame))) => frame,
        };
        let data = match frame.into_data() {
            Ok(data) => data,
            Err(frame) => return Poll::Ready(Some(Ok(frame))),
        };
        let wire = (|| -> Result<_, NativeObjectsError> {
            let mut wire = this.controls.take_wire(data.len())?;
            let credit = this
                .owners
                .0
                .pop()
                .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::InvalidData))?;
            wire.absorb(&mut credit.allocate()?)?;
            Ok(wire)
        })();
        let wire = match wire {
            Ok(wire) => wire,
            Err(error) => {
                this.controls.record(error);
                this.source.set(None);
                return Poll::Ready(Some(Err(this
                    .controls
                    .codec_failure()
                    .expect("native codec failure is prepared before RPC"))));
            }
        };
        // Ready frames retain their original claims through every alias.
        let data = bytes::Bytes::from_owner(super::Guarded {
            value: data,
            _allocation: Some(wire),
        });
        Poll::Ready(Some(Ok(http_body::Frame::data(data))))
    }
    fn is_end_stream(&self) -> bool {
        self.source
            .as_ref()
            .is_none_or(http_body::Body::is_end_stream)
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.source.as_ref().map_or_else(
            || http_body::SizeHint::with_exact(0),
            http_body::Body::size_hint,
        )
    }
}
impl<T: http_body::Body> http_body::Body for super::Guarded<T> {
    type Data = T::Data;
    type Error = T::Error;
    fn poll_frame(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        self.project().value.poll_frame(cx)
    }
    fn is_end_stream(&self) -> bool {
        self.value.is_end_stream()
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.value.size_hint()
    }
}
pub(super) struct PreparedWireBody {
    source: std::pin::Pin<Box<WireBodySlot>>,
    erasure: acyclic_native_runtime::ReadAllocationReservation,
    _allocation: acyclic_native_runtime::ReadAllocation,
}
impl PreparedWireBody {
    pub(super) fn new(
        controls: Arc<super::NativeTransportErrors>,
        wire_bytes: usize,
        owner_count: usize,
    ) -> Result<Self, Error> {
        use acyclic_native_runtime::{ReadAllocation, ReadAllocationReservation};
        controls.check()?;
        let bytes = owner_count
            .checked_mul(std::mem::size_of::<ReadAllocationReservation>())
            .ok_or_else(response::invalid)?;
        let mut credits = (
            Vec::new(),
            ReadAllocation::new(&controls.meter, bytes).map_err(|error| controls.fail(error))?,
        );
        credits
            .0
            .try_reserve_exact(owner_count)
            .map_err(|error| controls.fail(controls.meter.allocation_failed(error)))?;
        let capacity = credits
            .0
            .capacity()
            .checked_mul(std::mem::size_of::<ReadAllocationReservation>())
            .ok_or_else(response::invalid)?;
        credits
            .1
            .reconcile_capacity(capacity)
            .map_err(|error| controls.fail(error))?;
        for _ in 0..owner_count {
            credits.0.push(
                ReadAllocationReservation::new(
                    &controls.meter,
                    std::mem::size_of::<super::Guarded<bytes::Bytes>>() as u64,
                )
                .map_err(|error| controls.fail(error))?,
            );
        }
        let wire = ReadAllocation::adopt(&controls.meter, wire_bytes)
            .map_err(|error| controls.fail(error))?;
        controls
            .retain_wire(wire)
            .map_err(|error| controls.fail(error))?;
        let erasure = ReadAllocationReservation::new(
            &controls.meter,
            std::mem::size_of::<super::Guarded<std::pin::Pin<Box<WireBodySlot>>>>() as u64,
        )
        .map_err(|error| controls.fail(error))?;
        let allocation = ReadAllocation::new(&controls.meter, std::mem::size_of::<WireBodySlot>())
            .map_err(|error| controls.fail(error))?;
        Ok(Self {
            source: Box::pin(WireBodySlot {
                source: None,
                controls,
                owners: credits,
            }),
            erasure,
            _allocation: allocation,
        })
    }
    pub(super) fn initialize(
        mut self,
        source: tonic::body::Body,
    ) -> Result<tonic::body::Body, NativeObjectsError> {
        self.source.as_mut().project().source.set(Some(source));
        let mut erasure = self.erasure.allocate()?;
        self._allocation.absorb(&mut erasure)?;
        let body = super::Guarded {
            value: self.source,
            _allocation: Some(self._allocation),
        };
        // UnsyncBoxBody downcasts directly in tonic Body::new.
        Ok(tonic::body::Body::new(
            http_body_util::combinators::UnsyncBoxBody::new(body),
        ))
    }
}
pub(super) struct ObjectsChannel {
    pub(super) channel: Channel,
    pub(super) wire: Option<PreparedWireBody>,
}
#[derive(Debug, thiserror::Error)]
pub(super) enum ObjectsChannelError {
    #[error("{0}")]
    Transport(#[source] tonic::transport::Error),
    #[error("{0}")]
    Native(#[source] tonic::Status),
}
impl tonic::codegen::Service<tonic::codegen::http::Request<tonic::body::Body>> for ObjectsChannel {
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
    type Error = ObjectsChannelError;
    type Future = futures::future::Either<
        futures::future::MapErr<
            <Channel as tonic::codegen::Service<
                tonic::codegen::http::Request<tonic::body::Body>,
            >>::Future,
            fn(tonic::transport::Error) -> ObjectsChannelError,
        >,
        std::future::Ready<Result<Self::Response, Self::Error>>,
    >;
    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        tonic::codegen::Service::poll_ready(&mut self.channel, cx)
            .map_err(ObjectsChannelError::Transport)
    }
    fn call(&mut self, request: tonic::codegen::http::Request<tonic::body::Body>) -> Self::Future {
        let request = if let Some(wire) = self.wire.take() {
            let controls = wire.source.controls.clone();
            let (parts, source) = request.into_parts();
            match wire.initialize(source) {
                Ok(source) => tonic::codegen::http::Request::from_parts(parts, source),
                Err(error) => {
                    controls.record(error);
                    return futures::future::Either::Right(std::future::ready(Err(
                        ObjectsChannelError::Native(
                            controls
                                .codec_failure()
                                .expect("native codec failure is prepared before RPC"),
                        ),
                    )));
                }
            }
        } else {
            request
        };
        futures::future::Either::Left(
            tonic::codegen::Service::call(&mut self.channel, request)
                .map_err(ObjectsChannelError::Transport as fn(_) -> _),
        )
    }
}
