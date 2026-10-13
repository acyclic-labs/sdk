use super::*;

pub(super) struct NativeFaultRecordCursor {
    pub(super) source: acyclic_stream::NativeStreamOutput<acyclic_stream::NativeRecordStream>,
    pub(super) meter: std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
    pub(super) delivered: std::sync::Arc<std::sync::atomic::AtomicU64>,
    pub(super) fault: u8,
    pub(super) index: u64,
    pub(super) previous: Option<acyclic_stream::Record>,
    pub(super) finished: bool,
}

fn native_fixture_value(
    source: Option<&[u8]>,
    meter: &std::sync::Arc<dyn acyclic_native_runtime::NativeReadMeter>,
) -> std::io::Result<OwnedBytes> {
    let bytes = source.map_or(acyclic_stream::MAX_RECORD_BYTES + 1, <[u8]>::len);
    let copied = source.map_or(0, |source| source.len() as u64);
    meter.admit_examination(1, copied)?;
    let reservation = acyclic_native_runtime::ReadAllocationReservation::new(meter, bytes as u64)?;
    let allocation = reservation.allocate()?;
    let mut data = Vec::new();
    data.try_reserve_exact(bytes)
        .map_err(|error| meter.allocation_failed(error))?;
    meter.record_examination(1, copied)?;
    if let Some(source) = source {
        data.extend_from_slice(source);
    } else {
        data.resize(bytes, 0);
    }
    OwnedBytes::from_vec_with_guard(data, allocation, meter.clone()).map_err(
        |failure| match failure.into_error() {
            acyclic_stream::PreparedOwnedBytesError::Admission(error) => error,
            acyclic_stream::PreparedOwnedBytesError::NotFullyInitialized => std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "fixture buffer is not fully initialized",
            ),
        },
    )
}

impl futures::Stream for NativeFaultRecordCursor {
    type Item = std::io::Result<
        std::result::Result<
            acyclic_stream::NativeStreamOutput<acyclic_stream::Record>,
            StreamError,
        >,
    >;
    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        use std::task::Poll;
        let this = self.get_mut();
        if this.finished || this.fault == 3 {
            return Poll::Ready(None);
        }
        if this.fault == 6 && this.index != 0 {
            this.finished = true;
            return Poll::Ready(Some(Ok(Err(StreamError::Unavailable))));
        }
        let mut output = match this.source.value_mut().as_mut().poll_next(context) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Ok(Ok(output)))) => output,
            Poll::Ready(Some(error)) => return Poll::Ready(Some(error)),
            Poll::Ready(None) => {
                this.finished = true;
                return Poll::Ready(if this.fault == 2 {
                    this.previous.take().map(|record| {
                        Ok(Ok(acyclic_stream::NativeStreamOutput::new(
                            record,
                            acyclic_native_runtime::ReadAllocation::empty(&this.meter),
                        )))
                    })
                } else {
                    None
                });
            }
        };
        if this.index == 0 {
            match this.fault {
                1 => output.value_mut().sequence += 1,
                4 | 5 => {
                    let value = if this.fault == 4 {
                        Some(b"corrupt".as_slice())
                    } else {
                        None
                    };
                    let value = match native_fixture_value(value, &this.meter) {
                        Ok(value) => value,
                        Err(error) => return Poll::Ready(Some(Err(error))),
                    };
                    output.value_mut().value = value;
                }
                _ => {}
            }
        }
        if this.fault == 2 {
            this.previous = Some(output.value().clone());
        }
        this.index += 1;
        this.delivered.fetch_add(
            output.value().value.len() as u64,
            std::sync::atomic::Ordering::SeqCst,
        );
        Poll::Ready(Some(Ok(Ok(output))))
    }
}
