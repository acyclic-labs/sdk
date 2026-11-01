//! Dynamic buffers use the original preparation allocator and codec.
use super::*;
use crate::executor::PreparationIo;

pub(super) struct Owned<T> {
    pub(super) value: T,
    pub(super) credit: crate::NativeCustody,
}

impl<T> Owned<T> {
    pub(super) fn value(&self) -> &T {
        &self.value
    }
}

#[derive(Clone)]
pub(super) struct Buffers {
    #[cfg(feature = "filesystem")]
    allocator: Option<Arc<native_allocator::AllocatorState>>,
}

impl Buffers {
    pub(super) fn new(runtime: &CodeRuntime) -> Self {
        let _ = runtime;
        Self {
            #[cfg(feature = "filesystem")]
            allocator: runtime.native_allocator.clone(),
        }
    }

    pub(super) fn io(&self) -> Result<Option<PreparationIo>> {
        #[cfg(feature = "filesystem")]
        if let Some(allocator) = &self.allocator {
            return PreparationIo::from_native_work(allocator.work()).map(Some);
        }
        Ok(None)
    }

    pub(super) fn text(&self, text: &str) -> Result<Owned<String>> {
        let io = self.io()?;
        let value = crate::executor::stock_string_copy(text, io.as_ref())?;
        #[cfg(feature = "filesystem")]
        let credit = io
            .map(|io| io.into_native_custody(value.capacity() as u64))
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let credit = None;
        Ok(Owned { value, credit })
    }

    pub(super) fn format(&self, arguments: std::fmt::Arguments<'_>) -> Result<Owned<String>> {
        let io = self.io()?;
        let value = crate::executor::with_history_io(io.as_ref(), |work| {
            crate::executor::preparation_format(arguments, work)
        })?;
        #[cfg(feature = "filesystem")]
        let credit = io
            .map(|io| io.into_native_custody(value.capacity() as u64))
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let credit = None;
        Ok(Owned { value, credit })
    }

    pub(super) fn json<T: serde::Serialize>(&self, value: &T) -> Result<Owned<String>> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io()? {
            let remaining = io.remaining()?;
            let diagnostics =
                io.reserve_decode(crate::executor::json::sdk_json_diagnostic_bytes())?;
            let bytes = crate::executor::json::json_bytes_bounded(
                value,
                crate::executor::json::JsonBounds {
                    maximum_bytes: remaining.bytes_encoded,
                    maximum_items: remaining.items_examined,
                    maximum_depth: crate::executor::json::RECORD_MAXIMUM_DEPTH,
                },
                Some(crate::wire::ErrorCode::Invalid),
                &mut |charge| io.codec_charge(charge, true),
            )?;
            drop(diagnostics);
            let value = String::from_utf8(bytes)
                .map_err(|_| rejected("original JSON encoder returned invalid UTF-8"))?;
            let credit = Some(io.into_native_custody(value.capacity() as u64)?);
            return Ok(Owned { value, credit });
        }
        Ok(Owned {
            value: serde_json::to_string(value).map_err(|_| rejected("code value is not JSON"))?,
            credit: None,
        })
    }

    pub(super) fn decode(&self, text: &str) -> Result<CodeOutput> {
        #[cfg(feature = "filesystem")]
        if let Some(io) = self.io()? {
            use crate::filesystem::UploadRecord as _;
            let value: Value = io.decode_json(text.as_bytes())?;
            let credit = Some(io.into_native_custody(value.logical_owned_bytes()?)?);
            return Ok(CodeOutput { value, credit });
        }
        Ok(CodeOutput {
            value: serde_json::from_str(text).map_err(|_| rejected("code value is not JSON"))?,
            credit: None,
        })
    }
}

pub(super) struct PendingQueue {
    calls: Vec<QueuedCall>,
    io: Option<PreparationIo>,
    failure: Option<Error>,
}

impl PendingQueue {
    pub(super) fn new(buffers: &Buffers) -> Result<Self> {
        Ok(Self {
            calls: Vec::new(),
            io: buffers.io()?,
            failure: None,
        })
    }

    pub(super) fn push(&mut self, call: QueuedCall) -> Result<()> {
        if self.calls.len() == self.calls.capacity() {
            let capacity = self
                .calls
                .capacity()
                .checked_mul(2)
                .ok_or_else(|| rejected("code queue storage overflow"))?
                .max(1);
            crate::executor::stock_vec_reserve(&mut self.calls, capacity, self.io.as_ref())?;
        }
        self.calls.push(call);
        Ok(())
    }

    pub(super) fn fail(&mut self, error: Error) {
        if self.failure.is_none() {
            self.failure = Some(error);
        }
    }

    pub(super) fn check(&mut self) -> Result<()> {
        self.failure.take().map_or(Ok(()), Err)
    }

    pub(super) fn take(&mut self) -> Result<Owned<Vec<QueuedCall>>> {
        self.check()?;
        let calls = std::mem::take(&mut self.calls);
        #[cfg(feature = "filesystem")]
        let credit = self
            .io
            .as_ref()
            .map(|io| {
                let bytes = calls
                    .capacity()
                    .checked_mul(std::mem::size_of::<QueuedCall>())
                    .ok_or_else(|| rejected("code queue storage overflow"))?
                    as u64;
                io.take_native_custody(bytes)
            })
            .transpose()?;
        #[cfg(not(feature = "filesystem"))]
        let credit = None;
        Ok(Owned {
            value: calls,
            credit,
        })
    }
}
