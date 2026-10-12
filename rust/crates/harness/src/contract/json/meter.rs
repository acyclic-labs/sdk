use super::*;

#[derive(Debug)]
pub(super) enum CodecError {
    Root(Error),
    Interrupted,
    Bound(&'static str),
    Json(serde_json::Error),
    Allocation(std::collections::TryReserveError),
}
impl std::fmt::Display for CodecError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Root(error) => std::fmt::Display::fmt(error, formatter),
            Self::Interrupted => formatter.write_str("JSON admission interrupted"),
            Self::Bound(reason) => formatter.write_str(reason),
            Self::Json(error) => std::fmt::Display::fmt(error, formatter),
            Self::Allocation(error) => std::fmt::Display::fmt(error, formatter),
        }
    }
}
impl std::error::Error for CodecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Root(error) => Some(error),
            Self::Interrupted | Self::Bound(_) => None,
            Self::Json(error) => Some(error),
            Self::Allocation(error) => Some(error),
        }
    }
}
impl ser::Error for CodecError {
    fn custom<T: std::fmt::Display>(message: T) -> Self {
        Self::Json(ser::Error::custom(message))
    }
}
impl From<serde_json::Error> for CodecError {
    fn from(error: serde_json::Error) -> Self {
        Self::Json(error)
    }
}
impl From<std::collections::TryReserveError> for CodecError {
    fn from(error: std::collections::TryReserveError) -> Self {
        Self::Allocation(error)
    }
}
impl From<Error> for CodecError {
    fn from(error: Error) -> Self {
        Self::Root(error)
    }
}
impl CodecError {
    pub(super) fn into_io(self) -> std::io::Error {
        match self {
            Self::Root(_) => unreachable!("original errors stay outside serde's writer bridge"),
            Self::Interrupted => std::io::ErrorKind::Interrupted.into(),
            Self::Bound(_) => std::io::ErrorKind::InvalidData.into(),
            Self::Json(error) => std::io::Error::other(error),
            Self::Allocation(_) => unreachable!(
                "SDK reserve errors return directly to Root, not through serde's writer bridge"
            ),
        }
    }
    pub(super) fn into_root_with_code(self, native_code: Option<crate::wire::ErrorCode>) -> Error {
        #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
        if let Some(code) = native_code {
            return match self {
                Self::Root(error) => error,
                Self::Interrupted => {
                    unreachable!("interruption retains its original owned Root cause")
                }
                Self::Bound(reason) => Error::NativeBoundary { code, reason },
                Self::Json(source) => Error::NativeJson {
                    code,
                    source,
                    custody: None,
                },
                Self::Allocation(error) => Error::Allocation(error),
            };
        }
        #[cfg(not(all(feature = "filesystem", not(target_arch = "wasm32"))))]
        let _ = native_code;
        match self {
            Self::Root(error) => error,
            Self::Interrupted => unreachable!("interruption retains its original owned Root cause"),
            Self::Bound(reason) => Error::Invalid(reason.into()),
            Self::Json(error) => Error::Invalid(error.to_string()),
            Self::Allocation(error) => Error::Allocation(error),
        }
    }
    pub(super) fn into_error(self, code: Option<crate::wire::ErrorCode>) -> Error {
        self.into_root_with_code(code.map(|_| crate::wire::ErrorCode::Invalid))
    }
}

pub(super) struct Meter<'a> {
    pub(super) bounds: JsonBounds,
    pub(super) items: u64,
    pub(super) bytes: u64,
    pub(super) charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    pub(super) failure: Option<Error>,
    pub(super) retained: u64,
    pub(super) bound_failure: Option<&'static str>,
    pub(super) native_code: Option<crate::wire::ErrorCode>,
    pub(super) tree_only: bool,
}

impl Drop for Meter<'_> {
    fn drop(&mut self) {
        let retained = std::mem::take(&mut self.retained);
        if retained != 0 {
            let _ = (self.charge)(JsonCodecCharge::Release { bytes: retained });
        }
    }
}

impl Meter<'_> {
    pub(super) fn forward(&mut self, error: CodecError) -> CodecError {
        match error {
            CodecError::Root(error) => {
                self.failure = Some(error);
                CodecError::Interrupted
            }
            error => error,
        }
    }
    pub(super) fn root_error(&mut self, error: CodecError) -> Error {
        self.first_error()
            .unwrap_or_else(|| error.into_root_with_code(self.native_code))
    }
    pub(super) fn first_error(&mut self) -> Option<Error> {
        self.failure.take().or_else(|| {
            self.bound_failure
                .take()
                .map(|reason| CodecError::Bound(reason).into_root_with_code(self.native_code))
        })
    }
    pub(super) fn bound(&mut self, reason: &'static str) -> CodecError {
        if self.failure.is_none() && self.bound_failure.is_none() {
            self.bound_failure = Some(reason);
        }
        CodecError::Bound(reason)
    }
    pub(super) fn charge(&mut self, charge: JsonCodecCharge) -> JsonResult<()> {
        if self.failure.is_some() && !matches!(charge, JsonCodecCharge::Release { .. }) {
            return Err(CodecError::Interrupted);
        }
        if let Some(reason) = self.bound_failure {
            if !matches!(charge, JsonCodecCharge::Release { .. }) {
                return Err(CodecError::Bound(reason));
            }
        }
        match (self.charge)(charge) {
            Ok(()) => Ok(()),
            Err(error) => {
                if self.failure.is_none() {
                    self.failure = Some(error);
                }
                Err(CodecError::Interrupted)
            }
        }
    }

    pub(super) fn item(&mut self, depth: u32) -> JsonResult<()> {
        if depth > self.bounds.maximum_depth || self.items >= self.bounds.maximum_items {
            return Err(CodecError::Bound("JSON exceeds declared structural bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
        self.items += 1;
        Ok(())
    }

    pub(super) fn data(&mut self, bytes: u64) -> JsonResult<()> {
        let next = self
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| self.bound("JSON byte count overflow"))?;
        if next > self.bounds.maximum_bytes {
            return Err(self.bound("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work {
            bytes: if self.tree_only { 0 } else { bytes },
            items: 0,
        })?;
        self.bytes = next;
        Ok(())
    }

    pub(super) fn size<T: Serialize + ?Sized>(&mut self, value: &T) -> JsonResult<u64> {
        if !self.tree_only {
            return encoded_length(value).map_err(CodecError::from);
        }
        struct Sizing<'a, 'b> {
            meter: &'a mut Meter<'b>,
            bytes: u64,
        }
        impl std::io::Write for Sizing<'_, '_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                let next = self
                    .bytes
                    .checked_add(bytes.len() as u64)
                    .ok_or_else(|| self.meter.bound("JSON byte count overflow").into_io())?;
                self.meter
                    .charge(JsonCodecCharge::Inspect {
                        items: bytes.len() as u64,
                    })
                    .map_err(CodecError::into_io)?;
                self.bytes = next;
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let mut sizing = Sizing {
            meter: self,
            bytes: 0,
        };
        serde_json::to_writer(&mut sizing, value)?;
        Ok(sizing.bytes)
    }

    pub(super) fn retain(&mut self, bytes: usize) -> JsonResult<()> {
        let retained = self
            .retained
            .checked_add(bytes as u64)
            .ok_or_else(|| CodecError::Bound("JSON retention overflow"))?;
        self.charge(JsonCodecCharge::Retain {
            bytes: bytes as u64,
        })?;
        self.retained = retained;
        Ok(())
    }

    pub(super) fn release(&mut self, bytes: u64) -> JsonResult<()> {
        let retained = self
            .retained
            .checked_sub(bytes)
            .ok_or_else(|| CodecError::Bound("JSON retention accounting differs"))?;
        self.charge(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }
    pub(super) fn allocation(&mut self, requested_capacity: usize) -> JsonResult<()> {
        if requested_capacity != 0 {
            self.charge(JsonCodecCharge::AllocationOperation {
                bytes: requested_capacity as u64,
            })?;
        }
        Ok(())
    }

    pub(super) fn string(&mut self, value: &str) -> JsonResult<String> {
        if value.len() as u64 > self.bounds.maximum_bytes.saturating_sub(self.bytes) {
            return Err(CodecError::Bound("JSON exceeds declared byte bound"));
        }
        self.charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
        struct StringCounter<'a, 'b>(&'a mut Meter<'b>);
        impl std::io::Write for StringCounter<'_, '_> {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0
                    .data(bytes.len() as u64)
                    .map_err(CodecError::into_io)?;
                if self.0.tree_only {
                    self.0
                        .charge(JsonCodecCharge::Inspect {
                            items: bytes.len() as u64,
                        })
                        .map_err(CodecError::into_io)?;
                }
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        serde_json::to_writer(StringCounter(self), value)?;
        self.retain(value.len())?;
        self.charge(JsonCodecCharge::CopyAdmission {
            bytes: value.len() as u64,
        })?;
        let mut output = String::new();
        if !value.is_empty() {
            self.allocation(value.len())?;
            output
                .try_reserve_exact(value.len())
                .map_err(CodecError::from)?;
        }
        self.charge(JsonCodecCharge::Copy {
            bytes: value.len() as u64,
        })?;
        output.push_str(value);
        Ok(output)
    }
}

pub(super) struct Counter(u64);
impl std::io::Write for Counter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| std::io::Error::other("JSON byte count overflow"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(super) fn encoded_length<T: Serialize + ?Sized>(value: &T) -> serde_json::Result<u64> {
    let mut counter = Counter(0);
    serde_json::to_writer(&mut counter, value)?;
    Ok(counter.0)
}

pub(super) type JsonResult<T> = std::result::Result<T, CodecError>;
