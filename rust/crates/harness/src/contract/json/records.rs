use super::*;

/// SDK-private sizing of an already bounded, audited closed source.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_json_byte_bound_native_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, code: crate::wire::ErrorCode, charge: &mut F,
) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    count_json_bytes_trusted(value, bounds, Some(code), charge).map(|_| ())
}

struct RecordWriter<'a, 'b> {
    scan: Scan,
    meter: &'a mut Meter<'b>,
    output: Option<&'a mut Vec<u8>>,
    maximum: usize,
}
impl std::io::Write for RecordWriter<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let meter = &mut *self.meter;
        self.scan.push_inline(bytes, meter.bounds, &mut |event| {
            let event = match event {
                JsonCodecCharge::Read { bytes } if meter.tree_only => JsonCodecCharge::Inspect { items: bytes },
                JsonCodecCharge::Read { bytes } => JsonCodecCharge::Work { bytes, items: 0 },
                other => other,
            };
            meter.charge(event).map_err(|error| meter.root_error(error))
        }).map_err(|failure| match failure {
            AdmissionFailure::Root(error) => { meter.failure = Some(error); CodecError::Interrupted.into_io() },
            AdmissionFailure::Bound(reason) => meter.bound(reason).into_io(),
        })?;
        if let Some(output) = self.output.as_mut() {
            if bytes.len() > self.maximum - output.len() { return Err(meter.bound("JSON exceeds admitted output").into_io()); }
            meter.charge(JsonCodecCharge::Copy { bytes: bytes.len() as u64 }).map_err(CodecError::into_io)?;
            output.extend_from_slice(bytes);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> { Ok(()) }
}
impl RecordWriter<'_, '_> {
    fn serialize<T: Serialize + ?Sized>(&mut self, value: &T) -> Result<u64> {
        serde_json::to_writer(&mut *self, value).map_err(|error| self.meter.root_error(error.into()))?;
        if let Some(error) = self.meter.first_error() { return Err(error); }
        self.scan.end_inline().map_err(|failure| match failure {
            AdmissionFailure::Root(error) => error,
            AdmissionFailure::Bound(reason) => self.meter.root_error(CodecError::Bound(reason)),
        })?;
        Ok(self.scan.bytes)
    }
}
pub(in crate::contract) fn count_json_bytes_trusted<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<u64> {
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None, bound_failure: None, native_code, retained: 0, tree_only: true };
    meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }).map_err(|error| meter.root_error(error))?;
    let size = RecordWriter { scan: Scan::default(), meter: &mut meter, output: None, maximum: 0 }.serialize(value)?;
    meter.charge(JsonCodecCharge::Work { bytes: 0, items: 0 }).map_err(|error| meter.root_error(error))?;
    Ok(size)
}

/// Encodes a record after admitting its exact bytes and structural bounds.
pub fn json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T, bounds: JsonBounds, native_code: Option<crate::wire::ErrorCode>, charge: &mut F,
) -> Result<Vec<u8>> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let mut meter = Meter { bounds, items: 0, bytes: 0, charge, failure: None, bound_failure: None, native_code, retained: 0, tree_only: false };
    let size = RecordWriter { scan: Scan::default(), meter: &mut meter, output: None, maximum: 0 }.serialize(value)?;
    let size = usize::try_from(size).map_err(|_| meter.root_error(CodecError::Bound("JSON output exceeds address space")))?;
    meter.retain(size).map_err(|error| meter.root_error(error))?;
    meter.charge(JsonCodecCharge::CopyAdmission { bytes: size as u64 }).and_then(|()| meter.allocation(size)).map_err(|error| meter.root_error(error))?;
    let mut output = Vec::new();
    if size != 0 { output.try_reserve_exact(size).map_err(Error::Allocation)?; }
    let written = RecordWriter { scan: Scan::default(), meter: &mut meter, output: Some(&mut output), maximum: size }.serialize(value)?;
    if written != size as u64 { return Err(meter.root_error(CodecError::Bound("JSON serialization changed between admission and encoding"))); }
    meter.retained = 0;
    Ok(output)
}

/// Decodes an admitted schema with the supplied Serde seed.
pub fn json_from_slice_bounded<'de, S, F>(
    seed: S, bytes: &'de [u8], bounds: JsonBounds, maximum_owned_bytes: u64,
    code: Option<crate::wire::ErrorCode>, value_schema: Option<JsonValueSchema>, charge: &mut F,
) -> Result<S::Value>
where S: serde::de::DeserializeSeed<'de>, F: FnMut(JsonCodecCharge) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    let value_shape = admit_json_value_shape_inline(bytes, bounds, value_schema, charge)
        .map_err(|failure| failure.into_error(code))?;
    let scratch = (bytes.len() as u64).max(8).checked_mul(2)
        .ok_or_else(|| AdmissionFailure::Bound("JSON scratch bound overflow").into_error(code))?;
    let maximum_owned_bytes = maximum_owned_bytes.checked_add(scratch)
        .ok_or_else(|| AdmissionFailure::Bound("JSON owned bound overflow").into_error(code))?;
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    charge(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    let mut decoder = serde_json::Deserializer::from_slice(bytes);
    decoder.disable_recursion_limit();
    let value = seed.deserialize(&mut decoder).map_err(|error| decode_error(error, code))?;
    decoder.end().map_err(|error| decode_error(error, code))?;
    drop(decoder);
    Ok(value)
}

/// Converts an admitted borrowed or owned JSON value.
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub fn json_from_value_bounded<T: serde::de::DeserializeOwned, F: FnMut(JsonCodecCharge) -> Result<()>>(
    source: std::borrow::Cow<'_, Value>, bounds: JsonBounds, maximum_owned_bytes: u64, code: crate::wire::ErrorCode,
    value_schema: JsonValueSchema, charge: &mut F,
) -> Result<T> {
    let mut value_shape = json_owned_value_shape_bounded(source.as_ref(), bounds, value_schema, charge)?;
    if matches!(source, std::borrow::Cow::Borrowed(_)) {
        value_shape.origin = ShapeOrigin::BorrowedConversion;
    }
    charge(JsonCodecCharge::Admission { maximum_owned_bytes, value_shape })?;
    match source {
        std::borrow::Cow::Borrowed(value) => T::deserialize(value),
        std::borrow::Cow::Owned(value) => T::deserialize(value),
    }.map_err(|source| decode_error(source, Some(code)))
}

fn decode_error(source: serde_json::Error, code: Option<crate::wire::ErrorCode>) -> Error {
    #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
    if let Some(code) = code { return Error::NativeDecode { code, source, custody: None }; }
    let _ = code;
    Error::Invalid(source.to_string())
}
