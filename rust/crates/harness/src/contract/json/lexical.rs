use super::*;

const LEXICAL_BYTE_ERROR: &str = "JSON input exceeds declared byte bound";
const LEXICAL_NESTING_ERROR: &str = "invalid JSON nesting";
const LEXICAL_ITEM_ERROR: &str = "JSON exceeds declared structural bound";
const LEXICAL_INCOMPLETE_ERROR: &str = "incomplete JSON document";

/// Maximum ONE newly owned SDK lexical-error String payload.
pub(crate) const fn lexical_json_diagnostic_bytes() -> u64 {
    let labels = [LEXICAL_BYTE_ERROR, LEXICAL_NESTING_ERROR,
        LEXICAL_ITEM_ERROR, LEXICAL_INCOMPLETE_ERROR];
    let mut maximum = 0;
    let mut index = 0;
    while index < labels.len() {
        let bytes = labels[index].len() as u64;
        if bytes > maximum { maximum = bytes; }
        index += 1;
    }
    maximum
}

/// Allocation-free lexical admission before serde scratch/collections grow.
#[derive(Default)]
pub(super) struct Scan {
    pub(super) bytes: u64,
    pub(super) depth: u32,
    pub(super) items: u64,
    pub(super) string: bool,
    pub(super) escape: bool,
    pub(super) atom: bool,
    pub(super) inspector: Option<ValueInspector>,
}
pub(super) enum AdmissionFailure {
    Root(Error),
    Bound(&'static str),
}
impl From<Error> for AdmissionFailure {
    fn from(error: Error) -> Self { Self::Root(error) }
}
impl AdmissionFailure {
    pub(super) fn into_error(self, code: Option<crate::wire::ErrorCode>) -> Error {
        match self {
            Self::Root(error) => error,
            Self::Bound(reason) => {
                #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
                if code.is_some() {
                    return Error::NativeBoundary { code: crate::wire::ErrorCode::Invalid, reason };
                }
                let _ = code;
                Error::Invalid(reason.into())
            }
        }
    }
}
impl Scan {
    pub(super) fn with_value_schema(schema: Option<JsonValueSchema>, bounds: JsonBounds) -> Self {
        Self { inspector: Some(ValueInspector::new(schema, bounds)), ..Self::default() }
    }
    pub(super) fn value_shape(&self) -> std::result::Result<JsonValueShape, AdmissionFailure> {
        self.end_inline()?;
        self.inspector.as_ref().expect("Value shape requested only from configured source pass")
            .finish(self.bytes, self.items)
    }
    pub(super) fn push_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
        &mut self, bytes: &[u8], bounds: JsonBounds, charge: &mut F,
    ) -> std::result::Result<(), AdmissionFailure> {
        for &byte in bytes {
            if self.bytes >= bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR)); }
            charge(JsonCodecCharge::Read { bytes: 1 })?;
            self.bytes += 1;
            if let Some(inspector) = &mut self.inspector { inspector.push(byte, charge)?; }
            if self.string {
                if self.escape { self.escape = false; }
                else if byte == b'\\' { self.escape = true; }
                else if byte == b'"' { self.string = false; }
                continue;
            }
            let item_depth = self.depth;
            let item = match byte {
                b'"' => { self.string = true; self.atom = false; true }
                b'[' | b'{' => { self.depth += 1; self.atom = false; true }
                b']' | b'}' => { self.depth = self.depth.checked_sub(1).ok_or(AdmissionFailure::Bound(LEXICAL_NESTING_ERROR))?; self.atom = false; false }
                b':' | b',' | b' ' | b'\r' | b'\n' | b'\t' => { self.atom = false; false }
                _ => { let first = !self.atom; self.atom = true; first }
            };
            if item {
                if item_depth > bounds.maximum_depth || self.items >= bounds.maximum_items {
                    return Err(AdmissionFailure::Bound(LEXICAL_ITEM_ERROR));
                }
                charge(JsonCodecCharge::Work { bytes: 0, items: 1 })?;
                self.items += 1;
            }
        }
        Ok(())
    }
    pub(super) fn end_inline(&self) -> std::result::Result<(), AdmissionFailure> {
        if self.string || self.depth != 0 { return Err(AdmissionFailure::Bound(LEXICAL_INCOMPLETE_ERROR)); }
        Ok(())
    }
}

/// Bounds lexical tokens, including duplicate keys.
pub(crate) fn admit_json<F: FnMut(JsonCodecCharge) -> Result<()>>(bytes: &[u8], bounds: JsonBounds, charge: &mut F) -> Result<()> {
    let bounds = JsonBounds { maximum_depth: bounds.maximum_depth.min(256), ..bounds };
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR).into_error(None)); }
    let mut scan = Scan::default();
    scan.push_inline(bytes, bounds, charge).map_err(|failure| failure.into_error(None))?;
    scan.end_inline().map_err(|failure| failure.into_error(None))
}

pub(super) fn admit_json_value_shape_inline<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: Option<JsonValueSchema>, charge: &mut F,
) -> std::result::Result<JsonValueShape, AdmissionFailure> {
    if bytes.len() as u64 > bounds.maximum_bytes { return Err(AdmissionFailure::Bound(LEXICAL_BYTE_ERROR)); }
    let mut scan = Scan::with_value_schema(schema, bounds);
    scan.push_inline(bytes, bounds, charge)?;
    scan.value_shape()
}

/// The original source pass with a real closed SDK Value field descriptor.
pub(crate) fn admit_json_value_shape<F: FnMut(JsonCodecCharge) -> Result<()>>(
    bytes: &[u8], bounds: JsonBounds, schema: JsonValueSchema, charge: &mut F,
) -> Result<JsonValueShape> {
    admit_json_value_shape_inline(bytes, JsonBounds {
        maximum_depth: bounds.maximum_depth.min(256), ..bounds
    }, Some(schema), charge).map_err(|failure| failure.into_error(None))
}
