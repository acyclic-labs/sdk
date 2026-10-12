use super::*;
use std::cell::RefCell;
mod value;
use value::{Failure, Scope};

struct Writer<'a, 'b> {
    scope: &'a mut Scope<'b>,
    lexical: admission::Lexical,
    written: u64,
    output: Option<&'a mut Vec<u8>>,
    maximum: u64,
}
impl std::io::Write for Writer<'_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let result = (|| {
            let Some(written) = self.written.checked_add(bytes.len() as u64) else {
                return self.scope.bound("JSON byte count overflow");
            };
            if written > self.maximum {
                return self.scope.bound("JSON exceeds admitted output");
            }
            self.scope.event(JsonCodecCharge::Work {
                bytes: bytes.len() as u64,
                items: 0,
            })?;
            let mut charge = |event| (self.scope.charge)(event);
            let charge = RefCell::new(&mut charge as &mut dyn FnMut(JsonCodecCharge) -> Result<()>);
            for byte in bytes {
                if let Err(error) =
                    self.lexical
                        .byte(*byte, self.scope.bounds, self.scope.code, &charge)
                {
                    self.scope.failed = Some(error);
                    return Err(Failure::Stopped);
                }
            }
            if let Some(output) = self.output.as_mut() {
                self.scope.event(JsonCodecCharge::Copy {
                    bytes: bytes.len() as u64,
                })?;
                output.extend_from_slice(bytes);
            }
            self.written = written;
            Ok(())
        })();
        result.map_err(|_| std::io::ErrorKind::Interrupted)?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn write<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    output: Option<&mut Vec<u8>>,
    maximum: u64,
) -> Result<u64> {
    let mut writer = Writer {
        scope,
        lexical: admission::Lexical::default(),
        written: 0,
        output,
        maximum,
    };
    if let Err(error) = serde_json::to_writer(&mut writer, value) {
        return Err(writer.scope.error(error.into()));
    }
    if let Some(error) = writer.scope.failed.take() {
        return Err(error);
    }
    if writer.lexical.string || writer.lexical.depth != 0 {
        return Err(boundary("incomplete JSON document", writer.scope.code));
    }
    Ok(writer.written)
}
fn output<T: Serialize + ?Sized>(value: &T, scope: &mut Scope<'_>) -> Result<Vec<u8>> {
    let size = write(value, scope, None, scope.bounds.maximum_bytes)?;
    let length = usize::try_from(size)
        .map_err(|_| boundary("JSON output exceeds address space", scope.code))?;
    let result = scope
        .retain(size)
        .and_then(|()| scope.event(JsonCodecCharge::CopyAdmission { bytes: size }))
        .and_then(|()| scope.allocate(size));
    if let Err(error) = result {
        return Err(scope.error(error));
    }
    let mut bytes = Vec::new();
    bytes.try_reserve_exact(length).map_err(Error::Allocation)?;
    if bytes.capacity() > length {
        if let Err(error) = scope.retain((bytes.capacity() - length) as u64) {
            return Err(scope.error(error));
        }
    }
    let written = write(value, scope, Some(&mut bytes), size)?;
    if written != size {
        return Err(boundary(
            "JSON serialization changed between admission and encoding",
            scope.code,
        ));
    }
    Ok(bytes)
}
/// Compact record encoding with byte and structural admission before allocation.
pub fn json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = Scope::new(bounds, code, charge);
    let bytes = output(value, &mut scope)?;
    if let Err(error) = scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(scope.error(error));
    }
    scope.retained = 0;
    Ok(bytes)
}
/// Sorted-key encoding admitted before each owned payload or container grows.
pub fn canonical_json_bytes_bounded<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = Scope::new(bounds, code, charge);
    let value = value::build(value, &mut scope)?;
    let bytes = output(&value, &mut scope)?;
    let released = scope
        .retained
        .checked_sub(bytes.capacity() as u64)
        .ok_or_else(|| boundary("JSON retention accounting differs", code))?;
    drop(value);
    if let Err(error) = scope
        .release(released)
        .and_then(|()| scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 }))
    {
        return Err(scope.error(error));
    }
    scope.retained = 0;
    Ok(bytes)
}
/// Builds a JSON Value under the same payload, node and depth limits.
pub fn json_value_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Value> {
    let mut inspect = |event| {
        charge(match event {
            JsonCodecCharge::Work { bytes, items } if bytes != 0 => JsonCodecCharge::Inspect {
                items: bytes.saturating_add(items),
            },
            other => other,
        })
    };
    let mut scope = Scope::new(bounds, code, &mut inspect);
    let value = value::build(value, &mut scope)?;
    let owned = json_value_logical_owned_bytes(&value)?;
    let released = scope
        .retained
        .checked_sub(owned)
        .ok_or_else(|| boundary("JSON retention accounting differs", code))?;
    if let Err(error) = scope
        .release(released)
        .and_then(|()| scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 }))
    {
        return Err(scope.error(error));
    }
    scope.retained = 0;
    Ok(value)
}
pub(crate) fn count_json_bytes_trusted<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<u64> {
    let mut inspect = |event| {
        charge(match event {
            JsonCodecCharge::Work { bytes, items } if bytes != 0 => JsonCodecCharge::Inspect {
                items: bytes.saturating_add(items),
            },
            other => other,
        })
    };
    let mut scope = Scope::new(bounds, code, &mut inspect);
    if let Err(error) = scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(scope.error(error));
    }
    let size = write(value, &mut scope, None, bounds.maximum_bytes)?;
    if let Err(error) = scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 }) {
        return Err(scope.error(error));
    }
    Ok(size)
}
#[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
pub(crate) fn validate_json_byte_bound_native_trusted<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: crate::wire::ErrorCode,
    charge: &mut F,
) -> Result<()> {
    count_json_bytes_trusted(value, bounds, Some(code), charge).map(|_| ())
}
