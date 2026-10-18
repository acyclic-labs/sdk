use super::*;
use serde::ser::Serialize;
use serde_json::ser::{CompactFormatter, Formatter};
use std::{cell::RefCell, io::Write};

pub(super) struct Scope<'a> {
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
    failed: Option<Error>,
    retained: u64,
    lexical: admission::Lexical,
}
impl Scope<'_> {
    pub(super) fn event(&mut self, event: JsonCodecCharge) -> Result<()> {
        if self.failed.is_some() && !matches!(event, JsonCodecCharge::Release { .. }) {
            return Err(boundary("JSON admission interrupted", self.code));
        }
        (self.charge)(event)
    }
    pub(super) fn map_nodes(&mut self, entries: usize) -> Result<()> {
        // An insertion can split each ancestor and add a root. Branching is
        // at least two, so this bounds allocations even with full ancestors.
        let nodes = if entries == 0 {
            1
        } else {
            1 + usize::BITS - entries.leading_zeros()
        };
        for _ in 0..nodes {
            self.event(JsonCodecCharge::AllocationOperation {
                bytes: std::mem::size_of::<(String, Value)>() as u64,
            })?;
        }
        Ok(())
    }
    pub(super) fn retain(&mut self, bytes: u64) -> Result<()> {
        let retained = checked(self.retained.checked_add(bytes), "JSON retention overflow")?;
        self.event(JsonCodecCharge::Retain { bytes })?;
        self.retained = retained;
        Ok(())
    }
    pub(super) fn release(&mut self, bytes: u64) -> Result<()> {
        let retained = checked(
            self.retained.checked_sub(bytes),
            "JSON retention accounting differs",
        )?;
        self.event(JsonCodecCharge::Release { bytes })?;
        self.retained = retained;
        Ok(())
    }
    fn error(&mut self, error: serde_json::Error) -> Error {
        self.failed
            .take()
            .unwrap_or_else(|| records::decode_error(error, self.code))
    }
    fn stop(&mut self, error: Error) -> std::io::Error {
        if self.failed.is_none() {
            self.failed = Some(error);
        }
        std::io::ErrorKind::Other.into()
    }
}
impl Drop for Scope<'_> {
    fn drop(&mut self) {
        if self.retained != 0 {
            let _ = (self.charge)(JsonCodecCharge::Release {
                bytes: self.retained,
            });
        }
    }
}
type State<'a, 'b> = RefCell<&'a mut Scope<'b>>;
struct Writer<'a, 'b, 'c> {
    state: &'a State<'b, 'c>,
    admitted_items: u64,
    index: super::arrays::Index,
    prefix: usize,
    written: u64,
    maximum: u64,
    output: Option<&'a mut Vec<u8>>,
    digest: Option<&'a mut blake3::Hasher>,
    compare: Option<(&'a [u8], &'a mut bool)>,
}
impl Write for Writer<'_, '_, '_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        let mut scope = self.state.borrow_mut();
        if scope.failed.is_some() {
            return Ok(bytes.len());
        }
        let result = (|| {
            let written = checked(
                self.written.checked_add(bytes.len() as u64),
                "JSON byte count overflow",
            )?;
            if written > self.maximum {
                return Err(boundary("JSON exceeds admitted output", scope.code));
            }
            scope.event(JsonCodecCharge::Work {
                bytes: bytes.len() as u64,
                items: 0,
            })?;
            {
                let Scope {
                    lexical,
                    bounds,
                    code,
                    charge,
                    ..
                } = &mut **scope;
                let mut forward = |event| {
                    let event = match event {
                        JsonCodecCharge::Work { bytes, items } => {
                            let admitted = items.min(self.admitted_items);
                            self.admitted_items -= admitted;
                            JsonCodecCharge::Work {
                                bytes,
                                items: items - admitted,
                            }
                        }
                        event => event,
                    };
                    charge(event)
                };
                let charge =
                    RefCell::new(&mut forward as &mut dyn FnMut(JsonCodecCharge) -> Result<()>);
                for byte in bytes {
                    let depth = lexical.depth;
                    let string = lexical.string;
                    let item = lexical.byte(*byte, *bounds, *code, &charge)?;
                    if self.prefix != 0 {
                        let output = self.output.as_deref_mut().expect("indexed output");
                        self.index
                            .byte(&mut output[..self.prefix], *byte, depth, string, item)?;
                    }
                }
            }
            if let Some(output) = &mut self.output {
                scope.event(JsonCodecCharge::Copy {
                    bytes: bytes.len() as u64,
                })?;
                output.extend_from_slice(bytes);
            }
            if let Some(digest) = &mut self.digest {
                digest.update(bytes);
            }
            if let Some((expected, matched)) = &mut self.compare {
                **matched &= expected.get(self.written as usize..written as usize) == Some(bytes);
            }
            self.written = written;
            Ok(())
        })();
        // Defer admission failures to write()'s typed boundary. Returning an IO
        // error here makes Serde allocate an error box after budget exhaustion.
        if let Err(error) = result {
            scope.stop(error);
        }
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
struct Format<'a, 'b, 'c> {
    state: &'a State<'b, 'c>,
    canonical: bool,
    key: bool,
}
impl Formatter for Format<'_, '_, '_> {
    fn write_i128<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: i128,
    ) -> std::io::Result<()> {
        if self.canonical && !self.key && self.state.borrow().failed.is_none() {
            serde::Serializer::serialize_i128(serde_json::value::Serializer, value).map_err(
                |error| {
                    let mut scope = self.state.borrow_mut();
                    let code = scope.code;
                    scope.stop(records::decode_error(error, code))
                },
            )?;
        }
        CompactFormatter.write_i128(writer, value)
    }
    fn write_u128<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        value: u128,
    ) -> std::io::Result<()> {
        if self.canonical && !self.key && self.state.borrow().failed.is_none() {
            serde::Serializer::serialize_u128(serde_json::value::Serializer, value).map_err(
                |error| {
                    let mut scope = self.state.borrow_mut();
                    let code = scope.code;
                    scope.stop(records::decode_error(error, code))
                },
            )?;
        }
        CompactFormatter.write_u128(writer, value)
    }
    fn begin_object_key<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        first: bool,
    ) -> std::io::Result<()> {
        self.key = true;
        CompactFormatter.begin_object_key(writer, first)
    }
    fn end_object_key<W: Write + ?Sized>(&mut self, writer: &mut W) -> std::io::Result<()> {
        self.key = false;
        CompactFormatter.end_object_key(writer)
    }
    fn write_f32<W: Write + ?Sized>(&mut self, writer: &mut W, value: f32) -> std::io::Result<()> {
        if self.canonical && !self.key {
            CompactFormatter.write_f64(writer, f64::from(value))
        } else {
            CompactFormatter.write_f32(writer, value)
        }
    }
    fn write_raw_fragment<W: Write + ?Sized>(
        &mut self,
        writer: &mut W,
        fragment: &str,
    ) -> std::io::Result<()> {
        if self.state.borrow().failed.is_some() {
            return Ok(());
        }
        if !self.canonical {
            return CompactFormatter.write_raw_fragment(writer, fragment);
        }
        let (mut value, owned) = {
            let mut scope = self.state.borrow_mut();
            let result = (|| {
                let bounds = JsonBounds {
                    maximum_depth: scope
                        .bounds
                        .maximum_depth
                        .checked_sub(scope.lexical.depth)
                        .ok_or_else(|| boundary("JSON nesting limit exceeded", scope.code))?,
                    maximum_items: scope
                        .bounds
                        .maximum_items
                        .saturating_sub(scope.lexical.items),
                    ..scope.bounds
                };
                let code = scope.code;
                if fragment.len() as u64 > bounds.maximum_bytes {
                    return Err(boundary("JSON input exceeds declared byte bound", code));
                }
                let mut lexical = admission::Lexical::default();
                {
                    let mut forward = |event| scope.event(event);
                    let charge =
                        RefCell::new(&mut forward as &mut dyn FnMut(JsonCodecCharge) -> Result<()>);
                    for byte in fragment.as_bytes() {
                        charge.borrow_mut()(JsonCodecCharge::Read { bytes: 1 })?;
                        lexical.byte(*byte, bounds, code, &charge)?;
                    }
                }
                if lexical.string || lexical.depth != 0 {
                    return Err(boundary("incomplete JSON document", code));
                }
                let shape = JsonValueShape {
                    input_bytes: fragment.len() as u64,
                    lexical_items: lexical.items,
                    containers: lexical.containers,
                    virtual_items: 0,
                    raw_bytes: 0,
                    raw_frames: 0,
                    unresolved_raw: false,
                    origin: Origin::Slice,
                };
                let owned = json_value_decode_owned_bytes(shape)?;
                scope.retain(owned)?;
                let prefix = super::arrays::bytes(lexical.arrays)?;
                let length = checked(
                    fragment.len().checked_add(prefix),
                    "JSON indexed buffer overflow",
                )?;
                let bytes = length as u64;
                scope.event(JsonCodecCharge::CopyAdmission { bytes })?;
                scope.retain(bytes)?;
                if bytes != 0 {
                    scope.event(JsonCodecCharge::AllocationOperation { bytes })?;
                }
                let mut buffer = Vec::new();
                buffer
                    .try_reserve_exact(length)
                    .map_err(Error::Allocation)?;
                scope.retain(buffer.capacity() as u64 - bytes)?;
                buffer.resize(prefix, 0);
                let mut index = super::arrays::Index::default();
                let mut lexical = admission::Lexical::default();
                for byte in fragment.as_bytes() {
                    if prefix != 0 {
                        let depth = lexical.depth;
                        let string = lexical.string;
                        let item =
                            lexical.byte(*byte, bounds, code, &RefCell::new(&mut |_| Ok(())))?;
                        index.byte(&mut buffer[..prefix], *byte, depth, string, item)?;
                    }
                    buffer.push(*byte);
                }
                index.finish(&buffer[..prefix])?;
                scope.event(JsonCodecCharge::Copy { bytes })?;
                let (table, document) = buffer.split_at_mut(prefix);
                let value = super::decode::value(document, table, &mut scope)?;
                let capacity = buffer.capacity() as u64;
                drop(buffer);
                scope.release(capacity)?;
                Ok((value, owned))
            })();
            match result {
                Ok(value) => value,
                Err(error) => {
                    scope.stop(error);
                    return Ok(());
                }
            }
        };
        value.sort_all_objects();
        let result = serde_json::to_writer(writer, &value);
        drop(value);
        let mut scope = self.state.borrow_mut();
        scope.release(owned).map_err(|error| scope.stop(error))?;
        result.map_err(|error| {
            let code = scope.code;
            scope.stop(records::decode_error(error, code))
        })
    }
}
fn write<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    output: Option<&mut Vec<u8>>,
    digest: Option<&mut blake3::Hasher>,
    compare: Option<(&[u8], &mut bool)>,
    maximum: u64,
    canonical: bool,
) -> Result<u64> {
    let admitted_items = scope.lexical.items;
    scope.lexical = admission::Lexical::default();
    let state = RefCell::new(&mut *scope);
    let prefix = output.as_ref().map_or(0, |bytes| bytes.len());
    let mut writer = Writer {
        state: &state,
        admitted_items,
        index: super::arrays::Index::default(),
        prefix,
        written: 0,
        maximum,
        output,
        digest,
        compare,
    };
    let result = value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut writer,
        Format {
            state: &state,
            canonical,
            key: false,
        },
    ));
    let written = writer.written;
    // Never construct an index diagnostic after the original typed denial.
    let indexed = if result.is_ok() && state.borrow().failed.is_none() {
        writer
            .output
            .as_ref()
            .map(|output| writer.index.finish(&output[..prefix]))
    } else {
        None
    };
    drop(writer);
    result.map_err(|error| scope.error(error))?;
    if let Some(error) = scope.failed.take() {
        return Err(error);
    }
    if let Some(indexed) = indexed {
        indexed?;
    }
    if scope.lexical.string || scope.lexical.depth != 0 {
        return Err(boundary("incomplete JSON document", scope.code));
    }
    Ok(written)
}
fn output<T: Serialize + ?Sized>(
    value: &T,
    scope: &mut Scope<'_>,
    canonical: bool,
    indexed: bool,
) -> Result<(Vec<u8>, usize)> {
    let size = write(
        value,
        scope,
        None,
        None,
        None,
        scope.bounds.maximum_bytes,
        canonical,
    )?;
    let length = usize::try_from(size)
        .map_err(|_| boundary("JSON output exceeds address space", scope.code))?;
    let prefix = if indexed {
        super::arrays::bytes(scope.lexical.arrays)?
    } else {
        0
    };
    let capacity = checked(length.checked_add(prefix), "JSON indexed buffer overflow")?;
    scope.event(JsonCodecCharge::CopyAdmission {
        bytes: capacity as u64,
    })?;
    scope.retain(capacity as u64)?;
    if capacity != 0 {
        scope.event(JsonCodecCharge::AllocationOperation {
            bytes: capacity as u64,
        })?;
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(capacity)
        .map_err(Error::Allocation)?;
    scope.retain((bytes.capacity() - capacity) as u64)?;
    scope.event(JsonCodecCharge::Copy {
        bytes: prefix as u64,
    })?;
    bytes.resize(prefix, 0);
    if write(value, scope, Some(&mut bytes), None, None, size, canonical)? != size {
        return Err(boundary(
            "JSON serialization changed between admission and encoding",
            scope.code,
        ));
    }
    Ok((bytes, prefix))
}
fn build<T: Serialize + ?Sized>(value: &T, scope: &mut Scope<'_>) -> Result<Value> {
    let (mut bytes, prefix) = output(value, scope, true, true)?;
    let shape = JsonValueShape {
        input_bytes: (bytes.len() - prefix) as u64,
        lexical_items: scope.lexical.items,
        containers: scope.lexical.containers,
        virtual_items: 0,
        raw_bytes: 0,
        raw_frames: 0,
        unresolved_raw: false,
        origin: Origin::Slice,
    };
    let owned = json_value_decode_owned_bytes(shape)?;
    scope.retain(owned)?;
    let (table, document) = bytes.split_at_mut(prefix);
    let mut value = super::decode::value(document, table, scope)?;
    let actual = records::json_value_retained_owned_bytes(&value)?;
    let released = checked(
        owned
            .checked_sub(actual)
            .and_then(|release| release.checked_add(bytes.capacity() as u64)),
        "JSON retention accounting differs",
    )?;
    drop(bytes);
    scope.release(released)?;
    value.sort_all_objects();
    Ok(value)
}
fn scope<'a>(
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &'a mut dyn FnMut(JsonCodecCharge) -> Result<()>,
) -> Scope<'a> {
    Scope {
        bounds: JsonBounds {
            maximum_depth: bounds.maximum_depth.min(256),
            ..bounds
        },
        code,
        charge,
        failed: None,
        retained: 0,
        lexical: admission::Lexical::default(),
    }
}
/// Compact encoding admitted before its output buffer is allocated.
pub fn json_bytes_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = scope(bounds, code, charge);
    let (bytes, _) = output(value, &mut scope, false, false)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(bytes)
}
/// Sorted-key encoding with funded buffers and Value storage.
pub fn canonical_json_bytes_bounded<
    T: Serialize + ?Sized,
    F: FnMut(JsonCodecCharge) -> Result<()>,
>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Vec<u8>> {
    let mut scope = scope(bounds, code, charge);
    let value = build(value, &mut scope)?;
    let (bytes, _) = output(&value, &mut scope, false, false)?;
    let released = checked(
        scope.retained.checked_sub(bytes.capacity() as u64),
        "JSON retention accounting differs",
    )?;
    drop(value);
    scope.release(released)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(bytes)
}

pub(crate) fn canonical_digest_with_work<T: Serialize + ?Sized>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    observer: &mut dyn crate::store::HistoryIo,
) -> Result<[u8; 32]> {
    let observer = RefCell::new(observer);
    let mut charge = |event| observer.borrow_mut().codec_charge(event);
    let mut scope = scope(bounds, code, &mut charge);
    let value = build(value, &mut scope)?;
    let size = write(
        &value,
        &mut scope,
        None,
        None,
        None,
        bounds.maximum_bytes,
        false,
    )?;
    observer.borrow_mut().hashed_bytes(size)?;
    let mut digest = blake3::Hasher::new();
    if write(
        &value,
        &mut scope,
        None,
        Some(&mut digest),
        None,
        size,
        false,
    )? != size
    {
        return Err(boundary("canonical digest length changed", code));
    }
    drop(value);
    scope.release(scope.retained)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    Ok(*digest.finalize().as_bytes())
}

pub fn json_value_bounded<T: Serialize + ?Sized, F: FnMut(JsonCodecCharge) -> Result<()>>(
    value: &T,
    bounds: JsonBounds,
    code: Option<crate::wire::ErrorCode>,
    charge: &mut F,
) -> Result<Value> {
    let mut scope = scope(bounds, code, charge);
    let value = build(value, &mut scope)?;
    scope.event(JsonCodecCharge::Work { bytes: 0, items: 0 })?;
    scope.retained = 0;
    Ok(value)
}
