//! The only manual copy from a caller-owned typed array into Rust storage.
use js_sys::Uint8Array;
use wasm_bindgen::prelude::*;

#[wasm_bindgen(inline_js = "
const U8 = Uint8Array;
const prototype = Object.getPrototypeOf(U8.prototype);
const getter = name => Function.prototype.call.bind(
    Object.getOwnPropertyDescriptor(prototype, name).get);
const length = getter('byteLength');
const backing = getter('buffer');
const offset = getter('byteOffset');
const set = Function.prototype.call.bind(U8.prototype.set);
export function snapshotLength(view) { return length(view); }
export function snapshotBufferView(buffer) { return new U8(buffer); }
export function snapshotCopy(view, destination, count) {
    set(destination, new U8(backing(view), offset(view), count));
}
")]
extern "C" {
    #[wasm_bindgen(catch, js_name = snapshotBufferView)]
    fn buffer_view(buffer: &JsValue) -> Result<Uint8Array, JsValue>;
    #[wasm_bindgen(catch, js_name = snapshotLength)]
    fn length(view: &Uint8Array) -> Result<u32, JsValue>;
    #[wasm_bindgen(catch, js_name = snapshotCopy)]
    fn copy(view: &Uint8Array, destination: &mut [u8], count: u32) -> Result<(), JsValue>;
}

/// Copies an ArrayBuffer through the same captured constructor and intrinsics.
pub fn snapshot_buffer_with<R>(
    buffer: &JsValue,
    allocate: impl FnOnce(u64) -> Result<(Vec<u8>, R), JsValue>,
) -> Result<(Vec<u8>, R), JsValue> {
    snapshot_with(&buffer_view(buffer)?, allocate)
}

/// Reserve the intrinsic byte length, then copy exactly that admitted extent.
///
/// The reservation runs before allocation; it must not call untrusted JavaScript.
/// Captured typed-array intrinsics bypass caller-defined properties and methods.
/// No await separates admission from the copy, and shared or resizable sources
/// cannot enlarge the destination or expose Rust storage. Invalid or detached
/// views, failed reservations and failed allocations return their original error.
pub fn snapshot<R>(
    view: &Uint8Array,
    reserve: impl FnOnce(u64) -> Result<R, JsValue>,
) -> Result<(Vec<u8>, R), JsValue> {
    snapshot_with(view, |count| {
        let reservation = reserve(count)?;
        let mut bytes = Vec::new();
        bytes
            .try_reserve_exact(count as usize)
            .map_err(|_| JsValue::from_str("byte snapshot allocation failed"))?;
        Ok((bytes, reservation))
    })
}

/// Copies through the same captured intrinsics into caller-admitted storage.
/// The allocator receives the intrinsic extent once and returns its original
/// custody. An undersized allocation is rejected before any resize or copy.
pub fn snapshot_with<R>(
    view: &Uint8Array,
    allocate: impl FnOnce(u64) -> Result<(Vec<u8>, R), JsValue>,
) -> Result<(Vec<u8>, R), JsValue> {
    struct Snapshot<R> {
        bytes: Vec<u8>,
        custody: R,
    }
    let count = length(view)?;
    let (bytes, reservation) = allocate(u64::from(count))?;
    let mut snapshot = Snapshot {
        bytes,
        custody: reservation,
    };
    if snapshot.bytes.capacity() < count as usize {
        return Err(JsValue::from_str("byte snapshot allocation is undersized"));
    }
    snapshot.bytes.resize(count as usize, 0);
    copy(view, &mut snapshot.bytes, count)?;
    Ok((snapshot.bytes, snapshot.custody))
}

/// Admit a prefix of the intrinsic length before allocating or copying it.
///
/// The callback returns the exact admitted byte count and reservation; it must
/// not call untrusted JavaScript. The count cannot exceed the original length.
/// This shares the whole-snapshot intrinsic copy and its error guarantees.
pub fn snapshot_prefix<R>(
    view: &Uint8Array,
    admit: impl FnOnce(u32) -> Result<(u32, R), JsValue>,
) -> Result<(Vec<u8>, R), JsValue> {
    let available = length(view)?;
    let (count, reservation) = admit(available)?;
    if count > available {
        return Err(JsValue::from_str("byte snapshot prefix exceeds source"));
    }
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count as usize)
        .map_err(|_| JsValue::from_str("byte snapshot allocation failed"))?;
    bytes.resize(count as usize, 0);
    copy(view, &mut bytes, count)?;
    Ok((bytes, reservation))
}
