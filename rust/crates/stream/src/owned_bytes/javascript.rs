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
export function snapshotCopy(view, destination, count) {
    set(destination, new U8(backing(view), offset(view), count));
}
")]
extern "C" {
    #[wasm_bindgen(catch, js_name = snapshotLength)]
    fn length(view: &Uint8Array) -> Result<u32, JsValue>;
    #[wasm_bindgen(catch, js_name = snapshotCopy)]
    fn copy(view: &Uint8Array, destination: &mut [u8], count: u32) -> Result<(), JsValue>;
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
    let count = length(view)?;
    let reservation = reserve(u64::from(count))?;
    let mut bytes = Vec::new();
    bytes
        .try_reserve_exact(count as usize)
        .map_err(|_| JsValue::from_str("byte snapshot allocation failed"))?;
    bytes.resize(count as usize, 0);
    copy(view, &mut bytes, count)?;
    Ok((bytes, reservation))
}
