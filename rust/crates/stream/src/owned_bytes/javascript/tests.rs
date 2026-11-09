use super::*;
use std::cell::Cell;
use wasm_bindgen_test::wasm_bindgen_test;

#[wasm_bindgen(inline_js = "
export function shadowSnapshotProperties(view) {
    const fail = () => { throw new Error('caller property observed'); };
    Object.defineProperties(view, {
        byteLength: { get: fail }, buffer: { get: fail }, byteOffset: { get: fail }
    });
    view.set = fail;
    view.subarray = fail;
}
export function detachSnapshotBuffer(buffer) {
    structuredClone(buffer, { transfer: [buffer] });
}
")]
extern "C" {
    #[wasm_bindgen(js_name = shadowSnapshotProperties)]
    fn shadow(view: &Uint8Array);
    #[wasm_bindgen(js_name = detachSnapshotBuffer)]
    fn detach(buffer: &js_sys::ArrayBuffer);
}

struct Custody<'a>(&'a Cell<usize>);

impl Drop for Custody<'_> {
    fn drop(&mut self) {
        self.0.set(self.0.get() + 1);
    }
}

#[wasm_bindgen_test]
fn snapshots_preserve_exact_admission_and_retire_custody_on_copy_failure() -> Result<(), JsValue> {
    for size in [0, 1, 17, 4_096] {
        let expected: Vec<_> = (0..size).map(|index| (index % 251) as u8).collect();
        let view = Uint8Array::from(expected.as_slice());
        let buffer = view.buffer();
        shadow(&view);
        let admitted = Cell::new(0);
        let retired = Cell::new(0);
        let (bytes, custody) = snapshot_with(&view, |length| {
            assert_eq!(length, size as u64);
            admitted.set(admitted.get() + 1);
            Ok((Vec::with_capacity(size), Custody(&retired)))
        })?;
        assert_eq!(admitted.get(), 1);
        assert_eq!(bytes, expected);
        assert_eq!(retired.get(), 0);
        drop((bytes, custody));
        assert_eq!(retired.get(), 1);
        let (bytes, ()) = snapshot_buffer_with(&buffer, |length| {
            assert_eq!(length, size as u64);
            Ok((Vec::with_capacity(size), ()))
        })?;
        assert_eq!(bytes, expected);

        let denied: JsValue = js_sys::Error::new("original admission refusal").into();
        let result = snapshot(&view, |_| Err::<(), _>(denied.clone()));
        assert!(matches!(result, Err(error) if error == denied));

        if size != 0 {
            let retired = Cell::new(0);
            let result = snapshot_with(&view, |_| Ok((Vec::new(), Custody(&retired))));
            assert!(
                result.is_err(),
                "undersized admitted storage grew during copy"
            );
            assert_eq!(retired.get(), 1);

            let retired = Cell::new(0);
            let result = snapshot_with(&view, |_| {
                // A real transferred ArrayBuffer invalidates the already
                // admitted source before the captured intrinsic copy.
                detach(&buffer);
                Ok((Vec::with_capacity(size), Custody(&retired)))
            });
            assert!(result.is_err(), "a detached source was accepted");
            assert_eq!(retired.get(), 1);
        }
    }
    Ok(())
}
