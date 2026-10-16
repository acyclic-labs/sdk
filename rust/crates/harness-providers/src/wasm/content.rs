use super::to_js;
use acyclic_harness::{
    Error, Result,
    conversation::{BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileRef},
};
use acyclic_stream::OwnedBytes;
use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

pub(super) struct Reader(pub Function);

impl ContentResidencyVerifier for Reader {
    fn read<'a>(
        &'a self,
        file: &'a FileRef,
        maximum: u64,
        io: Option<&'a ContentIo>,
    ) -> Result<BoundedContentFuture<'a, OwnedBytes>> {
        if io.is_some() {
            return Err(Error::Unsupported(
                "browser content reader has no finite ledger".into(),
            ));
        }
        BoundedContentFuture::new(io, || async move {
            let encoded =
                to_js(file).map_err(|_| Error::Invalid("model file encoding failed".into()))?;
            let file = encoded;
            let promise = self
                .0
                .call2(
                    &JsValue::UNDEFINED,
                    &file,
                    &JsValue::from_f64(maximum as f64),
                )
                .map_err(|_| Error::Storage("model file read failed".into()))?;
            let bytes = JsFuture::from(Promise::resolve(&promise))
                .await
                .map_err(|_| Error::Storage("model file read failed".into()))?
                .dyn_into::<Uint8Array>()
                .map_err(|_| Error::Invalid("model file reader must return Uint8Array".into()))?;
            if u64::from(bytes.length()) > maximum {
                return Err(Error::Invalid(
                    "model file exceeds projection byte limit".into(),
                ));
            }
            Ok(OwnedBytes::from(bytes.to_vec()))
        })
    }
}
