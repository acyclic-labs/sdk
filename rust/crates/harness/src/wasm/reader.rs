use super::to_js_admitted;
use crate::{
    Error, Result,
    conversation::{BoundedContentFuture, ContentIo, ContentResidencyVerifier, FileRef},
};
use acyclic_stream::OwnedBytes;
use js_sys::{Function, Promise, Uint8Array};
use wasm_bindgen::{JsCast as _, JsValue};
use wasm_bindgen_futures::JsFuture;

/// Owner-authenticated browser reads used by model projection and providers.
/// The callback is scoped to its caller; Rust applies content integrity checks.
pub struct BrowserContentReader(pub Function);

impl ContentResidencyVerifier for BrowserContentReader {
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
            let encoded = to_js_admitted(file)
                .map_err(|_| Error::Invalid("model file encoding failed".into()))?;
            let promise = self
                .0
                .call2(
                    &JsValue::UNDEFINED,
                    &encoded,
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
