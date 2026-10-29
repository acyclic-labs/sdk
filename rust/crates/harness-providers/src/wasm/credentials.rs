//! Thin binding to host-protected fetch. No secret value crosses this boundary.
use crate::{
    AuthorizationScheme, BrowserTransport, CredentialBinding, CredentialResolver, CredentialSource,
    HttpTransport, invalid,
};
use acyclic_harness_core::Result;
use js_sys::{Function, Reflect, Uint8Array};
use std::sync::Arc;
use wasm_bindgen::{JsCast as _, JsValue};

#[cfg(test)]
mod tests;

pub(super) struct Resolver(pub Function);

impl CredentialResolver for Resolver {
    fn bind(
        &self,
        source: CredentialSource<'_>,
        origin: &str,
        scheme: AuthorizationScheme,
        transport: Arc<dyn HttpTransport>,
    ) -> Result<CredentialBinding> {
        let reference = match source {
            CredentialSource::ApiKey(key) => return Ok(CredentialBinding::api_key(key, transport)),
            CredentialSource::SecretReference(reference) => reference,
        };
        let scheme = match scheme {
            AuthorizationScheme::Bearer => "bearer",
            AuthorizationScheme::ApiKey => "api_key",
            AuthorizationScheme::GoogleApiKey => "google_api_key",
        };
        let refused =
            |cause| crate::http::browser::failure(cause, "protected model binding failed");
        let binding = self
            .0
            .call3(
                &JsValue::UNDEFINED,
                &origin.into(),
                &reference.into(),
                &scheme.into(),
            )
            .map_err(refused)?;
        let scope = Reflect::get(&binding, &"credentialScope".into())
            .map_err(refused)?
            .dyn_into::<Uint8Array>()
            .map_err(|_| invalid("credential scope must be bytes"))?;
        if scope.length() != 32 {
            return Err(invalid(
                "credential scope must contain 32 authenticated bytes",
            ));
        }
        let mut bytes = [0; 32];
        scope.copy_to(&mut bytes);
        let fetch = Reflect::get(&binding, &"fetch".into())
            .map_err(refused)?
            .dyn_into::<Function>()
            .map_err(|_| invalid("protected model fetch must be a function"))?
            .bind0(&binding);
        Ok(CredentialBinding::protected(
            bytes,
            Arc::new(BrowserTransport::new(fetch)),
        ))
    }
}
