//! Public OIDC admission over the unchanged captured Dex key and signed token.
#![no_main]

use acyclic_harness_core::{Result, oidc};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use serde_json::Value;

const CAPTURE: &str = "../../harness/fixtures/conformance/identity/dex/";
const JWKS: &[u8] = include_bytes!("../../harness/fixtures/conformance/identity/dex/jwks.body");
const RESPONSE: &str = include_str!("../../harness/fixtures/conformance/identity/dex/token.body");
const MANIFEST: &str =
    include_str!("../../harness/fixtures/conformance/identity/dex/manifest.json");

struct Source<'a>(&'a [u8]);
impl oidc::JwksSource for Source<'_> {
    fn fetch<'a>(
        &'a self,
        _: &'a str,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<Bytes>>>> {
        let bytes = Bytes::copy_from_slice(self.0);
        Box::pin(async move {
            Ok(Box::pin(futures::stream::once(async move { Ok(bytes) }))
                as BoxProviderStream<'static, Result<Bytes>>)
        })
    }
}

fuzz_target!(|data: &[u8]| {
    let manifest: Value = serde_json::from_str(MANIFEST).unwrap();
    let response: Value = serde_json::from_str(RESPONSE).unwrap();
    let token = response["id_token"].as_str().unwrap();
    let payload = token.split('.').nth(1).unwrap();
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload).unwrap()).unwrap();
    // The production clock is injected by its host. Replay the signed capture
    // at its original issuance time, then at its original expiry.
    let now = claims["iat"].as_u64().unwrap();
    let config = |algorithm| {
        oidc::Config::loopback(
            manifest["issuer"].as_str().unwrap().into(),
            manifest["audience"].as_str().unwrap().into(),
            manifest["jwks_uri"].as_str().unwrap().into(),
            algorithm,
        )
        .unwrap()
    };
    futures::executor::block_on(async {
        let mut verifier = oidc::Verifier::new(config(oidc::SigningAlgorithm::Rs256));
        verifier.refresh(&Source(JWKS), now).await.unwrap();
        let identity = verifier.verify(token, now).expect(CAPTURE);
        assert_eq!(identity.subject(), manifest["subject"].as_str().unwrap());
        assert_eq!(identity.issuer(), manifest["issuer"].as_str().unwrap());
        assert_eq!(identity.expires_at(), claims["exp"].as_u64().unwrap());
        assert!(verifier.verify(token, identity.expires_at()).is_err());
        // Change the actual signed bytes without producing another signature.
        // A missing verification call cannot satisfy this same oracle.
        let (message, signature) = token.rsplit_once('.').unwrap();
        let mut signature = URL_SAFE_NO_PAD.decode(signature).unwrap();
        let index = usize::from(data.first().copied().unwrap_or(0)) % signature.len();
        signature[index] ^= 1;
        let forged = format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature));
        assert!(
            verifier.verify(&forged, now).is_err(),
            "OIDC signature binding lost"
        );
        if let Ok(token) = std::str::from_utf8(data) {
            let _ = verifier.verify(token, now);
        }
        // All pinned key families reach their real JWKS decoder. A rejected
        // replacement must preserve the original authenticated snapshot.
        for algorithm in [
            oidc::SigningAlgorithm::Rs256,
            oidc::SigningAlgorithm::Es256,
            oidc::SigningAlgorithm::EdDsa,
        ] {
            let mut candidate = oidc::Verifier::new(config(algorithm));
            if candidate.refresh(&Source(data), now).await.is_ok() {
                let _ = candidate.verify(token, now);
            }
        }
        if verifier.refresh(&Source(data), now + 30).await.is_err() {
            assert_eq!(
                verifier.verify(token, now + 30).unwrap().subject(),
                identity.subject()
            );
        }
    });
});
