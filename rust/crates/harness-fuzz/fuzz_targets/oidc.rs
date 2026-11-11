//! Public OIDC admission over the unchanged captured Dex key and signed token.
#![no_main]

use acyclic_harness_core::{Result, oidc};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use serde_json::{Value, json};

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
        // Retain the captured signing key while exceeding the old key-count
        // and identifier caps. Only the caller's explicit policy may refuse it.
        let mut expanded: Value = serde_json::from_slice(JWKS).unwrap();
        let original_key = expanded["keys"][0].clone();
        let keys = expanded["keys"].as_array_mut().unwrap();
        for index in 0..65 {
            let mut key = original_key.clone();
            key["kid"] = format!("{}:{index}", "key".repeat(100)).into();
            keys.push(key);
        }
        let expanded = serde_json::to_vec(&expanded).unwrap();
        let mut unbounded = oidc::Verifier::new(config(oidc::SigningAlgorithm::Rs256));
        unbounded.refresh(&Source(&expanded), now).await.unwrap();
        assert_eq!(
            unbounded.verify(token, now).unwrap().subject(),
            identity.subject()
        );
        for limits in [
            oidc::Limits {
                maximum_jwks_bytes: Some(expanded.len() - 1),
                ..Default::default()
            },
            oidc::Limits {
                maximum_keys: Some(1),
                ..Default::default()
            },
        ] {
            let mut bounded = oidc::Verifier::new(
                config(oidc::SigningAlgorithm::Rs256)
                    .with_limits(limits)
                    .unwrap(),
            );
            assert!(bounded.refresh(&Source(&expanded), now).await.is_err());
        }
        let mut bounded_token = oidc::Verifier::new(
            config(oidc::SigningAlgorithm::Rs256)
                .with_limits(oidc::Limits {
                    maximum_token_bytes: Some(token.len() - 1),
                    ..Default::default()
                })
                .unwrap(),
        );
        bounded_token.refresh(&Source(JWKS), now).await.unwrap();
        assert!(bounded_token.verify(token, now).is_err());
        // Corrupt the captured RSA modulus without changing its size. Ring
        // rejects even moduli; refresh must do so before replacing good keys.
        let mut invalid_rsa: Value = serde_json::from_slice(JWKS).unwrap();
        let modulus = &mut invalid_rsa["keys"][0]["n"];
        let mut bytes = URL_SAFE_NO_PAD.decode(modulus.as_str().unwrap()).unwrap();
        *bytes.last_mut().unwrap() &= !1;
        *modulus = URL_SAFE_NO_PAD.encode(bytes).into();
        let invalid_rsa = serde_json::to_vec(&invalid_rsa).unwrap();
        assert!(
            verifier
                .refresh(&Source(&invalid_rsa), now + 30)
                .await
                .is_err(),
            "OIDC invalid RSA modulus replaced last-good keys"
        );
        assert_eq!(
            verifier.verify(token, now + 30).unwrap().subject(),
            identity.subject()
        );
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
        // Internal ES256 vector uses the actual maintained signer and the
        // capture's original claim bytes; no external response is fabricated.
        use p256::ecdsa::{Signature, SigningKey, signature::Signer as _};
        let signing = SigningKey::from_slice(&[1; 32]).unwrap();
        let point = signing.verifying_key().to_sec1_point(false);
        let ec_keys = serde_json::to_vec(&json!({"keys":[{
            "kty":"EC", "crv":"P-256", "alg":"ES256", "kid":"ec",
            "x":URL_SAFE_NO_PAD.encode(point.x().unwrap()),
            "y":URL_SAFE_NO_PAD.encode(point.y().unwrap())
        }]}))
        .unwrap();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256","kid":"ec"}"#);
        let message = format!("{header}.{payload}");
        let signature: Signature = signing.sign(message.as_bytes());
        let ec_token = format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()));
        let mut ec = oidc::Verifier::new(config(oidc::SigningAlgorithm::Es256));
        ec.refresh(&Source(&ec_keys), now).await.unwrap();
        assert_eq!(
            ec.verify(&ec_token, now).unwrap().subject(),
            identity.subject()
        );
        let invalid_point = serde_json::to_vec(&json!({"keys":[{
            "kty":"EC", "crv":"P-256", "alg":"ES256", "kid":"ec",
            "x":URL_SAFE_NO_PAD.encode([0; 32]),
            "y":URL_SAFE_NO_PAD.encode([0; 32])
        }]}))
        .unwrap();
        assert!(ec.refresh(&Source(&invalid_point), now + 30).await.is_err());
        assert_eq!(
            ec.verify(&ec_token, now + 30).unwrap().subject(),
            identity.subject()
        );

        // Authenticated arbitrary claims must reach the claims decoder, rather
        // than stopping every mutation at the signature boundary.
        let signing = ed25519_dalek::SigningKey::from_bytes(&[2; 32]);
        let ed_keys = serde_json::to_vec(&json!({"keys":[{
            "kty":"OKP", "crv":"Ed25519", "alg":"EdDSA", "kid":"ed",
            "x":URL_SAFE_NO_PAD.encode(signing.verifying_key().to_bytes())
        }]}))
        .unwrap();
        let mut ed = oidc::Verifier::new(config(oidc::SigningAlgorithm::EdDsa));
        ed.refresh(&Source(&ed_keys), now).await.unwrap();
        let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"EdDSA","kid":"ed"}"#);
        let signed = |payload: &str| {
            let message = format!("{header}.{payload}");
            let signature = signing.sign(message.as_bytes());
            format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature.to_bytes()))
        };
        assert_eq!(
            ed.verify(&signed(payload), now).unwrap().subject(),
            identity.subject()
        );
        let _ = ed.verify(&signed(&URL_SAFE_NO_PAD.encode(data)), now);

        // The Edwards identity has no signing secret. A permissive verifier
        // accepts its identity R and zero scalar for any original message.
        // This is an internal group-law vector, not an issuer response.
        let mut identity_point = [0; 32];
        identity_point[0] = 1;
        let identity_key = ed25519_dalek::VerifyingKey::from_bytes(&identity_point).unwrap();
        let weak_keys = serde_json::to_vec(&json!({"keys":[{
            "kty":"OKP", "crv":"Ed25519", "alg":"EdDSA", "kid":"ed",
            "x":URL_SAFE_NO_PAD.encode(identity_key.to_bytes())
        }]}))
        .unwrap();
        let mut identity_signature = [0; 64];
        identity_signature[0] = 1;
        let forged = format!(
            "{header}.{payload}.{}",
            URL_SAFE_NO_PAD.encode(identity_signature)
        );
        if ed.refresh(&Source(&weak_keys), now + 30).await.is_ok() {
            assert!(
                ed.verify(&forged, now + 30).is_err(),
                "OIDC weak issuer forged an authenticated identity"
            );
        } else {
            assert_eq!(
                ed.verify(&signed(payload), now + 30).unwrap().subject(),
                identity.subject()
            );
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
        if verifier.refresh(&Source(data), now + 60).await.is_err() {
            assert_eq!(
                verifier.verify(token, now + 60).unwrap().subject(),
                identity.subject()
            );
        }
    });
});
