//! Internal vectors generate real Ed25519 JWTs with jsonwebtoken and fixed public
//! test seeds. These are not claimed as captured Dex/provider responses.
use super::*;
use acyclic_stream::{BoxProviderFuture, BoxProviderStream};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use bytes::Bytes;
use std::sync::atomic::{AtomicUsize, Ordering};

pub(super) const ISSUER: &str = "https://identity.conformance.invalid";
pub(super) const AUDIENCE: &str = "conformance-turns";
pub(super) const SUBJECT: &str = "conformance-user";

pub(super) struct Source {
    pub bytes: Vec<u8>,
    pub reads: AtomicUsize,
    pub fail: bool,
}
impl Source {
    pub fn keys(keys: &[(u8, &str)]) -> Self {
        let keys: Vec<_> = keys.iter().map(|(seed, kid)| {
            let public = ed25519_dalek::SigningKey::from_bytes(&[*seed; 32]).verifying_key();
            json!({"kty":"OKP","crv":"Ed25519","kid":kid,"alg":"EdDSA","use":"sig","x":URL_SAFE_NO_PAD.encode(public.as_bytes())})
        }).collect();
        Self {
            bytes: json!({"keys":keys}).to_string().into_bytes(),
            reads: AtomicUsize::new(0),
            fail: false,
        }
    }
}
impl oidc::JwksSource for Source {
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxProviderFuture<
        'a,
        acyclic_harness_core::Result<
            BoxProviderStream<'static, acyclic_harness_core::Result<Bytes>>,
        >,
    > {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move {
            if self.fail || uri != format!("{ISSUER}/keys") {
                return Err(acyclic_harness_core::Error::Unauthorized(
                    "conformance JWKS fetch refused".into(),
                ));
            }
            let chunks: Vec<_> = self
                .bytes
                .chunks(67)
                .map(|chunk| Ok(Bytes::copy_from_slice(chunk)))
                .collect();
            Ok(Box::pin(futures::stream::iter(chunks))
                as BoxProviderStream<
                    'static,
                    acyclic_harness_core::Result<Bytes>,
                >)
        })
    }
}

pub(super) fn config() -> acyclic_harness_core::Result<oidc::Config> {
    oidc::Config::new(
        ISSUER.into(),
        AUDIENCE.into(),
        format!("{ISSUER}/keys"),
        oidc::SigningAlgorithm::EdDsa,
    )?
    .with_limits(oidc::Limits {
        refresh_interval_seconds: 1,
        maximum_age_seconds: 60,
        ..Default::default()
    })
}

pub(super) fn claims(now: u64) -> Value {
    json!({"iss":ISSUER,"aud":AUDIENCE,"sub":SUBJECT,"exp":now+3600})
}

// Independent deadline oracle reads the original signed test payload; it does
// not ask the verifier under test what expiry it decided to accept.
pub(super) fn expires(token: &str) -> Result<u64> {
    let payload = token.split('.').nth(1).ok_or("missing token payload")?;
    let claims: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(payload)?)?;
    claims
        .get("exp")
        .and_then(Value::as_u64)
        .ok_or_else(|| "missing test token expiry".into())
}

pub(super) fn token(seed: u8, kid: &str, claims: &Value) -> Result<String> {
    // RFC 8410 PKCS#8 encoding of a fixed disposable Ed25519 test seed.
    let mut der = vec![
        0x30, 0x2e, 0x02, 0x01, 0x00, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x04, 0x22, 0x04,
        0x20,
    ];
    der.extend_from_slice(&[seed; 32]);
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::EdDSA);
    header.kid = Some(kid.into());
    Ok(jsonwebtoken::encode(
        &header,
        claims,
        &jsonwebtoken::EncodingKey::from_ed_der(&der),
    )?)
}

pub(super) fn header_mutation(token: &str, field: &str, value: Value) -> Result<String> {
    let mut parts = token.split('.');
    let raw = parts.next().ok_or("missing token header")?;
    let mut header: Value = serde_json::from_slice(&URL_SAFE_NO_PAD.decode(raw)?)?;
    header[field] = value;
    Ok(format!(
        "{}.{}.{}",
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&header)?),
        parts.next().ok_or("missing claims")?,
        parts.next().ok_or("missing signature")?
    ))
}
