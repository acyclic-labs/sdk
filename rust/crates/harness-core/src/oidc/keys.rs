use super::{JwksSource, Result, refused};
use jsonwebtoken::{Algorithm, DecodingKey};
use serde::Deserialize;
use std::collections::BTreeMap;

/// Supported asymmetric signature families. Symmetric algorithms and `none`
/// cannot be represented in host configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SigningAlgorithm {
    /// RSA PKCS#1 v1.5 with SHA-256.
    Rs256,
    /// P-256 ECDSA with SHA-256.
    Es256,
    /// Ed25519.
    EdDsa,
}
impl SigningAlgorithm {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Rs256 => "RS256",
            Self::Es256 => "ES256",
            Self::EdDsa => "EdDSA",
        }
    }
    pub(super) fn algorithm(self) -> Algorithm {
        match self {
            Self::Rs256 => Algorithm::RS256,
            Self::Es256 => Algorithm::ES256,
            Self::EdDsa => Algorithm::EdDSA,
        }
    }
}

/// Shared refresh and token bounds, validated before use.
#[derive(Clone, Copy)]
pub struct Limits {
    /// Maximum serialized key-set size, at most 1 MiB.
    pub maximum_jwks_bytes: usize,
    /// Maximum key count, at most 64.
    pub maximum_keys: usize,
    /// Maximum compact JWT size, at most 64 KiB.
    pub maximum_token_bytes: usize,
    /// Minimum interval between attempts, including failed ones.
    pub refresh_interval_seconds: u64,
    /// Last-good set lifetime; failures never extend it.
    pub maximum_age_seconds: u64,
    /// Future `nbf` tolerance, at most 30 seconds. Expiry remains strict.
    pub clock_skew_seconds: u64,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            maximum_jwks_bytes: 65_536,
            maximum_keys: 16,
            maximum_token_bytes: 16_384,
            refresh_interval_seconds: 30,
            maximum_age_seconds: 3600,
            clock_skew_seconds: 0,
        }
    }
}

/// Exact host-pinned issuer, audience, JWKS URI and asymmetric algorithm.
pub struct Config {
    pub(super) issuer: String,
    pub(super) audience: String,
    pub(super) jwks_uri: String,
    pub(super) algorithm: SigningAlgorithm,
    pub(super) limits: Limits,
}
impl Config {
    /// Production configuration accepts HTTPS only.
    pub fn new(
        issuer: String,
        audience: String,
        jwks_uri: String,
        algorithm: SigningAlgorithm,
    ) -> Result<Self> {
        Self::build(issuer, audience, jwks_uri, algorithm, false)
    }
    /// Explicit local-development configuration also permits HTTP on literal
    /// 127.0.0.1 or ::1. It never permits plaintext remote endpoints.
    pub fn loopback(
        issuer: String,
        audience: String,
        jwks_uri: String,
        algorithm: SigningAlgorithm,
    ) -> Result<Self> {
        Self::build(issuer, audience, jwks_uri, algorithm, true)
    }
    fn build(
        issuer: String,
        audience: String,
        jwks_uri: String,
        algorithm: SigningAlgorithm,
        local: bool,
    ) -> Result<Self> {
        for value in [&issuer, &jwks_uri] {
            let uri = value
                .parse::<http::Uri>()
                .map_err(|_| refused("invalid OIDC URI"))?;
            if uri.host().is_none()
                || value.contains(['#', '@'])
                || !(uri.scheme_str() == Some("https")
                    || (local
                        && uri.scheme_str() == Some("http")
                        && matches!(uri.host(), Some("127.0.0.1" | "[::1]"))))
            {
                return Err(refused(
                    "OIDC URIs require HTTPS or explicit literal loopback",
                ));
            }
        }
        if audience.is_empty()
            || audience.len() > 1024
            || issuer.len() > 2048
            || jwks_uri.len() > 2048
        {
            return Err(refused("invalid OIDC configuration size"));
        }
        Ok(Self {
            issuer,
            audience,
            jwks_uri,
            algorithm,
            limits: Limits::default(),
        })
    }
    /// Set bounded host policy before creating the verifier.
    pub fn with_limits(mut self, limits: Limits) -> Result<Self> {
        if !(1..=1_048_576).contains(&limits.maximum_jwks_bytes)
            || !(1..=64).contains(&limits.maximum_keys)
            || !(1..=65_536).contains(&limits.maximum_token_bytes)
            || !(1..=3600).contains(&limits.refresh_interval_seconds)
            || limits.maximum_age_seconds < limits.refresh_interval_seconds
            || limits.maximum_age_seconds > 86_400
            || limits.clock_skew_seconds > 30
        {
            return Err(refused("invalid OIDC verification bounds"));
        }
        self.limits = limits;
        Ok(self)
    }
}

pub(super) struct Key {
    pub algorithm: Algorithm,
    pub decoding: DecodingKey,
}
#[derive(Deserialize)]
struct Set {
    keys: Vec<RawKey>,
}
#[derive(Deserialize)]
struct RawKey {
    kid: Option<String>,
    kty: String,
    alg: Option<String>,
    #[serde(rename = "use")]
    usage: Option<String>,
    key_ops: Option<Vec<String>>,
    n: Option<String>,
    e: Option<String>,
    crv: Option<String>,
    x: Option<String>,
    y: Option<String>,
}

pub(super) async fn fetch(
    source: &dyn JwksSource,
    config: &Config,
) -> Result<BTreeMap<String, Key>> {
    use futures::StreamExt as _;
    let mut body = source.fetch(&config.jwks_uri).await?;
    let mut bytes = Vec::new();
    let mut chunks = 0usize;
    while let Some(chunk) = body.next().await {
        let chunk = chunk?;
        chunks += 1;
        if chunks > config.limits.maximum_jwks_bytes.saturating_add(1)
            || chunk.len() > config.limits.maximum_jwks_bytes.saturating_sub(bytes.len())
        {
            return Err(refused(
                "OIDC key response exceeds bounded size or chunk count",
            ));
        }
        bytes.extend_from_slice(&chunk);
    }
    let set: Set = serde_json::from_slice(&bytes).map_err(|_| refused("invalid OIDC key set"))?;
    if set.keys.is_empty() || set.keys.len() > config.limits.maximum_keys {
        return Err(refused("OIDC key count is outside policy"));
    }
    let mut keys = BTreeMap::new();
    for raw in set.keys {
        // An issuer may publish signing families and encryption keys together.
        // Select verification candidates before validating their key material.
        if raw
            .alg
            .as_deref()
            .is_some_and(|alg| alg != config.algorithm.name())
            || raw.usage.as_deref().is_some_and(|usage| usage != "sig")
            || raw
                .key_ops
                .as_ref()
                .is_some_and(|ops| !ops.iter().any(|op| op == "verify"))
        {
            continue;
        }
        if raw.alg.is_none()
            && match config.algorithm {
                SigningAlgorithm::Rs256 => raw.kty != "RSA",
                SigningAlgorithm::Es256 => {
                    raw.kty != "EC" || raw.crv.as_deref().is_some_and(|curve| curve != "P-256")
                }
                SigningAlgorithm::EdDsa => {
                    raw.kty != "OKP" || raw.crv.as_deref().is_some_and(|curve| curve != "Ed25519")
                }
            }
        {
            continue;
        }
        let kid = raw
            .kid
            .filter(|kid| !kid.is_empty() && kid.len() <= 256)
            .ok_or_else(|| refused("OIDC verification key requires a bounded identifier"))?;
        if keys.contains_key(&kid) || raw.key_ops.as_ref().is_some_and(|ops| ops.len() != 1) {
            return Err(refused("ambiguous or disallowed OIDC verification key"));
        }
        let key = match (config.algorithm, raw.kty.as_str(), raw.crv.as_deref()) {
            (SigningAlgorithm::Rs256, "RSA", None) => {
                use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
                let modulus = URL_SAFE_NO_PAD
                    .decode(component(&raw.n)?)
                    .map_err(|_| refused("invalid OIDC RSA modulus"))?;
                if !(256..=1024).contains(&modulus.len())
                    || modulus
                        .first()
                        .is_none_or(|byte| *byte == 0 || (modulus.len() == 256 && byte & 0x80 == 0))
                {
                    return Err(refused("OIDC RSA key must have 2048..=8192 bits"));
                }
                let exponent = URL_SAFE_NO_PAD
                    .decode(component(&raw.e)?)
                    .map_err(|_| refused("invalid OIDC RSA exponent"))?;
                if exponent.len() > 4
                    || exponent
                        .first()
                        .is_none_or(|byte| *byte == 0 || (exponent.len() == 1 && *byte < 3))
                    || exponent.last().is_none_or(|byte| byte & 1 == 0)
                {
                    return Err(refused("OIDC RSA exponent is outside policy"));
                }
                DecodingKey::from_rsa_components(component(&raw.n)?, component(&raw.e)?)
            }
            (SigningAlgorithm::Es256, "EC", Some("P-256")) => {
                fixed_component(&raw.x)?;
                fixed_component(&raw.y)?;
                DecodingKey::from_ec_components(component(&raw.x)?, component(&raw.y)?)
            }
            (SigningAlgorithm::EdDsa, "OKP", Some("Ed25519")) => {
                fixed_component(&raw.x)?;
                DecodingKey::from_ed_components(component(&raw.x)?)
            }
            _ => return Err(refused("OIDC key type or algorithm is not allowed")),
        }
        .map_err(|_| refused("invalid OIDC public key"))?;
        (jsonwebtoken::crypto::rust_crypto::DEFAULT_PROVIDER.verifier_factory)(
            &config.algorithm.algorithm(),
            &key,
        )
        .map_err(|_| refused("invalid OIDC public key"))?;
        keys.insert(
            kid,
            Key {
                algorithm: config.algorithm.algorithm(),
                decoding: key,
            },
        );
    }
    if keys.is_empty() {
        return Err(refused("OIDC key set has no eligible verification keys"));
    }
    Ok(keys)
}

fn component(part: &Option<String>) -> Result<&str> {
    part.as_deref()
        .ok_or_else(|| refused("OIDC key component is missing"))
}

fn fixed_component(part: &Option<String>) -> Result<()> {
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    let bytes = URL_SAFE_NO_PAD
        .decode(component(part)?)
        .map_err(|_| refused("invalid OIDC curve coordinate"))?;
    if bytes.len() != 32 {
        return Err(refused("OIDC curve coordinate must contain 32 bytes"));
    }
    Ok(())
}
