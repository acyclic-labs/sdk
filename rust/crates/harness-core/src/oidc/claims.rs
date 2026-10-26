use super::{Config, Result, keys::Key, refused};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use std::collections::{BTreeMap, BTreeSet};

/// Identity returned only after a pinned-key signature and all claim checks.
pub struct VerifiedIdentity {
    issuer: String,
    subject: String,
    expires_at: u64,
}
impl VerifiedIdentity {
    /// Exact configured issuer, suitable for host-owned policy lookup.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }
    /// Exact signed subject, never an authority or grant by itself.
    pub fn subject(&self) -> &str {
        &self.subject
    }
    /// Original token expiry in UTC seconds.
    pub fn expires_at(&self) -> u64 {
        self.expires_at
    }
}

#[derive(Deserialize)]
struct Header {
    alg: String,
    kid: String,
    crit: Option<serde_json::Value>,
    b64: Option<serde_json::Value>,
    jku: Option<serde_json::Value>,
    x5u: Option<serde_json::Value>,
    jwk: Option<serde_json::Value>,
}
#[derive(Deserialize)]
struct Claims {
    iss: String,
    sub: String,
    aud: Audience,
    exp: u64,
    nbf: Option<u64>,
    azp: Option<String>,
}
#[derive(Deserialize)]
#[serde(untagged)]
enum Audience {
    One(String),
    Many(Vec<String>),
}

pub(super) fn verify(
    token: &str,
    now: u64,
    config: &Config,
    keys: &BTreeMap<String, Key>,
) -> Result<VerifiedIdentity> {
    if token.len() > config.limits.maximum_token_bytes {
        return Err(refused("OIDC token exceeds policy size"));
    }
    let mut parts = token.split('.');
    let encoded = parts.next().ok_or_else(|| refused("invalid OIDC token"))?;
    if parts.next().is_none() || parts.next().is_none() || parts.next().is_some() {
        return Err(refused("invalid OIDC compact token"));
    }
    let bytes = URL_SAFE_NO_PAD
        .decode(encoded)
        .map_err(|_| refused("invalid OIDC header encoding"))?;
    let header: Header =
        serde_json::from_slice(&bytes).map_err(|_| refused("invalid OIDC header"))?;
    if header.alg != config.algorithm.name()
        || header.kid.len() > 256
        || header.crit.is_some()
        || header.b64.is_some()
        || header.jku.is_some()
        || header.x5u.is_some()
        || header.jwk.is_some()
    {
        return Err(refused(
            "OIDC header algorithm or key source is not allowed",
        ));
    }
    let key = keys
        .get(&header.kid)
        .ok_or_else(|| refused("OIDC signing key is unknown"))?;
    let mut validation = jsonwebtoken::Validation::new(key.algorithm);
    // Time is deliberately checked below against the host-injected clock.
    // Signature, issuer and audience checks remain enabled in the JWT library.
    validation.validate_exp = false;
    validation.set_required_spec_claims(&["iss", "sub", "aud", "exp"]);
    validation.set_issuer(&[&config.issuer]);
    validation.set_audience(&[&config.audience]);
    let claims = jsonwebtoken::decode::<Claims>(token, &key.decoding, &validation)
        .map_err(|_| refused("OIDC signature or required claims are invalid"))?
        .claims;
    let audience = match &claims.aud {
        Audience::One(one) => vec![one.as_str()],
        Audience::Many(many) => many.iter().map(String::as_str).collect(),
    };
    let unique: BTreeSet<_> = audience.iter().copied().collect();
    if claims.iss != config.issuer
        || claims.sub.is_empty()
        || claims.sub.len() > 1024
        || claims.exp <= now
        || claims
            .nbf
            .is_some_and(|nbf| nbf > now.saturating_add(config.limits.clock_skew_seconds))
        || audience.is_empty()
        || audience.iter().any(|aud| aud.is_empty())
        || unique.len() != audience.len()
        || !unique.contains(config.audience.as_str())
        || (audience.len() > 1 && claims.azp.as_deref() != Some(config.audience.as_str()))
        || claims
            .azp
            .as_deref()
            .is_some_and(|azp| azp != config.audience)
    {
        return Err(refused(
            "OIDC issuer, subject, audience or time policy refused the token",
        ));
    }
    Ok(VerifiedIdentity {
        issuer: claims.iss,
        subject: claims.sub,
        expires_at: claims.exp,
    })
}
