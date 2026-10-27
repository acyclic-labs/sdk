//! Mixed-family key sets use generated adversarial vectors and the unchanged
//! captured Dex public RSA key. No unrelated key is eligible to admit a token.
use super::*;

pub(super) async fn verify(backend: &dyn Backend, now: u64) -> Result {
    let mut source = Source::keys(&[(7, "one")]);
    let original: Value = serde_json::from_slice(&source.bytes)?;
    let eligible = original["keys"][0].clone();
    let unrelated = vec![
        json!({"kty":"RSA","alg":"RS256","kid":"one"}),
        json!({"kty":"EC","crv":"P-256","alg":"ES256"}),
        json!({"kty":"oct","alg":"HS256"}),
        json!({"kty":"oct","alg":"none"}),
        json!({"kty":"RSA"}),
        json!({"kty":"EC","crv":"P-384"}),
        json!({"kty":"OKP","alg":"EdDSA","use":"enc"}),
        json!({"kty":"OKP","alg":"EdDSA","key_ops":["encrypt"]}),
    ];
    let mut mixed = unrelated.clone();
    mixed.insert(3, eligible.clone());
    source.bytes = serde_json::to_vec(&json!({"keys":mixed}))?;
    let mut verifier = oidc::Verifier::new(config()?);
    verifier.refresh(&source, now).await?;
    let valid = token(7, "one", &claims(now))?;
    admit(backend, &verifier, &source, &valid, now, true).await?;
    for algorithm in ["RS256", "ES256", "HS256", "none"] {
        admit(
            backend,
            &verifier,
            &source,
            &header_mutation(&valid, "alg", json!(algorithm))?,
            now,
            false,
        )
        .await?;
    }

    // Algorithm omission is permitted only when the key family matches.
    let mut implicit = eligible.clone();
    implicit
        .as_object_mut()
        .ok_or("JWK object missing")?
        .remove("alg");
    source.bytes = serde_json::to_vec(&json!({"keys":[unrelated[0], implicit]}))?;
    verifier.refresh(&source, now + 1).await?;
    verifier.verify(&valid, now + 1)?;

    // A replacement containing only unrelated keys must preserve last-good.
    let mut invalid = vec![json!({"keys":unrelated})];
    for (field, value) in [
        ("x", json!("AQ")),
        ("kid", Value::Null),
        ("crv", Value::Null),
        ("kty", json!("RSA")),
        ("key_ops", json!(["verify", "verify"])),
        ("key_ops", json!(["verify", "encrypt"])),
    ] {
        let mut malformed = eligible.clone();
        malformed[field] = value;
        invalid.push(json!({"keys":[eligible, malformed]}));
    }
    invalid.push(json!({"keys":[eligible, eligible]}));
    for (index, body) in invalid.into_iter().enumerate() {
        let at = now + u64::try_from(index)? + 2;
        source.bytes = serde_json::to_vec(&body)?;
        if verifier.refresh(&source, at).await.is_ok() {
            return Err("ineligible or malformed JWKS replaced last-good keys".into());
        }
        verifier.verify(&valid, at)?;
    }
    admit(backend, &verifier, &source, &valid, now + 10, true).await?;

    // The reported RSA/EC mixture exercises the same selection with a real RSA
    // key, including duplicate kid values belonging to other signing families.
    let dex: Value =
        serde_json::from_slice(include_bytes!("../../../fixtures/identity/dex/jwks.body"))?;
    let rsa = dex["keys"][0].clone();
    let mut other = eligible;
    other["kid"] = rsa["kid"].clone();
    source.bytes = serde_json::to_vec(&json!({"keys":[
        {"kty":"EC","alg":"ES256","crv":"P-256"}, other, rsa
    ]}))?;
    let rsa_config = oidc::Config::new(
        ISSUER.into(),
        AUDIENCE.into(),
        format!("{ISSUER}/keys"),
        oidc::SigningAlgorithm::Rs256,
    )?;
    let mut rsa_verifier = oidc::Verifier::new(rsa_config);
    rsa_verifier.refresh(&source, now).await?;
    admit(backend, &rsa_verifier, &source, &valid, now, false).await?;
    Ok(())
}
