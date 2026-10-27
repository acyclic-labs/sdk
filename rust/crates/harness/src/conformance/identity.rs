//! Actual OIDC admission into the same FilesystemTurnHost, including zero-read refusals.
mod key_sets;
mod tokens;
use crate::conformance::{Backend, Result, admission::fixture::Fixture};
use acyclic_harness_core::{
    Capabilities,
    conversation::VolumeOperation,
    core::{AggregateKind, Authority, AuthorityRoot},
    filesystem::{TurnAdmission, agent_authority},
    oidc,
};
use acyclic_stream::UnixMillisClock as _;
use serde_json::{Value, json};
use std::sync::atomic::Ordering;
use tokens::*;

pub(crate) async fn verify(backend: &dyn Backend) -> Result {
    let now = acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000;
    let source = Source::keys(&[(7, "one")]);
    let mut verifier = oidc::Verifier::new(config()?);
    verifier.refresh(&source, now).await?;
    let original = claims(now);
    let valid = token(7, "one", &original)?;
    admit(backend, &verifier, &source, &valid, now, true).await?;
    external(backend, &verifier, &valid, SUBJECT, now).await?;
    admit(
        backend,
        &verifier,
        &source,
        &token(8, "one", &original)?,
        now,
        false,
    )
    .await?;
    for (field, value) in [
        ("exp", json!(now)),
        ("aud", json!("another-host")),
        ("iss", json!("https://another-issuer.invalid")),
        ("sub", json!("another-user")),
        ("nbf", json!(now + 100)),
        ("sub", json!("")),
    ] {
        let mut changed = original.clone();
        changed[field] = value;
        admit(
            backend,
            &verifier,
            &source,
            &token(7, "one", &changed)?,
            now,
            false,
        )
        .await?;
    }
    for (field, value) in [
        ("alg", json!("none")),
        ("alg", json!("HS256")),
        ("alg", json!("HS384")),
        ("alg", json!("HS512")),
        ("jku", json!("https://attacker.invalid/keys")),
        ("x5u", json!("https://attacker.invalid/key")),
        ("jwk", json!({"kty":"oct","k":"Zm9yZ2Vk"})),
        ("crit", json!(["unknown"])),
    ] {
        admit(
            backend,
            &verifier,
            &source,
            &header_mutation(&valid, field, value)?,
            now,
            false,
        )
        .await?;
    }
    admit(
        backend,
        &verifier,
        &source,
        &token(7, "unknown", &original)?,
        now,
        false,
    )
    .await?;
    let mut multi = original.clone();
    multi["aud"] = json!([AUDIENCE, "another-client"]);
    admit(
        backend,
        &verifier,
        &source,
        &token(7, "one", &multi)?,
        now,
        false,
    )
    .await?;
    multi["azp"] = json!("another-client");
    admit(
        backend,
        &verifier,
        &source,
        &token(7, "one", &multi)?,
        now,
        false,
    )
    .await?;
    multi["azp"] = json!(AUDIENCE);
    admit(
        backend,
        &verifier,
        &source,
        &token(7, "one", &multi)?,
        now,
        true,
    )
    .await?;
    let mut short = original.clone();
    short["exp"] = json!(now + 20);
    admit(
        backend,
        &verifier,
        &source,
        &token(7, "one", &short)?,
        now,
        true,
    )
    .await?;
    rotation(backend, now).await?;
    refresh_bounds(now).await?;
    key_sets::verify(backend, now).await?;
    Ok(())
}

async fn admit(
    backend: &dyn Backend,
    verifier: &oidc::Verifier,
    source: &Source,
    token: &str,
    now: u64,
    allowed: bool,
) -> Result {
    let key_reads = source.reads.load(Ordering::SeqCst);
    let result = check(
        backend,
        verifier,
        token,
        now,
        Expected {
            subject: SUBJECT,
            allowed,
            maximum: Some(60),
            expires_at: expires(token)?,
        },
    )
    .await;
    if source.reads.load(Ordering::SeqCst) != key_reads {
        return Err("OIDC token admission performed key IO".into());
    }
    result
}

struct Expected<'a> {
    subject: &'a str,
    allowed: bool,
    maximum: Option<u32>,
    expires_at: u64,
}

pub(crate) async fn external(
    backend: &dyn Backend,
    verifier: &oidc::Verifier,
    token: &str,
    subject: &str,
    now: u64,
) -> Result {
    let expires_at = verifier.verify(token, now)?.expires_at();
    check(
        backend,
        verifier,
        token,
        now,
        Expected {
            subject,
            allowed: true,
            maximum: None,
            expires_at,
        },
    )
    .await
}

async fn check(
    backend: &dyn Backend,
    verifier: &oidc::Verifier,
    token: &str,
    now: u64,
    expected: Expected<'_>,
) -> Result {
    let fixture = Fixture::new(backend).await?;
    let audience = Authority {
        kind: AggregateKind::Conversation,
        id: "workload".into(),
    };
    let root = AuthorityRoot::new("conformance", [7; 32], audience.clone());
    let grants = Capabilities::new([
        fixture
            .binding
            .volume
            .capability(VolumeOperation::Read, &mut None)?,
        fixture
            .binding
            .volume
            .capability(VolumeOperation::Write, &mut None)?,
    ]);
    let parent = root.bootstrap_scope(
        "identity-parent",
        &grants,
        Some(acyclic_harness_core::AgentId::from_bytes(
            fixture.path.id().to_bytes(),
        )),
        &mut None,
    )?;
    let policy = oidc::AdmissionPolicy::new(expected.subject.into(), audience.clone(), parent)?;
    let policy = match expected.maximum {
        Some(seconds) => policy.with_maximum_seconds(seconds)?,
        None => policy,
    };
    // The parent was minted above using the runtime clock. Do not verify that
    // newly minted scope at the earlier instant when this vector began.
    let now = now.max(acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000);
    let admission = verifier.admit(token, &root, &policy, now);
    if !fixture.no_turn_io() {
        return Err("OIDC token admission performed turn IO".into());
    }
    match admission {
        Err(acyclic_harness_core::Error::Unauthorized(_)) if !expected.allowed => Ok(()),
        Err(error) => Err(format!("OIDC admission: {error}").into()),
        Ok(admission) if expected.allowed => {
            let TurnAdmission::Admitted { scope, .. } = &admission else {
                return Err("OIDC acquired a local-root fallback".into());
            };
            if scope.capabilities() != &grants {
                return Err("OIDC widened or changed the host grant".into());
            }
            let trust = root.public_workload_trust()?;
            let deadline = expected
                .expires_at
                .min(now + u64::from(expected.maximum.unwrap_or(900)));
            scope.verify_workload(&trust, &audience, deadline - 1, &mut None)?;
            if scope
                .verify_workload(&trust, &audience, deadline, &mut None)
                .is_ok()
            {
                return Err("OIDC scope exceeded the token/host deadline".into());
            }
            agent_authority(
                &root,
                &admission,
                &fixture.path,
                std::slice::from_ref(&fixture.binding),
                &[],
            )?;
            fixture
                .check(backend, root, admission, true, "OIDC valid token")
                .await
        }
        Ok(_) => Err("OIDC admitted an invalid token".into()),
    }
}

async fn rotation(backend: &dyn Backend, now: u64) -> Result {
    let now = now.max(acyclic_stream::SystemUnixMillisClock.now_unix_millis() / 1000);
    let mut source = Source::keys(&[(7, "one")]);
    let limits = oidc::Limits {
        refresh_interval_seconds: 1,
        maximum_age_seconds: 5,
        ..Default::default()
    };
    let mut verifier = oidc::Verifier::new(config()?.with_limits(limits)?);
    verifier.refresh(&source, now).await?;
    let one = token(7, "one", &claims(now))?;
    let two = token(8, "two", &claims(now))?;
    admit(backend, &verifier, &source, &two, now, false).await?;
    source.bytes = Source::keys(&[(7, "one"), (8, "two")]).bytes;
    verifier.refresh(&source, now + 1).await?;
    verifier.verify(&one, now + 1)?;
    verifier.verify(&two, now + 1)?;
    source.bytes = Source::keys(&[(8, "two")]).bytes;
    verifier.refresh(&source, now + 2).await?;
    admit(backend, &verifier, &source, &one, now + 2, false).await?;
    admit(backend, &verifier, &source, &two, now + 2, true).await?;
    source.fail = true;
    if verifier.refresh(&source, now + 3).await.is_ok() {
        return Err("failed refresh was accepted".into());
    }
    let reads = source.reads.load(Ordering::SeqCst);
    if verifier.refresh(&source, now + 3).await.is_ok()
        || source.reads.load(Ordering::SeqCst) != reads
    {
        return Err("failed JWKS refresh bypassed rate bounds".into());
    }
    verifier.verify(&two, now + 6)?;
    admit(backend, &verifier, &source, &two, now + 7, false).await?;
    Ok(())
}

async fn refresh_bounds(now: u64) -> Result {
    let mut source = Source::keys(&[(7, "one")]);
    let mut verifier = oidc::Verifier::new(config()?);
    verifier.refresh(&source, now).await?;
    let valid = token(7, "one", &claims(now))?;
    let mut duplicate = Source::keys(&[(7, "one"), (8, "one")]);
    let mut confused: Value = serde_json::from_slice(&source.bytes)?;
    confused["keys"][0]["alg"] = json!("HS256");
    let mut malformed: Value = serde_json::from_slice(&source.bytes)?;
    malformed["keys"][0]["x"] = json!("AQ");
    for (index, body) in [
        std::mem::take(&mut duplicate.bytes),
        serde_json::to_vec(&confused)?,
        serde_json::to_vec(&malformed)?,
        Source::keys(&[(7, "one"); 17]).bytes,
        vec![b' '; 65_537],
    ]
    .into_iter()
    .enumerate()
    {
        source.bytes = body;
        let at = now + u64::try_from(index)? + 1;
        if verifier.refresh(&source, at).await.is_ok() {
            return Err("invalid JWKS replaced last-good keys".into());
        }
        verifier.verify(&valid, at)?;
    }
    if verifier.verify(&valid, now - 1).is_ok() {
        return Err("clock rollback accepted cached keys".into());
    }
    Ok(())
}
