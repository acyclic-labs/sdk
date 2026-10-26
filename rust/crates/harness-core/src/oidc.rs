//! Trusted host composition: cached identity verification, then ordinary scope delegation.
//! Token admission never reads keys, storage, environment or the network.
mod claims;
mod keys;

use crate::{
    Error, Result,
    core::{Authority, AuthorityRoot, Scope},
    filesystem::TurnAdmission,
};
use acyclic_stream::{BoxProviderFuture, BoxProviderStream, ProviderPlatform};
use bytes::Bytes;
use std::collections::BTreeMap;

pub use claims::VerifiedIdentity;
pub use keys::{Config, Limits, SigningAlgorithm};

/// A single GET of the pinned URI, without redirects, retries or discovery.
/// The shared refresher bounds the streamed body and its total duration.
pub trait JwksSource: ProviderPlatform {
    /// Return a lazy body only for a successful response from this exact URI.
    fn fetch<'a>(
        &'a self,
        uri: &'a str,
    ) -> BoxProviderFuture<'a, Result<BoxProviderStream<'static, Result<Bytes>>>>;
}

/// Host-owned mapping for one exact subject. The parent already binds agent,
/// grants, audience and original issuer custody; JWT claims cannot select them.
pub struct AdmissionPolicy {
    subject: String,
    audience: Authority,
    parent: Scope,
    maximum_seconds: u32,
}

impl AdmissionPolicy {
    /// Bind an authenticated, agent-specific parent scope. The default is 15 minutes.
    pub fn new(subject: String, audience: Authority, parent: Scope) -> Result<Self> {
        if subject.is_empty() || parent.agent().is_none() {
            return Err(Error::Invalid(
                "OIDC policy requires a subject and agent-bound parent".into(),
            ));
        }
        Ok(Self {
            subject,
            audience,
            parent,
            maximum_seconds: 900,
        })
    }

    /// Narrow the host lifetime to 1..=900 seconds.
    pub fn with_maximum_seconds(mut self, seconds: u32) -> Result<Self> {
        if seconds == 0 || seconds > 900 {
            return Err(Error::Invalid(
                "OIDC policy lifetime must be 1..=900 seconds".into(),
            ));
        }
        self.maximum_seconds = seconds;
        Ok(self)
    }
}

/// A last-good, atomically replaced verification snapshot. Refresh is explicit
/// and serial through `&mut self`, separate from all request admission.
pub struct Verifier {
    config: Config,
    keys: BTreeMap<String, keys::Key>,
    fetched_at: Option<u64>,
    attempted_at: Option<u64>,
}

impl Verifier {
    /// Starts without keys, refusing every token until a successful refresh.
    pub fn new(config: Config) -> Self {
        Self {
            config,
            keys: BTreeMap::new(),
            fetched_at: None,
            attempted_at: None,
        }
    }

    /// Fetch a bounded replacement. Failed attempts consume the refresh interval
    /// and preserve the last-good set, whose original age continues to increase.
    pub async fn refresh(&mut self, source: &dyn JwksSource, now: u64) -> Result<()> {
        if self.attempted_at.is_some_and(|last| {
            now.checked_sub(last)
                .is_none_or(|age| age < self.config.limits.refresh_interval_seconds)
        }) {
            return Err(refused("OIDC key refresh is rate limited"));
        }
        self.attempted_at = Some(now);
        use acyclic_stream::runtime::{Executor as _, PlatformExecutor};
        use futures::future::{Either, select};
        let fetch = keys::fetch(source, &self.config);
        let timeout = PlatformExecutor.sleep(10_000);
        futures::pin_mut!(fetch, timeout);
        let replacement = match select(fetch, timeout).await {
            Either::Left((result, _)) => result?,
            Either::Right(_) => return Err(refused("OIDC key refresh timed out")),
        };
        self.keys = replacement;
        self.fetched_at = Some(now);
        Ok(())
    }

    /// Verify signature and claims against a fresh cached set. `now` is supplied
    /// by the trusted host UTC clock, never by the caller presenting the token.
    pub fn verify(&self, token: &str, now: u64) -> Result<VerifiedIdentity> {
        let age = self.fetched_at.and_then(|at| now.checked_sub(at));
        if age.is_none_or(|age| age >= self.config.limits.maximum_age_seconds) {
            return Err(refused("OIDC verification keys are unavailable or stale"));
        }
        claims::verify(token, now, &self.config, &self.keys)
    }

    /// The host selects a subject policy; only a verified matching identity can
    /// attenuate its original parent. No root is created and no grants are added.
    pub fn admit(
        &self,
        token: &str,
        root: &AuthorityRoot,
        policy: &AdmissionPolicy,
        now: u64,
    ) -> Result<TurnAdmission> {
        let identity = self.verify(token, now)?;
        if identity.subject() != policy.subject {
            return Err(refused("OIDC subject is not admitted by host policy"));
        }
        let issuer = root.with_audience(policy.audience.clone());
        let scope = issuer.issuer(&mut None)?.issue(
            &policy.parent,
            "oidc",
            policy.parent.capabilities(),
        )?;
        let deadline = identity
            .expires_at()
            .min(now.saturating_add(u64::from(policy.maximum_seconds)));
        let scope = scope.with_workload_expiry(
            deadline,
            &issuer.public_workload_trust()?,
            &policy.audience,
            now,
        )?;
        Ok(TurnAdmission::Admitted {
            audience: policy.audience.clone(),
            scope,
        })
    }
}

fn refused(reason: &'static str) -> Error {
    Error::Unauthorized(reason.into())
}
