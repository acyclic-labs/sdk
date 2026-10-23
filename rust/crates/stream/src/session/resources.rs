use super::*;
use acyclic_auth::authority::{PolicyDecision, Request, Target};
use acyclic_auth::contract::{GrantKind, Limits as Usage, Principal, RecordAttributes, ResourcePattern};
use acyclic_auth::verify::VerifiedCapability;

/// Immutable channel identity and the current verified credential. Only session
/// admission can construct this value; resource providers cannot mint authority.
#[derive(Clone)]
pub struct ResourceAuthorization {
    authority: Arc<Authority>,
    resource: Arc<str>,
    capability: Arc<std::sync::Mutex<Arc<VerifiedCapability>>>,
}
impl ResourceAuthorization {
    pub(super) fn new(authority: Arc<Authority>, resource: String, capability: Arc<VerifiedCapability>) -> Self {
        Self { authority, resource: resource.into(), capability: Arc::new(std::sync::Mutex::new(capability)) }
    }
    /// Issuer-attested structured identity, including delegation lineage.
    pub fn principal(&self) -> Result<Principal, StreamError> {
        let capability = self.capability.lock().map_err(|_| StreamError::AccessDenied)?;
        Ok(capability.effective().principal().clone())
    }
    /// Current trusted runtime clock for the volume policy's time conditions.
    pub fn now_unix_seconds(&self) -> Result<u64, StreamError> {
        self.authority.snapshot().map(|snapshot| snapshot.now_unix_seconds).map_err(|_| StreamError::AccessDenied)
    }
    /// Checks every capability ancestor, audience, holder, expiry and full path,
    /// intersecting the owning volume's policy. Headers belong to this entry.
    pub fn authorize(&self, resource: &str, keys: &[Vec<u8>], attributes: &RecordAttributes, policy: PolicyDecision) -> Result<(), StreamError> {
        if !acyclic_auth::valid_resource(resource) || !acyclic_auth::authority::resource_matches(&ResourcePattern { name: self.resource.to_string(), prefix: true }, resource) {
            return Err(StreamError::AccessDenied);
        }
        let snapshot = self.authority.snapshot().map_err(|_| StreamError::AccessDenied)?;
        let capability = self.capability.lock().map_err(|_| StreamError::AccessDenied)?.clone();
        capability.authorize(snapshot.trust.as_ref(), &Request {
            target: Target::Resource { action: "fs/list", resource },
            audience: &self.authority.audience,
            holder: self.authority.holder.as_deref(),
            now_unix_seconds: snapshot.now_unix_seconds,
            keys, attributes, usage: &Usage::default(), cpu_millis: 0, memory_bytes: 0,
        }, policy).map_err(|_| StreamError::AccessDenied)
    }
    /// Pushes semantic filesystem grant prefixes into the provider index. The
    /// full lossless path is still authorized per entry after indexed selection.
    pub fn filter_paths(&self, volume_resource: &str, filter: crate::FollowFilter) -> Result<crate::FollowFilter, StreamError> {
        let capability = self.capability.lock().map_err(|_| StreamError::AccessDenied)?.clone();
        let mut wire = filter.transport_filter()?;
        for grants in capability.effective().grants() {
            let mut ranges = Vec::new();
            let mut all = false;
            for grant in &grants.grants {
                let resource = match &grant.kind {
                    GrantKind::Resource(grant) => grant,
                    GrantKind::Execution(grant) => &grant.grant,
                    GrantKind::Billing(grant) => &grant.grant,
                    GrantKind::WebsocketConnect(_) => continue,
                };
                if !resource.actions.iter().any(|action| action == "fs/list") { continue; }
                if resource.resource.prefix && acyclic_auth::authority::resource_matches(&resource.resource, volume_resource) { all = true; break; }
                if resource.resource.name == volume_resource {
                    ranges.push(wire::KeyRange { start: Bytes::new(), end: Bytes::from_static(&[0]) });
                } else if let Some(path) = resource.resource.name.strip_prefix(volume_resource).and_then(|tail| tail.strip_prefix('/')) {
                    path_ranges(path, resource.resource.prefix, &mut ranges)?;
                }
            }
            if !all {
                if ranges.is_empty() { return Err(StreamError::AccessDenied); }
                wire.all_of.push(wire::SessionFilterClause { keys: ranges, attributes: vec![], all_keys: true });
            }
        }
        if let Some(path) = self.resource.strip_prefix(volume_resource).and_then(|tail| tail.strip_prefix('/')) {
            let mut ranges = Vec::new();
            path_ranges(path, true, &mut ranges)?;
            wire.all_of.push(wire::SessionFilterClause { keys: ranges, attributes: vec![], all_keys: true });
        }
        crate::FollowFilter::new(wire)
    }
    /// Waits on the runtime clock without polling, independently of data credit.
    pub async fn expires_at(&self, unix_seconds: u64) {
        self.authority.state.expires_at(unix_seconds).await;
    }
    pub(super) fn renew(&self, next: Arc<VerifiedCapability>) -> Result<(), authority::Rejection> {
        let mut current = self.capability.lock().map_err(|_| authority::Rejection::Forbidden)?;
        // Queued entries were authorized under these exact grants and identity.
        // A changed ceiling closes the channel so replay uses the new scope.
        if current.effective().principal() != next.effective().principal()
            || !current.effective().grants().eq(next.effective().grants()) {
            return Err(authority::Rejection::Forbidden);
        }
        *current = next;
        Ok(())
    }
}

/// Canonical product follow passed to its owning provider after verification.
pub struct ResourceFollowRequest {
    /// Owner-canonical resource name, including any path prefix.
    pub resource: String,
    /// Next stream sequence and optional immutable preceding commit anchor.
    pub from: u64,
    /// Preceding commit when the caller resumes an accepted cut.
    pub preceding: Option<crate::CommitId>,
    /// Requested selection intersected with all credential filter ceilings.
    pub filter: crate::FollowFilter,
    /// Verified channel authority; per-entry checks remain mandatory.
    pub authorization: ResourceAuthorization,
}

/// Owning-product follow with an independent revocation observation.
pub struct ResourceSubscription {
    /// Synchronous policy fence checked again before queued data is disclosed.
    pub revalidate: PublicationRevalidation,
    /// Completes on policy revocation even when record credit is exhausted.
    pub terminal: crate::BoxProviderFuture<'static, StreamError>,
    /// Indexed provider follow with ordered skipped-record cursor progress.
    pub open: crate::BoxProviderFuture<'static, Result<crate::FollowStream, StreamError>>,
}

/// Owning-product routing. A provider must intersect current volume policy and
/// verified authority before returning changes, and refuse unenforceable stores.
pub trait ResourceProvider: crate::ProviderPlatform + 'static {
    /// Prepares one follow; this does not accept caller-supplied policy claims.
    fn follow(&self, request: ResourceFollowRequest) -> Result<ResourceSubscription, StreamError>;
}

pub(super) fn source(
    open: crate::BoxProviderFuture<'static, Result<crate::FollowStream, StreamError>>,
    identity: wire::SessionChannel,
    preceding: Option<crate::CommitId>,
) -> Source {
    Box::pin(stream::try_unfold((Some(open), None::<crate::FollowStream>), move |(open, mut records)| async move {
        if let Some(open) = open {
            records = Some(open.await?);
            return Ok(Some((Event::Opened(preceding), (None, records))));
        }
        let update = records.as_mut().ok_or(StreamError::Unavailable)?.next().await.ok_or(StreamError::Unavailable)??;
        let event = match update {
            crate::FollowUpdate::Record(record) => Event::Record(record),
            crate::FollowUpdate::Progress { next, preceding } => Event::Progress(wire::SessionWatermark {
                accepted: next, durable: 0, after: Some(cursor(next)),
                preceding_commit_id: Bytes::copy_from_slice(preceding.as_bytes()),
            }),
        };
        Ok(Some((event, (None, records))))
    }).map(move |event| (identity, event)))
}

fn path_ranges(encoded: &str, prefix: bool, ranges: &mut Vec<wire::KeyRange>) -> Result<(), StreamError> {
    let mut key = Vec::new();
    for (index, component) in encoded.split('/').enumerate() {
        if index != 0 { key.push(b'/'); }
        let bytes = acyclic_auth::decode_component(component).map_err(|_| StreamError::InvalidArgument)?;
        if bytes.contains(&b'/') || bytes.contains(&0) { return Err(StreamError::InvalidArgument); }
        key.extend_from_slice(&bytes);
    }
    if key.len() >= crate::MAX_RECORD_KEY_BYTES {
        key.truncate(crate::MAX_RECORD_KEY_BYTES);
        let mut end = key.clone();
        while end.last() == Some(&255) { end.pop(); }
        if let Some(byte) = end.last_mut() { *byte += 1; }
        ranges.push(wire::KeyRange { start: key.into(), end: end.into() });
        return Ok(());
    }
    let mut end = key.clone(); end.push(0);
    ranges.push(wire::KeyRange { start: key.clone().into(), end: end.into() });
    if prefix {
        key.push(b'/'); let mut end = key.clone();
        *end.last_mut().ok_or(StreamError::InvalidArgument)? += 1;
        ranges.push(wire::KeyRange { start: key.into(), end: end.into() });
    }
    Ok(())
}
