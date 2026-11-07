use super::*;
use acyclic_auth::authority::{PolicyDecision, Request, Target};
use acyclic_auth::contract::{
    GrantKind, Limits as Usage, Principal, RecordAttributes, ResourcePattern,
};
use acyclic_auth::verify::VerifiedCapability;

/// Immutable channel identity and the current verified credential. Only session
/// admission can construct this value; resource providers cannot mint authority.
#[derive(Clone)]
pub struct ResourceAuthorization {
    authority: Arc<Authority>,
    pub(super) resource: Arc<ResourcePattern>,
    capability: Arc<std::sync::Mutex<Arc<VerifiedCapability>>>,
    pub(super) action: &'static str,
    call: Option<(Arc<ResourcePattern>, &'static str)>,
}
impl ResourceAuthorization {
    pub(super) fn new(
        authority: Arc<Authority>,
        resource: String,
        capability: Arc<VerifiedCapability>,
    ) -> Self {
        Self {
            authority,
            resource: Arc::new(ResourcePattern {
                name: resource,
                prefix: true,
            }),
            capability: Arc::new(std::sync::Mutex::new(capability)),
            action: crate::authorization::RESOURCE_FOLLOW_ACTION.as_str(),
            call: None,
        }
    }
    pub(super) fn dependent(&self, resource: ResourcePattern, action: &'static str) -> Self {
        Self {
            authority: Arc::clone(&self.authority),
            resource: Arc::new(resource),
            capability: Arc::clone(&self.capability),
            action,
            call: Some((Arc::clone(&self.resource), self.action)),
        }
    }
    /// The channel's current credential; `renew` replaces it in place.
    pub(super) fn current(&self) -> Result<Arc<VerifiedCapability>, StreamError> {
        self.capability
            .lock()
            .map(|capability| Arc::clone(&capability))
            .map_err(|_| StreamError::AccessDenied)
    }
    /// Issuer-attested structured identity, including delegation lineage.
    pub fn principal(&self) -> Result<Principal, StreamError> {
        Ok(self.current()?.effective().principal().clone())
    }
    /// Current trusted runtime clock for the volume policy's time conditions.
    pub fn now_unix_seconds(&self) -> Result<u64, StreamError> {
        self.authority
            .snapshot()
            .map(|snapshot| snapshot.now_unix_seconds)
            .map_err(|_| StreamError::AccessDenied)
    }
    /// Checks every capability ancestor, audience, holder, expiry and full path,
    /// intersecting the owning volume's policy. Headers belong to this entry.
    pub fn authorize(
        &self,
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &RecordAttributes,
        policy: PolicyDecision,
    ) -> Result<(), StreamError> {
        self.authorize_account(resource, keys, attributes, policy, None)
    }

    /// Check the certified account and resource against the same current
    /// credential. The owning product still supplies its current policy.
    pub fn authorize_in_account(
        &self,
        account: &[u8],
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &RecordAttributes,
        policy: PolicyDecision,
    ) -> Result<(), StreamError> {
        self.authorize_account(resource, keys, attributes, policy, Some(account))
    }

    fn authorize_account(
        &self,
        resource: &str,
        keys: &[Vec<u8>],
        attributes: &RecordAttributes,
        policy: PolicyDecision,
        account: Option<&[u8]>,
    ) -> Result<(), StreamError> {
        if !acyclic_auth::valid_resource(resource)
            || !acyclic_auth::authority::resource_matches(&self.resource, resource)
        {
            return Err(StreamError::AccessDenied);
        }
        let snapshot = self
            .authority
            .snapshot()
            .map_err(|_| StreamError::AccessDenied)?;
        let capability = self.current()?;
        if let Some(account) = account {
            let certified = capability
                .account_id()
                .map_err(|_| StreamError::AccessDenied)?;
            if certified.as_bytes() != account {
                return Err(StreamError::AccessDenied);
            }
        }
        let request = Request {
            target: Target::Resource {
                action: self.action,
                resource,
            },
            audience: &self.authority.audience,
            holder: self.authority.holder.as_deref(),
            now_unix_seconds: snapshot.now_unix_seconds,
            keys,
            attributes,
            usage: &Usage {
                rps: None,
                burst: None,
                bytes_per_s: None,
                channels: None,
                concurrency: None,
                bytes: None,
                spend_micros: None,
                tokens: None,
            },
            cpu_millis: 0,
            memory_bytes: 0,
        };
        let authorize = |request: &Request<'_>, policy| {
            capability
                .authorize(
                    snapshot.trust.as_ref(),
                    request,
                    policy,
                    acyclic_auth::authority::Scope::Exact,
                )
                .map_err(|_| StreamError::AccessDenied)
        };
        if let Some((resource, action)) = &self.call {
            // The call grant remains necessary after suspension or renewal.
            // Dependent entry facts must not stand in for the admitted call.
            let attributes =
                RecordAttributes::try_new(vec![]).map_err(|_| StreamError::InvalidArgument)?;
            authorize(
                &Request {
                    target: Target::Resource {
                        action,
                        resource: &resource.name,
                    },
                    keys: &[],
                    attributes: &attributes,
                    ..request
                },
                PolicyDecision::Allow,
            )?;
        }
        authorize(&request, policy)
    }
    /// Pushes semantic filesystem grant prefixes into the provider index. The
    /// full lossless path is still authorized per entry after indexed selection.
    pub fn filter_paths(
        &self,
        volume_resource: &str,
        filter: &crate::FollowFilter,
    ) -> Result<crate::FollowFilter, StreamError> {
        let capability = self.current()?;
        let mut wire = filter.transport_filter()?;
        for grants in capability.effective().grants() {
            let mut ranges = Vec::new();
            let mut all = false;
            for grant in &grants.grants {
                let resource = match &grant.kind {
                    GrantKind::Resource(grant) => grant,
                    GrantKind::Execution(grant) => &grant.grant,
                    GrantKind::Billing(grant) => &grant.grant,
                    GrantKind::Filesystem(grant) => &grant.grant,
                    GrantKind::WebsocketConnect(_) => continue,
                };
                if !resource.actions.iter().any(|action| action == self.action) {
                    continue;
                }
                if resource.resource.prefix
                    && acyclic_auth::authority::resource_matches(
                        &resource.resource,
                        volume_resource,
                    )
                {
                    all = true;
                    break;
                }
                if resource.resource.name == volume_resource {
                    ranges.push(wire::KeyRange {
                        start: Bytes::new(),
                        end: Bytes::from_static(&[0]),
                    });
                } else if let Some(path) = resource
                    .resource
                    .name
                    .strip_prefix(volume_resource)
                    .and_then(|tail| tail.strip_prefix('/'))
                {
                    // A prefix pattern's trailing `/` is not a component,
                    // exactly as `resource_matches` reads it.
                    let path = if resource.resource.prefix {
                        path.trim_end_matches('/')
                    } else {
                        path
                    };
                    path_ranges(path, resource.resource.prefix, &mut ranges)?;
                }
            }
            if !all {
                if ranges.is_empty() {
                    return Err(StreamError::AccessDenied);
                }
                wire.all_of.push(wire::SessionFilterClause {
                    keys: ranges,
                    attributes: vec![],
                    all_keys: true,
                });
            }
        }
        if let Some(path) = self
            .resource
            .name
            .strip_prefix(volume_resource)
            .and_then(|tail| tail.strip_prefix('/'))
        {
            let mut ranges = Vec::new();
            path_ranges(path, self.resource.prefix, &mut ranges)?;
            wire.all_of.push(wire::SessionFilterClause {
                keys: ranges,
                attributes: vec![],
                all_keys: true,
            });
        }
        crate::FollowFilter::new(wire)
    }
    /// Waits on the runtime clock without polling, independently of data credit.
    pub async fn expires_at(&self, unix_seconds: u64) -> Result<(), StreamError> {
        self.authority
            .state
            .expires_at(unix_seconds)
            .await
            .map_err(|_| StreamError::Unavailable)
    }
    pub(super) fn renew(&self, next: Arc<VerifiedCapability>) -> Result<(), authority::Rejection> {
        let mut current = self
            .capability
            .lock()
            .map_err(|_| authority::Rejection::Forbidden)?;
        // Queued entries were authorized under this certified account, grants
        // and identity. A matching principal does not bind the same account.
        // A changed ceiling closes the channel so replay uses the new scope.
        let (held, renewed) = (current.effective(), next.effective());
        let account = current
            .account_id()
            .map_err(|_| authority::Rejection::Forbidden)?;
        let next_account = next
            .account_id()
            .map_err(|_| authority::Rejection::Forbidden)?;
        if account != next_account
            || held.principal() != renewed.principal()
            || !held.grants().eq(renewed.grants())
        {
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
    pub revalidate: ResourceRevalidation,
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
    opening_cut: bool,
) -> Source {
    Box::pin(
        stream::try_unfold(
            (Some(open), None::<crate::FollowStream>),
            move |(open, mut records)| async move {
                if let Some(open) = open {
                    records = Some(open.await?);
                    return Ok(Some((
                        Event::Opened(preceding, None, None),
                        (None, records),
                    )));
                }
                let event = loop {
                    let update = records
                        .as_mut()
                        .ok_or(StreamError::Unavailable)?
                        .next()
                        .await
                        .ok_or(StreamError::Unavailable)??;
                    let event = match update {
                        crate::FollowUpdate::Opening { tail } => {
                            if !opening_cut {
                                continue;
                            }
                            Event::Progress(wire::SessionWatermark {
                                opening_tail: Some(tail),
                                accepted: tail,
                                ..Default::default()
                            })
                        }
                        crate::FollowUpdate::Record(record) => Event::Record(record),
                        crate::FollowUpdate::Progress { next, preceding } => {
                            Event::Progress(wire::SessionWatermark {
                                opening_tail: None,
                                accepted: next,
                                durable: 0,
                                after: Some(cursor(next)),
                                preceding_commit_id: Bytes::copy_from_slice(preceding.as_bytes()),
                            })
                        }
                    };
                    break event;
                };
                Ok(Some((event, (None, records))))
            },
        )
        .map(move |event| (identity, event)),
    )
}

fn path_ranges(
    encoded: &str,
    prefix: bool,
    ranges: &mut Vec<wire::KeyRange>,
) -> Result<(), StreamError> {
    let mut key = Vec::new();
    for (index, component) in encoded.split('/').enumerate() {
        if index != 0 {
            key.push(b'/');
        }
        for byte in acyclic_auth::component_bytes(component) {
            let byte = byte.map_err(|_| StreamError::InvalidArgument)?;
            if byte == b'/' || byte == 0 {
                return Err(StreamError::InvalidArgument);
            }
            key.push(byte);
        }
    }
    if key.len() >= crate::MAX_RECORD_KEY_BYTES {
        key.truncate(crate::MAX_RECORD_KEY_BYTES);
        let mut end = key.clone();
        while end.last() == Some(&255) {
            end.pop();
        }
        if let Some(byte) = end.last_mut() {
            *byte += 1;
        }
        ranges.push(wire::KeyRange {
            start: key.into(),
            end: end.into(),
        });
        return Ok(());
    }
    let mut end = key.clone();
    end.push(0);
    ranges.push(wire::KeyRange {
        start: key.clone().into(),
        end: end.into(),
    });
    if prefix {
        key.push(b'/');
        let mut end = key.clone();
        *end.last_mut().ok_or(StreamError::InvalidArgument)? += 1;
        ranges.push(wire::KeyRange {
            start: key.into(),
            end: end.into(),
        });
    }
    Ok(())
}

/// Synchronous owning-policy fence checked immediately before disclosure.
#[cfg(not(target_arch = "wasm32"))]
pub type ResourceRevalidation = Arc<dyn Fn() -> Result<(), StreamError> + Send + Sync>;
/// Synchronous browser owning-policy fence checked before disclosure.
#[cfg(target_arch = "wasm32")]
pub type ResourceRevalidation = Arc<dyn Fn() -> Result<(), StreamError>>;

pub(super) fn terminal(
    terminal: crate::BoxProviderFuture<'static, StreamError>,
    identity: wire::SessionChannel,
) -> Source {
    Box::pin(stream::once(async move { (identity, Err(terminal.await)) }))
}
