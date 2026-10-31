//! Compose the existing native source watch with Auth's credential cache.
use super::*;
use acyclic_auth::credential::{
    CredentialAction, CredentialBinding, CredentialCache, CredentialCacheError, CredentialError,
    CredentialLease, CredentialRequest, CredentialResolution, CredentialRevision,
    CredentialSnapshot, CredentialSource, CredentialValidity, CredentialWatch, ResolvedCredential,
};
use acyclic_native_runtime::private_file::PublishedFile;
use std::sync::atomic::AtomicBool;
use std::sync::{Mutex, OnceLock, Weak};

/// A credential source could not establish its original observation.
#[derive(Debug, thiserror::Error)]
pub enum CredentialSourceError {
    /// Preserve the native producer's failure and physical cleanup owner.
    #[error("credential source observation failed: {0}")]
    Source(#[from] DemandError),
    /// The platform cannot establish a delivery cut; never reuse stale bytes.
    #[error("credential source delivery is unconfirmed")]
    Unconfirmed,
    /// The original producer, invocation or completion owner is gone.
    #[error("credential source owner is unavailable")]
    Unavailable,
}

fn error(error: impl std::error::Error + Send + Sync + 'static) -> CredentialError {
    CredentialError::Provider(Box::new(error))
}

/// The original provider either observed its opened source or completed its
/// original durable publication. Observing a concurrent winner cannot claim
/// this attempt's publication, and a descriptor alone cannot establish freshness.
pub enum NativeCredentialUpdate {
    /// Keep the revision supplied with the original version-opened input.
    Observed(CredentialSnapshot),
    /// Retain the same completed publisher; admission also requires its exact
    /// selected-name event and no later relevant mutation at a confirmed cut.
    Published {
        /// Provider-owned token and authenticated absolute expiry.
        snapshot: CredentialSnapshot,
        /// The same descriptor used by the original durable publisher.
        file: PublishedFile,
    },
}

#[derive(Default)]
struct Observation {
    file_identity: Option<[u8; 32]>,
    // One most recent exact selected-name rebound, never a history of events.
    rebound: Option<(u64, [u8; 32])>,
    // Closing a writable publisher can itself notify the native watch. Retain
    // it until its snapshot is already invalid, rather than suppress that event.
    published: Option<PublishedFile>,
}

struct State {
    // Each binding is a separate Auth cache partition, even when two callers
    // share the same native root or select the same file.
    identity: [u8; 16],
    epoch: u64,
    path: NamespacePath,
    observation: Mutex<Observation>,
    // One generation per relevant event, unrelated to permission or expiry.
    generation: AtomicU64,
    lost: AtomicBool,
    cache: OnceLock<Weak<CredentialCache>>,
    watch: OnceLock<Weak<Watch>>,
    // The source sink or retained cache can outlive its public source handle.
    _allocation: crate::NativeAllocation,
}

impl State {
    fn revision(&self) -> Option<CredentialRevision> {
        if self.lost.load(Ordering::Acquire) {
            return None;
        }
        Some(self.revision_at(self.generation.load(Ordering::Acquire)))
    }

    fn revision_at(&self, generation: u64) -> CredentialRevision {
        let mut revision = [0; 32];
        revision[..16].copy_from_slice(&self.identity);
        revision[16..24].copy_from_slice(&self.epoch.to_le_bytes());
        revision[24..].copy_from_slice(&generation.to_le_bytes());
        CredentialRevision::Bytes(revision)
    }

    fn advance(&self) -> Option<u64> {
        match self
            .generation
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
                value.checked_add(1)
            }) {
            Ok(previous) => Some(previous + 1),
            Err(_) => {
                self.lost.store(true, Ordering::Release);
                None
            }
        }
    }

    fn notify(&self, terminal: bool) {
        // Publish validity before waking consumers; no observation/accounting
        // lock is held while a waker may reenter Auth.
        if let Some(cache) = self.cache.get().and_then(Weak::upgrade) {
            let _ = if terminal || self.lost.load(Ordering::Acquire) {
                cache.invalidate()
            } else {
                cache.source_changed()
            };
        }
    }

    fn invalidate(&self, terminal: bool) {
        if terminal {
            self.lost.store(true, Ordering::Release);
        }
        let retired = match self.observation.lock() {
            Ok(mut observation) => {
                observation.rebound = None;
                self.advance();
                observation.published.take()
            }
            Err(_) => {
                self.lost.store(true, Ordering::Release);
                None
            }
        };
        self.notify(terminal);
        drop(retired);
    }
}

impl SourceChangeSink for State {
    fn source_changed(
        &self,
        changes: &[SourceChange],
        _: &mut crate::kernel::KernelAccounting<'_>,
    ) -> Result<(), DemandError> {
        let mut observation = self
            .observation
            .lock()
            .map_err(|_| DemandError::SourceUnavailable)?;
        let mut affected = false;
        let mut retired = None;
        let mut changes = changes.iter().peekable();
        while let Some(change) = changes.next() {
            let relevant = match change {
                SourceChange::Name(path) | SourceChange::Entry(path) => self.path.is_within(path),
                SourceChange::Node(node) => observation.file_identity.as_ref() == Some(node),
                SourceChange::Everything | SourceChange::Unconfirmed => true,
            };
            if !relevant {
                continue;
            }
            affected = true;
            observation.rebound = None;
            if retired.is_none() {
                retired = observation.published.take();
            }
            let generation = self.advance();
            // NativeChanges emits the observed node immediately after its name
            // in the SAME ordered batch. Consume only that pair. An in-place
            // entry event, ancestor change, missing pair or later node mutation
            // cannot establish a publication observation.
            if let SourceChange::Name(path) = change {
                if path == &self.path {
                    if let Some(SourceChange::Node(node)) = changes.peek().copied() {
                        let node = *node;
                        changes.next();
                        observation.file_identity = Some(node);
                        observation.rebound = generation.map(|generation| (generation, node));
                    }
                }
            }
        }
        drop(observation);
        if affected {
            self.notify(false);
        }
        drop(retired);
        Ok(())
    }

    fn delivery_lost(&self) {
        self.invalidate(true);
    }
}

struct Watch {
    physical: Box<dyn SourceWatch>,
    original: crate::NativeMaterializationWork,
}

impl Watch {
    async fn fence(self: Arc<Self>) -> Result<(), CredentialError> {
        spawn_blocking(crate::obs::in_context(
            crate::obs::caller_context(),
            move || {
                let stop = self.original.cancellation();
                NativeHostWork::check(&self.original).map_err(error)?;
                let mut accounting = crate::kernel::KernelAccounting::native(&self.original, stop);
                let confirmed = self.physical.fence(&mut accounting).map_err(error)?;
                NativeHostWork::check(&self.original).map_err(error)?;
                if confirmed != SourceFence::Confirmed || !self.physical.is_exact() {
                    return Err(error(CredentialSourceError::Unconfirmed));
                }
                Ok(())
            },
        ))
        .await
        .map_err(|_| error(CredentialSourceError::Unavailable))?
    }
}

struct Owner {
    cache: Arc<CredentialCache>,
    state: Arc<State>,
    watch: Arc<Watch>,
    // Retain the same held root and prepaid allocation through physical drop.
    _source: NativeDemandSource,
}

impl Drop for Owner {
    fn drop(&mut self) {
        self.state.invalidate(true);
    }
}

/// One protected native source and original invocation lifetime. Clones share
/// Auth's cache/singleflight and the same physical watch. Request leases and
/// change futures never keep this source owner alive.
///
/// The caller must supply the already admitted source containing exactly the
/// refresh callback's protected input. Paths and source revisions grant no
/// authority. The callback retains its original transport, funded allocation,
/// conditional publication and authenticated expiry; it must not start a new
/// native work root. A source that cannot confirm delivery is refused.
#[derive(Clone)]
pub struct NativeCredentialSource(Arc<Owner>);

impl NativeCredentialSource {
    /// Register before loading any credential. Refresh gets the exact rejected
    /// lease, this source's revision and the selected file's original identity
    /// and version, plus the file opened at exactly that version. Read through
    /// that original file, retain conditional publication, and return a snapshot
    /// as `Observed` with the same revision. Parsing stays with the provider. Other files
    /// under this root do not invalidate this source.
    ///
    /// `Published` must retain the original completed publisher. The same watch
    /// must observe its selected-name rebound, with no later relevant mutation,
    /// before a confirmed cut. Missing/foreign evidence refuses even when the
    /// write committed; never redispatch a publication after this refusal.
    /// The clock must be the original owner's clock, never a relative TTL.
    pub async fn bind<F, Fut, C>(
        source: NativeDemandSource,
        path: NamespacePath,
        original: crate::NativeMaterializationWork,
        clock: C,
        refresh: F,
    ) -> Result<Self, CredentialError>
    where
        F: Fn(Option<CredentialLease>, CredentialRevision, SourceNode, Box<dyn DemandFile>) -> Fut
            + Send
            + Sync
            + 'static,
        Fut: Future<Output = Result<NativeCredentialUpdate, CredentialError>> + Send + 'static,
        C: Fn() -> u64 + Send + Sync + 'static,
    {
        let allocation = original
            .claim(
                path.retained_bytes()
                    + crate::source_watch::arc_control_bytes::<Owner>()
                    + crate::source_watch::arc_control_bytes::<State>()
                    + crate::source_watch::arc_control_bytes::<Watch>()
                    + crate::source_watch::arc_control_bytes::<CredentialCache>()
                    + crate::source_watch::arc_control_bytes::<F>(),
                5,
                WorkCounters::UNCHARGED,
            )
            .map_err(error)?;
        let state = Arc::new(State {
            identity: *crate::identity::fresh_uuid().as_bytes(),
            epoch: source.reference().epoch,
            path,
            observation: Mutex::new(Observation::default()),
            generation: AtomicU64::new(0),
            lost: AtomicBool::new(false),
            cache: OnceLock::new(),
            watch: OnceLock::new(),
            _allocation: allocation,
        });
        let refresh = Arc::new(refresh);
        let cache = Arc::new(CredentialCache::new(
            {
                let state = state.clone();
                let source = source.clone();
                move |rejected| {
                    let state = state.clone();
                    let source = source.clone();
                    let refresh = refresh.clone();
                    let watch = state.watch.get().and_then(Weak::upgrade);
                    async move {
                        let watch =
                            watch.ok_or_else(|| error(CredentialSourceError::Unavailable))?;
                        NativeHostWork::check(&watch.original).map_err(error)?;
                        // A prior cached publication is no longer reusable when
                        // refresh starts. Retire its descriptor before the next
                        // writer, then drain its original close notification.
                        let retired = state
                            .observation
                            .lock()
                            .map_err(|_| error(CredentialSourceError::Unavailable))?
                            .published
                            .take();
                        if retired.is_some() {
                            drop(retired);
                            watch.clone().fence().await?;
                        }
                        let initial_generation = state.generation.load(Ordering::Acquire);
                        let revision = state
                            .revision()
                            .ok_or_else(|| error(CredentialSourceError::Unavailable))?;
                        let node = source
                            .lookup(
                                source.reference(),
                                &state.path,
                                &watch.original,
                                watch.original.cancellation(),
                            )
                            .await
                            .map_err(error)?
                            .ok_or_else(|| error(DemandError::Absent))?;
                        if node.kind != SourceNodeKind::RegularFile {
                            return Err(error(DemandError::NotRegularFile));
                        }
                        state
                            .observation
                            .lock()
                            .map_err(|_| error(CredentialSourceError::Unavailable))?
                            .file_identity = Some(node.file_identity);
                        let file = source
                            .open_file(
                                source.reference(),
                                &state.path,
                                node.version,
                                &watch.original,
                                watch.original.cancellation(),
                            )
                            .await
                            .map_err(error)?;
                        let result = refresh(rejected, revision.clone(), node, file).await?;
                        match result {
                            NativeCredentialUpdate::Observed(snapshot) => {
                                watch.fence().await?;
                                Ok(snapshot)
                            }
                            NativeCredentialUpdate::Published { snapshot, file } => {
                                if snapshot.revision() != revision {
                                    return Err(error(CredentialCacheError::Stale));
                                }
                                // Observe the original held descriptor once at cold
                                // publication admission, never on a warm check.
                                let original = watch.original.clone();
                                let (file, identity) = spawn_blocking(move || {
                                    let mut work = original;
                                    NativeHostWork::check(&work).map_err(error)?;
                                    let metadata =
                                        crate::native_host::stat_file(file.as_file(), &mut work)
                                            .map_err(error)?;
                                    NativeHostWork::check(&work).map_err(error)?;
                                    Ok::<_, CredentialError>((file, file_identity(&metadata)))
                                })
                                .await
                                .map_err(|_| error(CredentialSourceError::Unavailable))??;
                                watch.fence().await?;
                                let mut observation = state
                                    .observation
                                    .lock()
                                    .map_err(|_| error(CredentialSourceError::Unavailable))?;
                                let (generation, observed) = observation
                                    .rebound
                                    .ok_or_else(|| error(CredentialCacheError::Stale))?;
                                if state.lost.load(Ordering::Acquire)
                                    || generation <= initial_generation
                                    || generation != state.generation.load(Ordering::Acquire)
                                    || observed != identity
                                {
                                    return Err(error(CredentialCacheError::Stale));
                                }
                                observation.rebound = None;
                                // Any preceding accepted publication was retired by
                                // the rebound event, before this lock was acquired.
                                observation.published = Some(file);
                                Ok(snapshot.with_revision(state.revision_at(generation)))
                            }
                        }
                    }
                }
            },
            {
                let state = state.clone();
                let stop = original.cancellation().clone();
                move || CredentialValidity {
                    now_unix_seconds: clock(),
                    revision: (!stop.is_cancelled()).then(|| state.revision()).flatten(),
                }
            },
        ));
        state
            .cache
            .set(Arc::downgrade(&cache))
            .map_err(|_| error(CredentialSourceError::Unavailable))?;
        let physical = spawn_blocking(crate::obs::in_context(crate::obs::caller_context(), {
            let source = source.clone();
            let original = original.clone();
            let state = state.clone();
            move || source.watch(state, &original, original.cancellation())
        }))
        .await
        .map_err(|_| error(CredentialSourceError::Unavailable))?
        .map_err(error)?
        .ok_or_else(|| error(CredentialSourceError::Unconfirmed))?;
        let watch = Arc::new(Watch { physical, original });
        state
            .watch
            .set(Arc::downgrade(&watch))
            .map_err(|_| error(CredentialSourceError::Unavailable))?;
        let owner = Arc::new(Owner {
            cache,
            state,
            watch,
            _source: source,
        });
        owner.watch.clone().fence().await?;
        Ok(Self(owner))
    }
}

struct Request {
    // Release weak control custody before the cache's original allocation.
    owner: Weak<Owner>,
    lease: CredentialLease,
    cache: Arc<CredentialCache>,
}

impl CredentialRequest for Request {
    fn token(&self) -> &str {
        self.lease.token()
    }

    fn binding(&self) -> Option<CredentialBinding> {
        Some(CredentialBinding {
            revision: self.lease.revision(),
            expires_at_unix_seconds: self.lease.expires_at(),
        })
    }

    fn check(&self) -> CredentialAction<()> {
        if let Err(failure) = self.cache.check(&self.lease) {
            return Err(error(failure)).into();
        }
        let watch = self.owner.upgrade().map(|owner| owner.watch.clone());
        let cache = self.cache.clone();
        let lease = self.lease.clone();
        CredentialAction::Pending(Box::pin(async move {
            let watch = watch.ok_or_else(|| error(CredentialSourceError::Unavailable))?;
            watch.fence().await?;
            cache.check(&lease).map_err(error)
        }))
    }

    fn reject(&self) -> CredentialAction<bool> {
        self.cache.reject(&self.lease).map_err(error).into()
    }
}

impl CredentialSource for NativeCredentialSource {
    fn resolve(&self) -> CredentialResolution<'_> {
        Box::pin(async move {
            let stop = self.0.watch.original.cancellation_wait().map_err(error)?;
            let resolution = self.0.cache.resolve();
            futures::pin_mut!(stop, resolution);
            let lease = match futures::future::select(stop, resolution).await {
                futures::future::Either::Left(_) => {
                    return Err(error(CredentialSourceError::Unavailable));
                }
                futures::future::Either::Right((result, _)) => result.map_err(error)?,
            };
            ResolvedCredential::from_custodian(Request {
                lease,
                cache: self.0.cache.clone(),
                owner: Arc::downgrade(&self.0),
            })
            .await
        })
    }

    fn watch(&self) -> CredentialAction<Option<CredentialWatch>> {
        let result = (|| {
            let watch = self.0.cache.watch().map_err(error)?;
            let validity = watch.validity();
            let stop = self.0.watch.original.cancellation_wait().map_err(error)?;
            let state = self.0.state.clone();
            let cancelled = self.0.watch.original.cancellation().clone();
            Ok(Some(CredentialWatch::from_custodian(
                validity,
                async move {
                    futures::pin_mut!(watch, stop);
                    match futures::future::select(stop, watch).await {
                        futures::future::Either::Right((result, _)) => {
                            if cancelled.is_cancelled() || state.revision().is_none() {
                                Err(CredentialCacheError::Unavailable)
                            } else {
                                result
                            }
                        }
                        futures::future::Either::Left(_) => Err(CredentialCacheError::Unavailable),
                    }
                },
            )))
        })();
        result.into()
    }
}

#[cfg(all(test, target_os = "linux"))]
mod tests;
