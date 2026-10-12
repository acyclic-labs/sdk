use super::*;

/// Bounded self-contained local backend options.
#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LocalOptions {
    /// Durable root containing canonical Stream and Objects provider state.
    pub root: PathBuf,
    /// Durable hierarchical Stream provider limits.
    pub stream: acyclic_stream::LocalStreamLimits,
    /// Durable immutable Objects provider limits.
    pub objects: acyclic_objects::LocalObjectsLimits,
    /// Bounded disposable immutable-object accelerator shared by every handle.
    pub object_cache: crate::cache::ObjectCacheOptions,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl LocalOptions {

    /// Creates local options with the SDK's bounded backend defaults.
    #[must_use]
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self {
            root: root.into(),
            stream: acyclic_stream::LocalStreamLimits::default(),
            objects: acyclic_objects::LocalObjectsLimits::default(),
            object_cache: crate::cache::ObjectCacheOptions::default(),
        }
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) struct LocalRootRegistration {
    pub(super) options: LocalOptions,
    pub(super) live: Weak<FsInner<LocalAuthorityBackend, LocalObjectBackend>>,
    pub(super) opening: Arc<tokio::sync::Mutex<()>>,
    pub(super) lifecycle: Arc<tokio::sync::Mutex<()>>,
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) type LocalRootRegistry = std::collections::BTreeMap<PathBuf, LocalRootRegistration>;

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) fn prepare_local_root_open(
    registry: &mut LocalRootRegistry,
    root: &Path,
    options: &LocalOptions,
) -> Arc<tokio::sync::Mutex<()>> {
    if let Some(registration) = registry.get(root) {
        return Arc::clone(&registration.opening);
    }
    let opening = Arc::new(tokio::sync::Mutex::new(()));
    let lifecycle = Arc::new(tokio::sync::Mutex::new(()));
    registry.insert(
        root.to_path_buf(),
        LocalRootRegistration {
            options: options.clone(),
            live: Weak::new(),
            opening: Arc::clone(&opening),
            lifecycle: Arc::clone(&lifecycle),
        },
    );
    opening
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
pub(super) async fn run_local_initialization<T, F, I>(
    ownership: tokio::sync::OwnedMutexGuard<()>,
    span: crate::obs::OperationSpan,
    initialize: I,
) -> Result<T, FsError>
where
    T: Send + 'static,
    F: std::future::Future<Output = T> + Send + 'static,
    I: FnOnce(tokio::sync::OwnedMutexGuard<()>) -> F + Send + 'static,
{
    // Tokio detaches a spawned task when its JoinHandle is dropped. That is intentional here:
    tokio::spawn(async move {
        crate::obs::in_span(&span, async move { initialize(ownership).await }).await
    })
    .await
    .map_err(|_| FsError::LocalInitializationWorker)
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl<O> Fs<LocalAuthorityBackend, O> {

    /// Flushes the authority frames written under
    pub fn flush_deferred_authority(&self) -> Result<(), acyclic_stream::StreamError> {
        self.inner.authority.provider().flush()
    }
}

#[cfg(all(feature = "local", not(target_arch = "wasm32")))]
impl Fs<LocalAuthorityBackend, LocalObjectBackend> {

    /// Observes release of every provider derived from this local root.
    #[doc(hidden)]
    #[must_use]
    pub fn local_root_release_barrier(
        &self,
    ) -> Option<acyclic_native_runtime::OwnershipReleaseBarrier> {
        self.inner
            ._local_root_lifecycle
            .as_ref()
            .map(OwnershipAnchor::release_barrier)
    }


    /// Opens the infrastructure-free durable local composition.
    pub async fn local(options: LocalOptions) -> Result<Self, FsError> {
        let span = crate::obs::span!(
            INFO,
            "acyclic.fs.local",
            outcome = crate::obs::Empty,
            error.kind = crate::obs::Empty,
        );
        let worker_context = span.clone();
        crate::obs::outcome_on(
            &span,
            crate::obs::in_span(&span, async move {
                static REGISTRY: OnceLock<tokio::sync::Mutex<LocalRootRegistry>> = OnceLock::new();
                let requested_root = options.root.clone();
                let root = tokio::task::spawn_blocking(crate::obs::in_context(
                    worker_context.clone(),
                    move || {
                        std::fs::create_dir_all(&requested_root)?;
                        std::fs::canonicalize(requested_root)
                    },
                ))
                .await
                .map_err(|_| FsError::LocalInitializationWorker)?
                .map_err(FsError::LocalRoot)?;
                let mut options = options;
                options.root = root.clone();
                let registry =
                    REGISTRY.get_or_init(|| tokio::sync::Mutex::new(LocalRootRegistry::new()));
                let opening = {
                    let mut registrations = registry.lock().await;
                    if let Some(existing) = registrations.get(&root)
                        && let Some(inner) = existing.live.upgrade()
                    {
                        if existing.options != options {
                            return Err(FsError::LocalOptionsConflict);
                        }
                        return Ok(Self { inner });
                    }
                    prepare_local_root_open(&mut registrations, &root, &options)
                };
                // Opening a root may wait for old provider handles or native I/O to finish. Keep that
                let _opening = opening.lock().await;
                let lifecycle = {
                    let registrations = registry.lock().await;
                    let existing = registrations
                        .get(&root)
                        .ok_or(FsError::LocalInitializationWorker)?;
                    if let Some(inner) = existing.live.upgrade() {
                        if existing.options != options {
                            return Err(FsError::LocalOptionsConflict);
                        }
                        return Ok(Self { inner });
                    }
                    Arc::clone(&existing.lifecycle)
                };
                let ownership = lifecycle.lock_owned().await;
                let open_options = options.clone();
                let fs = run_local_initialization(
                    ownership,
                    worker_context.clone(),
                    move |ownership| async move {
                        let ownership = OwnershipAnchor::new(ownership);
                        Self::open_local_unshared(open_options, Some(ownership)).await
                    },
                )
                .await??;
                let mut registrations = registry.lock().await;
                let existing = registrations
                    .get_mut(&root)
                    .ok_or(FsError::LocalInitializationWorker)?;
                existing.options = options;
                existing.live = Arc::downgrade(&fs.inner);
                Ok(fs)
            })
            .await,
        )
    }


    pub(super) async fn open_local_unshared(
        options: LocalOptions,
        lifecycle: Option<OwnershipAnchor>,
    ) -> Result<Self, FsError> {
        let LocalOptions {
            root,
            stream,
            objects,
            object_cache,
        } = options;
        // The Stream and Objects providers own disjoint directories, so their
        let open_stream = async {
            Ok::<_, FsError>(std::sync::Arc::new(match &lifecycle {
                Some(ownership) => {
                    acyclic_stream::LocalStream::open_with_ownership_anchor(
                        root.join("stream"),
                        stream,
                        ownership.clone(),
                    )
                    .await?
                }
                None => acyclic_stream::LocalStream::open(root.join("stream"), stream).await?,
            }))
        };
        let open_objects = async {
            let maximum_object_bytes = objects.maximum_object_bytes;
            let objects = std::sync::Arc::new(match &lifecycle {
                Some(ownership) => {
                    acyclic_objects::v1::local::LocalObjects::open(
                        root.join("objects"),
                        objects,
                        Some(ownership.clone()),
                    )
                    .await?
                }
                None => {
                    acyclic_objects::v1::local::LocalObjects::open(root.join("objects"), objects, None)
                        .await?
                }
            });
            let bucket = acyclic_objects::v1::wire::BucketRef {
                name: "filesystem-objects".to_owned(),
            };
            match objects
                .head_bucket(acyclic_objects::v1::wire::HeadBucketRequest {
                    bucket: Some(bucket.clone()),
                })
                .await
            {
                Ok(_) => {}
                Err(error) if error.code == acyclic_objects::v1::wire::ErrorCode::NotFound => {
                    objects
                        .create_bucket(acyclic_objects::v1::wire::CreateBucketRequest {
                            name: bucket.name.clone(),
                            mutation: None,
                        })
                        .await
                        .map_err(FsError::LocalObjectsBucket)?;
                }
                Err(error) => return Err(FsError::LocalObjectsBucket(error)),
            }
            let objects = crate::cache::CachedObjectStore::new(
                crate::LogicalObjectStore::new(objects, bucket)
                    .with_object_limit(maximum_object_bytes)?,
                object_cache,
            )?;
            crate::staged_objects::StagedObjects::open(objects, root.clone())
                .await
                .map_err(FsError::LocalStaging)
        };
        let (stream, objects) = futures::future::join(open_stream, open_objects).await;
        let (stream, objects) = (stream?, objects?);
        Ok(Self {
            inner: Arc::new(FsInner {
                authority: crate::distributed::StreamAuthorityStore::new(stream),
                objects,
                capabilities: EmbeddedCapabilities { durable: cfg!(any(unix, windows)) },
                workspace_namespace: [0; 16],
                path_index: crate::path_index::MemoryGenerationPathIndex::default(),
                _local_root_lifecycle: lifecycle,
            }),
        })
    }
}

