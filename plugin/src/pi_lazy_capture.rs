//! Native lazy selections retain the original authored/overlay/shadow cut.

use crate::{LocalAuthorityBackend, LocalLazyWorkspace, LocalObjectBackend, SharedPhysicalRoot};
use acyclic_fs::{CancellationToken, LazySnapshotRef, WorkBudget, WorkCounters};
use acyclic_harness::{
    Error, Result,
    fork::{
        Capture, CapturedResource, ForkCaptureProvider, ForkRequest, ForkSeed, ForkSeedVerifier,
        ForkSelection, ResourceRevision,
    },
    resources::ProviderRef,
};
use acyclic_stream::BoxProviderFuture;
use std::{future::Future, sync::Arc};

const LAZY_DIAGNOSTIC_LABEL_BYTES: u64 =
    crate::pi_root_provider::ROOT_ARTIFACT_DIAGNOSTIC_LABEL_BYTES
        + crate::pi_root_provider::pi_serde_diagnostic_labels_bytes(&[
            "ResourceRevision",
            "History",
            "Project",
            "PrivateVolume",
            "Artifact",
            "Sandbox",
            "Checkpoint",
            "Context",
            "history",
            "project",
            "private_volume",
            "artifact",
            "sandbox",
            "checkpoint",
            "context",
            "kind",
            "reference",
            "ResourceRef",
            "ProviderRef",
            "namespace",
            "family",
            "version",
            "key",
            "provider",
            "volume",
            "generation",
            "paths",
            "Capture",
            "Captured",
            "Unsupported",
            "InFlight",
            "Indeterminate",
            "captured",
            "unsupported",
            "in_flight",
            "indeterminate",
            "CapturedResource",
            "source",
            "revision",
            "value",
            "ForkSelection",
            "required",
        ]);

pub(crate) struct PiLazyCapture {
    source: Arc<LocalLazyWorkspace>,
    frozen: LazySnapshotRef,
    revision: ResourceRevision,
    _metadata_custody: acyclic_fs::NativeAllocation,
}

impl PiLazyCapture {
    pub(crate) async fn new(
        source: Arc<LocalLazyWorkspace>,
        frozen: LazySnapshotRef,
        budget: WorkBudget,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<Arc<Self>> {
        if !budget.has_finite_allowances() || frozen.workspace_id != source.workspace().id() {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi native cut does not belong to its actual source workspace",
            });
        }
        let (revision, mut metadata) = {
            #[derive(serde::Serialize)]
            struct Provider {
                namespace: &'static str,
                family: &'static str,
                version: &'static str,
            }
            #[derive(serde::Serialize)]
            struct Reference<'a> {
                kind: acyclic_harness::resources::ResourceKind,
                provider: Provider,
                key: &'a [u8],
                version: Option<&'static str>,
            }
            #[derive(serde::Serialize)]
            #[serde(tag = "kind", content = "reference", rename_all = "snake_case")]
            enum Revision<'a> {
                Artifact(Reference<'a>),
            }
            let bytes = crate::pi_root_provider::canonical_pi_value(
                &Revision::Artifact(Reference {
                    kind: acyclic_harness::resources::ResourceKind::Artifact,
                    provider: Provider {
                        namespace: "acyclic.pi",
                        family: "native-lazy",
                        version: "1",
                    },
                    key: &frozen.id.into_bytes(),
                    version: None,
                }),
                crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
                work,
            )?;
            let (revision, custody) = crate::pi_root_provider::guarded_pi_decode::<ResourceRevision>(
                &bytes.0,
                crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
                crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64,
                acyclic_harness::wire::ErrorCode::Invalid,
                acyclic_harness::JsonValueSchema::None,
                LAZY_DIAGNOSTIC_LABEL_BYTES,
                work,
            )?;
            (revision, custody)
        };
        source
            .validate_snapshot(
                frozen,
                &acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                    work,
                    (work).cancellation(),
                )
                .map_err(crate::pi_lazy_capture::native_lazy_error)?,
            )
            .await
            .map_err(native_lazy_error)?;
        let mut slot = {
            let claim_bytes =
                (std::mem::size_of::<Self>() + 2 * std::mem::size_of::<usize>()) as u64;
            work.claim(
                claim_bytes,
                u64::from(claim_bytes != 0),
                acyclic_fs::WorkCounters::UNCHARGED,
            )
        }?;
        metadata.absorb(&mut slot)?;
        Ok(Arc::new(Self {
            source,
            frozen,
            revision,
            _metadata_custody: metadata,
        }))
    }

    pub(crate) const fn frozen(&self) -> LazySnapshotRef {
        self.frozen
    }
    pub(crate) fn selection(
        &self,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(ForkSelection, acyclic_fs::NativeAllocation)> {
        #[derive(serde::Serialize)]
        struct Selection<'a> {
            required: bool,
            revision: &'a ResourceRevision,
        }
        let maximum = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64;
        let bytes = crate::pi_root_provider::canonical_pi_value(
            &Selection {
                required: true,
                revision: &self.revision,
            },
            maximum,
            work,
        )?;
        crate::pi_root_provider::guarded_pi_decode::<ForkSelection>(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            LAZY_DIAGNOSTIC_LABEL_BYTES,
            work,
        )
    }

    async fn selected(
        &self,
        request: &ForkRequest,
        selection: &ForkSelection,
        work: &acyclic_fs::NativeMaterializationWork,
    ) -> Result<(Capture, acyclic_fs::NativeAllocation)> {
        if !selection.required || selection.revision != self.revision {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi lazy capture differs from its original required immutable selection",
            });
        }
        let mut member = false;
        for selected in &request.selections {
            work.charge(WorkCounters {
                items_examined: 1,
                ..WorkCounters::UNCHARGED
            })?;
            if selected == selection {
                member = true;
            }
        }
        if !member {
            return Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unauthorized,
                reason: "Pi lazy capture is not a member of the original fork request",
            });
        }
        self.source
            .validate_snapshot(
                self.frozen,
                &acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                    work,
                    (work).cancellation(),
                )
                .map_err(crate::pi_lazy_capture::native_lazy_error)?,
            )
            .await
            .map_err(native_lazy_error)?;
        #[derive(serde::Serialize)]
        struct Resource<'a> {
            source: &'a ResourceRevision,
            revision: &'a ResourceRevision,
        }
        #[derive(serde::Serialize)]
        #[serde(tag = "kind", content = "value", rename_all = "snake_case")]
        enum ResultValue<'a> {
            Captured(Resource<'a>),
        }
        let maximum = crate::MAXIMUM_CONTROL_MESSAGE_BYTES as u64;
        let bytes = crate::pi_root_provider::canonical_pi_value(
            &ResultValue::Captured(Resource {
                source: &self.revision,
                revision: &self.revision,
            }),
            maximum,
            work,
        )?;
        crate::pi_root_provider::guarded_pi_decode::<Capture>(
            &bytes.0,
            maximum,
            maximum,
            acyclic_harness::wire::ErrorCode::Invalid,
            acyclic_harness::JsonValueSchema::None,
            LAZY_DIAGNOSTIC_LABEL_BYTES,
            work,
        )
    }
}

impl ForkCaptureProvider for PiLazyCapture {
    fn provider(&self) -> &ProviderRef {
        self.revision.provider()
    }
    fn capture<'a>(
        &'a self,
        _request: &'a ForkRequest,
        _selection: &'a ForkSelection,
    ) -> BoxProviderFuture<'a, Result<Capture>> {
        Box::pin(async {
            Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "Pi capture requires native work",
            })
        })
    }
    fn reconcile<'a>(
        &'a self,
        _request: &'a ForkRequest,
        _selection: &'a ForkSelection,
    ) -> BoxProviderFuture<'a, Result<Option<Capture>>> {
        Box::pin(async {
            Err(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "Pi capture reconciliation requires native work",
            })
        })
    }

    fn capture_with_native_work<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<acyclic_harness::NativeFutureBundle<'a, (Capture, acyclic_fs::NativeAllocation)>>
    {
        acyclic_harness::NativeFutureBundle::new(work, || self.selected(request, selection, work))
    }

    fn reconcile_with_native_work<'a>(
        &'a self,
        request: &'a ForkRequest,
        selection: &'a ForkSelection,
        work: &'a acyclic_fs::NativeMaterializationWork,
    ) -> Result<
        acyclic_harness::NativeFutureBundle<'a, (Option<Capture>, acyclic_fs::NativeAllocation)>,
    > {
        acyclic_harness::NativeFutureBundle::new(work, || async move {
            let (capture, custody) = self.selected(request, selection, work).await?;
            Ok((Some(capture), custody))
        })
    }
}

impl ForkSeedVerifier for PiLazyCapture {
    fn provider(&self) -> &ProviderRef {
        self.revision.provider()
    }
    fn verify<'a, 'w: 'a>(
        &'a self,
        seed: &'a ForkSeed,
        observer: &'a mut Option<&'w mut dyn acyclic_harness::store::HistoryIo>,
    ) -> Result<acyclic_harness::store::HistoryContentFuture<'a, ()>> {
        let work = observer
            .as_ref()
            .and_then(|io| io.original_native_work())
            .cloned()
            .ok_or(Error::NativeBoundary {
                code: acyclic_harness::wire::ErrorCode::Unsupported,
                reason: "Pi seed verification requires native work",
            })?;
        acyclic_harness::NativeFutureBundle::new(&work.clone(), || async move {
            let mut found = false;
            for capture in &seed.resources {
                work.charge(WorkCounters {
                    items_examined: 1,
                    ..WorkCounters::UNCHARGED
                })?;
                if capture.source.provider() == self.revision.provider()
                    || capture.revision.provider() == self.revision.provider()
                {
                    if found || capture.source != self.revision || capture.revision != self.revision
                    {
                        return Err(Error::NativeBoundary {
                            code: acyclic_harness::wire::ErrorCode::Unauthorized,
                            reason: "published Pi lazy capture changed its original native cut",
                        });
                    }
                    found = true;
                }
            }
            if !found {
                return Err(Error::NativeBoundary {
                    code: acyclic_harness::wire::ErrorCode::Unauthorized,
                    reason: "published Pi fork lost its required immutable native cut",
                });
            }
            self.source
                .validate_snapshot(
                    self.frozen,
                    &acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(
                        &work,
                        (&work).cancellation(),
                    )
                    .map_err(crate::pi_lazy_capture::native_lazy_error)?,
                )
                .await
                .map_err(native_lazy_error)
        })
        .map(acyclic_harness::store::HistoryContentFuture::Native)
    }
}

/// The Root's actual created-source guard spans the watcher, rebind and freeze on the command's existing ledger.
pub(crate) async fn freeze_root_source_with_native_work<P, F, G>(
    original_root: acyclic_harness::TaskId,
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    physical: &SharedPhysicalRoot,
    source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork,
    mut guard: G,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)>
where
    P: acyclic_stream::StreamProvider,
    G: FnMut() -> F + Send,
    F: Future<Output = Result<()>> + Send,
{
    if owner.task_id() != original_root || context.durable_task_id() != Some(original_root) {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Root snapshot requires its actual original Root context and owner",
        });
    }
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
        )
        .await?;
    let observed = physical.observe_with_native_work(work, &mut guard).await?;
    guard().await?;
    let accounting =
        acyclic_fs::lazy_workspace::LazyTreapWork::native_with_work(work, work.cancellation())
            .map_err(native_lazy_error)?;
    let rebound = source
        .rebind_source(&accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if rebound.as_state().source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source rebind changed its guarded watcher observation",
        });
    }
    let frozen = source
        .snapshot(&accounting)
        .await
        .map_err(native_lazy_error)?;
    guard().await?;
    if frozen.source != observed.0.source {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Conflict,
            reason: "Root source freeze changed its guarded watcher observation",
        });
    }
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
        )
        .await?;
    drop(rebound);
    drop(observed.0);
    Ok((frozen, observed.1))
}

pub(crate) fn native_lazy_error(source: acyclic_fs::LazyWorkspaceError) -> Error {
    let (source, custody) = source.into_native_failure();
    match source {
        acyclic_fs::LazyWorkspaceError::NativeWork(cause) => Error::NativeWork { cause, custody },
        source => Error::NativeSource { source, custody },
    }
}

/// Only an active source's ORIGINAL native view owner may checkpoint its dirty mounts.
pub(crate) async fn freeze_child_source<P: acyclic_stream::StreamProvider>(
    context: &acyclic_harness::runtime::TaskContext,
    owner: &acyclic_harness::durable_host::TaskJournalOwner<P>,
    view: &acyclic_harness::filesystem::NativeVolumeView<LocalAuthorityBackend, LocalObjectBackend>,
    source: &LocalLazyWorkspace,
    work: &acyclic_fs::NativeMaterializationWork,
) -> Result<(LazySnapshotRef, acyclic_fs::NativeAllocation)> {
    if context.durable_task_id() != Some(owner.task_id()) {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi source checkpoint requires its actual source task context",
        });
    }
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
        )
        .await?;
    let checkpoint = view
        .checkpoint_projects_with_native_work(&owner, work)?
        .await?;
    owner
        .verify(
            false,
            &mut Some(&mut acyclic_harness::crate::store::HistoryWork::new(crate::filesystem::PreparationIo::from_native_work(work)?)),
        )
        .await?;
    let mut selected = checkpoint
        .0
        .iter()
        .filter(|(_, snapshot)| snapshot.workspace_id == source.workspace().id());
    let snapshot = selected
        .next()
        .ok_or_else(|| Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi checkpoint has no actual selected source workspace",
        })?
        .1;
    if selected.next().is_some() {
        return Err(Error::NativeBoundary {
            code: acyclic_harness::wire::ErrorCode::Unauthorized,
            reason: "Pi checkpoint repeats a selected source workspace",
        });
    }
    drop(checkpoint.0);
    Ok((snapshot, checkpoint.1))
}
