use super::*;
use crate::access::{MAXIMUM_POLICY_RECORD_BYTES, POLICY_METADATA_KEY};
use crate::kernel::{AttributeClass, OwnedSourceChunk};

mod binding;

fn policy_name(
    maximum_bytes: u32,
    accounting: &mut KernelAccounting<'_>,
) -> Result<Owned<AttributeName>, FsError> {
    let copied = WorkCounters {
        bytes_copied: POLICY_METADATA_KEY.len() as u64,
        ..WorkCounters::UNCHARGED
    };
    accounting.admit(copied)?;
    let mut bytes = Owned::<Vec<u8>>::allocate(POLICY_METADATA_KEY.len(), accounting)?;
    bytes.value.extend_from_slice(POLICY_METADATA_KEY);
    accounting.record_completed(copied)?;
    match AttributeName::new(AttributeClass::PosixXattr, bytes.value, maximum_bytes) {
        Ok(name) => Ok(Owned::new(name, bytes.allocation, bytes.bytes)),
        Err(error) => {
            retire_allocation(bytes.allocation, bytes.bytes, accounting)?;
            Err(CanonicalDecodeError::Invariant(error.into()).into())
        }
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Fs<A, O> {
    pub(super) async fn initial_policy_metadata(
        &self,
        id: VolumeId,
        config: VolumeConfig,
        bytes: &[u8],
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<FileMetadata, FsError> {
        if bytes.len() as u64 > MAXIMUM_POLICY_RECORD_BYTES.min(config.limits.maximum_read_bytes) {
            return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
        }
        let volume = Volume {
            fs: self.clone(),
            id,
            config,
        };
        let name = policy_name(config.limits.maximum_component_bytes, accounting)?;
        let result = attributes::change_attribute(
            &volume,
            FileMetadata::default(),
            &name.value,
            Some((bytes, NamedAttributeWriteMode::Create)),
            MAXIMUM_POLICY_RECORD_BYTES.min(config.limits.maximum_read_bytes),
            accounting,
            cancellation,
        )
        .await;
        name.retire(accounting)?;
        result
    }
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> Checkout<A, O> {
    /// Selects the shared policy program using this checkout's authenticated
    /// metadata revision. A head movement alone never invalidates its paths.
    /// The first caller pays compilation work; subsequent callers pay only the
    /// revision lookup and their own principal/capability binding.
    pub async fn compiled_access_policy<W: FsWork>(
        &mut self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Arc<crate::access::CompiledPolicy>, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .compiled_access_policy_accounted(&mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    async fn compiled_access_policy_accounted(
        &mut self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Arc<crate::access::CompiledPolicy>, FsError> {
        let metadata = self
            .read_metadata_with_accounting(self.root.root_file_id, accounting, cancellation)
            .await?;
        let fs = self.volume.fs.clone();
        let cache = &fs.inner.policy_cache;
        let revision = metadata.named_attributes;
        accounting.charge_items(1)?;
        if let Some(view) = cache.get(self.volume.id, revision) {
            return Ok(view);
        }
        let name = policy_name(
            self.volume.config.limits.maximum_component_bytes,
            accounting,
        )?;
        let bytes = attributes::read_attribute(
            &self.volume,
            metadata,
            &name.value,
            MAXIMUM_POLICY_RECORD_BYTES,
            accounting,
            cancellation,
        )
        .await;
        name.retire(accounting)?;
        let bytes = bytes?.ok_or(FsError::InvalidAccessPolicy)?;
        // No provider I/O is performed under this single-flight lock. A
        // concurrent miss may read the same record, but compiles it once.
        let _compilation = cache
            .compilation
            .lock()
            .map_err(|_| FsError::InvalidAccessPolicy)?;
        if let Some(view) = cache.get(self.volume.id, revision) {
            return Ok(view);
        }
        compile_cached_policy(&self.volume, revision, &bytes.bytes, accounting)
    }

    /// Reads the policy record from this checkout's pinned volume metadata.
    pub async fn read_access_policy<W: FsWork>(
        &mut self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<
        (
            Option<OwnedSourceChunk>,
            crate::kernel::MetadataField<ObjectId>,
        ),
        FsError,
    > {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            let name = policy_name(
                self.volume.config.limits.maximum_component_bytes,
                &mut accounting,
            )?;
            let result = async {
                let metadata = self
                    .read_metadata_with_accounting(
                        self.root.root_file_id,
                        &mut accounting,
                        cancellation,
                    )
                    .await?;
                let bytes = attributes::read_attribute(
                    &self.volume,
                    metadata,
                    &name.value,
                    MAXIMUM_POLICY_RECORD_BYTES,
                    &mut accounting,
                    cancellation,
                )
                .await?;
                Ok((bytes, metadata.named_attributes))
            }
            .await;
            name.retire(&mut accounting)?;
            result
        }
        .await;
        work.finish(result, &accounting)
    }

    /// Authenticated policy revision without reading the policy payload.
    pub async fn access_policy_revision<W: FsWork>(
        &mut self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<crate::kernel::MetadataField<ObjectId>, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .read_metadata_with_accounting(self.root.root_file_id, &mut accounting, cancellation)
            .await
            .map(|metadata| metadata.named_attributes);
        work.finish(result, &accounting)
    }

    /// Stages an owner-authorized policy record in the private overlay.
    /// Publication uses the checkout's ordinary generation fence.
    pub async fn write_access_policy<W: FsWork>(
        &mut self,
        bytes: &[u8],
        mode: NamedAttributeWriteMode,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<(), FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = async {
            accounting.check()?;
            if self.mode.mutations == MutationMode::None {
                return Err(FsError::MutationNotAllowed);
            }
            if bytes.len() as u64
                > MAXIMUM_POLICY_RECORD_BYTES.min(self.volume.config.limits.maximum_read_bytes)
            {
                return Err(FsError::FileRead(FileRangeReadError::InvalidRange));
            }
            #[cfg(not(target_arch = "wasm32"))]
            if let Some(original) = accounting.native_work() {
                self.validate_native_work(original)?;
            }
            let name = policy_name(
                self.volume.config.limits.maximum_component_bytes,
                &mut accounting,
            )?;
            let result = async {
                let root = self.root.root_file_id;
                let metadata = self
                    .read_metadata_with_accounting(root, &mut accounting, cancellation)
                    .await?;
                let metadata = attributes::change_attribute(
                    &self.volume,
                    metadata,
                    &name.value,
                    Some((bytes, mode)),
                    MAXIMUM_POLICY_RECORD_BYTES.min(self.volume.config.limits.maximum_read_bytes),
                    &mut accounting,
                    cancellation,
                )
                .await?;
                self.set_attributes_with_accounting(
                    root,
                    metadata,
                    None,
                    &mut accounting,
                    cancellation,
                )
                .await
            }
            .await;
            name.retire(&mut accounting)?;
            result
        }
        .await;
        work.finish(result, &accounting)
    }
}

// Keep each owner's natural borrow in its existing async factory. Both
// entry points expand the same accounted protocol, with no owner copy or
// additional future layer.
macro_rules! policy_revision {
    ($volume:expr, $accounting:ident, $cancellation:ident) => {{
        let before = readonly_standalone_live($accounting);
        let result = async {
            let volume = $volume;
            let (generation, observed) = volume
                .resolve_head_generation($accounting, $cancellation)
                .await?;
            let metadata = {
                let (root, _root_allocation) = read_generation_root_with_accounting(
                    &volume.fs.inner.objects,
                    generation,
                    volume.config,
                    $accounting,
                    $cancellation,
                )
                .await?;
                if root.volume_id != volume.id {
                    return Err(FsError::VolumeMismatch);
                }
                let (record, _record_allocation) = lookup_file_record_async_with_accounting(
                    &volume.fs.inner.objects,
                    root.file_table,
                    root.root_file_id,
                    DecodeLimits::for_volume(volume.config),
                    $accounting,
                    $cancellation,
                    None,
                )
                .await?;
                read_record_metadata_to_file_root_with_accounting(
                    volume,
                    record.ok_or(FsError::NotFound)?,
                    $accounting,
                    $cancellation,
                )
                .await?
            };
            let mut authority = authority_work($accounting, $cancellation)?;
            let current = volume
                .fs
                .inner
                .authority
                .head(volume_authority_id(volume.id), &mut authority)
                .await;
            record_authority_work($accounting, &authority)?;
            Ok(
                crate::facade::policy_head_is_current(observed, current?).then_some((
                    GenerationId::from_root(generation),
                    metadata.named_attributes,
                )),
            )
        }
        .await;
        readonly_cleanup_standalone($accounting, before, 0).and(result)
    }};
}

impl<A: AsyncAuthorityStore + 'static, O: AsyncObjectStore + 'static> crate::Workspace<A, O> {
    /// Reads the current policy revision without constructing a checkout.
    /// Returns None if publication overtakes the metadata read.
    pub async fn current_access_policy_revision<W: FsWork>(
        &self,
        work: W,
        cancellation: &CancellationToken,
    ) -> W::Output<Option<(GenerationId, MetadataField<ObjectId>)>, FsError> {
        let mut accounting = work.accounting(cancellation);
        let result = self
            .current_access_policy_revision_accounted(&mut accounting, cancellation)
            .await;
        work.finish(result, &accounting)
    }

    pub(crate) async fn current_access_policy_revision_accounted(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(GenerationId, MetadataField<ObjectId>)>, FsError> {
        policy_revision!(&self.volume, accounting, cancellation)
    }
}

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Volume<A, O> {
    pub(crate) async fn current_access_policy_revision_accounted(
        &self,
        accounting: &mut KernelAccounting<'_>,
        cancellation: &CancellationToken,
    ) -> Result<Option<(GenerationId, MetadataField<ObjectId>)>, FsError> {
        policy_revision!(self, accounting, cancellation)
    }
}

pub(crate) fn policy_head_is_current(observed: Head, current: Head) -> bool {
    observed == current
}

fn compile_cached_policy<A: AsyncAuthorityStore, O: AsyncObjectStore>(
    volume: &Volume<A, O>,
    revision: MetadataField<ObjectId>,
    bytes: &[u8],
    accounting: &mut KernelAccounting<'_>,
) -> Result<Arc<crate::access::CompiledPolicy>, FsError> {
    let cache = &volume.fs.inner.policy_cache;
    // Serde's externally tagged records retain Content while decoding.
    // Admit that bounded temporary before decoding, then retire it with
    // the NFA/DFA preparation storage after the shared entry is funded.
    let decode_bytes = (bytes.len() as u64)
        .checked_mul((size_of::<crate::access::Rule>() + 128) as u64)
        .ok_or(crate::WorkError::Overflow)?;
    let mut decode = accounting.claim_owned(decode_bytes, 1)?;
    let mut stages = [None, None];
    let mut stage = 0;
    let mut admission_error = None;
    let compiled = (|| {
        accounting.charge(WorkCounters {
            items_examined: bytes.len() as u64,
            source_bytes_read: bytes.len() as u64,
            ..WorkCounters::UNCHARGED
        })?;
        let policy = serde_json::from_slice(bytes).map_err(|_| FsError::InvalidAccessPolicy)?;
        let mut remaining =
            usize::try_from(accounting.remaining()?.items_examined).unwrap_or(usize::MAX);
        let before = remaining;
        let compiled = crate::access::CompiledPolicy::compile_with_work(
            &policy,
            volume.config,
            &mut remaining,
            |bytes| match accounting.claim_owned(bytes as u64, 1) {
                Ok(allocation) => {
                    *stages.get_mut(stage).ok_or(crate::access::InvalidPolicy(
                        "invalid policy compilation stage",
                    ))? = Some((allocation, bytes as u64));
                    stage += 1;
                    Ok(())
                }
                Err(error) => {
                    admission_error = Some(error);
                    Err(crate::access::InvalidPolicy(
                        "policy storage was not admitted",
                    ))
                }
            },
        );
        accounting.charge_items((before - remaining) as u64)?;
        if let Some(error) = admission_error {
            return Err(error);
        }
        let mut compiled = compiled.map_err(|_| FsError::InvalidAccessPolicy)?;
        let bytes = compiled.logical_owned_bytes();
        // The capacity is fixed on this Fs's first policy use, derived
        // from its host memory ceiling, never from head generations or
        // principal count. A standalone embedding supplies its budget.
        #[cfg(not(target_arch = "wasm32"))]
        let capacity = accounting
            .native_work()
            .map_or(accounting.remaining()?.peak_allocation_bytes / 4, |work| {
                work.shared_cache_capacity()
            });
        #[cfg(target_arch = "wasm32")]
        let capacity = accounting.remaining()?.peak_allocation_bytes / 4;
        cache.initialize(capacity)?;
        compiled.cache_charge = Some(cache.charge(bytes)?);
        let compiled = Arc::new(compiled);
        cache.insert(volume.id, revision, Arc::clone(&compiled))?;
        Ok(compiled)
    })();
    for (mut allocation, bytes) in stages.into_iter().flatten() {
        accounting.release_owned(&mut allocation, bytes)?;
    }
    accounting.release_owned(&mut decode, decode_bytes)?;
    compiled
}
