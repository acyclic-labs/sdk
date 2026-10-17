use super::*;
use crate::access::{MAXIMUM_POLICY_RECORD_BYTES, POLICY_METADATA_KEY};
use crate::kernel::{AttributeClass, OwnedSourceChunk};

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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> Checkout<A, O> {
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

impl<A: AsyncAuthorityStore, O: AsyncObjectStore> crate::Workspace<A, O> {
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
        let before = readonly_standalone_live(accounting);
        let result = async {
            let volume = &self.volume;
            let (generation, observed) = volume
                .resolve_head_generation(accounting, cancellation)
                .await?;
            let metadata = {
                let (root, _root_allocation) = read_generation_root_with_accounting(
                    &volume.fs.inner.objects,
                    generation,
                    volume.config,
                    accounting,
                    cancellation,
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
                    accounting,
                    cancellation,
                    None,
                )
                .await?;
                read_record_metadata_to_file_root_with_accounting(
                    volume,
                    record.ok_or(FsError::NotFound)?,
                    accounting,
                    cancellation,
                )
                .await?
            };
            let mut authority = authority_work(accounting, cancellation)?;
            let current = volume
                .fs
                .inner
                .authority
                .head(volume_authority_id(volume.id), &mut authority)
                .await;
            record_authority_work(accounting, &authority)?;
            Ok(
                crate::facade::policy_head_is_current(observed, current?).then_some((
                    GenerationId::new(generation.digest),
                    metadata.named_attributes,
                )),
            )
        }
        .await;
        readonly_cleanup_standalone(accounting, before, 0).and(result)
    }
}

pub(crate) fn policy_head_is_current(observed: Head, current: Head) -> bool {
    observed == current
}
