use super::*;

#[napi]
impl NativeCheckout {
    /// Replaces one logical file range in the private COW overlay.
    #[napi]
    pub async fn write_file(
        &self,
        path: String,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                path,
                acyclic_fs::ContentChange::Write {
                    offset: bigint_u64(&offset, "offset")?,
                    bytes: (bytes::Bytes::from(bytes.to_vec())).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMutationResult {
            file_id: None,
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Replaces one logical file range by stable file identity.
    #[napi]
    pub async fn write_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                file_id,
                acyclic_fs::ContentChange::Write {
                    offset: bigint_u64(&offset, "offset")?,
                    bytes: (bytes::Bytes::from(bytes.to_vec())).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Changes a regular file's logical length.
    #[napi]
    pub async fn resize_file(
        &self,
        path: String,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                path,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: bigint_u64(&logical_bytes, "logicalBytes")?,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Changes a regular file's logical length by stable file identity.
    #[napi]
    pub async fn resize_file_by_id(
        &self,
        file_id: Buffer,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                file_id,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: bigint_u64(&logical_bytes, "logicalBytes")?,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Punches a hole or records physically allocated zeros.
    #[napi]
    pub async fn zero_file_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                path,
                acyclic_fs::ContentChange::ZeroRange {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    allocated: allocated,
                    extend: extend,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Punches a hole or records allocated zeros by stable file identity.
    #[napi]
    pub async fn zero_file_range_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        allocated: bool,
        extend: bool,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                file_id,
                acyclic_fs::ContentChange::ZeroRange {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    allocated: allocated,
                    extend: extend,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Allocates sparse holes without replacing existing content.
    #[napi]
    pub async fn preallocate_file(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                path,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Allocates sparse holes by stable file identity.
    #[napi]
    pub async fn preallocate_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                file_id,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Clones one logical range by immutable extent reference.
    #[napi]
    pub async fn clone_file_range(
        &self,
        source: String,
        source_offset: BigInt,
        destination: String,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<NativeMutationResult> {
        let request = FileCloneRequest {
            source: native_path(&source, self.config)?,
            source_offset: bigint_u64(&source_offset, "sourceOffset")?,
            destination: native_path(&destination, self.config)?,
            destination_offset: bigint_u64(&destination_offset, "destinationOffset")?,
            length: bigint_u64(&length, "length")?,
        };
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                request.destination,
                acyclic_fs::ContentChange::CloneFrom {
                    source: request.source,
                    source_offset: request.source_offset,
                    offset: request.destination_offset,
                    length: request.length,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Clones one sparse range between stable file identities.
    #[napi]
    pub async fn clone_file_range_by_id(
        &self,
        source_file_id: Buffer,
        source_offset: BigInt,
        destination_file_id: Buffer,
        destination_offset: BigInt,
        length: BigInt,
    ) -> Result<NativeMutationResult> {
        let source_file_id = FileId::from_bytes(fixed_16(&source_file_id)?);
        let destination_file_id = FileId::from_bytes(fixed_16(&destination_file_id)?);
        let mut checkout = self.inner.lock().await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = checkout
            .change_content(
                destination_file_id,
                acyclic_fs::ContentChange::CloneFrom {
                    source: source_file_id,
                    source_offset: bigint_u64(&source_offset, "sourceOffset")?,
                    offset: bigint_u64(&destination_offset, "destinationOffset")?,
                    length: bigint_u64(&length, "length")?,
                },
                acyclic_fs::ContentTimes::Preserve,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        mutation_result(receipt.work)
    }
}
