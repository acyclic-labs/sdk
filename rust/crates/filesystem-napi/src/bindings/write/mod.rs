use super::*;

#[napi]
impl NativeCheckout {
    #[napi]
    pub async fn write_file(
        &self,
        path: String,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Write {
                    offset: bigint_u64(&offset, "offset")?,
                    bytes: (bytes::Bytes::from(bytes.to_vec())).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn write_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = checkout_mutation!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Write {
                    offset: bigint_u64(&offset, "offset")?,
                    bytes: (bytes::Bytes::from(bytes.to_vec())).into(),
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn resize_file(
        &self,
        path: String,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: bigint_u64(&logical_bytes, "logicalBytes")?,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn resize_file_by_id(
        &self,
        file_id: Buffer,
        logical_bytes: BigInt,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = checkout_mutation!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Resize {
                    logical_bytes: bigint_u64(&logical_bytes, "logicalBytes")?,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

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
        let receipt = checkout_mutation!(
            self,
            change_content(
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
            )
        )?;
        mutation_result(receipt.work)
    }

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
        let receipt = checkout_mutation!(
            self,
            change_content(
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
            )
        )?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn preallocate_file(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            change_content(
                path,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

    #[napi]
    pub async fn preallocate_file_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
        keep_size: bool,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = checkout_mutation!(
            self,
            change_content(
                file_id,
                acyclic_fs::ContentChange::Preallocate {
                    range: ByteRange {
                        offset: bigint_u64(&offset, "offset")?,
                        length: bigint_u64(&length, "length")?,
                    },
                    keep_size: keep_size,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

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
        let receipt = checkout_mutation!(
            self,
            change_content(
                request.destination,
                acyclic_fs::ContentChange::CloneFrom {
                    source: request.source,
                    source_offset: request.source_offset,
                    offset: request.destination_offset,
                    length: request.length,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }

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
        let receipt = checkout_mutation!(
            self,
            change_content(
                destination_file_id,
                acyclic_fs::ContentChange::CloneFrom {
                    source: source_file_id,
                    source_offset: bigint_u64(&source_offset, "sourceOffset")?,
                    offset: bigint_u64(&destination_offset, "destinationOffset")?,
                    length: bigint_u64(&length, "length")?,
                },
                acyclic_fs::ContentTimes::Preserve,
            )
        )?;
        mutation_result(receipt.work)
    }
}
