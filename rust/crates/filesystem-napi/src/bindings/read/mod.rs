use super::*;

/// Exact no-follow lookup result.
#[napi(object)]
pub struct NativeLookup {
    pub exists: bool,
    pub file_id: Option<Buffer>,
    pub file_kind: Option<FileKind>,
    pub resolved_components: u32,
    pub work_json: String,
}

/// One original-order entry from a shared path lookup batch.
#[napi(object)]
pub struct NativeBatchLookupEntry {
    pub exists: bool,
    pub file_id: Option<Buffer>,
    pub file_kind: Option<FileKind>,
    pub resolved_components: u32,
}

/// One bounded shared-frontier path lookup receipt.
#[napi(object)]
pub struct NativeBatchLookup {
    pub entries: Vec<NativeBatchLookupEntry>,
    pub retained_allocation_bytes: BigInt,
    pub work_json: String,
}

/// One bounded regular-file range.
#[napi(object)]
pub struct NativeFileRead {
    pub bytes: Buffer,
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    #[napi]
    pub async fn lookup_no_follow(&self, path: String) -> Result<NativeLookup> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, lookup_no_follow(&path,))?;
        let record = receipt.value.record;
        Ok(NativeLookup {
            exists: record.is_some(),
            file_id: record.map(|value| Buffer::from(value.file_id.into_bytes().to_vec())),
            file_kind: record.map(|value| value.kind),
            resolved_components: u32::from(receipt.value.resolved_components),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn lookup_batch_no_follow(&self, paths: Vec<String>) -> Result<NativeBatchLookup> {
        let maximum =
            usize::try_from(self.config.limits.maximum_paths_per_batch).unwrap_or(usize::MAX);
        if paths.is_empty() || paths.len() > maximum || paths.capacity() > maximum {
            return Err(Error::new(
                Status::InvalidArg,
                "lookup path batch is empty or excessive",
            ));
        }
        let paths = paths
            .iter()
            .map(|path| native_path(path, self.config))
            .collect::<Result<Vec<_>>>()?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, lookup_batch_no_follow(&paths,))?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| {
                let record = entry.record;
                NativeBatchLookupEntry {
                    exists: record.is_some(),
                    file_id: record.map(|value| Buffer::from(value.file_id.into_bytes().to_vec())),
                    file_kind: record.map(|value| value.kind),
                    resolved_components: u32::from(entry.resolved_components),
                }
            })
            .collect();
        Ok(NativeBatchLookup {
            entries,
            retained_allocation_bytes: bigint(receipt.value.retained_allocation_bytes),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn stat_no_follow(&self, path: String) -> Result<NativeStat> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, lookup_no_follow_with_metadata(&path,))?;
        let (record, metadata_canonical_bytes) = match receipt.value {
            Some(value) => (
                Some(encode_file_record(value.record)),
                Some(Buffer::from(
                    encode_file_metadata(
                        value.metadata,
                        |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                        |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                    )
                    .map_err(napi_error)?,
                )),
            ),
            None => (None, None),
        };
        Ok(NativeStat {
            exists: record.is_some(),
            record,
            metadata_canonical_bytes,
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn read_file_record_by_id(&self, file_id: Buffer) -> Result<NativeFileRecordRead> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, read_file_record_by_id(file_id,))?;
        Ok(NativeFileRecordRead {
            record: encode_file_record(receipt.value),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub fn resolve_files<'env>(
        &self,
        env: &'env Env,
        paths: Array<'env>,
    ) -> Result<PromiseRaw<'env, NativeResolvedFiles>> {
        let parsed = (|| {
            let maximum = self.config.limits.maximum_paths_per_batch;
            if paths.len() > maximum {
                return Err(Error::new(
                    Status::InvalidArg,
                    "resolved file batch exceeds the configured bound",
                ));
            }
            let length = usize::try_from(paths.len()).map_err(napi_error)?;
            let mut parsed = Vec::new();
            parsed
                .try_reserve_exact(length)
                .map_err(|error| napi_error(error.to_string()))?;
            for index in 0..paths.len() {
                let path = paths.get::<String>(index)?.ok_or_else(|| {
                    Error::new(Status::InvalidArg, "resolved file paths must be strings")
                })?;
                parsed.push(native_path(&path, self.config)?);
            }
            Ok(parsed)
        })();
        let checkout = Arc::clone(&self.inner);
        let cancellation = self.cancellation.clone();
        env.spawn_future(async move {
            let parsed = parsed?;
            let reader = {
                let checkout = checkout.lock().await;
                checkout.pinned_reader().map_err(napi_error)?
            };
            let receipt = reader
                .resolve_files(
                    &parsed,
                    acyclic_fs::performance::BOUNDARY_BUDGET,
                    &cancellation,
                )
                .await
                .map_err(napi_error)?;
            Ok(NativeResolvedFiles {
                files: std::sync::Mutex::new(
                    receipt
                        .value
                        .into_iter()
                        .map(|file| {
                            file.map(|inner| NativeResolvedFile {
                                inner,
                                cancellation: cancellation.clone(),
                            })
                        })
                        .collect(),
                ),
                work_json: work_json(&receipt.work)?,
            })
        })
    }

    #[napi]
    pub async fn read_file_range(
        &self,
        path: String,
        offset: BigInt,
        length: BigInt,
    ) -> Result<NativeFileRead> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            read_file_range(
                &path,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
            )
        )?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn read_file_range_by_id(
        &self,
        file_id: Buffer,
        offset: BigInt,
        length: BigInt,
    ) -> Result<NativeFileRead> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            read_file_range_by_id(
                file_id,
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
            )
        )?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn read_symbolic_link(&self, path: String) -> Result<NativeFileRead> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, read_symbolic_link(&path,))?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn read_reparse_point(&self, path: String) -> Result<NativeFileRead> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(self, checkout, read_reparse_point(&path,))?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }
}
