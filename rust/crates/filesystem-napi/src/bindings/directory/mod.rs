use super::*;

/// One canonical directory entry.
#[napi(object)]
pub struct NativeDirectoryEntry {
    pub name: Buffer,
    pub file_id: Buffer,
    pub file_kind: FileKind,
}

/// One bounded authenticated directory cursor page.
#[napi(object)]
pub struct NativeDirectoryPage {
    pub entries: Vec<NativeDirectoryEntry>,
    pub has_more: bool,
    pub work_json: String,
}

/// One directory binding paired with its complete record and metadata.
#[napi(object)]
pub struct NativeDirectoryRecordEntry {
    pub name: Buffer,
    pub record: NativeFileRecord,
    pub metadata_canonical_bytes: Buffer,
}

/// One page-efficient directory record enumeration receipt.
#[napi(object)]
pub struct NativeDirectoryRecordPage {
    pub entries: Vec<NativeDirectoryRecordEntry>,
    pub has_more: bool,
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    #[napi]
    pub async fn list_directory(
        &self,
        path: String,
        after: Option<String>,
        maximum_entries: NapiU32,
    ) -> Result<NativeDirectoryPage> {
        let maximum_entries = maximum_entries.into_inner();

        let path = native_path(&path, self.config)?;
        let after = after
            .as_deref()
            .map(|value| native_name(value, self.config))
            .transpose()?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout_call!(
            self,
            checkout,
            list_directory(&path, after.as_ref(), maximum_entries,)
        )?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| NativeDirectoryEntry {
                name: Buffer::from(entry.name.as_bytes().to_vec()),
                file_id: Buffer::from(entry.file_id.into_bytes().to_vec()),
                file_kind: entry.kind,
            })
            .collect();
        Ok(NativeDirectoryPage {
            entries,
            has_more: receipt.value.has_more,
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn list_directory_records(
        &self,
        path: String,
        after: Option<String>,
        maximum_entries: NapiU32,
    ) -> Result<NativeDirectoryRecordPage> {
        let maximum_entries = maximum_entries.into_inner();

        let path = native_path(&path, self.config)?;
        let after = after
            .as_deref()
            .map(|value| native_name(value, self.config))
            .transpose()?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = Box::pin(
            checkout.list_directory_records(
                &path,
                after
                    .as_ref()
                    .map_or(std::ops::Bound::Unbounded, std::ops::Bound::Excluded),
                maximum_entries,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            ),
        )
        .await
        .map_err(napi_error)?;
        let entries = receipt
            .value
            .entries
            .into_iter()
            .map(|entry| {
                Ok(NativeDirectoryRecordEntry {
                    name: Buffer::from(entry.name.as_bytes().to_vec()),
                    record: encode_file_record(entry.record),
                    metadata_canonical_bytes: Buffer::from(
                        encode_file_metadata(
                            entry.metadata,
                            |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                            |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                        )
                        .map_err(napi_error)?,
                    ),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(NativeDirectoryRecordPage {
            entries,
            has_more: receipt.value.has_more,
            work_json: work_json(&receipt.work)?,
        })
    }
}
