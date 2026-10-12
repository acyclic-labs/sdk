use super::*;

/// One canonical directory entry.
#[napi(object)]
pub struct NativeDirectoryEntry {
    /// Portable UTF-8 name bytes.
    pub name: Buffer,
    /// Stable file identity.
    pub file_id: Buffer,
    /// Canonical file kind.
    pub file_kind: FileKind,
}

/// One bounded authenticated directory cursor page.
#[napi(object)]
pub struct NativeDirectoryPage {
    /// Ordered entries strictly after the cursor.
    pub entries: Vec<NativeDirectoryEntry>,
    /// Whether another page exists.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One directory binding paired with its complete record and metadata.
#[napi(object)]
pub struct NativeDirectoryRecordEntry {
    /// Exact canonical name bytes.
    pub name: Buffer,
    /// Complete path-independent child record.
    pub record: NativeFileRecord,
    /// Complete canonical metadata bytes referenced by the record.
    pub metadata_canonical_bytes: Buffer,
}

/// One page-efficient directory record enumeration receipt.
#[napi(object)]
pub struct NativeDirectoryRecordPage {
    /// Ordered records strictly after the supplied cursor.
    pub entries: Vec<NativeDirectoryRecordEntry>,
    /// Whether another page exists.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

#[napi]
impl NativeCheckout {
    /// Returns one bounded ordered directory page.
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
        let receipt = checkout
            .list_directory(
                &path,
                after.as_ref(),
                maximum_entries,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
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

    /// Returns a bounded ordered directory page containing complete file
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
