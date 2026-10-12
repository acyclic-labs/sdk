use super::*;

/// One ordered resolve result with its exact work receipt.
#[napi]
pub struct NativeResolvedFiles {
    pub(in crate::bindings) files: std::sync::Mutex<Vec<Option<NativeResolvedFile>>>,
    pub(in crate::bindings) work_json: String,
}

#[napi]
impl NativeResolvedFiles {
    #[napi(getter)]
    pub fn length(&self) -> Result<u32> {
        u32::try_from(self.files.lock().map_err(napi_error)?.len()).map_err(napi_error)
    }

    #[napi(getter)]
    pub fn work_json(&self) -> String {
        self.work_json.clone()
    }

    #[napi]
    pub fn take(&self, index: NapiU32) -> Result<Option<NativeResolvedFile>> {
        let index = index.into_inner();

        self.files
            .lock()
            .map_err(napi_error)?
            .get_mut(usize::try_from(index).map_err(napi_error)?)
            .ok_or_else(|| Error::new(Status::InvalidArg, "resolved file index is out of bounds"))
            .map(Option::take)
    }
}

/// One immutable file resolved against a pinned checkout generation.
#[napi]
pub struct NativeResolvedFile {
    pub(in crate::bindings) inner: ResolvedFile<LocalAuthorityBackend, LocalObjectBackend>,
    pub(in crate::bindings) cancellation: CancellationToken,
}

#[napi]
impl NativeResolvedFile {
    #[napi(getter)]
    #[must_use]
    pub fn kind(&self) -> FileKind {
        self.inner.description().kind
    }

    #[napi(getter)]
    #[must_use]
    pub fn logical_bytes(&self) -> BigInt {
        self.inner.description().logical_bytes.into()
    }

    #[napi(getter)]
    pub fn metadata_canonical_bytes(&self) -> Result<Buffer> {
        encode_file_metadata(
            self.inner.description().metadata,
            |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
            |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
        )
        .map(Buffer::from)
        .map_err(napi_error)
    }

    #[napi]
    pub async fn read_range(&self, offset: BigInt, length: BigInt) -> Result<NativeFileRead> {
        let receipt = self
            .inner
            .read_range(
                ByteRange {
                    offset: bigint_u64(&offset, "offset")?,
                    length: bigint_u64(&length, "length")?,
                },
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.bytes.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }

    #[napi]
    pub async fn read_symbolic_link(&self) -> Result<NativeFileRead> {
        let receipt = self
            .inner
            .read_symbolic_link(acyclic_fs::performance::BOUNDARY_BUDGET, &self.cancellation)
            .await
            .map_err(napi_error)?;
        Ok(NativeFileRead {
            bytes: Buffer::from(receipt.value.to_vec()),
            work_json: work_json(&receipt.work)?,
        })
    }
}
