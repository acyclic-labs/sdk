use super::*;

/// One immutable file resolved against a pinned checkout generation.
#[wasm_bindgen]
pub struct BrowserResolvedFile {
    pub(in crate::bindings) engine:
        ResolvedFile<BrowserAuthority, CachedObjectStore<BrowserObjects>>,
}

/// One original-order resolved path batch and its shared work receipt.
#[wasm_bindgen]
pub struct BrowserResolvedFiles {
    pub(in crate::bindings) files: Vec<Option<BrowserResolvedFile>>,
    pub(in crate::bindings) work: acyclic_fs::WorkCounters,
}

#[wasm_bindgen]
impl BrowserResolvedFiles {
    /// Number of original-order results.
    #[wasm_bindgen(getter)]
    #[must_use]
    pub fn length(&self) -> usize {
        self.files.len()
    }

    /// Exact work receipt for the shared namespace traversal.
    #[wasm_bindgen(getter)]
    pub fn work(&self) -> Result<Ts<BrowserWorkCounters>, JsValue> {
        ts(browser_work(self.work))
    }

    /// Transfers one generation-bound handle to JavaScript. Each index may be taken once.
    pub fn take(&mut self, index: usize) -> Result<Option<BrowserResolvedFile>, JsValue> {
        self.files
            .get_mut(index)
            .ok_or_else(|| js_error("resolved file index is out of bounds"))
            .map(Option::take)
    }
}

#[wasm_bindgen]
impl BrowserResolvedFile {
    /// Terminal file kind authenticated by the pinned generation.
    #[wasm_bindgen(getter)]
    pub fn kind(&self) -> Result<Ts<FileKind>, JsValue> {
        let kind = self.engine.description().kind;
        ts(kind)
    }

    /// Logical content length authenticated by the pinned generation.
    #[wasm_bindgen(getter, js_name = logicalBytes)]
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        self.engine.description().logical_bytes
    }

    /// Complete canonical metadata authenticated by the pinned generation.
    #[wasm_bindgen(getter, js_name = metadataCanonicalBytes)]
    pub fn metadata_canonical_bytes(&self) -> Result<Vec<u8>, JsValue> {
        let metadata = self.engine.description().metadata;
        encode_file_metadata(
            metadata,
            |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
            |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
        )
        .map_err(js_error)
    }

    /// Reads one exact logical range without another namespace lookup.
    #[wasm_bindgen(js_name = readRange)]
    pub async fn read_range(
        &self,
        offset: u64,
        length: u64,
    ) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let range = ByteRange { offset, length };
        let receipt = self
            .engine
            .read_range(
                range,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &cancellation,
            )
            .await
            .map_err(js_error)?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.bytes.to_vec(),
            work: browser_work(receipt.work),
        })
    }

    /// Reads opaque symbolic-link target bytes without another namespace lookup.
    #[wasm_bindgen(js_name = readSymbolicLink)]
    pub async fn read_symbolic_link(&self) -> Result<Ts<BrowserFileReadResult>, JsValue> {
        let cancellation = CancellationToken::default();
        let receipt = self
            .engine
            .read_symbolic_link(acyclic_fs::performance::BOUNDARY_BUDGET, &cancellation)
            .await
            .map_err(js_error)?;
        ts(BrowserFileReadResult {
            bytes: receipt.value.to_vec(),
            work: browser_work(receipt.work),
        })
    }
}
