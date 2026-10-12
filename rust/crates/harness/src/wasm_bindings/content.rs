use super::*;


pub(super) fn exact_nonnegative_u64(value: f64, field: &str) -> Result<u64, JsValue> {
    if !crate::conversation::is_exact_js_number(value) || value < 0.0 || value.fract() != 0.0 {
        return Err(JsValue::from_str(&format!(
            "{field} must be a safe non-negative integer"
        )));
    }
    value
        .to_string()
        .parse::<u64>()
        .map_err(|_| JsValue::from_str(&format!("{field} is outside the supported range")))
}

#[wasm_bindgen]
impl WasmContentStore {
    /// Creates a bounded content store for the supplied volume.
    #[wasm_bindgen(constructor)]
    pub fn new(
        volume: JsValue,
        maximum_file_bytes: f64,
        maximum_path_bytes: f64,
        maximum_resident_bytes: f64,
        maximum_resident_files: f64,
    ) -> Result<Self, JsValue> {
        let volume: VolumeRef = from_js(volume)?;
        let store = crate::memory_store::MemoryStore::new(
            volume,
            exact_nonnegative_u64(maximum_file_bytes, "maximum_file_bytes")?,
            exact_nonnegative_u64(maximum_path_bytes, "maximum_path_bytes")?,
            exact_nonnegative_u64(maximum_resident_bytes, "maximum_resident_bytes")?,
            exact_nonnegative_u64(maximum_resident_files, "maximum_resident_files")?,
        )
        .map_err(js_error)?;
        Ok(Self { store })
    }

    /// Stores one immutable file and optionally advances its path head.
    pub fn stage(
        &mut self,
        path: String,
        bytes: Vec<u8>,
        media_type: String,
        display_name: String,
        update_path: bool,
    ) -> Result<JsValue, JsValue> {
        let reference = self
            .store
            .stage(&path, &bytes, &media_type, &display_name, update_path)
            .map_err(js_error)?;
        to_js_admitted(&reference)
    }

    /// Reads only an exact, resident immutable reference owned by this store.
    pub fn read(&self, file: JsValue) -> Result<Vec<u8>, JsValue> {
        let file: FileRef = from_js(file)?;
        self.store.read(&file).map_err(js_error)
    }

    /// Tests local residency without exposing mutable storage maps.
    pub fn has(&self, file: JsValue) -> Result<bool, JsValue> {
        let file: FileRef = from_js(file)?;
        self.store.has(&file).map_err(js_error)
    }

    /// Reports whether a new file at `path` would conflict with a file or directory already retained by this provider.
    #[wasm_bindgen(js_name = pathConflicts)]
    pub fn path_conflicts(&self, path: String) -> bool {
        self.store.path_conflicts(&path)
    }

    /// Returns a generation-pinned directory page from Rust-owned path state.
    pub fn list(
        &self,
        path: String,
        generation: JsValue,
        after: Option<String>,
        maximum: f64,
    ) -> Result<JsValue, JsValue> {
        let generation = self.requested_generation(generation)?;
        let maximum = usize::try_from(exact_nonnegative_u64(maximum, "maximum")?)
            .map_err(|_| JsValue::from_str("maximum is outside the supported range"))?;
        let page = self
            .store
            .list(&path, Some(generation), after.as_deref(), maximum)
            .map_err(js_error)?;
        to_js_admitted(&page)
    }

    /// Resolves a path at or before an explicit generation.
    pub fn read_path(&self, path: String, generation: JsValue) -> Result<JsValue, JsValue> {
        let generation = self.requested_generation(generation)?;
        let file = self
            .store
            .read_path(&path, Some(generation))
            .map_err(js_error)?;
        to_js_admitted(&file)
    }

    /// Returns the current path generation as an exact JavaScript bigint.
    pub fn generation(&self) -> Result<JsValue, JsValue> {
        to_js(&self.store.generation())
    }

    fn requested_generation(&self, value: JsValue) -> Result<u64, JsValue> {
        let requested = if value.is_null() || value.is_undefined() {
            None
        } else {
            Some(from_js::<u64>(value)?)
        };
        let generation = requested.unwrap_or_else(|| self.store.generation());
        if generation > self.store.generation() {
            return Err(JsValue::from_str(
                "private directory generation is unavailable",
            ));
        }
        Ok(generation)
    }
}
