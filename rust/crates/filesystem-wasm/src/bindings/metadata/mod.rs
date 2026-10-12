use super::*;

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserMetadataResult {
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) canonical_bytes: Vec<u8>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserNamedAttributeResult {
    pub(in crate::bindings) exists: bool,
    #[tsify(type = "Uint8Array | undefined")]
    pub(in crate::bindings) bytes: Option<serde_bytes::ByteBuf>,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
#[tsify(large_number_types_as_bigints)]
pub struct BrowserNamedAttributePageResult {
    pub(in crate::bindings) entries: Vec<BrowserNamedAttributeNameResult>,
    pub(in crate::bindings) has_more: bool,
    pub(in crate::bindings) work: BrowserWorkCounters,
}

#[derive(Serialize, Tsify)]
#[serde(rename_all = "camelCase")]
pub struct BrowserNamedAttributeNameResult {
    #[tsify(type = "\"posix-xattr\" | \"windows-stream\" | \"mac-resource-fork\"")]
    pub(in crate::bindings) attribute_class: &'static str,
    #[serde(with = "serde_bytes")]
    #[tsify(type = "Uint8Array")]
    pub(in crate::bindings) name: Vec<u8>,
}

#[wasm_bindgen]
impl BrowserCheckout {
    /// Reads complete canonical metadata bytes for one path.
    #[wasm_bindgen(js_name = readMetadata)]
    pub async fn read_metadata(
        &mut self,
        path: String,
    ) -> Result<Ts<BrowserMetadataResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let receipt = browser_call!(self, read_metadata(&path,))?;
        ts(BrowserMetadataResult {
            canonical_bytes: encode_file_metadata(
                receipt.value,
                |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
            )
            .map_err(js_error)?,
            work: browser_work(receipt.work),
        })
    }

    /// Reads complete canonical metadata by stable file identity.
    #[wasm_bindgen(js_name = readMetadataById)]
    pub async fn read_metadata_by_id(
        &mut self,
        file_id: Vec<u8>,
    ) -> Result<Ts<BrowserMetadataResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let receipt = browser_call!(self, read_metadata_by_id(file_id,))?;
        ts(BrowserMetadataResult {
            canonical_bytes: encode_file_metadata(
                receipt.value,
                |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
            )
            .map_err(js_error)?,
            work: browser_work(receipt.work),
        })
    }

    /// Atomically replaces complete canonical metadata for one path.
    #[wasm_bindgen(js_name = setMetadata)]
    pub async fn set_metadata(
        &mut self,
        path: String,
        canonical_bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let metadata = decode_file_metadata(
            &canonical_bytes,
            browser_decode_limits(self.engine.volume_config().limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(js_error)?;
        let receipt = browser_call!(
            self,
            apply_authored_transaction(vec![acyclic_fs::AuthoredMutation::SetMetadata {
                path,
                metadata
            }],)
        )?;
        mutation_value(receipt.work)
    }

    /// Replaces complete canonical metadata by stable file identity.
    #[wasm_bindgen(js_name = setMetadataById)]
    pub async fn set_metadata_by_id(
        &mut self,
        file_id: Vec<u8>,
        canonical_bytes: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            &canonical_bytes,
            browser_decode_limits(self.engine.volume_config().limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(js_error)?;
        let receipt = browser_call!(self, set_attributes_by_id(file_id, metadata, None,))?;
        mutation_value(receipt.work)
    }

    /// Atomically replaces metadata and optional logical size for one path.
    #[wasm_bindgen(js_name = setAttributes)]
    pub async fn set_attributes(
        &mut self,
        path: String,
        canonical_bytes: Vec<u8>,
        logical_bytes: Option<u64>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let metadata = decode_file_metadata(
            &canonical_bytes,
            browser_decode_limits(self.engine.volume_config().limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(js_error)?;
        let receipt = browser_call!(self, set_attributes(path, metadata, logical_bytes,))?;
        mutation_value(receipt.work)
    }

    /// Atomically replaces metadata and optional logical size by file identity.
    #[wasm_bindgen(js_name = setAttributesById)]
    pub async fn set_attributes_by_id(
        &mut self,
        file_id: Vec<u8>,
        canonical_bytes: Vec<u8>,
        logical_bytes: Option<u64>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            &canonical_bytes,
            browser_decode_limits(self.engine.volume_config().limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(js_error)?;
        let receipt = browser_call!(
            self,
            set_attributes_by_id(file_id, metadata, logical_bytes,)
        )?;
        mutation_value(receipt.work)
    }

    /// Reads one exact named attribute value.
    #[wasm_bindgen(js_name = readNamedAttribute)]
    pub async fn read_named_attribute(
        &mut self,
        path: String,
        attribute_class: String,
        name: Vec<u8>,
    ) -> Result<Ts<BrowserNamedAttributeResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let name =
            browser_attribute_name(&attribute_class, name, self.engine.volume_config().limits)?;
        let receipt = browser_call!(self, read_named_attribute(&path, &name,))?;
        ts(BrowserNamedAttributeResult {
            exists: receipt.value.is_some(),
            bytes: receipt.value.map(|value| value.bytes.to_vec().into()),
            work: browser_work(receipt.work),
        })
    }

    /// Returns one bounded ordered named-attribute page.
    #[wasm_bindgen(js_name = listNamedAttributes)]
    pub async fn list_named_attributes(
        &mut self,
        path: String,
        after_class: Option<String>,
        after_name: Option<Vec<u8>>,
        #[wasm_bindgen(unchecked_param_type = "number")] maximum_entries: JsValue,
    ) -> Result<Ts<BrowserNamedAttributePageResult>, JsValue> {
        let maximum_entries = wasm_u32(&maximum_entries)?;

        let path = browser_path(&path, self.engine.volume_config())?;
        let after = match (after_class, after_name) {
            (None, None) => None,
            (Some(class), Some(name)) => Some(browser_attribute_name(
                &class,
                name,
                self.engine.volume_config().limits,
            )?),
            _ => return Err(js_error("named-attribute cursor is incomplete")),
        };
        let receipt = browser_call!(
            self,
            list_named_attributes(&path, after.as_ref(), maximum_entries,)
        )?;
        ts(BrowserNamedAttributePageResult {
            entries: receipt
                .value
                .entries
                .into_iter()
                .map(|entry| BrowserNamedAttributeNameResult {
                    attribute_class: browser_attribute_class_name(entry.name.class()),
                    name: entry.name.as_bytes().to_vec(),
                })
                .collect(),
            has_more: receipt.value.has_more,
            work: browser_work(receipt.work),
        })
    }

    /// Inserts or replaces one exact named attribute.
    #[wasm_bindgen(js_name = writeNamedAttribute)]
    pub async fn write_named_attribute(
        &mut self,
        path: String,
        attribute_class: String,
        name: Vec<u8>,
        bytes: Vec<u8>,
        mode: String,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let name =
            browser_attribute_name(&attribute_class, name, self.engine.volume_config().limits)?;
        let mode = browser_attribute_write_mode(&mode)?;
        let receipt = browser_call!(
            self,
            change_named_attribute(path, &name, Some((bytes.as_slice(), mode)),)
        )?;
        mutation_value(receipt.work)
    }

    /// Removes one exact named attribute.
    #[wasm_bindgen(js_name = removeNamedAttribute)]
    pub async fn remove_named_attribute(
        &mut self,
        path: String,
        attribute_class: String,
        name: Vec<u8>,
    ) -> Result<Ts<BrowserMutationResult>, JsValue> {
        let path = browser_path(&path, self.engine.volume_config())?;
        let name =
            browser_attribute_name(&attribute_class, name, self.engine.volume_config().limits)?;
        let receipt = browser_call!(self, change_named_attribute(path, &name, None,))?;
        mutation_value(receipt.work)
    }
}

pub(in crate::bindings) fn browser_attribute_class(value: &str) -> Result<AttributeClass, JsValue> {
    match value {
        "posix-xattr" => Ok(AttributeClass::PosixXattr),
        "windows-stream" => Ok(AttributeClass::WindowsStream),
        "mac-resource-fork" => Ok(AttributeClass::MacResourceFork),
        _ => Err(js_error("unknown named-attribute class")),
    }
}

pub(in crate::bindings) const fn browser_attribute_class_name(
    value: AttributeClass,
) -> &'static str {
    match value {
        AttributeClass::PosixXattr => "posix-xattr",
        AttributeClass::WindowsStream => "windows-stream",
        AttributeClass::MacResourceFork => "mac-resource-fork",
    }
}

pub(in crate::bindings) fn browser_attribute_name(
    class: &str,
    name: Vec<u8>,
    limits: VolumeLimits,
) -> Result<AttributeName, JsValue> {
    AttributeName::new(
        browser_attribute_class(class)?,
        name,
        limits.maximum_component_bytes,
    )
    .map_err(js_error)
}

pub(in crate::bindings) fn browser_attribute_write_mode(
    value: &str,
) -> Result<NamedAttributeWriteMode, JsValue> {
    match value {
        "upsert" => Ok(NamedAttributeWriteMode::Upsert),
        "create" => Ok(NamedAttributeWriteMode::Create),
        "replace" => Ok(NamedAttributeWriteMode::Replace),
        _ => Err(js_error("unknown named-attribute write mode")),
    }
}
