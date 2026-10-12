use super::*;

/// Complete canonical metadata bytes plus exact work.
#[napi(object)]
pub struct NativeMetadataResult {
    /// Versioned canonical metadata record.
    pub canonical_bytes: Buffer,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// Optional exact named-attribute value plus exact work.
#[napi(object)]
pub struct NativeNamedAttributeResult {
    /// Whether the attribute exists.
    pub exists: bool,
    /// Exact bytes when present.
    pub bytes: Option<Buffer>,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One bounded ordered named-attribute page.
#[napi(object)]
pub struct NativeNamedAttributePage {
    /// Exact canonical class/name pairs.
    pub entries: Vec<NativeNamedAttributeName>,
    /// Whether another canonical page follows.
    pub has_more: bool,
    /// Exact machine-readable work receipt.
    pub work_json: String,
}

/// One exact canonical named-attribute identity.
#[napi(object)]
pub struct NativeNamedAttributeName {
    /// Canonical source-semantics class.
    pub attribute_class: String,
    /// Exact raw name.
    pub name: Buffer,
}

#[napi]
impl NativeCheckout {
    /// Reads complete canonical metadata bytes for one path.
    #[napi]
    pub async fn read_metadata(&self, path: String) -> Result<NativeMetadataResult> {
        let path = native_path(&path, self.config)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout
            .read_metadata(
                &path,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMetadataResult {
            canonical_bytes: Buffer::from(
                encode_file_metadata(
                    receipt.value,
                    |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                    |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                )
                .map_err(napi_error)?,
            ),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Reads complete canonical metadata bytes by stable file identity.
    #[napi]
    pub async fn read_metadata_by_id(&self, file_id: Buffer) -> Result<NativeMetadataResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = checkout
            .read_metadata_by_id(
                file_id,
                acyclic_fs::performance::BOUNDARY_BUDGET,
                &self.cancellation,
            )
            .await
            .map_err(napi_error)?;
        Ok(NativeMetadataResult {
            canonical_bytes: Buffer::from(
                encode_file_metadata(
                    receipt.value,
                    |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
                    |_| acyclic_fs::kernel::CanonicalDecodeError::AllocationFailed,
                )
                .map_err(napi_error)?,
            ),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Atomically replaces complete canonical metadata for one path.
    #[napi]
    pub async fn set_metadata(
        &self,
        path: String,
        canonical_bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(napi_error)?;
        let receipt = checkout_mutation!(
            self,
            apply_authored_transaction(vec![acyclic_fs::AuthoredMutation::SetMetadata {
                path,
                metadata
            }],)
        )?;
        mutation_result(receipt.work)
    }

    /// Atomically replaces complete canonical metadata by stable file identity.
    #[napi]
    pub async fn set_metadata_by_id(
        &self,
        file_id: Buffer,
        canonical_bytes: Buffer,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(napi_error)?;
        let receipt = checkout_mutation!(self, set_attributes_by_id(file_id, metadata, None,))?;
        mutation_result(receipt.work)
    }

    /// Atomically replaces metadata and optional logical size for one path.
    #[napi]
    pub async fn set_attributes(
        &self,
        path: String,
        canonical_bytes: Buffer,
        logical_bytes: Option<BigInt>,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(napi_error)?;
        let logical_bytes = logical_bytes
            .as_ref()
            .map(|value| bigint_u64(value, "logicalBytes"))
            .transpose()?;
        let receipt = checkout_mutation!(self, set_attributes(path, metadata, logical_bytes,))?;
        mutation_result(receipt.work)
    }

    /// Atomically replaces metadata and optional logical size by file identity.
    #[napi]
    pub async fn set_attributes_by_id(
        &self,
        file_id: Buffer,
        canonical_bytes: Buffer,
        logical_bytes: Option<BigInt>,
    ) -> Result<NativeMutationResult> {
        let file_id = FileId::from_bytes(fixed_16(&file_id)?);
        let metadata = decode_file_metadata(
            canonical_bytes.as_ref(),
            native_decode_limits(self.config.limits),
            &mut |_| Ok::<_, acyclic_fs::kernel::CanonicalDecodeError>(()),
        )
        .map_err(napi_error)?;
        let logical_bytes = logical_bytes
            .as_ref()
            .map(|value| bigint_u64(value, "logicalBytes"))
            .transpose()?;
        let receipt = checkout_mutation!(
            self,
            set_attributes_by_id(file_id, metadata, logical_bytes,)
        )?;
        mutation_result(receipt.work)
    }

    /// Reads one exact named-attribute value.
    #[napi]
    pub async fn read_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
    ) -> Result<NativeNamedAttributeResult> {
        let path = native_path(&path, self.config)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = Box::pin(checkout.read_named_attribute(
            &path,
            &name,
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeNamedAttributeResult {
            exists: receipt.value.is_some(),
            bytes: receipt
                .value
                .map(|value| Buffer::from(value.bytes.to_vec())),
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Returns one bounded ordered named-attribute page.
    #[napi]
    pub async fn list_named_attributes(
        &self,
        path: String,
        after_class: Option<String>,
        after_name: Option<Buffer>,
        maximum_entries: NapiU32,
    ) -> Result<NativeNamedAttributePage> {
        let maximum_entries = maximum_entries.into_inner();

        let path = native_path(&path, self.config)?;
        let after = match (after_class, after_name) {
            (None, None) => None,
            (Some(class), Some(name)) => Some(native_attribute_name(
                &class,
                name.to_vec(),
                self.config.limits,
            )?),
            _ => {
                return Err(Error::new(
                    Status::InvalidArg,
                    "named-attribute cursor is incomplete",
                ));
            }
        };
        let mut checkout = Box::pin(self.inner.lock()).await;
        let receipt = Box::pin(checkout.list_named_attributes(
            &path,
            after.as_ref(),
            maximum_entries,
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        Ok(NativeNamedAttributePage {
            entries: receipt
                .value
                .entries
                .into_iter()
                .map(|entry| NativeNamedAttributeName {
                    attribute_class: native_attribute_class_name(entry.name.class()).to_owned(),
                    name: Buffer::from(entry.name.as_bytes().to_vec()),
                })
                .collect(),
            has_more: receipt.value.has_more,
            work_json: work_json(&receipt.work)?,
        })
    }

    /// Inserts or replaces one exact named attribute.
    #[napi]
    pub async fn write_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
        bytes: Buffer,
        mode: String,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mode = native_attribute_write_mode(&mode)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(checkout.change_named_attribute(
            path,
            &name,
            Some((bytes.as_ref(), mode)),
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        mutation_result(receipt.work)
    }

    /// Removes one exact named attribute.
    #[napi]
    pub async fn remove_named_attribute(
        &self,
        path: String,
        attribute_class: String,
        name: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let name = native_attribute_name(&attribute_class, name.to_vec(), self.config.limits)?;
        let mut checkout = Box::pin(self.inner.lock()).await;
        checkout.ensure_publication_resolved().map_err(napi_error)?;
        let receipt = Box::pin(checkout.change_named_attribute(
            path,
            &name,
            None,
            acyclic_fs::performance::BOUNDARY_BUDGET,
            &self.cancellation,
        ))
        .await
        .map_err(napi_error)?;
        mutation_result(receipt.work)
    }
}

pub(in crate::bindings) fn native_attribute_class(value: &str) -> Result<AttributeClass> {
    match value {
        "posix-xattr" => Ok(AttributeClass::PosixXattr),
        "windows-stream" => Ok(AttributeClass::WindowsStream),
        "mac-resource-fork" => Ok(AttributeClass::MacResourceFork),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown named-attribute class",
        )),
    }
}

pub(in crate::bindings) const fn native_attribute_class_name(
    value: AttributeClass,
) -> &'static str {
    match value {
        AttributeClass::PosixXattr => "posix-xattr",
        AttributeClass::WindowsStream => "windows-stream",
        AttributeClass::MacResourceFork => "mac-resource-fork",
    }
}

pub(in crate::bindings) fn native_attribute_name(
    class: &str,
    name: Vec<u8>,
    limits: VolumeLimits,
) -> Result<AttributeName> {
    AttributeName::new(
        native_attribute_class(class)?,
        name,
        limits.maximum_component_bytes,
    )
    .map_err(napi_error)
}

pub(in crate::bindings) fn native_attribute_write_mode(
    value: &str,
) -> Result<NamedAttributeWriteMode> {
    match value {
        "upsert" => Ok(NamedAttributeWriteMode::Upsert),
        "create" => Ok(NamedAttributeWriteMode::Create),
        "replace" => Ok(NamedAttributeWriteMode::Replace),
        _ => Err(Error::new(
            Status::InvalidArg,
            "unknown named-attribute write mode",
        )),
    }
}
