use super::*;

#[napi]
impl NativeCheckout {
    /// Creates one regular file in the private COW overlay.
    #[napi]
    pub async fn create_file(&self, path: String, bytes: Buffer) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateFile {
                path: path,
                bytes: (bytes::Bytes::from(bytes.to_vec())).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Creates one empty directory in the private COW overlay.
    #[napi]
    pub async fn create_directory(&self, path: String) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateDirectory {
                path: path,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Creates one symbolic link with exact opaque target bytes.
    #[napi]
    pub async fn create_symbolic_link(
        &self,
        path: String,
        target: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateSymbolicLink {
                path: path,
                target: (bytes::Bytes::from(target.to_vec())).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Creates an exact empty FIFO, socket, or mounted-volume boundary.
    #[napi]
    pub async fn create_special(&self, path: String, kind: String) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let kind = empty_special_kind(&kind)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateEmptySpecial {
                path: path,
                kind: kind,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Creates an exact POSIX character or block device identity.
    #[napi]
    pub async fn create_device(
        &self,
        path: String,
        kind: String,
        major: NapiU32,
        minor: NapiU32,
    ) -> Result<NativeMutationResult> {
        let major = major.into_inner();
        let minor = minor.into_inner();
        let path = native_path(&path, self.config)?;
        let kind = device_kind(&kind)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateDevice {
                path: path,
                kind: kind,
                major: major,
                minor: minor,
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Creates an opaque exact Windows reparse-point payload.
    #[napi]
    pub async fn create_reparse_point(
        &self,
        path: String,
        payload: Buffer,
    ) -> Result<NativeMutationResult> {
        let path = native_path(&path, self.config)?;
        let receipt = checkout_mutation!(
            self,
            create(acyclic_fs::AuthoredMutation::CreateReparsePoint {
                path: path,
                payload: (bytes::Bytes::from(payload.to_vec())).into(),
                metadata: acyclic_fs::kernel::FileMetadata::default(),
            },)
        )?;
        encode_created_mutation(&receipt)
    }

    /// Removes one namespace binding.
    #[napi]
    pub async fn remove(
        &self,
        path: String,
        expected_file_id: Option<Buffer>,
    ) -> Result<NativeMutationResult> {
        let expected = expected_file_id
            .as_deref()
            .map(fixed_16)
            .transpose()?
            .map(FileId::from_bytes);
        let receipt = checkout_mutation!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Remove {
                path: native_path(&path, self.config)?,
                expected_file_id: (expected).map_or(
                    acyclic_fs::kernel::MetadataField::Unavailable,
                    acyclic_fs::kernel::MetadataField::Value,
                ),
            }],)
        )?;
        mutation_result(receipt.work)
    }

    /// Atomically renames one binding within the volume.
    #[napi]
    pub async fn rename(
        &self,
        source: String,
        destination: String,
        replace: bool,
    ) -> Result<NativeMutationResult> {
        let source = native_path(&source, self.config)?;
        let destination = native_path(&destination, self.config)?;
        let receipt = checkout_mutation!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Rename {
                source: source,
                destination: destination,
                replace: replace,
            }],)
        )?;
        mutation_result(receipt.work)
    }

    /// Creates one hard link within the volume.
    #[napi]
    pub async fn hard_link(
        &self,
        source: String,
        destination: String,
    ) -> Result<NativeMutationResult> {
        let source = native_path(&source, self.config)?;
        let destination = native_path(&destination, self.config)?;
        let receipt = checkout_mutation!(
            self,
            mutate(vec![acyclic_fs::kernel::Mutation::Link {
                source: source,
                destination: destination,
            }],)
        )?;
        mutation_result(receipt.work)
    }
}
