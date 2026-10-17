use super::*;
use crate::{
    context::{ContextDiscoveryLimits, ContextDiscoveryPolicy, ContextManifest, DiscoveredContext},
    conversation::{ContentResidencyVerifier, FileRef},
    filesystem::FilesystemContentVerifier,
};

pub(super) mod binding;
#[cfg(feature = "native-execution")]
mod checkout;

fn publication_operation(domain: &[u8], key: &[u8]) -> OperationId {
    let mut hash = blake3::Hasher::new();
    hash.update(domain);
    hash.update(key);
    let mut bytes = [0; 16];
    bytes.copy_from_slice(&hash.finalize().as_bytes()[..16]);
    OperationId::from_bytes(bytes)
}

fn manifest_operation(operation: OperationId) -> OperationId {
    publication_operation(
        b"acyclic.local.context-manifest/v1\0",
        &operation.into_bytes(),
    )
}

fn manifest_path(operation: OperationId) -> String {
    format!("inputs/{operation}.manifest.json")
}

impl LocalHarness {
    pub(super) fn context_reader(
        &self,
    ) -> Result<Arc<FilesystemContentVerifier<LocalAuthorityBackend, LocalObjectBackend>>> {
        FilesystemContentVerifier::new(
            self.files.clone(),
            &self.verifier,
            &self.signed,
            self.limits.file_bytes,
            None,
        )
    }

    #[cfg(test)]
    pub(super) async fn retained_context_manifest(
        &self,
        operation: OperationId,
    ) -> Result<FileRef> {
        self.observe_context_manifest(operation, self.input_publisher()?.as_ref())
            .await?
            .ok_or_else(|| Error::Storage("retained local input has no context manifest".into()))
    }

    async fn observe_context_manifest(
        &self,
        operation: OperationId,
        publisher: &dyn ContentPublisher,
    ) -> Result<Option<FileRef>> {
        let (reference, _) = publisher
            .observe_staged_once(manifest_operation(operation), self.limits.file_bytes, None)?
            .await?;
        if let Some(reference) = &reference {
            self.limits.validate_file(reference, &mut None)?;
        }
        if reference
            .as_ref()
            .is_some_and(|file| file.path() != manifest_path(operation))
        {
            return Err(Error::Conflict(
                "retained context manifest has another path".into(),
            ));
        }
        Ok(reference)
    }

    #[cfg(test)]
    pub(super) async fn stage_context_manifest(&self, operation: OperationId) -> Result<FileRef> {
        self.stage_context_manifest_with(operation, self.input_publisher()?.as_ref())
            .await
    }

    pub(super) async fn stage_context_manifest_with(
        &self,
        operation: OperationId,
        publisher: &dyn ContentPublisher,
    ) -> Result<FileRef> {
        if let Some(reference) = self.observe_context_manifest(operation, publisher).await? {
            return Ok(reference);
        }
        let path = manifest_path(operation);
        if path.len() > self.limits.path_bytes {
            return Err(Error::Invalid(
                "context manifest path exceeds path bound".into(),
            ));
        }
        #[cfg(feature = "hooks")]
        let hooks =
            crate::native_settings::NativeSettings::load_hooks(Arc::clone(&self.settings)).await?;
        #[cfg(feature = "hooks")]
        let hook_configuration = if hooks.is_empty() {
            None
        } else {
            Some(hooks.publish(publisher, self.limits).await?)
        };
        let policy = ContextDiscoveryPolicy::default();
        let limits = ContextDiscoveryLimits::default();
        let maximum = limits.instruction_bytes.min(self.limits.file_bytes);
        let discovery = self
            .instructions(&policy.instruction_names, maximum, publisher)
            .await?;
        let manifest = ContextManifest::new(discovery, Vec::new(), self.tools())?;
        #[cfg(any(feature = "native-execution", feature = "hooks"))]
        let mut manifest = manifest;
        #[cfg(feature = "native-execution")]
        if let Some(workspace) = &self.workspace {
            manifest
                .conventions
                .push(workspace.context_line().to_owned());
        }
        #[cfg(feature = "hooks")]
        manifest.sources.extend(hook_configuration);
        let bytes = manifest.encode(self.limits)?;
        Ok(publisher
            .stage_once(
                manifest_operation(operation),
                &path,
                &bytes,
                "application/json",
                "context-manifest.json",
                None,
            )?
            .await?
            .0)
    }

    async fn instructions(
        &self,
        names: &[String],
        maximum: u64,
        publisher: &dyn ContentPublisher,
    ) -> Result<DiscoveredContext> {
        #[cfg(feature = "native-execution")]
        if self.checkout_context
            && let Some(workspace) = &self.workspace
        {
            return self
                .checkout_instructions(workspace, names, maximum, publisher)
                .await;
        }
        let _ = publisher;
        self.volume_instructions(names, maximum, self.context_reader()?.as_ref())
            .await
    }

    pub(super) async fn volume_instructions(
        &self,
        names: &[String],
        maximum: u64,
        reader: &dyn ContentResidencyVerifier,
    ) -> Result<DiscoveredContext> {
        let workspace = crate::filesystem::workspace_ref(
            self.project.provider().clone(),
            &self.project.storage_name(&mut None)?,
        )?;
        let generation = self.files.resolve(&workspace, None).await?.generation;
        let mut discovery = DiscoveredContext::default();
        for name in names {
            if name.len() > self.limits.path_bytes {
                return Err(Error::Invalid("instruction path exceeds path bound".into()));
            }
            match reader
                .read_private_path(&self.project, "", name, Some(&generation), maximum, None)?
                .await
            {
                Ok((file, bytes, _)) => {
                    #[cfg(test)]
                    if let Some(observer) = self.context_read_observer {
                        observer(self, name).await?;
                    }
                    ContextDiscoveryLimits {
                        instruction_bytes: maximum,
                        ..Default::default()
                    }
                    .validate_instruction(&bytes)?;
                    self.limits.validate_file(&file, &mut None)?;
                    discovery.instructions.push(file.into());
                }
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(discovery)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // Property: every accepted instruction round-trips without normalization;
        // exactly the positive byte bound, UTF-8 and NUL constraints determine acceptance.
        #[test]
        fn instruction_text_round_trip(bytes in prop::collection::vec(any::<u8>(), 0..128), maximum in 0u64..128) {
            let limits = ContextDiscoveryLimits { instruction_bytes: maximum, ..Default::default() };
            let accepted = limits.validate_instruction(&bytes).is_ok();
            prop_assert_eq!(accepted, maximum > 0 && bytes.len() as u64 <= maximum && std::str::from_utf8(&bytes).is_ok() && !bytes.contains(&0));
            if accepted {
                prop_assert_eq!(std::str::from_utf8(&bytes).unwrap().as_bytes(), bytes.as_slice());
                let mut invalid = bytes;
                invalid.push(0);
                prop_assert!(limits.validate_instruction(&invalid).is_err());
            }
        }

        // Property: generated request paths round-trip their exact operation,
        // remain in the private input subtree, and cannot alias another turn.
        #[test]
        fn manifest_paths_round_trip(operation in any::<[u8;16]>(), other in any::<[u8;16]>()) {
            let operation = OperationId::from_bytes(operation);
            let other = OperationId::from_bytes(other);
            let path = manifest_path(operation);
            crate::conversation::validate_content_path(&path, &mut None)?;
            let name = path.strip_prefix("inputs/").unwrap().strip_suffix(".manifest.json").unwrap();
            let decoded: OperationId = serde_json::from_value(serde_json::Value::String(name.into()))?;
            prop_assert_eq!(decoded, operation);
            prop_assert!(path.len() <= Limits::default().path_bytes);
            prop_assert_eq!(manifest_path(operation), path.clone());
            if operation != other {
                prop_assert_ne!(path, manifest_path(other));
            }
            prop_assert_eq!(manifest_operation(operation), manifest_operation(decoded));
        }
    }
}
