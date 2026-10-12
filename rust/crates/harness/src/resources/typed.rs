use super::*;
macro_rules! typed_reference {
    ($name:ident, $kind:ident, $description:literal) => {
        typed_reference!($name, $kind, $description, {
            /// Creates a provider-owned typed reference.
            pub fn new(
                provider: ProviderRef,
                key: Vec<u8>,
                version: Option<String>,
            ) -> Result<Self> {
                ResourceRef::new(ResourceKind::$kind, provider, key, version, &mut None).map(Self)
            }
        });
    };
    ($name:ident, $kind:ident, $description:literal, {$($constructor:item)*}) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(pub(super) ResourceRef);

        impl $name {
            $($constructor)*

            pub(crate) fn from_resource_with_work(
                resource: ResourceRef,
                work: &mut Option<&mut dyn HistoryIo>,
            ) -> Result<Self> {
                let value = Self(resource);
                value.validate(work)?;
                Ok(value)
            }
            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                self.0.logical_owned_bytes()
            }
            /// Borrows the shared provider-neutral representation.
            #[must_use]
            pub const fn as_resource(&self) -> &ResourceRef {
                &self.0
            }

            /// Validates an untrusted serialized reference and its semantic kind.
            pub fn validate(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<()> {
                self.0.validate(work)?;
                if self.0.kind() != ResourceKind::$kind {
                    return Err(invalid("typed resource kind mismatch"));
                }
                Ok(())
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(
                deserializer: D,
            ) -> std::result::Result<Self, D::Error> {
                let resource = ResourceRef::deserialize(deserializer)?;
                if resource.kind() != ResourceKind::$kind {
                    return Err(serde::de::Error::custom("typed resource kind mismatch"));
                }
                Ok(Self(resource))
            }
        }
    };
}

typed_reference!(
    WorkspaceRef,
    Workspace,
    "Reference to a mutable Filesystem workspace."
);
typed_reference!(
    GenerationRef,
    Generation,
    "Reference to an immutable Filesystem generation.",
    {
        /// Copies an identity under the optional native work handle.
        pub fn new(
            provider: &ProviderRef,
            key: &[u8],
            version: Option<&str>,
            work: crate::NativeWork<'_>,
        ) -> Result<(Self, crate::NativeCustody)> {
            #[cfg(all(feature = "filesystem", not(target_arch = "wasm32")))]
            if let Some(work) = work {
                let mut io = crate::filesystem::PreparationIo::from_native_work(work)
                    .map(crate::store::HistoryWork::new)?;
                let mut observed = Some(&mut io as &mut dyn HistoryIo);
                provider.validate(&mut observed)?;
                if key.is_empty() {
                    return Err(invalid("resource key is empty"));
                }
                validate_text(version, &mut observed, "resource version is invalid")?;
                let mut quota = crate::wire_codec::CodecQuota::new(&mut observed, 0)?;
                let provider = crate::core::copy_provider(provider, &mut quota)?;
                let key = crate::core::copy_copy_vector(key, &mut quota)?;
                let version = version
                    .map(|v| crate::core::copy_text(v, &mut quota))
                    .transpose()?;
                let value = Self(ResourceRef {
                    kind: ResourceKind::Generation,
                    provider,
                    key,
                    version,
                });
                quota.transfer();
                drop(quota);
                return Ok((value, Some(io.into_custody()?)));
            }
            let _ = work;
            ResourceRef::new(
                ResourceKind::Generation,
                provider.clone(),
                key.to_vec(),
                version.map(str::to_owned),
                &mut None,
            )
            .map(|value| (Self(value), None))
        }
    }
);
typed_reference!(
    ArtifactRef,
    Artifact,
    "Reference to immutable content or a machine image."
);
typed_reference!(
    SandboxRef,
    Sandbox,
    "Reference to a Machines-owned isolated sandbox."
);
typed_reference!(
    CheckpointRef,
    Checkpoint,
    "Reference to an immutable sandbox checkpoint."
);
typed_reference!(StreamRef, Stream, "Reference to a Stream-owned history.");
typed_reference!(
    ContextRef,
    Context,
    "Reference to an immutable model context."
);
typed_reference!(RunRef, Run, "Reference to a recoverable model run.");

impl WorkspaceRef {
    pub(crate) fn into_resource(self) -> ResourceRef {
        self.0
    }
}
impl CheckpointRef {
    pub(crate) fn clone_with_work(&self, work: &mut Option<&mut dyn HistoryIo>) -> Result<Self> {
        self.0.copy(work).map(Self)
    }
}
