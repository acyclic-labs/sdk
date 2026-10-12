use super::*;

macro_rules! typed_reference {
    ($name:ident, $kind:ident, $description:literal) => {
        #[doc = $description]
        #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
        #[serde(transparent)]
        pub struct $name(ResourceRef);

        impl $name {
            /// Creates a provider-owned typed reference.
            pub fn new(
                provider: ProviderRef,
                key: Vec<u8>,
                version: Option<String>,
            ) -> Result<Self> {
                ResourceRef::new(ResourceKind::$kind, provider, key, version, &mut None).map(Self)
            }

            pub(crate) fn from_resource_with_work(
                resource: ResourceRef,
                work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
            ) -> Result<Self> {
                let value = Self(resource);
                value.validate(work)?;
                Ok(value)
            }

            /// Borrows the shared provider-neutral representation.
            #[must_use]
            pub const fn as_resource(&self) -> &ResourceRef {
                &self.0
            }

            pub(crate) fn logical_owned_bytes(&self) -> Result<u64> {
                self.0.logical_owned_bytes()
            }

            /// Validates the reference on the optional history observer.
            pub fn validate(
                &self,
                work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
            ) -> Result<()> {
                self.0.validate(work)?;
                if self.0.kind() != ResourceKind::$kind {
                    return Err(resource_validation_error(
                        "typed resource kind mismatch",
                        work,
                    ));
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
    pub(crate) fn clone_with_work(
        &self,
        work: &mut Option<&mut dyn crate::store::history::HistoryIo>,
    ) -> Result<Self> {
        if work.is_none() {
            return Ok(self.clone());
        }
        let mut quota = crate::wire_codec::CodecQuota::new(work, 0)?;
        let reference = self.as_resource();
        let provider = crate::core::copy_provider(reference.provider(), &mut quota)?;
        let key = crate::core::copy_copy_vector(reference.key(), &mut quota)?;
        let version = reference
            .version()
            .map(|value| crate::core::copy_text(value, &mut quota))
            .transpose()?;
        let reference = ResourceRef::new(reference.kind(), provider, key, version, quota.work())?;
        let output = Self::from_resource_with_work(reference, quota.work())?;
        quota.transfer();
        Ok(output)
    }
}
